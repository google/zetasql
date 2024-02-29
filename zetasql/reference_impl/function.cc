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

#include "zetasql/reference_impl/function.h"

#include <stddef.h>

#include <algorithm>
#include <any>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/timestamp.pb.h"
#include "google/protobuf/wrappers.pb.h"
#include "google/type/date.pb.h"
#include "google/type/timeofday.pb.h"
#include "google/protobuf/descriptor.pb.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/initialize_required_fields.h"
#include "zetasql/common/internal_value.h"
#include "zetasql/proto/anon_output_with_report.pb.h"
#include "zetasql/public/anonymization_utils.h"
#include "zetasql/public/cast.h"
#include "zetasql/public/catalog_helper.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/collator.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/functions/arithmetics.h"
#include "zetasql/public/functions/bitcast.h"
#include "zetasql/public/functions/bitwise.h"
#include "zetasql/public/functions/common_proto.h"
#include "zetasql/public/functions/comparison.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/functions/differential_privacy.pb.h"
#include "zetasql/public/functions/distance.h"
#include "zetasql/public/types/type.h"
#include "zetasql/reference_impl/operator.h"
#include "absl/container/flat_hash_set.h"
#include "zetasql/base/check.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_format.h"
#include "absl/strings/substitute.h"
#include "absl/time/civil_time.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "zetasql/public/functions/string_format.h"
#include "zetasql/public/functions/generate_array.h"
#include "zetasql/public/functions/json.h"
#include "zetasql/public/functions/like.h"
#include "zetasql/public/functions/math.h"
#include "zetasql/public/functions/net.h"
#include "zetasql/public/functions/normalize_mode.pb.h"
#include "zetasql/public/functions/numeric.h"
#include "zetasql/public/functions/parse_date_time.h"
#include "zetasql/public/functions/percentile.h"
#include "zetasql/public/functions/regexp.h"
#include "zetasql/public/functions/string.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/proto_value_conversion.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/common.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/proto_util.h"
#include "zetasql/reference_impl/tuple_comparator.h"
#include "zetasql/reference_impl/type_parameter_constraints.h"
#include "absl/algorithm/container.h"
#include "absl/base/optimization.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "zetasql/base/source_location.h"
#include "absl/types/span.h"
#include "algorithms/algorithm.h"
#include "algorithms/bounded-mean.h"
#include "algorithms/bounded-standard-deviation.h"
#include "algorithms/bounded-sum.h"
#include "algorithms/bounded-variance.h"
#include "proto/confidence-interval.pb.h"
#include "proto/data.pb.h"
#include "algorithms/quantiles.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/exactfloat.h"
#include "re2/re2.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

ABSL_RETIRED_FLAG(bool, zetasql_lock_regexp_func, false, "retired");

ABSL_FLAG(bool, zetasql_reference_impl_validate_timestamp_precision, false,
          "When set, some operations will validate that TIMESTAMP has the "
          "expected number of significant fractional digits when "
          "FEATURE_TIMESTAMP_NANOS is not enuabled.");

namespace zetasql {

namespace {

static bool IsTypeWithDistinguishableTies(const Type* type,
                                          const CollatorList& collator_list) {
  return type->IsInterval() || (type->IsString() && !collator_list.empty());
}

// Add() and Subtract() are helper methods with a uniform signature for all
// numeric types. They do not handle NULLs.
template <typename T>
bool Add(const Value& src, Value* dst, absl::Status* status) {
  T out;
  if (functions::Add(src.Get<T>(), dst->Get<T>(), &out, status)) {
    *dst = Value::Make<T>(out);
    return true;
  }
  return false;
}

template <typename T>
bool Subtract(const Value& src, Value* dst, absl::Status* status) {
  T out;
  if (functions::Subtract(src.Get<T>(), dst->Get<T>(), &out, status)) {
    *dst = Value::Make<T>(out);
    return true;
  }
  return false;
}

template <typename T>
bool IsNegativeOrNaN(const Value& src);

template <>
bool IsNegativeOrNaN<int64_t>(const Value& src) {
  return !src.is_null() && src.int64_value() < 0;
}

template <>
bool IsNegativeOrNaN<uint64_t>(const Value& src) {
  return false;
}

template <>
bool IsNegativeOrNaN<double>(const Value& src) {
  return !src.is_null() &&
         (std::isnan(src.double_value()) || src.double_value() < 0);
}

template <>
bool IsNegativeOrNaN<NumericValue>(const Value& src) {
  return !src.is_null() && src.numeric_value() < NumericValue();
}

template <>
bool IsNegativeOrNaN<BigNumericValue>(const Value& src) {
  return !src.is_null() && src.bignumeric_value() < BigNumericValue();
}

bool IsNaN(const Value& src) {
  if (src.type()->IsFloat() && (std::isnan(src.float_value()))) {
    return true;
  }
  if (src.type()->IsDouble() && (std::isnan(src.double_value()))) {
    return true;
  }
  return false;
}

template <typename OutType, typename InType>
struct UnaryExecutor {
  typedef bool (*ptr)(InType, OutType*, absl::Status* error);
};

template <typename OutType, typename InType = OutType>
bool InvokeUnary(typename UnaryExecutor<OutType, InType>::ptr function,
                 absl::Span<const Value> args, Value* result,
                 absl::Status* status) {
  ABSL_CHECK_EQ(1, args.size());
  OutType out;
  if (!function(args[0].template Get<InType>(), &out, status)) {
    return false;
  }
  *result = Value::Make<OutType>(out);
  return true;
}

template <typename OutType, typename InType = OutType>
bool SafeInvokeUnary(typename UnaryExecutor<OutType, InType>::ptr function,
                     absl::Span<const Value> args, Value* result,
                     absl::Status* status) {
  if (!InvokeUnary<OutType, InType>(function, args, result, status)) {
    if (!ShouldSuppressError(*status,
                             ResolvedFunctionCallBase::SAFE_ERROR_MODE)) {
      return false;
    }

    *status = absl::OkStatus();
    *result = Value::MakeNull<OutType>();
  }
  return true;
}

template <typename OutType, typename InType1, typename InType2>
struct BinaryExecutor {
  typedef bool (*ptr)(InType1, InType2, OutType*, absl::Status* error);
};

template <typename OutType, typename InType1 = OutType,
          typename InType2 = OutType>
bool InvokeBinary(
    typename BinaryExecutor<OutType, InType1, InType2>::ptr function,
    absl::Span<const Value> args, Value* result, absl::Status* status) {
  ABSL_CHECK_EQ(2, args.size());
  OutType out;
  if (!function(args[0].template Get<InType1>(),
                args[1].template Get<InType2>(), &out, status)) {
    return false;
  }
  *result = Value::Make<OutType>(out);
  return true;
}

template <typename OutType, typename InType1 = OutType,
          typename InType2 = OutType>
bool SafeInvokeBinary(
    typename BinaryExecutor<OutType, InType1, InType2>::ptr function,
    absl::Span<const Value> args, Value* result, absl::Status* status) {
  if (!InvokeBinary<OutType, InType1, InType2>(function, args, result,
                                               status)) {
    if (!ShouldSuppressError(*status,
                             ResolvedFunctionCallBase::SAFE_ERROR_MODE)) {
      return false;
    }

    *status = absl::OkStatus();
    *result = Value::MakeNull<OutType>();
  }
  return true;
}

template <typename OutType, typename InType1, typename InType2,
          typename InType3>
struct TernaryRoundExecutor {
  typedef bool (*ptr)(InType1, InType2, InType3, OutType*, absl::Status* error);
};

template <typename OutType, typename InType1 = OutType, typename InType2,
          typename IntType3>
bool InvokeRoundTernary(typename TernaryRoundExecutor<OutType, InType1, InType2,
                                                      IntType3>::ptr function,
                        absl::Span<const Value> args, Value* result,
                        absl::Status* status) {
  ABSL_CHECK_EQ(3, args.size());  // Crash OK
  functions::RoundingMode rounding_mode =
      static_cast<functions::RoundingMode>(args[2].enum_value());
  OutType out;
  if (!function(args[0].template Get<InType1>(), args[1].int64_value(),
                rounding_mode, &out, status)) {
    return false;
  }
  *result = Value::Make<OutType>(out);
  return true;
}

template <typename OutType, typename FunctionType, class... Args>
bool InvokeString(FunctionType function, Value* result, absl::Status* status,
                  Args... args) {
  OutType out;
  if (!function(args..., &out, status)) {
    return false;
  }
  *result = Value::String(out);
  return true;
}

template <typename OutType, typename FunctionType, class... Args>
bool InvokeBytes(FunctionType function, Value* result, absl::Status* status,
                 Args... args) {
  OutType out;
  if (!function(args..., &out, status)) {
    return false;
  }
  *result = Value::Bytes(out);
  return true;
}

template <typename OutType, typename FunctionType, class... Args>
bool InvokeNullableString(FunctionType function, Value* result,
                          absl::Status* status, Args... args) {
  OutType out;
  bool is_null = true;
  *status = function(args..., &out, &is_null);
  if (!status->ok()) return false;
  *result = is_null ? Value::NullString() : Value::String(out);
  return true;
}

template <typename OutType, typename FunctionType, class... Args>
bool InvokeNullableBytes(FunctionType function, Value* result,
                         absl::Status* status, Args... args) {
  OutType out;
  bool is_null = true;
  *status = function(args..., &out, &is_null);
  if (!status->ok()) return false;
  *result = is_null ? Value::NullBytes() : Value::Bytes(out);
  return true;
}

template <typename OutType, typename FunctionType, class... Args>
bool Invoke(FunctionType function, Value* result, absl::Status* status,
            Args... args) {
  OutType out;
  if (!function(args..., &out, status)) {
    return false;
  }
  *result = Value::Make<OutType>(out);
  return true;
}

// Generates an array from start to end inclusive with the specified step size.
template <typename T, typename TStep, Value (*TMakeValue)(T)>
absl::Status GenerateArrayHelper(T start, T end, TStep step,
                                 EvaluationContext* context,
                                 std::vector<Value>* values) {
  std::vector<T> values_temp;
  absl::Status status =
      functions::GenerateArray<T, TStep>(start, end, step, &values_temp);
  if (!status.ok()) {
    return status;
  }

  int64_t bytes_so_far = 0;
  for (T& value : values_temp) {
    Value tracked_value = TMakeValue(value);
    bytes_so_far += tracked_value.physical_byte_size();
    if (bytes_so_far > context->options().max_value_byte_size) {
      return MakeMaxArrayValueByteSizeExceededError(
          context->options().max_value_byte_size,
          zetasql_base::SourceLocation::current());
    }
    values->push_back(tracked_value);
  }

  return absl::OkStatus();
}

// Generates an array from start to end inclusive with the specified step size.
template <typename T>
absl::Status GenerateArray(T start, T end, T step, EvaluationContext* context,
                           std::vector<Value>* values) {
  return GenerateArrayHelper<T, T, Value::Make<T>>(start, end, step, context,
                                                   values);
}

template <>
absl::Status GenerateArray(NumericValue start, NumericValue end,
                           NumericValue step, EvaluationContext* context,
                           std::vector<Value>* values) {
  return GenerateArrayHelper<NumericValue, NumericValue, Value::Numeric>(
      start, end, step, context, values);
}

template <>
absl::Status GenerateArray(BigNumericValue start, BigNumericValue end,
                           BigNumericValue step, EvaluationContext* context,
                           std::vector<Value>* values) {
  return GenerateArrayHelper<BigNumericValue, BigNumericValue,
                             Value::BigNumeric>(start, end, step, context,
                                                values);
}

// Define a a similar function to Value::Date(int32_t) for template matching
// to be happy.
Value MakeDate(int64_t in) { return Value::Date(in); }
Value MakeTimestamp(absl::Time in) { return Value::Timestamp(in); }

absl::Status GenerateDateArray(int64_t start, int64_t end, int64_t step,
                               functions::DateTimestampPart step_unit,
                               EvaluationContext* context,
                               std::vector<Value>* values) {
  functions::DateIncrement increment;
  increment.unit = step_unit;
  increment.value = step;

  return GenerateArrayHelper<int64_t, functions::DateIncrement, MakeDate>(
      start, end, increment, context, values);
}

absl::Status GenerateTimestampArray(absl::Time start, absl::Time end,
                                    int64_t step,
                                    functions::DateTimestampPart step_unit,
                                    EvaluationContext* context,
                                    std::vector<Value>* values) {
  functions::TimestampIncrement increment;
  increment.unit = step_unit;
  increment.value = step;

  return GenerateArrayHelper<absl::Time, functions::TimestampIncrement,
                             MakeTimestamp>(start, end, increment, context,
                                            values);
}

template <typename T>
Value CreateValueFromOptional(std::optional<T> opt) {
  if (opt.has_value()) {
    return Value::Make<T>(opt.value());
  }
  return Value::MakeNull<T>();
}

// This class exists purely for static initialization reasons.
class FunctionMap {
 public:
  FunctionMap();
  FunctionMap(const FunctionMap&) = delete;

  const absl::flat_hash_map<FunctionKind, std::string>&
  function_debug_name_by_kind() const {
    return function_debug_name_by_kind_;
  }

  const absl::flat_hash_map<std::string, FunctionKind>& function_kind_by_name()
      const {
    return function_kind_by_name_;
  }

 private:
  // We use string_view here to reduce stack frame usage in debug mode for
  // FunctionMap::FunctionMap.  We are not concerned about the performance
  // implications here since it is called once per process invocation.
  void RegisterFunction(FunctionKind kind, absl::string_view name,
                        absl::string_view debug_name) {
    ABSL_CHECK(function_debug_name_by_kind_.try_emplace(kind, debug_name).second)
        << "Duplicate function debug_name: " << debug_name;
    if (!name.empty()) {
      ABSL_CHECK(function_kind_by_name_.try_emplace(name, kind).second)
          << "Duplicate function name: " << name;
    }
  }

  absl::flat_hash_map<FunctionKind, std::string> function_debug_name_by_kind_;
  absl::flat_hash_map<std::string, FunctionKind> function_kind_by_name_;
};

FunctionMap::FunctionMap() {
  static constexpr absl::string_view kPrivate = "";  // for private functions
  // We break registration into multiple lambdas to reduce stack frame size
  // in debug builds.
  [this]() {
    RegisterFunction(FunctionKind::kAdd, "$add", "Add");
    RegisterFunction(FunctionKind::kSafeAdd, "safe_add", "SafeAdd");
    RegisterFunction(FunctionKind::kAnd, "$and", "And");
    RegisterFunction(FunctionKind::kAndAgg, kPrivate, "AndAgg");
    RegisterFunction(FunctionKind::kAnyValue, "any_value", "AnyValue");
    RegisterFunction(FunctionKind::kArrayAgg, "array_agg", "ArrayAgg");
    RegisterFunction(FunctionKind::kArrayConcat, "array_concat", "ArrayConcat");
    RegisterFunction(FunctionKind::kArrayConcatAgg, "array_concat_agg",
                     "ArrayConcatAgg");
    RegisterFunction(FunctionKind::kArrayLength, "array_length", "ArrayLength");
    RegisterFunction(FunctionKind::kArrayToString, "array_to_string",
                     "ArrayToString");
    RegisterFunction(FunctionKind::kArrayReverse, "array_reverse",
                     "ArrayReverse");
    RegisterFunction(FunctionKind::kArrayAtOffset, "$array_at_offset",
                     "ArrayAtOffset");
    RegisterFunction(FunctionKind::kArrayAtOrdinal, "$array_at_ordinal",
                     "ArrayAtOrdinal");
    RegisterFunction(FunctionKind::kSafeArrayAtOffset, "$safe_array_at_offset",
                     "SafeArrayAtOffset");
    RegisterFunction(FunctionKind::kSafeArrayAtOrdinal,
                     "$safe_array_at_ordinal", "SafeArrayAtOrdinal");
    RegisterFunction(FunctionKind::kSubscript, "$subscript", "Subscript");
    RegisterFunction(FunctionKind::kArrayIsDistinct, "array_is_distinct",
                     "ArrayIsDistinct");
    RegisterFunction(FunctionKind::kAvg, "avg", "Avg");
    RegisterFunction(FunctionKind::kBitwiseAnd, "$bitwise_and", "BitwiseAnd");
    RegisterFunction(FunctionKind::kBitwiseLeftShift, "$bitwise_left_shift",
                     "BitwiseLeftShift");
    RegisterFunction(FunctionKind::kBitwiseNot, "$bitwise_not", "BitwiseNot");
    RegisterFunction(FunctionKind::kBitwiseOr, "$bitwise_or", "BitwiseOr");
    RegisterFunction(FunctionKind::kBitwiseRightShift, "$bitwise_right_shift",
                     "BitwiseRightShift");
    RegisterFunction(FunctionKind::kBitwiseXor, "$bitwise_xor", "BitwiseXor");
    RegisterFunction(FunctionKind::kBitAnd, "bit_and", "BitAnd");
    RegisterFunction(FunctionKind::kBitOr, "bit_or", "BitOr");
    RegisterFunction(FunctionKind::kBitXor, "bit_xor", "BitXor");
    RegisterFunction(FunctionKind::kBitCount, "bit_count", "BitCount");
    RegisterFunction(FunctionKind::kCast, "cast", "Cast");
    RegisterFunction(FunctionKind::kBitCastToInt32, "bit_cast_to_int32",
                     "BitCastToInt32");
    RegisterFunction(FunctionKind::kBitCastToInt64, "bit_cast_to_int64",
                     "BitCastToInt64");
    RegisterFunction(FunctionKind::kBitCastToUint32, "bit_cast_to_uint32",
                     "BitCastToUint32");
    RegisterFunction(FunctionKind::kBitCastToUint64, "bit_cast_to_uint64",
                     "BitCastToUint64");
    RegisterFunction(FunctionKind::kCount, "count", "Count");
    RegisterFunction(FunctionKind::kCountIf, "countif", "CountIf");
    RegisterFunction(FunctionKind::kDateAdd, "date_add", "Date_add");
    RegisterFunction(FunctionKind::kDateSub, "date_sub", "Date_sub");
    RegisterFunction(FunctionKind::kDatetimeAdd, "datetime_add",
                     "Datetime_add");
    RegisterFunction(FunctionKind::kDatetimeSub, "datetime_sub",
                     "Datetime_sub");
    RegisterFunction(FunctionKind::kDatetimeDiff, "datetime_diff",
                     "Datetime_diff");
    RegisterFunction(FunctionKind::kDateTrunc, "date_trunc", "Date_trunc");
    RegisterFunction(FunctionKind::kDatetimeTrunc, "datetime_trunc",
                     "Datetime_trunc");
    RegisterFunction(FunctionKind::kLastDay, "last_day", "Last_day");
    RegisterFunction(FunctionKind::kDateDiff, "date_diff", "Date_diff");
    RegisterFunction(FunctionKind::kDivide, "$divide", "Divide");
    RegisterFunction(FunctionKind::kSafeDivide, "safe_divide", "SafeDivide");
    RegisterFunction(FunctionKind::kDiv, "div", "Div");
    RegisterFunction(FunctionKind::kEqual, "$equal", "Equal");
    RegisterFunction(FunctionKind::kIsDistinct, "$is_distinct_from",
                     "IsDistinct");
    RegisterFunction(FunctionKind::kIsNotDistinct, "$is_not_distinct_from",
                     "IsDistinct");
    RegisterFunction(FunctionKind::kExists, "exists", "Exists");
    RegisterFunction(FunctionKind::kGenerateArray, "generate_array",
                     "GenerateArray");
    RegisterFunction(FunctionKind::kGenerateDateArray, "generate_date_array",
                     "GenerateDateArray");
    RegisterFunction(FunctionKind::kGenerateTimestampArray,
                     "generate_timestamp_array", "GenerateTimestampArray");
    RegisterFunction(FunctionKind::kRangeBucket, "range_bucket", "RangeBucket");
    RegisterFunction(FunctionKind::kProtoMapAtKey, "$proto_map_at_key",
                     "ProtoMapAtKey");
    RegisterFunction(FunctionKind::kSafeProtoMapAtKey, "$safe_proto_map_at_key",
                     "SafeProtoMapAtKey");
    RegisterFunction(FunctionKind::kModifyMap, "modify_map", "ModifyMap");
    RegisterFunction(FunctionKind::kContainsKey, "contains_key", "ContainsKey");
    RegisterFunction(FunctionKind::kJsonExtract, "json_extract", "JsonExtract");
    RegisterFunction(FunctionKind::kJsonExtractScalar, "json_extract_scalar",
                     "JsonExtractScalar");
    RegisterFunction(FunctionKind::kJsonExtractArray, "json_extract_array",
                     "JsonExtractArray");
    RegisterFunction(FunctionKind::kJsonExtractStringArray,
                     "json_extract_string_array", "JsonExtractStringArray");
    RegisterFunction(FunctionKind::kJsonQuery, "json_query", "JsonQuery");
    RegisterFunction(FunctionKind::kJsonValue, "json_value", "JsonValue");
    RegisterFunction(FunctionKind::kJsonQueryArray, "json_query_array",
                     "JsonQueryArray");
    RegisterFunction(FunctionKind::kJsonValueArray, "json_value_array",
                     "JsonValueArray");
    RegisterFunction(FunctionKind::kToJson, "to_json", "ToJson");
    RegisterFunction(FunctionKind::kInt64, "int64", "Int64");
    RegisterFunction(FunctionKind::kDouble, "float64", "Float64");
    RegisterFunction(FunctionKind::kBool, "bool", "Bool");
    RegisterFunction(FunctionKind::kJsonType, "json_type", "JsonType");
    RegisterFunction(FunctionKind::kLaxBool, "lax_bool", "LaxBool");
    RegisterFunction(FunctionKind::kLaxInt64, "lax_int64", "LaxInt64");
    RegisterFunction(FunctionKind::kLaxDouble, "lax_float64", "LaxFloat64");
    RegisterFunction(FunctionKind::kLaxString, "lax_string", "LaxString");
    RegisterFunction(FunctionKind::kToJsonString, "to_json_string",
                     "ToJsonString");
    RegisterFunction(FunctionKind::kParseJson, "parse_json", "ParseJson");
    RegisterFunction(FunctionKind::kJsonArray, "json_array", "JsonArray");
    RegisterFunction(FunctionKind::kJsonObject, "json_object", "JsonObject");
    RegisterFunction(FunctionKind::kJsonRemove, "json_remove", "JsonRemove");
    RegisterFunction(FunctionKind::kJsonSet, "json_set", "JsonSet");
    RegisterFunction(FunctionKind::kJsonStripNulls, "json_strip_nulls",
                     "JsonStripNulls");
    RegisterFunction(FunctionKind::kJsonArrayInsert, "json_array_insert",
                     "JsonArrayInsert");
    RegisterFunction(FunctionKind::kJsonArrayAppend, "json_array_append",
                     "JsonArrayAppend");
    RegisterFunction(FunctionKind::kGreatest, "greatest", "Greatest");
  }();
  [this]() {
    RegisterFunction(FunctionKind::kIsNull, "$is_null", "IsNull");
    RegisterFunction(FunctionKind::kIsTrue, "$is_true", "IsTrue");
    RegisterFunction(FunctionKind::kIsFalse, "$is_false", "IsFalse");
    RegisterFunction(FunctionKind::kLeast, "least", "Least");
    RegisterFunction(FunctionKind::kLess, "$less", "Less");
    RegisterFunction(FunctionKind::kLessOrEqual, "$less_or_equal",
                     "LessOrEqual");
    RegisterFunction(FunctionKind::kLike, "$like", "Like");
    RegisterFunction(FunctionKind::kLikeWithCollation, "$like_with_collation",
                     "LikeWithCollation");
    RegisterFunction(FunctionKind::kLikeAny, "$like_any", "LikeAny");
    RegisterFunction(FunctionKind::kLikeAnyWithCollation,
                     "$like_any_with_collation", "LikeAnyWithCollation");
    RegisterFunction(FunctionKind::kLikeAll, "$like_all", "LikeAll");
    RegisterFunction(FunctionKind::kLikeAllWithCollation,
                     "$like_all_with_collation", "LikeAllWithCollation");
    RegisterFunction(FunctionKind::kLikeAnyArray, "$like_any_array",
                     "LikeAnyArray");
    RegisterFunction(FunctionKind::kLikeAllArray, "$like_all_array",
                     "LikeAllArray");
    RegisterFunction(FunctionKind::kLogicalAnd, "logical_and", "LogicalAnd");
    RegisterFunction(FunctionKind::kLogicalOr, "logical_or", "LogicalOr");
    RegisterFunction(FunctionKind::kMakeProto, "make_proto", "MakeProto");
    RegisterFunction(FunctionKind::kMax, "max", "Max");
    RegisterFunction(FunctionKind::kMin, "min", "Min");
    RegisterFunction(FunctionKind::kMod, "mod", "Mod");
    RegisterFunction(FunctionKind::kMultiply, "$multiply", "Multiply");
    RegisterFunction(FunctionKind::kSafeMultiply, "safe_multiply",
                     "SafeMultiply");
    RegisterFunction(FunctionKind::kNot, "$not", "Not");
    RegisterFunction(FunctionKind::kOr, "$or", "Or");
    RegisterFunction(FunctionKind::kOrAgg, kPrivate, "OrAgg");
    RegisterFunction(FunctionKind::kStringAgg, "string_agg", "StringAgg");
    RegisterFunction(FunctionKind::kSubtract, "$subtract", "Subtract");
    RegisterFunction(FunctionKind::kSafeSubtract, "safe_subtract",
                     "SafeSubtract");
    RegisterFunction(FunctionKind::kSum, "sum", "Sum");
    RegisterFunction(FunctionKind::kTimeAdd, "time_add", "Time_add");
    RegisterFunction(FunctionKind::kTimeSub, "time_sub", "Time_sub");
    RegisterFunction(FunctionKind::kTimeDiff, "time_diff", "Time_diff");
    RegisterFunction(FunctionKind::kTimeTrunc, "time_trunc", "Time_trunc");
    RegisterFunction(FunctionKind::kArrayFilter, "array_filter",
                     "Array_filter");
    RegisterFunction(FunctionKind::kArrayTransform, "array_transform",
                     "Array_transform");
    RegisterFunction(FunctionKind::kTimestampDiff, "timestamp_diff",
                     "Timestamp_diff");
    RegisterFunction(FunctionKind::kTimestampAdd, "timestamp_add",
                     "Timestamp_add");
    RegisterFunction(FunctionKind::kTimestampSub, "timestamp_sub",
                     "Timestamp_sub");
    RegisterFunction(FunctionKind::kTimestampTrunc, "timestamp_trunc",
                     "Timestamp_trunc");
    RegisterFunction(FunctionKind::kUnaryMinus, "$unary_minus", "UnaryMinus");
    RegisterFunction(FunctionKind::kSafeNegate, "safe_negate", "SafeNegate");
    RegisterFunction(FunctionKind::kAbs, "abs", "Abs");
    RegisterFunction(FunctionKind::kSign, "sign", "Sign");
    RegisterFunction(FunctionKind::kRound, "round", "Round");
    RegisterFunction(FunctionKind::kTrunc, "trunc", "Trunc");
    RegisterFunction(FunctionKind::kCeil, "ceil", "Ceil");
    RegisterFunction(FunctionKind::kFloor, "floor", "Floor");
    RegisterFunction(FunctionKind::kIsNan, "is_nan", "IsNan");
    RegisterFunction(FunctionKind::kIsInf, "is_inf", "IsInf");
    RegisterFunction(FunctionKind::kIeeeDivide, "ieee_divide", "IeeeDivide");
    RegisterFunction(FunctionKind::kSqrt, "sqrt", "Sqrt");
    RegisterFunction(FunctionKind::kCbrt, "cbrt", "Cbrt");
    RegisterFunction(FunctionKind::kPow, "pow", "Pow");
    RegisterFunction(FunctionKind::kExp, "exp", "Exp");
    RegisterFunction(FunctionKind::kNaturalLogarithm, "ln", "NaturalLogarithm");
    RegisterFunction(FunctionKind::kDecimalLogarithm, "log10",
                     "DecimalLogarithm");
    RegisterFunction(FunctionKind::kLogarithm, "log", "Logarithm");
    RegisterFunction(FunctionKind::kCos, "cos", "Cos");
    RegisterFunction(FunctionKind::kCosh, "cosh", "Cosh");
    RegisterFunction(FunctionKind::kAcos, "acos", "Acos");
    RegisterFunction(FunctionKind::kAcosh, "acosh", "Acosh");
    RegisterFunction(FunctionKind::kSin, "sin", "Sin");
    RegisterFunction(FunctionKind::kSinh, "sinh", "Sinh");
    RegisterFunction(FunctionKind::kAsin, "asin", "Asin");
    RegisterFunction(FunctionKind::kAsinh, "asinh", "Asinh");
    RegisterFunction(FunctionKind::kTan, "tan", "Tan");
    RegisterFunction(FunctionKind::kTanh, "tanh", "Tanh");
    RegisterFunction(FunctionKind::kAtan, "atan", "Atan");
    RegisterFunction(FunctionKind::kAtanh, "atanh", "Atanh");
    RegisterFunction(FunctionKind::kAtan2, "atan2", "Atan2");
    RegisterFunction(FunctionKind::kCsc, "csc", "Csc");
    RegisterFunction(FunctionKind::kSec, "sec", "Sec");
    RegisterFunction(FunctionKind::kCot, "cot", "Cot");
    RegisterFunction(FunctionKind::kCsch, "csch", "Csch");
    RegisterFunction(FunctionKind::kSech, "sech", "Sech");
    RegisterFunction(FunctionKind::kCoth, "coth", "Coth");
    RegisterFunction(FunctionKind::kPi, "pi", "Pi");
    RegisterFunction(FunctionKind::kPiNumeric, "pi_numeric", "Pi_numeric");
    RegisterFunction(FunctionKind::kPiBigNumeric, "pi_bignumeric",
                     "Pi_bignumeric");
    RegisterFunction(FunctionKind::kCorr, "corr", "Corr");
    RegisterFunction(FunctionKind::kCovarPop, "covar_pop", "Covar_pop");
    RegisterFunction(FunctionKind::kCovarSamp, "covar_samp", "Covar_samp");
    RegisterFunction(FunctionKind::kStddevPop, "stddev_pop", "Stddev_pop");
    RegisterFunction(FunctionKind::kStddevSamp, "stddev_samp", "Stddev_samp");
    RegisterFunction(FunctionKind::kVarPop, "var_pop", "Var_pop");
    RegisterFunction(FunctionKind::kVarSamp, "var_samp", "Var_samp");
    RegisterFunction(FunctionKind::kAnonSum, "anon_sum", "Anon_sum");
    RegisterFunction(FunctionKind::kAnonSumWithReportProto,
                     "$anon_sum_with_report_proto", "AnonSumWithReportProto");
    RegisterFunction(FunctionKind::kAnonSumWithReportJson,
                     "$anon_sum_with_report_json", "AnonSumWithReportJson");
    RegisterFunction(FunctionKind::kAnonAvg, "anon_avg", "Anon_avg");
    RegisterFunction(FunctionKind::kAnonAvgWithReportProto,
                     "$anon_avg_with_report_proto", "AnonAvgWithReportProto");
    RegisterFunction(FunctionKind::kAnonAvgWithReportJson,
                     "$anon_avg_with_report_json", "AnonAvgWithReportJson");
    RegisterFunction(FunctionKind::kAnonVarPop, "anon_var_pop", "Anon_var_pop");
    RegisterFunction(FunctionKind::kAnonStddevPop, "anon_stddev_pop",
                     "Anon_stddev_pop");
    RegisterFunction(FunctionKind::kAnonQuantiles, "anon_quantiles",
                     "Anon_quantiles");
    RegisterFunction(FunctionKind::kAnonQuantilesWithReportProto,
                     "$anon_quantiles_with_report_proto",
                     "AnonQuantilesWithReportProto");
    RegisterFunction(FunctionKind::kAnonQuantilesWithReportJson,
                     "$anon_quantiles_with_report_json",
                     "AnonQuantilesWithReportJson");

    RegisterFunction(FunctionKind::kDifferentialPrivacySum,
                     "$differential_privacy_sum", "Differential_privacy_sum");
    RegisterFunction(FunctionKind::kDifferentialPrivacyAvg,
                     "$differential_privacy_avg", "Differential_privacy_avg");
    RegisterFunction(FunctionKind::kDifferentialPrivacyVarPop,
                     "$differential_privacy_var_pop",
                     "Differential_privacy_var_pop");
    RegisterFunction(FunctionKind::kDifferentialPrivacyStddevPop,
                     "$differential_privacy_stddev_pop",
                     "Differential_privacy_stddev_pop");
    RegisterFunction(FunctionKind::kDifferentialPrivacyQuantiles,
                     "$differential_privacy_approx_quantiles",
                     "Differential_privacy_approx_quantiles");
  }();
  [this]() {
    RegisterFunction(FunctionKind::kByteLength, "byte_length", "ByteLength");
    RegisterFunction(FunctionKind::kCharLength, "char_length", "CharLength");
    RegisterFunction(FunctionKind::kConcat, "concat", "Concat");
    RegisterFunction(FunctionKind::kEndsWith, "ends_with", "EndsWith");
    RegisterFunction(FunctionKind::kEndsWithWithCollation,
                     "$ends_with_with_collation", "EndsWithWithCollation");
    RegisterFunction(FunctionKind::kFormat, "format", "Format");
    RegisterFunction(FunctionKind::kLength, "length", "Length");
    RegisterFunction(FunctionKind::kLower, "lower", "Lower");
    RegisterFunction(FunctionKind::kLtrim, "ltrim", "Ltrim");
    RegisterFunction(FunctionKind::kRegexpMatch, "regexp_match", "RegexpMatch");
    RegisterFunction(FunctionKind::kRegexpContains, "regexp_contains",
                     "RegexpContains");
    RegisterFunction(FunctionKind::kRegexpExtract, "regexp_extract",
                     "RegexpExtract");
    RegisterFunction(FunctionKind::kRegexpExtractAll, "regexp_extract_all",
                     "RegexpExtract");
    RegisterFunction(FunctionKind::kRegexpInstr, "regexp_instr", "RegexpInstr");
    RegisterFunction(FunctionKind::kRegexpReplace, "regexp_replace",
                     "RegexpReplace");
    RegisterFunction(FunctionKind::kReplace, "replace", "Replace");
    RegisterFunction(FunctionKind::kReplaceWithCollation,
                     "$replace_with_collation", "ReplaceWithCollation");
    RegisterFunction(FunctionKind::kRtrim, "rtrim", "Rtrim");
    RegisterFunction(FunctionKind::kSplit, "split", "Split");
    RegisterFunction(FunctionKind::kSplitWithCollation, "$split_with_collation",
                     "SplitWithCollation");
    RegisterFunction(FunctionKind::kStartsWith, "starts_with", "StartsWith");
    RegisterFunction(FunctionKind::kStartsWithWithCollation,
                     "$starts_with_with_collation", "StartsWithWithCollation");
    RegisterFunction(FunctionKind::kStrpos, "strpos", "Strpos");
    RegisterFunction(FunctionKind::kStrposWithCollation,
                     "$strpos_with_collation", "StrposWithCollation");
    RegisterFunction(FunctionKind::kInstr, "instr", "Instr");
    RegisterFunction(FunctionKind::kInstrWithCollation, "$instr_with_collation",
                     "InstrWithCollation");
    RegisterFunction(FunctionKind::kSubstr, "substr", "Substr");
    RegisterFunction(FunctionKind::kTrim, "trim", "Trim");
    RegisterFunction(FunctionKind::kUpper, "upper", "Upper");
    RegisterFunction(FunctionKind::kLpad, "lpad", "Lpad");
    RegisterFunction(FunctionKind::kRpad, "rpad", "Rpad");
    RegisterFunction(FunctionKind::kLeft, "left", "Left");
    RegisterFunction(FunctionKind::kRight, "right", "Right");
    RegisterFunction(FunctionKind::kRepeat, "repeat", "Repeat");
    RegisterFunction(FunctionKind::kReverse, "reverse", "Reverse");
    RegisterFunction(FunctionKind::kSafeConvertBytesToString,
                     "safe_convert_bytes_to_string",
                     "SafeConvertBytesToString");
    RegisterFunction(FunctionKind::kNormalize, "normalize", "Normalize");
    RegisterFunction(FunctionKind::kNormalizeAndCasefold,
                     "normalize_and_casefold", "NormalizeAndCasefold");
    RegisterFunction(FunctionKind::kToBase64, "to_base64", "ToBase64");
    RegisterFunction(FunctionKind::kFromBase64, "from_base64", "FromBase64");
    RegisterFunction(FunctionKind::kToHex, "to_hex", "ToHex");
    RegisterFunction(FunctionKind::kFromHex, "from_hex", "FromHex");
    RegisterFunction(FunctionKind::kAscii, "ascii", "Ascii");
    RegisterFunction(FunctionKind::kUnicode, "unicode", "Unicode");
    RegisterFunction(FunctionKind::kChr, "chr", "Chr");
    RegisterFunction(FunctionKind::kToCodePoints, "to_code_points",
                     "ToCodePoints");
    RegisterFunction(FunctionKind::kCodePointsToString, "code_points_to_string",
                     "CodePointsToString");
    RegisterFunction(FunctionKind::kCodePointsToBytes, "code_points_to_bytes",
                     "CodePointsToBytes");
    RegisterFunction(FunctionKind::kSoundex, "soundex", "Soundex");
    RegisterFunction(FunctionKind::kTranslate, "translate", "Translate");
    RegisterFunction(FunctionKind::kInitCap, "initcap", "InitCap");
    RegisterFunction(FunctionKind::kCollationKey, kPrivate, "CollationKey");
    RegisterFunction(FunctionKind::kCollate, "collate", "Collate");
    RegisterFunction(FunctionKind::kParseNumeric, "parse_numeric",
                     "Parse_numeric");
    RegisterFunction(FunctionKind::kParseBignumeric, "parse_bignumeric",
                     "Parse_bignumeric");
    RegisterFunction(FunctionKind::kCurrentDate, "current_date",
                     "Current_date");
    RegisterFunction(FunctionKind::kCurrentDatetime, "current_datetime",
                     "Current_datetime");
    RegisterFunction(FunctionKind::kCurrentTime, "current_time",
                     "Current_time");
    RegisterFunction(FunctionKind::kCurrentTimestamp, "current_timestamp",
                     "Current_timestamp");
    RegisterFunction(FunctionKind::kDateFromUnixDate, "date_from_unix_date",
                     "Date_from_unix_date");
    RegisterFunction(FunctionKind::kUnixDate, "unix_date", "Unix_date");
    RegisterFunction(FunctionKind::kExtractFrom, "$extract", "Extract");
    RegisterFunction(FunctionKind::kExtractDateFrom, "$extract_date",
                     "Extract");
    RegisterFunction(FunctionKind::kExtractTimeFrom, "$extract_time",
                     "Extract");
    RegisterFunction(FunctionKind::kExtractDatetimeFrom, "$extract_datetime",
                     "Extract");
    RegisterFunction(FunctionKind::kExtractOneofCase, "$extract_oneof_case",
                     "ExtractOneofCase");
    RegisterFunction(FunctionKind::kFormatDate, "format_date", "Format_date");
    RegisterFunction(FunctionKind::kFormatDatetime, "format_datetime",
                     "Format_datetime");
    RegisterFunction(FunctionKind::kFormatTime, "format_time", "Format_time");
    RegisterFunction(FunctionKind::kFormatTimestamp, "format_timestamp",
                     "Format_timestamp");
    RegisterFunction(FunctionKind::kTimestampSeconds, "timestamp_seconds",
                     "Timestamp_seconds");
    RegisterFunction(FunctionKind::kTimestampMillis, "timestamp_millis",
                     "Timestamp_millis");
    RegisterFunction(FunctionKind::kTimestampMicros, "timestamp_micros",
                     "Timestamp_micros");
    RegisterFunction(FunctionKind::kTimestampFromUnixSeconds,
                     "timestamp_from_unix_seconds",
                     "Timestamp_from_unix_seconds");
    RegisterFunction(FunctionKind::kTimestampFromUnixMillis,
                     "timestamp_from_unix_millis",
                     "Timestamp_from_unix_millis");
    RegisterFunction(FunctionKind::kTimestampFromUnixMicros,
                     "timestamp_from_unix_micros",
                     "Timestamp_from_unix_micros");
    RegisterFunction(FunctionKind::kSecondsFromTimestamp, "unix_seconds",
                     "Unix_seconds");
    RegisterFunction(FunctionKind::kMillisFromTimestamp, "unix_millis",
                     "Unix_millis");
    RegisterFunction(FunctionKind::kMicrosFromTimestamp, "unix_micros",
                     "Unix_micros");
    RegisterFunction(FunctionKind::kString, "string", "String");
    RegisterFunction(FunctionKind::kParseDate, "parse_date", "Parse_date");
    RegisterFunction(FunctionKind::kParseDatetime, "parse_datetime",
                     "Parse_datetime");
    RegisterFunction(FunctionKind::kParseTime, "parse_time", "Parse_time");
    RegisterFunction(FunctionKind::kParseTimestamp, "parse_timestamp",
                     "Parse_timestamp");
    RegisterFunction(FunctionKind::kIntervalCtor, "$interval", "$interval");
    RegisterFunction(FunctionKind::kMakeInterval, "make_interval",
                     "make_interval");
    RegisterFunction(FunctionKind::kJustifyHours, "justify_hours",
                     "justify_hours");
    RegisterFunction(FunctionKind::kJustifyDays, "justify_days",
                     "justify_days");
    RegisterFunction(FunctionKind::kJustifyInterval, "justify_interval",
                     "justify_interval");
    RegisterFunction(FunctionKind::kFromProto, "from_proto", "From_proto");
    RegisterFunction(FunctionKind::kToProto, "to_proto", "To_proto");
    RegisterFunction(FunctionKind::kEnumValueDescriptorProto,
                     "enum_value_descriptor_proto",
                     "Enum_value_descriptor_proto");
    RegisterFunction(FunctionKind::kDate, "date", "Date");
    RegisterFunction(FunctionKind::kTimestamp, "timestamp", "Timestamp");
    RegisterFunction(FunctionKind::kTime, "time", "Time");
    RegisterFunction(FunctionKind::kDatetime, "datetime", "Datetime");
    RegisterFunction(FunctionKind::kDateBucket, "date_bucket", "Date_bucket");
    RegisterFunction(FunctionKind::kDateTimeBucket, "datetime_bucket",
                     "Datetime_bucket");
    RegisterFunction(FunctionKind::kTimestampBucket, "timestamp_bucket",
                     "Timestamp_bucket");
  }();
  [this]() {
    RegisterFunction(FunctionKind::kNetFormatIP, "net.format_ip",
                     "Net.Format_ip");
    RegisterFunction(FunctionKind::kNetParseIP, "net.parse_ip", "Net.Parse_ip");
    RegisterFunction(FunctionKind::kNetFormatPackedIP, "net.format_packed_ip",
                     "Net.Format_packed_ip");
    RegisterFunction(FunctionKind::kNetParsePackedIP, "net.parse_packed_ip",
                     "Net.Parse_packed_ip");
    RegisterFunction(FunctionKind::kNetIPInNet, "net.ip_in_net",
                     "Net.Ip_in_net");
    RegisterFunction(FunctionKind::kNetMakeNet, "net.make_net", "Net.Make_net");
    RegisterFunction(FunctionKind::kNetHost, "net.host", "Net.Host");
    RegisterFunction(FunctionKind::kNetRegDomain, "net.reg_domain",
                     "Net.Reg_domain");
    RegisterFunction(FunctionKind::kNetPublicSuffix, "net.public_suffix",
                     "Net.Public_suffix");
    RegisterFunction(FunctionKind::kNetIPFromString, "net.ip_from_string",
                     "Net.Ip_from_string");
    RegisterFunction(FunctionKind::kNetSafeIPFromString,
                     "net.safe_ip_from_string", "Net.Safe_ip_from_string");
    RegisterFunction(FunctionKind::kNetIPToString, "net.ip_to_string",
                     "Net.Ip_to_string");
    RegisterFunction(FunctionKind::kNetIPNetMask, "net.ip_net_mask",
                     "Net.Ip_net_mask");
    RegisterFunction(FunctionKind::kNetIPTrunc, "net.ip_trunc", "Net.ip_trunc");
    RegisterFunction(FunctionKind::kNetIPv4FromInt64, "net.ipv4_from_int64",
                     "Net.Ipv4_from_int64");
    RegisterFunction(FunctionKind::kNetIPv4ToInt64, "net.ipv4_to_int64",
                     "Net.Ipv4_to_int64");
    RegisterFunction(FunctionKind::kDenseRank, "dense_rank", "Dense_rank");
    RegisterFunction(FunctionKind::kRank, "rank", "Rank");
    RegisterFunction(FunctionKind::kRowNumber, "row_number", "Row_number");
    RegisterFunction(FunctionKind::kPercentRank, "percent_rank",
                     "Percent_rank");
    RegisterFunction(FunctionKind::kCumeDist, "cume_dist", "Cume_dist");
    RegisterFunction(FunctionKind::kNtile, "ntile", "Ntile");
    RegisterFunction(FunctionKind::kFirstValue, "first_value", "First_value");
    RegisterFunction(FunctionKind::kLastValue, "last_value", "Last_value");
    RegisterFunction(FunctionKind::kNthValue, "nth_value", "Nth_value");
    RegisterFunction(FunctionKind::kLead, "lead", "Lead");
    RegisterFunction(FunctionKind::kLag, "lag", "Lag");
    RegisterFunction(FunctionKind::kPercentileCont, "percentile_cont",
                     "Percentile_cont");
    RegisterFunction(FunctionKind::kPercentileDisc, "percentile_disc",
                     "Percentile_disc");
    RegisterFunction(FunctionKind::kRand, "rand", "Rand");
    RegisterFunction(FunctionKind::kGenerateUuid, "generate_uuid",
                     "Generate_Uuid");
    RegisterFunction(FunctionKind::kMd5, "md5", "Md5");
    RegisterFunction(FunctionKind::kSha1, "sha1", "Sha1");
    RegisterFunction(FunctionKind::kSha256, "sha256", "Sha256");
    RegisterFunction(FunctionKind::kSha512, "sha512", "Sha512");
    RegisterFunction(FunctionKind::kFarmFingerprint, "farm_fingerprint",
                     "FarmFingerprint");
    RegisterFunction(FunctionKind::kError, "error", "Error");
    RegisterFunction(FunctionKind::kArrayIncludes, "array_includes",
                     "ArrayIncludes");
    RegisterFunction(FunctionKind::kArrayIncludesAny, "array_includes_any",
                     "ArrayIncludesAny");
    RegisterFunction(FunctionKind::kArrayIncludesAll, "array_includes_all",
                     "ArrayIncludesAll");
    RegisterFunction(FunctionKind::kArrayFirst, "array_first", "ArrayFirst");
    RegisterFunction(FunctionKind::kArrayLast, "array_last", "ArrayLast");
    RegisterFunction(FunctionKind::kArraySlice, "array_slice", "ArraySlice");
    RegisterFunction(FunctionKind::kArrayFirstN, "array_first_n",
                     "array_first_n");
    RegisterFunction(FunctionKind::kArrayLastN, "array_last_n", "array_last_n");
    RegisterFunction(FunctionKind::kArrayRemoveFirstN, "array_remove_first_n",
                     "array_remove_first_n");
    RegisterFunction(FunctionKind::kArrayRemoveLastN, "array_remove_last_n",
                     "array_remove_last_n");
    RegisterFunction(FunctionKind::kArrayMin, "array_min", "ArrayMin");
    RegisterFunction(FunctionKind::kArrayMax, "array_max", "ArrayMax");
    RegisterFunction(FunctionKind::kRangeCtor, "range", "Range");
    RegisterFunction(FunctionKind::kRangeIsStartUnbounded,
                     "range_is_start_unbounded", "RangeIsStartUnbounded");
    RegisterFunction(FunctionKind::kRangeIsEndUnbounded,
                     "range_is_end_unbounded", "RangeIsEndUnbounded");
    RegisterFunction(FunctionKind::kRangeStart, "range_start", "RangeStart");
    RegisterFunction(FunctionKind::kRangeEnd, "range_end", "RangeEnd");
    RegisterFunction(FunctionKind::kRangeOverlaps, "range_overlaps",
                     "RangeOverlaps");
    RegisterFunction(FunctionKind::kRangeIntersect, "range_intersect",
                     "RangeIntersect");
    RegisterFunction(FunctionKind::kGenerateRangeArray, "generate_range_array",
                     "GenerateRangeArray");
    RegisterFunction(FunctionKind::kRangeContains, "range_contains",
                     "RangeContains");
    RegisterFunction(FunctionKind::kArraySum, "array_sum", "ArraySum");
    RegisterFunction(FunctionKind::kArrayAvg, "array_avg", "ArrayAvg");
    RegisterFunction(FunctionKind::kArrayOffset, "array_offset", "ArrayOffset");
    RegisterFunction(FunctionKind::kArrayFind, "array_find", "ArrayFind");
    RegisterFunction(FunctionKind::kArrayOffsets, "array_offsets",
                     "ArrayOffsets");
    RegisterFunction(FunctionKind::kArrayFindAll, "array_find_all",
                     "ArrayFindAll");
    RegisterFunction(FunctionKind::kCosineDistance, "cosine_distance",
                     "CosineDistance");
    RegisterFunction(FunctionKind::kEuclideanDistance, "euclidean_distance",
                     "EuclideanDistance");
    RegisterFunction(FunctionKind::kEditDistance, "edit_distance",
                     "EditDistance");
  }();
}  // NOLINT(readability/fn_size)

const FunctionMap& GetFunctionMap() {
  static const FunctionMap* function_map = new FunctionMap();
  return *function_map;
}

// An empty ValueTraits template allows for compile-time errors when
// non-supported types are used.
template <TypeKind type>
struct ValueTraits;

// Traits for zetasql String
template <>
struct ValueTraits<TYPE_STRING> {
  static const std::string& FromValue(const Value& value) {
    return value.string_value();
  }

  static Value ToValue(absl::string_view out) { return Value::String(out); }

  static Value ToArray(absl::Span<const Value> values) {
    return Value::Array(types::StringArrayType(), values);
  }

  static Value NullValue() { return Value::NullString(); }

  static absl::string_view DebugString() { return "STRING"; }

  static constexpr zetasql::functions::RegExp::PositionUnit RegExpUnit() {
    return zetasql::functions::RegExp::PositionUnit::kUtf8Chars;
  }
};

// Traits for zetasql bytes
template <>
struct ValueTraits<TYPE_BYTES> {
  static const std::string& FromValue(const Value& value) {
    return value.bytes_value();
  }

  static Value ToValue(absl::string_view out) { return Value::Bytes(out); }

  static Value ToArray(absl::Span<const Value> values) {
    return Value::Array(types::BytesArrayType(), values);
  }

  static Value NullValue() { return Value::NullBytes(); }

  static absl::string_view DebugString() { return "BYTES"; }

  static constexpr zetasql::functions::RegExp::PositionUnit RegExpUnit() {
    return zetasql::functions::RegExp::PositionUnit::kBytes;
  }
};

// Helper function for regexp_contains.
template <TypeKind type>
static absl::StatusOr<Value> Contains(absl::Span<const Value> x,
                                      const functions::RegExp& regexp) {
  ZETASQL_RET_CHECK_EQ(x.size(), 2);
  absl::Status status;
  bool out;
  if (!regexp.Contains(ValueTraits<type>::FromValue(x[0]), &out, &status)) {
    return status;
  }
  return Value::Bool(out);
}

// Helper function for regexp_match.
template <TypeKind type>
static absl::StatusOr<Value> Match(absl::Span<const Value> x,
                                   const functions::RegExp& regexp) {
  ZETASQL_RET_CHECK_EQ(x.size(), 2);
  absl::Status status;
  bool out;
  if (!regexp.Match(ValueTraits<type>::FromValue(x[0]), &out, &status)) {
    return status;
  }
  return Value::Bool(out);
}

// Helper function for regexp_extract.
template <TypeKind type>
static absl::StatusOr<Value> Extract(absl::Span<const Value> x,
                                     const functions::RegExp& regexp) {
  ZETASQL_RET_CHECK_LE(x.size(), 4);
  ZETASQL_RET_CHECK_GE(x.size(), 2);
  int64_t position = 1;
  int64_t occurrence_index = 1;
  if (x.size() >= 3) {
    position = x[2].int64_value();
    if (x.size() == 4) {
      occurrence_index = x[3].int64_value();
    }
  }

  absl::Status status;
  absl::string_view out;
  bool is_null;
  if (!regexp.Extract(/*str=*/ValueTraits<type>::FromValue(x[0]),
                      ValueTraits<type>::RegExpUnit(), position,
                      occurrence_index, &out, &is_null, &status)) {
    return status;
  }
  if (is_null) {
    return ValueTraits<type>::NullValue();
  } else {
    return ValueTraits<type>::ToValue(out);
  }
}

// Helper function for regexp_instr.
template <TypeKind type>
static absl::StatusOr<Value> Instr(absl::Span<const Value> x,
                                   const functions::RegExp& regexp) {
  ZETASQL_RET_CHECK_LE(x.size(), 5);
  ZETASQL_RET_CHECK_GE(x.size(), 2);
  absl::Status status;
  functions::RegExp::InstrParams options;
  for (const Value& arg : x) {
    if (arg.is_null()) {
      return Value::NullInt64();
    }
  }
  options.input_str = ValueTraits<type>::FromValue(x[0]);
  options.position_unit = ValueTraits<type>::RegExpUnit();

  if (x.size() >= 3) {
    options.position = x[2].int64_value();
    if (x.size() >= 4) {
      options.occurrence_index = x[3].int64_value();
      if (x.size() == 5) {
        if (x[4].int64_value() == 1) {
          options.return_position = functions::RegExp::kEndOfMatch;
        } else if (x[4].int64_value() == 0) {
          options.return_position = functions::RegExp::kStartOfMatch;
        } else {
          return absl::Status(absl::StatusCode::kOutOfRange,
                              "Invalid return_position_after_match.");
        }
      }
    }
  }
  int64_t out;
  options.out = &out;
  if (!regexp.Instr(options, &status)) {
    return status;
  }
  return Value::Int64(out);
}

// Helper function for regexp_replace.
template <TypeKind type>
static absl::StatusOr<Value> Replace(absl::Span<const Value> x,
                                     const functions::RegExp& regexp) {
  ZETASQL_RET_CHECK_EQ(x.size(), 3);
  absl::Status status;
  std::string out;
  if (!regexp.Replace(ValueTraits<type>::FromValue(x[0]),
                      ValueTraits<type>::FromValue(x[2]), &out, &status)) {
    return status;
  }
  return ValueTraits<type>::ToValue(out);
}

// Helper function for regexp_extractall.
template <TypeKind type>
static absl::StatusOr<Value> ExtractAll(absl::Span<const Value> x,
                                        const functions::RegExp& regexp) {
  ZETASQL_RET_CHECK_EQ(x.size(), 2);
  absl::Status status;
  std::vector<Value> values;
  functions::RegExp::ExtractAllIterator iter =
      regexp.CreateExtractAllIterator(ValueTraits<type>::FromValue(x[0]));

  while (true) {
    absl::string_view out;
    if (!iter.Next(&out, &status)) {
      break;
    }
    values.push_back(ValueTraits<type>::ToValue(out));
  }
  if (!status.ok()) {
    return status;
  }
  return ValueTraits<type>::ToArray(values);
}

absl::Status UpdateCovariance(long double x, long double y, long double mean_x,
                              long double mean_y, long double pair_count,
                              long double* covar) {
  absl::Status error;
  long double old_pair_count, delta_x, delta_y, tmp;

  // Stable one-pass covariance algorithm per
  // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Covariance
  // covar = ((covar * old_pair_count) +
  //     ((x - mean_x) * (y - mean_y) * old_pair_count) / pair_count) /
  //     pair_count;

  // pair_count - 1
  ZETASQL_RET_CHECK(functions::Subtract(pair_count, 1.0L, &old_pair_count, &error));
  ZETASQL_RET_CHECK_OK(error);

  // x - mean_x
  ZETASQL_RET_CHECK(functions::Subtract(x, mean_x, &delta_x, &error));
  ZETASQL_RET_CHECK_OK(error);

  // y - mean_y
  ZETASQL_RET_CHECK(functions::Subtract(y, mean_y, &delta_y, &error));
  ZETASQL_RET_CHECK_OK(error);

  // (x - mean_x) * (y - mean_y)
  ZETASQL_RET_CHECK(functions::Multiply(delta_x, delta_y, &tmp, &error));
  ZETASQL_RET_CHECK_OK(error);

  // (x - mean_x) * (y - mean_y) * old_pair_count
  ZETASQL_RET_CHECK(functions::Multiply(tmp, old_pair_count, &tmp, &error));
  ZETASQL_RET_CHECK_OK(error);

  // (x - mean_x) * (y - mean_y) * old_pair_count / pair_count
  ZETASQL_RET_CHECK(functions::Divide(tmp, pair_count, &tmp, &error));
  ZETASQL_RET_CHECK_OK(error);

  // covar * old_pair_count
  ZETASQL_RET_CHECK(functions::Multiply(*covar, old_pair_count, covar, &error));
  ZETASQL_RET_CHECK_OK(error);

  // covar * old_pair_count +
  //     ((x - mean_x) * (y - mean_y) * old_pair_count) / pair_count
  ZETASQL_RET_CHECK(functions::Add(*covar, tmp, covar, &error));
  ZETASQL_RET_CHECK_OK(error);

  // (covar * old_pair_count +
  //     ((x - mean_x) * (y - mean_y) * old_pair_count) / pair_count) /
  //     pair_count
  ZETASQL_RET_CHECK(functions::Divide(*covar, pair_count, covar, &error));
  ZETASQL_RET_CHECK_OK(error);

  return absl::OkStatus();
}

absl::Status UpdateMeanAndVariance(long double arg, long double count,
                                   long double* mean, long double* variance) {
  absl::Status error;

  if (!std::isfinite(*variance)) {
    // We've encountered nan and or +inf/-inf before, so there's
    // no need to update mean and variance any further as the result
    // will be nan for any stat.
    return absl::OkStatus();
  }

  if (!std::isfinite(arg)) {
    *variance = std::numeric_limits<double>::quiet_NaN();
    return absl::OkStatus();
  }

  // Stable one-pass variance algorithm based on code in
  // //stats/base/samplestats.h:
  // frac = 1 / count
  // delta = x - avg
  // avg += delta * frac
  // variance += frac * ((1.0 - frac) * delta ^ 2 - variance)

  const long double frac = 1.0L / count;

  // delta = x - mean
  long double delta;
  ZETASQL_RET_CHECK(functions::Subtract(arg, *mean, &delta, &error));
  ZETASQL_RET_CHECK_OK(error);

  // mean += delta * frac
  long double tmp;
  ZETASQL_RET_CHECK(functions::Multiply(delta, frac, &tmp, &error));
  ZETASQL_RET_CHECK_OK(error);
  ZETASQL_RET_CHECK(functions::Add(*mean, tmp, mean, &error));
  ZETASQL_RET_CHECK_OK(error);

  // (1-frac) * delta
  ZETASQL_RET_CHECK(functions::Multiply(1 - frac, delta, &tmp, &error));
  ZETASQL_RET_CHECK_OK(error);

  // (1-frac) * delta ^ 2
  ZETASQL_RET_CHECK(functions::Multiply(tmp, delta, &tmp, &error));
  ZETASQL_RET_CHECK_OK(error);

  // (1-frac) * delta ^ 2 - variance
  ZETASQL_RET_CHECK(functions::Subtract(tmp, *variance, &tmp, &error));
  ZETASQL_RET_CHECK_OK(error);

  // frac * ((1-frac) * delta ^ 2 - variance)
  ZETASQL_RET_CHECK(functions::Multiply(frac, tmp, &tmp, &error));
  ZETASQL_RET_CHECK_OK(error);

  // variance += frac * ((1-frac) * delta ^ 2 - variance)
  ZETASQL_RET_CHECK(functions::Add(*variance, tmp, variance, &error));
  ZETASQL_RET_CHECK_OK(error);

  return absl::OkStatus();
}

class ProtoMapFunction : public BuiltinScalarFunction {
 public:
  ProtoMapFunction(FunctionKind kind, const Type* output_type,
                   ProductMode product_mode)
      : BuiltinScalarFunction(kind, output_type), product_mode_(product_mode) {}

  bool Eval(absl::Span<const TupleData* const> params,
            absl::Span<const Value> args, EvaluationContext* context,
            Value* result, absl::Status* status) const override {
    absl::StatusOr<Value> result_or = EvalInternal(args, context);
    if (!result_or.ok()) {
      *status = result_or.status();
      return false;
    }

    *result = *std::move(result_or);
    return true;
  }

 private:
  absl::StatusOr<Value> EvalInternal(absl::Span<const Value> args,
                                     EvaluationContext* context) const {
    ZETASQL_RET_CHECK_GE(args.size(), 1)
        << "All map functions have a map as their first arg.";
    ZETASQL_RET_CHECK_GE(args.size(), 2) << "All map functions have at least two args.";
    ZETASQL_RET_CHECK(IsProtoMap(args[0].type()))
        << "All map functions have a map as their first arg.";

    if (args[0].is_null()) {
      return Value::Null(output_type());
    }

    const Type* key_type;
    const Type* value_type;
    switch (kind()) {
      case FunctionKind::kSafeProtoMapAtKey:
      case FunctionKind::kProtoMapAtKey:
        key_type = args[1].type();
        value_type = output_type();
        break;
      case FunctionKind::kContainsKey:
        key_type = args[1].type();
        value_type = nullptr;
        break;
      case FunctionKind::kModifyMap:
        ZETASQL_RET_CHECK_GE(args.size(), 3) << "MODIFY_MAP must have at least 3 args.";
        key_type = args[1].type();
        value_type = args[2].type();
        break;
      default:
        return absl::InternalError(absl::StrCat(
            "ProtoMapFunction called with non-proto map FunctionKind: ",
            kind()));
    }

    auto is_sql_equals = [](const Value& left, const Value& right) {
      Value result = left.SqlEquals(right);
      return !result.is_null() && result.bool_value();
    };

    google::protobuf::DynamicMessageFactory factory;
    std::vector<std::pair<Value, Value>> map;
    ZETASQL_RETURN_IF_ERROR(ParseProtoMap(args[0], key_type, value_type, map));

    // TODO: Collation should be propagated from the second arg
    //     of `CONTAINS_KEY` and `$proto_map_at_key`.
    bool equal_values_are_distinguishable =
        value_type != nullptr &&
        IsTypeWithDistinguishableTies(value_type, /*collator_list=*/{});

    switch (kind()) {
      case FunctionKind::kSafeProtoMapAtKey:
      case FunctionKind::kProtoMapAtKey: {
        if (args[1].is_null()) {
          return Value::Null(output_type());
        }
        ZETASQL_RET_CHECK(!args[0].is_null());
        // We need to know if the input array has a fixed order or not because
        // in the event of *ties* we deterministically take the "last" entry.
        // If the input array does not know its order we can't know which entry
        // is "last" and the function is non-deterministic.
        bool is_input_fully_ordered = args[0].num_elements() <= 1 ||
                                      (InternalValue::GetOrderKind(args[0]) ==
                                       InternalValue::kPreservesOrder);
        int found = false;
        Value found_value = Value::Null(output_type());
        // Iterate from the back because when there are ties, we need the last
        // one.
        for (int i = args[0].num_elements() - 1; i >= 0; --i) {
          ZETASQL_RET_CHECK(args[1].is_valid());
          ZETASQL_RET_CHECK(map[i].first.is_valid());
          if (is_sql_equals(args[1], map[i].first)) {
            if (!found) {
              found = true;
              found_value = map[i].second;
              if (is_input_fully_ordered) {
                // When we know the input has a determined order, then its
                // enough to find the last example. The last copy of the key
                // wins.
                break;
              }
              continue;
            }
            ZETASQL_RET_CHECK(!is_input_fully_ordered);
            if (equal_values_are_distinguishable ||
                !is_sql_equals(found_value, map[i].second)) {
              context->SetNonDeterministicOutput();
              break;
            }
            // If we get here, we found a tied key but the value is not
            // distinguisable to the value at the first copy of the key,
            // so $proto_map_at_key is still deterministc.
          }
        }
        if (!found) {
          if (kind() == FunctionKind::kSafeProtoMapAtKey) {
            return Value::Null(output_type());
          }
          return absl::OutOfRangeError(absl::StrCat(
              "Key not found in map: ", args[1].GetSQLLiteral(product_mode_)));
        }
        ZETASQL_RET_CHECK(found_value.type()->Equals(output_type()));
        return found_value;
      }
      case FunctionKind::kContainsKey: {
        if (args[1].is_null()) {
          return Value::Bool(false);
        }
        ZETASQL_RET_CHECK(!args[0].is_null());
        auto find_with_key = [&](const Value& key) {
          return absl::c_find_if(map,
                                 [&](const std::pair<Value, Value>& entry) {
                                   return is_sql_equals(entry.first, key);
                                 });
        };
        return Value::Bool(find_with_key(args[1]) != map.end());
      }
      case FunctionKind::kModifyMap: {
        return ModifyMap(std::move(map), args, context);
      }
      default:
        return absl::InternalError(absl::StrCat(
            "ProtoMapFunction called with non-proto map FunctionKind:",
            kind()));
    }
  }

  absl::StatusOr<Value> ModifyMap(std::vector<std::pair<Value, Value>> map,
                                  absl::Span<const Value> args,
                                  EvaluationContext* context) const {
    const int num_mods = (args.size() - 1) / 2;
    ZETASQL_RET_CHECK(args.size() % 2)
        << "MODIFY_MAP: should have an odd number of args";
    ZETASQL_RET_CHECK_LE(3, args.size())
        << "MODIFY_MAP: should have at least three args";

    absl::flat_hash_set<Value> seen_keys;

    // Change the contents of 'map' according to the replacement args. Also
    // check for erroneous keys.
    for (int i = 0; i < num_mods; ++i) {
      const int key_arg_index = 1 + i * 2;
      const Value& mod_key = args[key_arg_index];
      const bool already_seen = !seen_keys.insert(mod_key).second;
      if (already_seen) {
        return absl::OutOfRangeError(
            absl::StrCat("MODIFY_MAP: Only one instance of each key is "
                         "allowed. Found multiple instances of key: ",
                         mod_key.GetSQLLiteral(product_mode_)));
      }
      if (mod_key.is_null()) {
        return absl::OutOfRangeError(
            absl::StrCat("MODIFY_MAP: All key arguments must be non-NULL, "
                         "but found NULL at argument ",
                         key_arg_index));
      }

      // Erase any entries from map that have the same key as the
      // modification.
      map.erase(std::remove_if(map.begin(), map.end(),
                               [&](const std::pair<Value, Value>& entry) {
                                 return entry.first == mod_key;
                               }),
                map.end());

      const Value& mod_value = args[2 + i * 2];
      if (mod_value.is_null()) continue;
      map.push_back(std::make_pair(mod_key, mod_value));
    }

    const ProtoType* const element_type =
        output_type()->AsArray()->element_type()->AsProto();
    std::vector<Value> output_array;
    output_array.reserve(map.size());
    for (const auto& entry : map) {
      const Value& value = entry.second;
      const Value& key = entry.first;

      std::string element_str;
      // coded_output and output must be destroyed before element_str is valid.
      {
        google::protobuf::io::StringOutputStream output(&element_str);
        google::protobuf::io::CodedOutputStream coded_output(&output);
        bool unused_nondeterministic;
        ZETASQL_RETURN_IF_ERROR(ProtoUtil::WriteField(
            {}, element_type->map_key(), FieldFormat::DEFAULT_FORMAT, key,
            &unused_nondeterministic, &coded_output));
        ZETASQL_RETURN_IF_ERROR(ProtoUtil::WriteField(
            {}, element_type->map_value(), FieldFormat::DEFAULT_FORMAT, value,
            &unused_nondeterministic, &coded_output));
      }
      output_array.push_back(
          Value::Proto(element_type, absl::Cord(std::move(element_str))));
    }

    // Even though the above algorithm is stable and maintins the order of the
    // input array elements, the MODIFY_MAP function does not define where in
    // the artbitrary element order the new key-value pair goes.
    InternalValue::OrderPreservationKind output_orderedness =
        InternalValue::kIgnoresOrder;
    return InternalValue::ArrayChecked(
        output_type()->AsArray(), output_orderedness, std::move(output_array));
  }

  ProductMode product_mode_;
};

absl::Status ConcatError(int64_t max_output_size, zetasql_base::SourceLocation src) {
  return zetasql_base::OutOfRangeErrorBuilder(src)
         << absl::StrCat("Output of CONCAT exceeds max allowed output size of ",
                         max_output_size, " bytes");
}

absl::StatusOr<JSONValueConstRef> GetJSONValueConstRef(
    const Value& json, const JSONParsingOptions& json_parsing_options,
    JSONValue& json_storage) {
  if (json.is_validated_json()) {
    return json.json_value();
  }
  ZETASQL_ASSIGN_OR_RETURN(json_storage,
                   JSONValue::ParseJSONString(json.json_value_unparsed(),
                                              json_parsing_options));
  return json_storage.GetConstRef();
}

}  // namespace

absl::Status MakeMaxArrayValueByteSizeExceededError(
    int64_t max_value_byte_size, const zetasql_base::SourceLocation& source_loc) {
  return zetasql_base::ResourceExhaustedErrorBuilder(source_loc)
         << "Arrays are limited to " << max_value_byte_size << " bytes";
}

functions::TimestampScale GetTimestampScale(const LanguageOptions& options) {
  if (options.LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
    return functions::TimestampScale::kNanoseconds;
  } else {
    return functions::TimestampScale::kMicroseconds;
  }
}

ABSL_CONST_INIT absl::Mutex BuiltinFunctionRegistry::mu_(absl::kConstInit);

/* static */ absl::StatusOr<BuiltinScalarFunction*>
BuiltinFunctionRegistry::GetScalarFunction(FunctionKind kind,
                                           const Type* output_type) {
  absl::MutexLock lock(&mu_);
  auto it = GetFunctionMap().find(kind);
  if (it != GetFunctionMap().end()) {
    return it->second(output_type);
  } else {
    return zetasql_base::UnimplementedErrorBuilder(zetasql_base::SourceLocation::current())
           << BuiltinFunctionCatalog::GetDebugNameByKind(kind)
           << " is an optional function implementation which is not present "
              "in this binary or has not been registered";
  }
}

/* static */ void BuiltinFunctionRegistry::RegisterScalarFunction(
    std::initializer_list<FunctionKind> kinds,
    const std::function<BuiltinScalarFunction*(FunctionKind, const Type*)>&
        constructor) {
  absl::MutexLock lock(&mu_);
  for (FunctionKind kind : kinds) {
    GetFunctionMap()[kind] = [kind,
                              constructor](const zetasql::Type* output_type) {
      return constructor(kind, output_type);
    };
  }
}

/* static */ absl::flat_hash_map<
    FunctionKind, BuiltinFunctionRegistry::ScalarFunctionConstructor>&
BuiltinFunctionRegistry::GetFunctionMap() {
  static auto* map =
      new absl::flat_hash_map<FunctionKind, ScalarFunctionConstructor>();
  return *map;
}

// Sets the provided EvaluationContext to have non-deterministic output if the
// given array has more than one element and is not order-preserving.
void MaybeSetNonDeterministicArrayOutput(const Value& array,
                                         EvaluationContext* context) {
  ABSL_DCHECK(array.type()->IsArray());
  if (!array.is_null() && array.num_elements() > 1 &&
      (InternalValue::GetOrderKind(array) == InternalValue::kIgnoresOrder)) {
    context->SetNonDeterministicOutput();
  }
}

// This method is used only for setting non-deterministic output.
// This method does not detect floating point types within STRUCTs or PROTOs,
// which would be too expensive to call for each row.
// Geography type internally contains floating point data, and thus treated
// the same way for this purpose.
bool HasFloatingPoint(const Type* type) {
  return type->IsFloatingPoint() || type->IsGeography() ||
         (type->IsArray() &&
          (type->AsArray()->element_type()->IsFloatingPoint() ||
           type->AsArray()->element_type()->IsGeography()));
}

// Function used to switch on a (function kind, type) pair.
static constexpr uint64_t FCT(FunctionKind function_kind, TypeKind type_kind) {
  return (static_cast<uint64_t>(function_kind) << 32) + type_kind;
}

// Function used to switch on a (function kind, type, type)
// triple.
static constexpr uint64_t FCT2(FunctionKind function_kind, TypeKind type_kind1,
                               TypeKind type_kind2) {
  return (static_cast<uint64_t>(function_kind) << 32) +
         (static_cast<uint64_t>(type_kind1) << 16) + type_kind2;
}

// Function used to switch on a (function kind, type, args.size())
// triple.
static constexpr uint64_t FCT_TYPE_ARITY(FunctionKind function_kind,
                                         TypeKind type_kind, size_t arity) {
  return (static_cast<uint64_t>(function_kind) << 32) + (type_kind << 16) +
         arity;
}

absl::StatusOr<FunctionKind> BuiltinFunctionCatalog::GetKindByName(
    const absl::string_view name) {
  const FunctionKind* kind =
      zetasql_base::FindOrNull(GetFunctionMap().function_kind_by_name(), name);
  if (kind == nullptr) {
    return absl::UnimplementedError(
        absl::StrCat("Unsupported built-in function: ", name));
  }
  return *kind;
}

std::string BuiltinFunctionCatalog::GetDebugNameByKind(FunctionKind kind) {
  return zetasql_base::FindWithDefault(GetFunctionMap().function_debug_name_by_kind(),
                              kind);
}

std::string BuiltinScalarFunction::debug_name() const {
  return BuiltinFunctionCatalog::GetDebugNameByKind(kind());
}

static absl::Status ValidateInputTypesSupportEqualityComparison(
    FunctionKind kind, absl::Span<const Type* const> input_types) {
  for (auto type : input_types) {
    if (!ValidateTypeSupportsEqualityComparison(type).ok()) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Inputs to " << BuiltinFunctionCatalog::GetDebugNameByKind(kind)
             << " must support equality comparison: " << type->DebugString();
    }
  }
  return absl::OkStatus();
}

static absl::Status ValidateInputTypesSupportOrderComparison(
    FunctionKind kind, absl::Span<const Type* const> input_types) {
  for (auto type : input_types) {
    if (!ValidateTypeSupportsOrderComparison(type).ok()) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Inputs to " << BuiltinFunctionCatalog::GetDebugNameByKind(kind)
             << " must support order comparison: " << type->DebugString();
    }
  }
  return absl::OkStatus();
}

static absl::Status ValidateSupportedTypes(
    const LanguageOptions& language_options,
    absl::Span<const Type* const> input_types) {
  for (auto type : input_types) {
    if (!type->IsSupportedType(language_options)) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Type not found: "
             << type->TypeName(language_options.product_mode());
    }
  }
  return absl::OkStatus();
}

static absl::Status CheckVectorDistanceInputType(
    const std::vector<const Type*>& input_types) {
  ZETASQL_RET_CHECK_EQ(input_types.size(), 2) << absl::Substitute(
      "input type size must be exactly 2 but got $0", input_types.size());

  for (int i = 0; i < input_types.size(); ++i) {
    ZETASQL_RET_CHECK(input_types[i]->IsArray()) << "both input types must be array";
  }
  if (input_types[0]->AsArray()->element_type()->IsDouble()) {
    ZETASQL_RET_CHECK(input_types[1]->AsArray()->element_type()->IsDouble())
        << "array element type must be both DOUBLE";
    return absl::OkStatus();
  } else if (input_types[0]->AsArray()->element_type()->IsFloat()) {
    ZETASQL_RET_CHECK(input_types[1]->AsArray()->element_type()->IsFloat())
        << "array element type must be both FLOAT";
    return absl::OkStatus();
  }

  for (int i = 0; i < input_types.size(); ++i) {
    ZETASQL_RET_CHECK(input_types[i]->AsArray()->element_type()->IsStruct())
        << "array element type must be struct";
    ZETASQL_RET_CHECK_EQ(
        input_types[i]->AsArray()->element_type()->AsStruct()->num_fields(), 2)
        << "array struct element type must have exactly 2 fields";
    ZETASQL_RET_CHECK(input_types[i]
                  ->AsArray()
                  ->element_type()
                  ->AsStruct()
                  ->fields()[1]
                  .type->IsDouble())
        << "array struct 2nd element type must be DOUBLE";
  }
  auto key_type0 =
      input_types[0]->AsArray()->element_type()->AsStruct()->fields()[0].type;
  auto key_type1 =
      input_types[1]->AsArray()->element_type()->AsStruct()->fields()[0].type;
  ZETASQL_RET_CHECK_EQ(key_type0, key_type1) << "key types must be the same";

  return absl::OkStatus();
}

static absl::StatusOr<std::unique_ptr<SimpleBuiltinScalarFunction>>
CreateCosineDistanceFunction(std::vector<const Type*>& input_types,
                             const Type* output_type) {
  ZETASQL_RET_CHECK_OK(CheckVectorDistanceInputType(input_types));

  bool is_signature_dense =
      input_types[0]->AsArray()->element_type()->IsDouble() ||
      input_types[0]->AsArray()->element_type()->IsFloat();
  if (is_signature_dense) {
    return std::make_unique<CosineDistanceFunctionDense>(
        FunctionKind::kCosineDistance, output_type);
  }

  bool is_int64 = input_types[0]
                      ->AsArray()
                      ->element_type()
                      ->AsStruct()
                      ->field(0)
                      .type->IsInt64();
  if (is_int64) {
    return std::make_unique<CosineDistanceFunctionSparseInt64Key>(
        FunctionKind::kCosineDistance, output_type);
  }

  bool is_string = input_types[0]
                       ->AsArray()
                       ->element_type()
                       ->AsStruct()
                       ->field(0)
                       .type->IsString();
  ZETASQL_RET_CHECK(is_string) << "input type must be either STRUCT with INT64 index "
                          "field or STRING index field";
  return std::make_unique<CosineDistanceFunctionSparseStringKey>(
      FunctionKind::kCosineDistance, output_type);
}

static absl::StatusOr<std::unique_ptr<SimpleBuiltinScalarFunction>>
CreateEuclideanDistanceFunction(std::vector<const Type*>& input_types,
                                const Type* output_type) {
  ZETASQL_RET_CHECK_OK(CheckVectorDistanceInputType(input_types));
  bool is_signature_dense =
      input_types[0]->AsArray()->element_type()->IsDouble() ||
      input_types[0]->AsArray()->element_type()->IsFloat();
  if (is_signature_dense) {
    return std::make_unique<EuclideanDistanceFunctionDense>(
        FunctionKind::kEuclideanDistance, output_type);
  }

  bool is_int64 = input_types[0]
                      ->AsArray()
                      ->element_type()
                      ->AsStruct()
                      ->field(0)
                      .type->IsInt64();
  if (is_int64) {
    return std::make_unique<EuclideanDistanceFunctionSparseInt64Key>(
        FunctionKind::kEuclideanDistance, output_type);
  }

  bool is_string = input_types[0]
                       ->AsArray()
                       ->element_type()
                       ->AsStruct()
                       ->field(0)
                       .type->IsString();
  ZETASQL_RET_CHECK(is_string) << "input type must be either STRUCT with INT64 index "
                          "field or STRING index field";
  return std::make_unique<EuclideanDistanceFunctionSparseStringKey>(
      FunctionKind::kEuclideanDistance, output_type);
}

absl::StatusOr<std::unique_ptr<ScalarFunctionCallExpr>>
BuiltinScalarFunction::CreateCast(
    const LanguageOptions& language_options, const Type* output_type,
    std::unique_ptr<ValueExpr> argument, std::unique_ptr<ValueExpr> format,
    std::unique_ptr<ValueExpr> time_zone, const TypeModifiers& type_modifiers,
    bool return_null_on_error, ResolvedFunctionCallBase::ErrorMode error_mode,
    std::unique_ptr<ExtendedCompositeCastEvaluator> extended_cast_evaluator) {
  ZETASQL_ASSIGN_OR_RETURN(auto null_on_error_exp,
                   ConstExpr::Create(Value::Bool(return_null_on_error)));

  ZETASQL_RETURN_IF_ERROR(ValidateSupportedTypes(
      language_options, {output_type, argument->output_type()}));

  std::vector<std::unique_ptr<ValueExpr>> args;
  args.push_back(std::move(argument));
  args.push_back(std::move(null_on_error_exp));
  if (format != nullptr) {
    args.push_back(std::move(format));
  }
  if (time_zone != nullptr) {
    args.push_back(std::move(time_zone));
  }

  return ScalarFunctionCallExpr::Create(
      std::make_unique<CastFunction>(
          output_type, std::move(extended_cast_evaluator),
          std::move(type_modifiers.type_parameters())),
      std::move(args), error_mode);
}

absl::StatusOr<std::unique_ptr<ScalarFunctionCallExpr>>
BuiltinScalarFunction::CreateCall(
    FunctionKind kind, const LanguageOptions& language_options,
    const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> arguments,
    ResolvedFunctionCallBase::ErrorMode error_mode) {
  std::vector<std::unique_ptr<AlgebraArg>> converted_arguments;
  converted_arguments.reserve(arguments.size());
  for (auto& e : arguments) {
    converted_arguments.push_back(std::make_unique<ExprArg>(std::move(e)));
  }

  return CreateCall(kind, language_options, output_type,
                    std::move(converted_arguments), error_mode);
}

absl::StatusOr<std::unique_ptr<ScalarFunctionCallExpr>>
BuiltinScalarFunction::CreateCall(
    FunctionKind kind, const LanguageOptions& language_options,
    const Type* output_type, std::vector<std::unique_ptr<AlgebraArg>> arguments,
    ResolvedFunctionCallBase::ErrorMode error_mode) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<BuiltinScalarFunction> function,
      CreateValidated(kind, language_options, output_type, arguments));
  return ScalarFunctionCallExpr::Create(std::move(function),
                                        std::move(arguments), error_mode);
}

absl::StatusOr<BuiltinScalarFunction*>
BuiltinScalarFunction::CreateValidatedRaw(
    FunctionKind kind, const LanguageOptions& language_options,
    const Type* output_type,
    const std::vector<std::unique_ptr<AlgebraArg>>& arguments) {
  std::vector<const Type*> input_types;
  input_types.reserve(arguments.size());
  for (const auto& expr : arguments) {
    input_types.push_back(expr->node()->output_type());
  }
  ZETASQL_RETURN_IF_ERROR(ValidateSupportedTypes(language_options, {output_type}));
  ZETASQL_RETURN_IF_ERROR(ValidateSupportedTypes(language_options, input_types));
  switch (kind) {
    case FunctionKind::kAdd:
    case FunctionKind::kSubtract:
    case FunctionKind::kMultiply:
    case FunctionKind::kDivide:
    case FunctionKind::kDiv:
    case FunctionKind::kSafeAdd:
    case FunctionKind::kSafeSubtract:
    case FunctionKind::kSafeMultiply:
    case FunctionKind::kSafeDivide:
    case FunctionKind::kMod:
    case FunctionKind::kUnaryMinus:
    case FunctionKind::kSafeNegate:
      return new ArithmeticFunction(kind, output_type);
    case FunctionKind::kEqual:
    case FunctionKind::kLess:
    case FunctionKind::kLessOrEqual:
    case FunctionKind::kIsDistinct:
    case FunctionKind::kIsNotDistinct:
      ZETASQL_RETURN_IF_ERROR(
          ValidateInputTypesSupportEqualityComparison(kind, input_types));
      return new ComparisonFunction(kind, output_type);
    case FunctionKind::kLeast:
      ZETASQL_RETURN_IF_ERROR(
          ValidateInputTypesSupportOrderComparison(kind, input_types));
      return new LeastFunction(output_type);
    case FunctionKind::kGreatest:
      ZETASQL_RETURN_IF_ERROR(
          ValidateInputTypesSupportOrderComparison(kind, input_types));
      return new GreatestFunction(output_type);
    case FunctionKind::kAnd:
    case FunctionKind::kNot:
    case FunctionKind::kOr:
      return new LogicalFunction(kind, output_type);
    case FunctionKind::kExists:
      return new ExistsFunction(kind, output_type);
    case FunctionKind::kIsNull:
    case FunctionKind::kIsTrue:
    case FunctionKind::kIsFalse:
      return new IsFunction(kind, output_type);
    case FunctionKind::kCast:
      return new CastFunction(kind, output_type);
    case FunctionKind::kBitCastToInt32:
    case FunctionKind::kBitCastToInt64:
    case FunctionKind::kBitCastToUint32:
    case FunctionKind::kBitCastToUint64:
      return new BitCastFunction(kind, output_type);
    case FunctionKind::kLike: {
      ZETASQL_RETURN_IF_ERROR(
          ValidateInputTypesSupportEqualityComparison(kind, input_types));
      ZETASQL_ASSIGN_OR_RETURN(auto fct,
                       CreateLikeFunction(kind, output_type, arguments));
      return fct.release();
    }
    case FunctionKind::kLikeAny: {
      ZETASQL_RETURN_IF_ERROR(
          ValidateInputTypesSupportEqualityComparison(kind, input_types));
      ZETASQL_ASSIGN_OR_RETURN(auto fct,
                       CreateLikeAnyFunction(kind, output_type, arguments));
      return fct.release();
    }
    case FunctionKind::kLikeAll: {
      ZETASQL_RETURN_IF_ERROR(
          ValidateInputTypesSupportEqualityComparison(kind, input_types));
      ZETASQL_ASSIGN_OR_RETURN(auto fct,
                       CreateLikeAllFunction(kind, output_type, arguments));
      return fct.release();
    }
    case FunctionKind::kLikeAnyArray:
    case FunctionKind::kLikeAllArray: {
      ZETASQL_RET_CHECK_EQ(arguments.size(), 2);
      ZETASQL_RETURN_IF_ERROR(
          ValidateInputTypesSupportEqualityComparison(kind, input_types));
      ZETASQL_ASSIGN_OR_RETURN(auto fct, CreateLikeAnyAllArrayFunction(
                                     kind, output_type, arguments));
      return fct.release();
    }
    case FunctionKind::kLikeWithCollation:
    case FunctionKind::kLikeAnyWithCollation:
    case FunctionKind::kLikeAllWithCollation:
      ZETASQL_RETURN_IF_ERROR(
          ValidateInputTypesSupportEqualityComparison(kind, input_types));
      return BuiltinFunctionRegistry::GetScalarFunction(kind, output_type);
    case FunctionKind::kBitwiseNot:
    case FunctionKind::kBitwiseOr:
    case FunctionKind::kBitwiseXor:
    case FunctionKind::kBitwiseAnd:
    case FunctionKind::kBitwiseLeftShift:
    case FunctionKind::kBitwiseRightShift:
      return new BitwiseFunction(kind, output_type);
    case FunctionKind::kBitCount:
      return new BitCountFunction;
    case FunctionKind::kArrayAtOrdinal:
      return new ArrayElementFunction(1, false /* safe */, output_type);
    case FunctionKind::kArrayAtOffset:
      return new ArrayElementFunction(0, false /* safe */, output_type);
    case FunctionKind::kSafeArrayAtOrdinal:
      return new ArrayElementFunction(1, true /* safe */, output_type);
    case FunctionKind::kSafeArrayAtOffset:
      return new ArrayElementFunction(0, true /* safe */, output_type);
    case FunctionKind::kSafeProtoMapAtKey:
    case FunctionKind::kProtoMapAtKey:
    case FunctionKind::kContainsKey:
    case FunctionKind::kModifyMap:
      return new ProtoMapFunction(kind, output_type,
                                  language_options.product_mode());
    case FunctionKind::kAbs:
    case FunctionKind::kSign:
    case FunctionKind::kRound:
    case FunctionKind::kTrunc:
    case FunctionKind::kCeil:
    case FunctionKind::kFloor:
    case FunctionKind::kIsNan:
    case FunctionKind::kIsInf:
    case FunctionKind::kIeeeDivide:
    case FunctionKind::kSqrt:
    case FunctionKind::kCbrt:
    case FunctionKind::kPow:
    case FunctionKind::kExp:
    case FunctionKind::kNaturalLogarithm:
    case FunctionKind::kDecimalLogarithm:
    case FunctionKind::kLogarithm:
    case FunctionKind::kCos:
    case FunctionKind::kCosh:
    case FunctionKind::kAcos:
    case FunctionKind::kAcosh:
    case FunctionKind::kSin:
    case FunctionKind::kSinh:
    case FunctionKind::kAsin:
    case FunctionKind::kAsinh:
    case FunctionKind::kTan:
    case FunctionKind::kTanh:
    case FunctionKind::kAtan:
    case FunctionKind::kAtanh:
    case FunctionKind::kAtan2:
    case FunctionKind::kCsc:
    case FunctionKind::kSec:
    case FunctionKind::kCot:
    case FunctionKind::kCsch:
    case FunctionKind::kSech:
    case FunctionKind::kCoth:
      return new MathFunction(kind, output_type);
    case FunctionKind::kPi:
    case FunctionKind::kPiNumeric:
    case FunctionKind::kPiBigNumeric:
      return new NullaryFunction(kind, output_type);
    case FunctionKind::kConcat:
      return new ConcatFunction(kind, output_type);
    case FunctionKind::kLower:
    case FunctionKind::kUpper:
      return new CaseConverterFunction(kind, output_type);
    case FunctionKind::kArrayFilter:
      ZETASQL_RET_CHECK_EQ(2, arguments.size());
      ZETASQL_RET_CHECK(arguments[1]->inline_lambda_expr() != nullptr);
      return new ArrayFilterFunction(
          kind, output_type, arguments[1]->mutable_inline_lambda_expr());
    case FunctionKind::kArrayTransform:
      ZETASQL_RET_CHECK_EQ(2, arguments.size());
      ZETASQL_RET_CHECK(arguments[1]->inline_lambda_expr() != nullptr);
      return new ArrayTransformFunction(
          kind, output_type, arguments[1]->mutable_inline_lambda_expr());
    case FunctionKind::kLength:
    case FunctionKind::kByteLength:
    case FunctionKind::kCharLength:
    case FunctionKind::kStartsWith:
    case FunctionKind::kEndsWith:
    case FunctionKind::kSubstr:
    case FunctionKind::kTrim:
    case FunctionKind::kLtrim:
    case FunctionKind::kRtrim:
    case FunctionKind::kLeft:
    case FunctionKind::kRight:
    case FunctionKind::kReplace:
    case FunctionKind::kStrpos:
    case FunctionKind::kInstr:
    case FunctionKind::kSafeConvertBytesToString:
    case FunctionKind::kNormalize:
    case FunctionKind::kNormalizeAndCasefold:
    case FunctionKind::kToBase64:
    case FunctionKind::kFromBase64:
    case FunctionKind::kToHex:
    case FunctionKind::kFromHex:
    case FunctionKind::kLpad:
    case FunctionKind::kRpad:
    case FunctionKind::kRepeat:
    case FunctionKind::kReverse:
    case FunctionKind::kSoundex:
    case FunctionKind::kAscii:
    case FunctionKind::kUnicode:
    case FunctionKind::kChr:
    case FunctionKind::kTranslate:
    case FunctionKind::kInitCap:
      return new StringFunction(kind, output_type);
    case FunctionKind::kCollate:
      return new CollateFunction(kind, output_type);
    case FunctionKind::kParseNumeric:
    case FunctionKind::kParseBignumeric:
      return new NumericFunction(kind, output_type);
    case FunctionKind::kToCodePoints:
      return new ToCodePointsFunction;
    case FunctionKind::kCodePointsToString:
    case FunctionKind::kCodePointsToBytes:
      return new CodePointsToFunction(kind, output_type);
    case FunctionKind::kFormat:
      return new FormatFunction(output_type);
    case FunctionKind::kRegexpContains:
    case FunctionKind::kRegexpMatch:
    case FunctionKind::kRegexpExtract:
    case FunctionKind::kRegexpExtractAll:
    case FunctionKind::kRegexpInstr:
    case FunctionKind::kRegexpReplace: {
      ZETASQL_ASSIGN_OR_RETURN(auto fct,
                       CreateRegexpFunction(kind, output_type, arguments));
      return fct.release();
    }
    case FunctionKind::kSplit:
      return new SplitFunction(kind, output_type);
    case FunctionKind::kGenerateArray:
    case FunctionKind::kGenerateDateArray:
    case FunctionKind::kGenerateTimestampArray:
      return new GenerateArrayFunction(output_type);
    case FunctionKind::kRangeBucket:
      return new RangeBucketFunction();
    case FunctionKind::kSubscript:
    case FunctionKind::kJsonExtract:
    case FunctionKind::kJsonExtractScalar:
    case FunctionKind::kJsonExtractArray:
    case FunctionKind::kJsonExtractStringArray:
    case FunctionKind::kJsonQuery:
    case FunctionKind::kJsonValue:
    case FunctionKind::kJsonQueryArray:
    case FunctionKind::kJsonValueArray:
    case FunctionKind::kToJson:
    case FunctionKind::kToJsonString:
    case FunctionKind::kParseJson:
    case FunctionKind::kJsonType:
    case FunctionKind::kInt64:
    case FunctionKind::kDouble:
    case FunctionKind::kBool:
    case FunctionKind::kLaxBool:
    case FunctionKind::kLaxInt64:
    case FunctionKind::kLaxDouble:
    case FunctionKind::kLaxString:
    case FunctionKind::kJsonArray:
    case FunctionKind::kJsonObject:
    case FunctionKind::kJsonRemove:
    case FunctionKind::kJsonSet:
    case FunctionKind::kJsonStripNulls:
    case FunctionKind::kJsonArrayInsert:
    case FunctionKind::kJsonArrayAppend:
      return BuiltinFunctionRegistry::GetScalarFunction(kind, output_type);
    case FunctionKind::kStartsWithWithCollation:
    case FunctionKind::kEndsWithWithCollation:
    case FunctionKind::kReplaceWithCollation:
    case FunctionKind::kStrposWithCollation:
    case FunctionKind::kInstrWithCollation:
    case FunctionKind::kSplitWithCollation:
    case FunctionKind::kCollationKey:
      return BuiltinFunctionRegistry::GetScalarFunction(kind, output_type);
    case FunctionKind::kArrayConcat:
      return new ArrayConcatFunction(kind, output_type);
    case FunctionKind::kArrayLength:
      return new ArrayLengthFunction(kind, output_type);
    case FunctionKind::kArrayToString:
      return new ArrayToStringFunction(kind, output_type);
    case FunctionKind::kArrayReverse:
      return new ArrayReverseFunction(output_type);
    case FunctionKind::kArrayIsDistinct:
      return new ArrayIsDistinctFunction(kind, output_type);
    case FunctionKind::kArrayIncludes:
      ZETASQL_RET_CHECK_EQ(arguments.size(), 2);
      if (arguments[1]->value_expr() != nullptr) {
        return new ArrayIncludesFunction();
      } else {
        ZETASQL_RET_CHECK(arguments[1]->inline_lambda_expr() != nullptr);
        return new ArrayIncludesFunctionWithLambda(
            kind, output_type, arguments[1]->mutable_inline_lambda_expr());
      }
    case FunctionKind::kArrayIncludesAny:
      return new ArrayIncludesArrayFunction(/*require_all=*/false);
    case FunctionKind::kArrayIncludesAll:
      return new ArrayIncludesArrayFunction(/*require_all=*/true);
    case FunctionKind::kArrayFirst:
    case FunctionKind::kArrayLast:
      return new ArrayFirstLastFunction(kind, output_type);
    case FunctionKind::kArraySlice:
    case FunctionKind::kArrayFirstN:
    case FunctionKind::kArrayLastN:
    case FunctionKind::kArrayRemoveFirstN:
    case FunctionKind::kArrayRemoveLastN:
      return new ArraySliceFunction(kind, output_type);
    case FunctionKind::kArraySum:
    case FunctionKind::kArrayAvg:
      return new ArraySumAvgFunction(kind, output_type);
    case FunctionKind::kCurrentDate:
    case FunctionKind::kCurrentDatetime:
    case FunctionKind::kCurrentTime:
      if (input_types.empty()) {
        return new NullaryFunction(kind, output_type);
      }
      return new DateTimeUnaryFunction(kind, output_type);
    case FunctionKind::kCurrentTimestamp:
      return new NullaryFunction(kind, output_type);
    case FunctionKind::kDateFromUnixDate:
    case FunctionKind::kUnixDate:
      return new DateTimeUnaryFunction(kind, output_type);
    case FunctionKind::kDateAdd:
    case FunctionKind::kDateSub:
    case FunctionKind::kDateDiff:
    case FunctionKind::kDatetimeAdd:
    case FunctionKind::kDatetimeSub:
    case FunctionKind::kDatetimeDiff:
    case FunctionKind::kTimeAdd:
    case FunctionKind::kTimeSub:
    case FunctionKind::kTimeDiff:
    case FunctionKind::kTimestampAdd:
    case FunctionKind::kTimestampSub:
    case FunctionKind::kTimestampDiff:
      return new DateTimeDiffFunction(kind, output_type);
    case FunctionKind::kDatetimeTrunc:
    case FunctionKind::kTimeTrunc:
    case FunctionKind::kDateTrunc:
    case FunctionKind::kTimestampTrunc:
      return new DateTimeTruncFunction(kind, output_type);
    case FunctionKind::kLastDay:
      return new LastDayFunction(kind, output_type);
    case FunctionKind::kExtractFrom:
      return new ExtractFromFunction(kind, output_type);
    case FunctionKind::kExtractDateFrom:
      return new ExtractDateFromFunction(kind, output_type);
    case FunctionKind::kExtractTimeFrom:
      return new ExtractTimeFromFunction(kind, output_type);
    case FunctionKind::kExtractDatetimeFrom:
      return new ExtractDatetimeFromFunction(kind, output_type);
    case FunctionKind::kFormatDate:
    case FunctionKind::kFormatDatetime:
    case FunctionKind::kFormatTimestamp:
      return new FormatDateDatetimeTimestampFunction(kind, output_type);
    case FunctionKind::kFormatTime:
      return new FormatTimeFunction(kind, output_type);
    case FunctionKind::kTimestamp:
      return new TimestampConversionFunction(kind, output_type);
    case FunctionKind::kDate:
    case FunctionKind::kTime:
    case FunctionKind::kDatetime:
      return new CivilTimeConstructionAndConversionFunction(kind, output_type);
    case FunctionKind::kTimestampSeconds:
    case FunctionKind::kTimestampMillis:
    case FunctionKind::kTimestampMicros:
    case FunctionKind::kTimestampFromUnixSeconds:
    case FunctionKind::kTimestampFromUnixMillis:
    case FunctionKind::kTimestampFromUnixMicros:
      return new TimestampFromIntFunction(kind, output_type);
    case FunctionKind::kSecondsFromTimestamp:
    case FunctionKind::kMillisFromTimestamp:
    case FunctionKind::kMicrosFromTimestamp:
      return new IntFromTimestampFunction(kind, output_type);
    case FunctionKind::kDateBucket:
    case FunctionKind::kDateTimeBucket:
    case FunctionKind::kTimestampBucket:
      return new DateTimeBucketFunction(kind, output_type);
    case FunctionKind::kString:
      return new StringConversionFunction(kind, output_type);
    case FunctionKind::kFromProto:
      return new FromProtoFunction(kind, output_type);
    case FunctionKind::kToProto:
      return new ToProtoFunction(kind, output_type);
    case FunctionKind::kEnumValueDescriptorProto:
      return new EnumValueDescriptorProtoFunction(kind, output_type);
    case FunctionKind::kParseDate:
      return new ParseDateFunction(kind, output_type);
    case FunctionKind::kParseDatetime:
      return new ParseDatetimeFunction(kind, output_type);
    case FunctionKind::kParseTime:
      return new ParseTimeFunction(kind, output_type);
    case FunctionKind::kParseTimestamp:
      return new ParseTimestampFunction(kind, output_type);
    case FunctionKind::kIntervalCtor:
    case FunctionKind::kMakeInterval:
    case FunctionKind::kJustifyHours:
    case FunctionKind::kJustifyDays:
    case FunctionKind::kJustifyInterval:
      return new IntervalFunction(kind, output_type);
    case FunctionKind::kNetFormatIP:
    case FunctionKind::kNetParseIP:
    case FunctionKind::kNetFormatPackedIP:
    case FunctionKind::kNetParsePackedIP:
    case FunctionKind::kNetIPInNet:
    case FunctionKind::kNetMakeNet:
    case FunctionKind::kNetHost:
    case FunctionKind::kNetRegDomain:
    case FunctionKind::kNetPublicSuffix:
    case FunctionKind::kNetIPFromString:
    case FunctionKind::kNetSafeIPFromString:
    case FunctionKind::kNetIPToString:
    case FunctionKind::kNetIPNetMask:
    case FunctionKind::kNetIPTrunc:
    case FunctionKind::kNetIPv4FromInt64:
    case FunctionKind::kNetIPv4ToInt64:
      return new NetFunction(kind, output_type);
    case FunctionKind::kMakeProto:
      ZETASQL_RET_CHECK_FAIL() << "MakeProto needs extra parameters";
      break;
    case FunctionKind::kRand:
      return new RandFunction;
    case FunctionKind::kGenerateUuid:
      // UUID functions are optional.
      return BuiltinFunctionRegistry::GetScalarFunction(kind, output_type);
    case FunctionKind::kMd5:
    case FunctionKind::kSha1:
    case FunctionKind::kSha256:
    case FunctionKind::kSha512:
    case FunctionKind::kFarmFingerprint:
      // Hash functions are optional.
      return BuiltinFunctionRegistry::GetScalarFunction(kind, output_type);
    case FunctionKind::kError:
      return new ErrorFunction(output_type);
    case FunctionKind::kRangeCtor:
    case FunctionKind::kRangeIsStartUnbounded:
    case FunctionKind::kRangeIsEndUnbounded:
    case FunctionKind::kRangeStart:
    case FunctionKind::kRangeEnd:
    case FunctionKind::kRangeOverlaps:
    case FunctionKind::kRangeIntersect:
    case FunctionKind::kGenerateRangeArray:
    case FunctionKind::kRangeContains:
      return BuiltinFunctionRegistry::GetScalarFunction(kind, output_type);
    case FunctionKind::kCosineDistance: {
      ZETASQL_ASSIGN_OR_RETURN(auto f,
                       CreateCosineDistanceFunction(input_types, output_type));
      return f.release();
    }
    case FunctionKind::kEuclideanDistance: {
      ZETASQL_ASSIGN_OR_RETURN(
          auto f, CreateEuclideanDistanceFunction(input_types, output_type));
      return f.release();
    }
    case FunctionKind::kEditDistance:
      return new EditDistanceFunction(kind, output_type);
    default:
      ZETASQL_RET_CHECK_FAIL() << BuiltinFunctionCatalog::GetDebugNameByKind(kind)
                       << " is not a scalar function";
      break;
  }
}  // NOLINT(readability/fn_size)

absl::StatusOr<std::unique_ptr<BuiltinScalarFunction>>
BuiltinScalarFunction::CreateValidated(
    FunctionKind kind, const LanguageOptions& language_options,
    const Type* output_type,
    const std::vector<std::unique_ptr<AlgebraArg>>& arguments) {
  ZETASQL_ASSIGN_OR_RETURN(
      BuiltinScalarFunction * func,
      CreateValidatedRaw(kind, language_options, output_type, arguments));
  return std::unique_ptr<BuiltinScalarFunction>(func);
}

namespace {
absl::StatusOr<std::unique_ptr<RE2>> GetLikePatternRegexp(
    const ValueExpr& arg) {
  if (arg.IsConstant() &&
      (arg.output_type()->IsString() || arg.output_type()->IsBytes())) {
    const ConstExpr& pattern_expr = static_cast<const ConstExpr&>(arg);
    if (!pattern_expr.value().is_null()) {
      // Build and precompile the regexp.
      const std::string& pattern =
          pattern_expr.value().type_kind() == TYPE_STRING
              ? pattern_expr.value().string_value()
              : pattern_expr.value().bytes_value();
      std::unique_ptr<RE2> regexp;
      ZETASQL_RETURN_IF_ERROR(functions::CreateLikeRegexp(
          pattern, arg.output_type()->kind(), &regexp));
      return regexp;
    }
  }
  // The pattern is not a constant expression or it is null; build and
  // compile the regexp at evaluation time.
  return nullptr;
}
}  // namespace

absl::StatusOr<std::unique_ptr<BuiltinScalarFunction>>
BuiltinScalarFunction::CreateLikeFunction(
    FunctionKind kind, const Type* output_type,
    const std::vector<std::unique_ptr<AlgebraArg>>& arguments) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RE2> regexp,
                   GetLikePatternRegexp(*arguments[1]->value_expr()));
  return std::unique_ptr<BuiltinScalarFunction>(
      new LikeFunction(kind, output_type, std::move(regexp)));
}

absl::StatusOr<std::unique_ptr<BuiltinScalarFunction>>
BuiltinScalarFunction::CreateLikeAnyFunction(
    FunctionKind kind, const Type* output_type,
    const std::vector<std::unique_ptr<AlgebraArg>>& arguments) {
  std::vector<std::unique_ptr<RE2>> regexp;
  for (int i = 1; i < arguments.size(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(regexp.emplace_back(),
                     GetLikePatternRegexp(*arguments[i]->value_expr()));
  }
  return std::unique_ptr<BuiltinScalarFunction>(
      new LikeAnyFunction(kind, output_type, std::move(regexp)));
}

absl::StatusOr<std::unique_ptr<BuiltinScalarFunction>>
BuiltinScalarFunction::CreateLikeAllFunction(
    FunctionKind kind, const Type* output_type,
    const std::vector<std::unique_ptr<AlgebraArg>>& arguments) {
  std::vector<std::unique_ptr<RE2>> regexp;
  for (int i = 1; i < arguments.size(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(regexp.emplace_back(),
                     GetLikePatternRegexp(*arguments[i]->value_expr()));
  }
  return std::unique_ptr<BuiltinScalarFunction>(
      new LikeAllFunction(kind, output_type, std::move(regexp)));
}

absl::StatusOr<std::unique_ptr<BuiltinScalarFunction>>
BuiltinScalarFunction::CreateLikeAnyAllArrayFunction(
    FunctionKind kind, const Type* output_type,
    const std::vector<std::unique_ptr<AlgebraArg>>& arguments) {
  std::vector<std::unique_ptr<RE2>> regexp;

  // The second argument to this function will be an array.
  // Theses values are unpacked in order to generate the regular expressions
  // needed for the LIKE expressions when they are evaluated. Non-constant
  // values will have their regular expressions generated at execution time.
  const ValueExpr* value_expression = arguments[1]->value_expr();
  if (value_expression->IsConstant() &&
      value_expression->output_type()->IsArray()) {
    const ConstExpr* pattern_list =
        static_cast<const ConstExpr*>(arguments[1]->value_expr());
    if (!pattern_list->value().is_null()) {
      for (int i = 0; i < pattern_list->value().num_elements(); ++i) {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ConstExpr> pattern,
                         ConstExpr::Create(pattern_list->value().element(i)));
        ZETASQL_ASSIGN_OR_RETURN(regexp.emplace_back(),
                         GetLikePatternRegexp(*pattern.get()));
      }
    }
  }

  if (kind == FunctionKind::kLikeAnyArray) {
    return std::make_unique<LikeAnyArrayFunction>(kind, output_type,
                                                  std::move(regexp));
  } else {
    return std::make_unique<LikeAllArrayFunction>(kind, output_type,
                                                  std::move(regexp));
  }
}

absl::StatusOr<std::unique_ptr<ScalarFunctionCallExpr>>
ArrayMinMaxFunction::CreateCall(
    FunctionKind kind, const LanguageOptions& language_options,
    const Type* output_type, std::vector<std::unique_ptr<AlgebraArg>> arguments,
    ResolvedFunctionCallBase::ErrorMode error_mode,
    CollatorList collator_list) {
  ZETASQL_RET_CHECK_LE(collator_list.size(), 1);

  std::unique_ptr<BuiltinScalarFunction> function =
      std::make_unique<ArrayMinMaxFunction>(kind, output_type,
                                            std::move(collator_list));
  return ScalarFunctionCallExpr::Create(std::move(function),
                                        std::move(arguments), error_mode);
}

absl::StatusOr<std::unique_ptr<ScalarFunctionCallExpr>>
ArrayFindFunctions::CreateCall(
    FunctionKind kind, const LanguageOptions& language_options,
    const Type* output_type, std::vector<std::unique_ptr<AlgebraArg>> arguments,
    ResolvedFunctionCallBase::ErrorMode error_mode,
    CollatorList collator_list) {
  // The collation information of the array element type of the first argument
  // needs to match that of the second argument.
  // We currently only support collation_list with only one element.
  ZETASQL_RET_CHECK_LE(collator_list.size(), 1);

  std::unique_ptr<BuiltinScalarFunction> function;
  if (arguments[1]->value_expr() != nullptr) {
    function = std::make_unique<ArrayFindFunctions>(kind, output_type,
                                                    std::move(collator_list));
  } else {
    ZETASQL_RET_CHECK(arguments[1]->inline_lambda_expr() != nullptr);
    function = std::make_unique<ArrayFindFunctions>(
        kind, output_type, std::move(collator_list),
        arguments[1]->mutable_inline_lambda_expr());
  }

  return ScalarFunctionCallExpr::Create(std::move(function),
                                        std::move(arguments), error_mode);
}

namespace {

absl::StatusOr<std::unique_ptr<const functions::RegExp>> CreateRegexp(
    const Value& arg) {
  ZETASQL_RET_CHECK(!arg.is_null());
  if (arg.type_kind() == TYPE_STRING) {
    return functions::MakeRegExpUtf8(arg.string_value());
  } else if (arg.type_kind() == TYPE_BYTES) {
    return functions::MakeRegExpBytes(arg.bytes_value());
  } else {
    return ::zetasql_base::UnimplementedErrorBuilder()
           << "Unsupported argument type for Regexp functions."
           << arg.type()->ShortTypeName(ProductMode::PRODUCT_INTERNAL);
  }
}

}  // namespace

absl::StatusOr<std::unique_ptr<BuiltinScalarFunction>>
BuiltinScalarFunction::CreateRegexpFunction(
    FunctionKind kind, const Type* output_type,
    const std::vector<std::unique_ptr<AlgebraArg>>& arguments) {
  std::vector<const Type*> input_types;
  input_types.reserve(arguments.size());
  for (const auto& expr : arguments) {
    input_types.push_back(expr->value_expr()->output_type());
  }
  // This may be null if the pattern is non-const or encounters an error.
  std::unique_ptr<const functions::RegExp> const_regexp;
  if (arguments[1]->value_expr()->IsConstant()) {
    const ConstExpr* pattern =
        static_cast<const ConstExpr*>(arguments[1]->value_expr());
    if (!pattern->value().is_null()) {
      // We ignore errors here, falling back to runtime error handling to ensure
      // SAFE function variants work correctly. This has the downside that if
      // no rows are generated, this will not generate an error (which might
      // otherwise be helpful).
      if (auto tmp = CreateRegexp(pattern->value()); tmp.ok()) {
        const_regexp = std::move(tmp).value();
      }
    }
  }

  return std::make_unique<RegexpFunction>(std::move(const_regexp), kind,
                                          output_type);
}

bool BuiltinScalarFunction::HasNulls(absl::Span<const Value> args) {
  for (const auto& value : args) {
    if (value.is_null()) return true;
  }
  return false;
}
// REQUIRES: all inputs are non-null.
static Value FindNaN(absl::Span<const Value> args) {
  for (const auto& value : args) {
    switch (value.type_kind()) {
      case TYPE_DOUBLE:
        if (std::isnan(value.double_value())) {
          return value;
        }
        break;
      case TYPE_FLOAT:
        if (std::isnan(value.float_value())) {
          return value;
        }
        break;
      default:
        break;
    }
  }
  return Value::Invalid();  // not found
}

static bool HasNaNs(absl::Span<const Value> args) {
  return FindNaN(args).is_valid();
}

bool LeastFunction::Eval(absl::Span<const TupleData* const> params,
                         absl::Span<const Value> args,
                         EvaluationContext* context, Value* result,
                         absl::Status* status) const {
  ABSL_DCHECK_GT(args.size(), 0);
  for (int i = 0; i < args.size(); i++) {
    if (*status = ValidateMicrosPrecision(args[i], context); !status->ok()) {
      return false;
    }
  }
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  if (HasNaNs(args)) {
    *result = FindNaN(args);
    return true;
  }
  *result = args[0];
  for (int i = 1; i < args.size(); i++) {
    if (args[i].LessThan(*result)) {
      *result = args[i];
    }
  }
  return true;
}

bool GreatestFunction::Eval(absl::Span<const TupleData* const> params,
                            absl::Span<const Value> args,
                            EvaluationContext* context, Value* result,
                            absl::Status* status) const {
  ABSL_DCHECK_GT(args.size(), 0);
  for (int i = 0; i < args.size(); i++) {
    if (*status = ValidateMicrosPrecision(args[i], context); !status->ok()) {
      return false;
    }
  }
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  if (HasNaNs(args)) {
    *result = FindNaN(args);
    return true;
  }
  *result = args[0];
  for (int i = 1; i < args.size(); i++) {
    if (result->LessThan(args[i])) {
      *result = args[i];
    }
  }
  return true;
}

absl::StatusOr<Value> ToCodePointsFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_EQ(args.size(), 1);
  if (args[0].is_null()) return Value::Null(output_type());

  std::vector<int64_t> codepoints;
  absl::Status status;
  switch (args[0].type_kind()) {
    case TYPE_BYTES:
      if (!functions::BytesToCodePoints(args[0].bytes_value(), &codepoints,
                                        &status)) {
        return status;
      }
      break;
    case TYPE_STRING:
      if (!functions::StringToCodePoints(args[0].string_value(), &codepoints,
                                         &status)) {
        return status;
      }
      break;
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported argument type for to_code_points.";
  }
  return values::Int64Array(codepoints);
}

absl::StatusOr<Value> CodePointsToFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_EQ(args.size(), 1);
  if (args[0].is_null()) return Value::Null(output_type());

  MaybeSetNonDeterministicArrayOutput(args[0], context);

  std::vector<int64_t> codepoints;
  codepoints.reserve(args[0].elements().size());
  for (const Value& element : args[0].elements()) {
    if (element.is_null()) {
      return Value::Null(output_type());
    }
    codepoints.push_back(element.int64_value());
  }
  std::string out;
  absl::Status status;
  switch (output_type()->kind()) {
    case TYPE_BYTES:
      if (!functions::CodePointsToBytes(codepoints, &out, &status)) {
        return status;
      }
      return values::Bytes(out);
    case TYPE_STRING:
      if (!functions::CodePointsToString(codepoints, &out, &status)) {
        return status;
      }
      return values::String(out);
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported argument type for code_points_to_string.";
  }
}

absl::StatusOr<Value> FormatFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_GE(args.size(), 1);
  if (args[0].is_null()) return Value::NullString();
  ABSL_DCHECK(args[0].type()->IsString());

  std::string output;
  bool is_null;
  absl::Span<const Value> values(args);
  values.remove_prefix(1);
  ZETASQL_RETURN_IF_ERROR(functions::StringFormatUtf8(
      args[0].string_value(), values,
      context->GetLanguageOptions().product_mode(), &output,
      &is_null, true));
  Value value;
  if (is_null) {
    value = Value::NullString();
  } else {
    if (context->IsDeterministicOutput()) {
      for (const Value& value : values) {
        if (HasFloatingPoint(value.type())) {
          context->SetNonDeterministicOutput();
          break;
        }
      }
    }
    value = Value::String(output);
  }
  if (value.physical_byte_size() > context->options().max_value_byte_size) {
    return zetasql_base::OutOfRangeErrorBuilder()
           << "Formatted values are limited to "
           << context->options().max_value_byte_size << " bytes";
  }
  return value;
}

absl::StatusOr<Value> GenerateArrayFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_GE(args.size(), 2);
  ABSL_DCHECK_LE(args.size(), 4);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }

  const bool has_step = args.size() >= 3;
  std::vector<Value> range_values;
  switch (args[0].type_kind()) {
    case TYPE_INT64:
      ZETASQL_RETURN_IF_ERROR(GenerateArray(
          args[0].int64_value(), args[1].int64_value(),
          has_step ? args[2].int64_value() : 1, context, &range_values));
      break;
    case TYPE_UINT64:
      ZETASQL_RETURN_IF_ERROR(GenerateArray(
          args[0].uint64_value(), args[1].uint64_value(),
          has_step ? args[2].uint64_value() : 1, context, &range_values));
      break;
    case TYPE_NUMERIC:
      ZETASQL_RETURN_IF_ERROR(
          GenerateArray(args[0].numeric_value(), args[1].numeric_value(),
                        has_step ? args[2].numeric_value() : NumericValue(1LL),
                        context, &range_values));
      break;
    case TYPE_BIGNUMERIC:
      ZETASQL_RETURN_IF_ERROR(GenerateArray(
          args[0].bignumeric_value(), args[1].bignumeric_value(),
          has_step ? args[2].bignumeric_value() : BigNumericValue(1), context,
          &range_values));
      break;
    case TYPE_DOUBLE:
      ZETASQL_RETURN_IF_ERROR(GenerateArray(
          args[0].double_value(), args[1].double_value(),
          has_step ? args[2].double_value() : 1.0, context, &range_values));
      break;
    case TYPE_DATE: {
      int64_t step = 1;
      functions::DateTimestampPart step_unit = functions::DAY;
      if (has_step) {
        step_unit =
            static_cast<functions::DateTimestampPart>(args[3].enum_value());
        step = args[2].int64_value();
      }
      ZETASQL_RETURN_IF_ERROR(GenerateDateArray(args[0].date_value(),
                                        args[1].date_value(), step, step_unit,
                                        context, &range_values));
      break;
    }
    case TYPE_TIMESTAMP: {
      // The resolver requires a step for GENERATE_TIMESTAMP_ARRAY.
      ZETASQL_RET_CHECK(has_step);
      const int64_t step = args[2].int64_value();
      const functions::DateTimestampPart step_unit =
          static_cast<functions::DateTimestampPart>(args[3].enum_value());
      ZETASQL_RETURN_IF_ERROR(GenerateTimestampArray(args[0].ToTime(), args[1].ToTime(),
                                             step, step_unit, context,
                                             &range_values));
      break;
    }
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported argument type for generate_array.";
  }
  Value array_value = Value::Array(output_type()->AsArray(), range_values);
  if (array_value.physical_byte_size() >
      context->options().max_value_byte_size) {
    return MakeMaxArrayValueByteSizeExceededError(
        context->options().max_value_byte_size,
        zetasql_base::SourceLocation::current());
  }
  return array_value;
}

namespace {

absl::Status CheckArrayElementInRangeBucket(absl::Span<const Value> elements,
                                            size_t idx) {
  const Value& value = elements[idx];
  if (value.is_null()) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "Elements in input array to RANGE_BUCKET cannot be null. Null "
           << "element found at position " << idx + 1;
  }

  if (IsNaN(value)) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "Elements in input array to RANGE_BUCKET cannot be NaN. NaN "
           << "element found at position " << idx + 1;
  }

  const size_t next_idx = idx + 1;
  if (ABSL_PREDICT_TRUE(next_idx < elements.size())) {
    if (elements[next_idx].LessThan(value)) {
      return ::zetasql_base::OutOfRangeErrorBuilder()
             << "Elements in input array to RANGE_BUCKET must be in ascending "
             << "order. Nonconforming elements found at position " << idx + 1
             << " and " << idx + 2;
    }
  }

  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<Value> RangeBucketFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (HasNulls(args) || IsNaN(args[0])) {
    return Value::NullInt64();
  }

  MaybeSetNonDeterministicArrayOutput(args[1], context);

  const Value& value = args[0];
  const Value& array = args[1];
  const auto& elements = array.elements();

  for (size_t idx = 0; idx < elements.size(); idx++) {
    ZETASQL_RETURN_IF_ERROR(CheckArrayElementInRangeBucket(elements, idx));
  }

  auto it = std::upper_bound(elements.begin(), elements.end(), value,
                             [](const Value& value, const Value& element) {
                               return value.SqlLessThan(element).bool_value();
                             });

  return zetasql::values::Int64(it - elements.begin());
}

absl::Status ArithmeticFunction::AddIntervalHelper(
    const Value& arg, const IntervalValue& interval, Value* result,
    EvaluationContext* context) const {
  switch (arg.type()->kind()) {
    case TYPE_DATE: {
      DatetimeValue datetime;
      ZETASQL_RETURN_IF_ERROR(
          functions::AddDate(arg.date_value(), interval, &datetime));
      *result = Value::Datetime(datetime);
      break;
    }
    case TYPE_TIMESTAMP: {
      absl::Time timestamp;
      ZETASQL_RETURN_IF_ERROR(functions::AddTimestamp(
          arg.ToTime(), context->GetDefaultTimeZone(), interval, &timestamp));
      *result = Value::Timestamp(timestamp);
      break;
    }
    case TYPE_DATETIME: {
      DatetimeValue datetime;
      ZETASQL_RETURN_IF_ERROR(
          functions::AddDatetime(arg.datetime_value(), interval, &datetime));
      *result = Value::Datetime(datetime);
      break;
    }
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported arithmetic function: " << debug_name() << "("
             << arg.type()->TypeName(PRODUCT_EXTERNAL) << ", INTERVAL)";
  }
  return absl::OkStatus();
}

bool ArithmeticFunction::Eval(absl::Span<const TupleData* const> params,
                              absl::Span<const Value> args,
                              EvaluationContext* context, Value* result,
                              absl::Status* status) const {
  if (kind() == FunctionKind::kUnaryMinus ||
      kind() == FunctionKind::kSafeNegate) {
    ABSL_DCHECK_EQ(1, args.size());
  } else {
    ABSL_DCHECK_EQ(2, args.size());
  }
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }

  switch (FCT(kind(), args[0].type_kind())) {
    case FCT(FunctionKind::kAdd, TYPE_INT64):
      if (args[1].type()->IsDate()) {
        int32_t date;
        *status =
            functions::AddDate(args[1].date_value(), zetasql::functions::DAY,
                               args[0].int64_value(), &date);
        if (status->ok()) {
          *result = Value::Date(date);
        }
        return status->ok();
      }
      return InvokeBinary<int64_t>(&functions::Add<int64_t>, args, result,
                                   status);
    case FCT(FunctionKind::kAdd, TYPE_UINT64):
      return InvokeBinary<uint64_t>(&functions::Add<uint64_t>, args, result,
                                    status);
    case FCT(FunctionKind::kAdd, TYPE_DOUBLE):
      return InvokeBinary<double>(&functions::Add<double>, args, result,
                                  status);
    case FCT(FunctionKind::kAdd, TYPE_NUMERIC):
      return InvokeBinary<NumericValue>(&functions::Add<NumericValue>, args,
                                        result, status);
    case FCT(FunctionKind::kAdd, TYPE_BIGNUMERIC):
      return InvokeBinary<BigNumericValue>(&functions::Add<BigNumericValue>,
                                           args, result, status);

    case FCT(FunctionKind::kAdd, TYPE_DATE): {
      switch (args[1].type()->kind()) {
        case TYPE_INT64: {
          int32_t date;
          *status = functions::AddDate(args[0].date_value(),
                                       zetasql::functions::DAY,
                                       args[1].int64_value(), &date);
          if (status->ok()) {
            *result = Value::Date(date);
          }
          break;
        }
        case TYPE_INTERVAL:
          *status = AddIntervalHelper(args[0], args[1].interval_value(), result,
                                      context);
          break;
        default:
          *status = ::zetasql_base::UnimplementedErrorBuilder()
                    << "Unsupported arithmetic function: " << debug_name();
      }
      return status->ok();
    }
    case FCT(FunctionKind::kAdd, TYPE_TIMESTAMP):
    case FCT(FunctionKind::kAdd, TYPE_DATETIME):
      *status =
          AddIntervalHelper(args[0], args[1].interval_value(), result, context);
      return status->ok();
    case FCT(FunctionKind::kAdd, TYPE_INTERVAL): {
      switch (args[1].type()->kind()) {
        case TYPE_INTERVAL: {
          auto status_interval =
              args[0].interval_value() + args[1].interval_value();
          if (status_interval.ok()) {
            *result = Value::Interval(*status_interval);
          } else {
            *status = status_interval.status();
          }
          break;
        }
        default:
          *status = AddIntervalHelper(args[1], args[0].interval_value(), result,
                                      context);
          break;
      }
      return status->ok();
    }
    case FCT(FunctionKind::kSubtract, TYPE_INT64):
      return InvokeBinary<int64_t>(&functions::Subtract<int64_t>, args, result,
                                   status);
    case FCT(FunctionKind::kSubtract, TYPE_UINT64):
      return InvokeBinary<int64_t, uint64_t, uint64_t>(
          &functions::Subtract<uint64_t, int64_t>, args, result, status);
    case FCT(FunctionKind::kSubtract, TYPE_DOUBLE):
      return InvokeBinary<double>(&functions::Subtract<double>, args, result,
                                  status);
    case FCT(FunctionKind::kSubtract, TYPE_NUMERIC):
      return InvokeBinary<NumericValue>(&functions::Subtract<NumericValue>,
                                        args, result, status);
    case FCT(FunctionKind::kSubtract, TYPE_BIGNUMERIC):
      return InvokeBinary<BigNumericValue>(
          &functions::Subtract<BigNumericValue>, args, result, status);

    case FCT(FunctionKind::kSubtract, TYPE_DATE): {
      switch (args[1].type()->kind()) {
        case TYPE_INT64: {
          int32_t date;
          *status = functions::SubDate(args[0].date_value(),
                                       zetasql::functions::DAY,
                                       args[1].int64_value(), &date);
          if (status->ok()) {
            *result = Value::Date(date);
          }
          break;
        }
        case TYPE_DATE: {
          auto status_interval = functions::IntervalDiffDates(
              args[0].date_value(), args[1].date_value());
          *status = status_interval.status();
          if (status->ok()) {
            *result = Value::Interval(*status_interval);
          }
          break;
        }
        case TYPE_INTERVAL:
          *status = AddIntervalHelper(args[0], -args[1].interval_value(),
                                      result, context);
          break;
        default:
          *status = ::zetasql_base::UnimplementedErrorBuilder()
                    << "Unsupported arithmetic function: " << debug_name();
      }
      return status->ok();
    }
    case FCT(FunctionKind::kSubtract, TYPE_TIMESTAMP): {
      switch (args[1].type()->kind()) {
        case TYPE_TIMESTAMP: {
          auto status_interval = functions::IntervalDiffTimestamps(
              args[0].ToTime(), args[1].ToTime());
          *status = status_interval.status();
          if (status->ok()) {
            *result = Value::Interval(*status_interval);
          }
          break;
        }
        case TYPE_INTERVAL:
          *status = AddIntervalHelper(args[0], -args[1].interval_value(),
                                      result, context);
          break;
        default:
          *status = ::zetasql_base::UnimplementedErrorBuilder()
                    << "Unsupported arithmetic function: " << debug_name();
      }
      return status->ok();
    }
    case FCT(FunctionKind::kSubtract, TYPE_DATETIME): {
      switch (args[1].type()->kind()) {
        case TYPE_DATETIME: {
          auto status_interval = functions::IntervalDiffDatetimes(
              args[0].datetime_value(), args[1].datetime_value());
          *status = status_interval.status();
          if (status->ok()) {
            *result = Value::Interval(*status_interval);
          }
          break;
        }
        case TYPE_INTERVAL:
          *status = AddIntervalHelper(args[0], -args[1].interval_value(),
                                      result, context);
          break;
        default:
          *status = ::zetasql_base::UnimplementedErrorBuilder()
                    << "Unsupported arithmetic function: " << debug_name();
      }
      return status->ok();
    }
    case FCT(FunctionKind::kSubtract, TYPE_TIME): {
      auto status_interval = functions::IntervalDiffTimes(args[0].time_value(),
                                                          args[1].time_value());
      *status = status_interval.status();
      if (status->ok()) {
        *result = Value::Interval(*status_interval);
      }
      return status->ok();
    }
    case FCT(FunctionKind::kSubtract, TYPE_INTERVAL): {
      auto status_interval =
          args[0].interval_value() - args[1].interval_value();
      if (status_interval.ok()) {
        *result = Value::Interval(*status_interval);
        return true;
      } else {
        *status = status_interval.status();
        return false;
      }
    }

    case FCT(FunctionKind::kMultiply, TYPE_INT64):
      if (args[1].type()->IsInt64()) {
        return InvokeBinary<int64_t>(&functions::Multiply<int64_t>, args,
                                     result, status);
      } else if (args[1].type()->IsInterval()) {
        auto status_interval = args[1].interval_value() * args[0].int64_value();
        if (status_interval.ok()) {
          *result = Value::Interval(*status_interval);
        } else {
          *status = status_interval.status();
        }
      }
      return status->ok();
    case FCT(FunctionKind::kMultiply, TYPE_UINT64):
      return InvokeBinary<uint64_t>(&functions::Multiply<uint64_t>, args,
                                    result, status);
    case FCT(FunctionKind::kMultiply, TYPE_DOUBLE):
      return InvokeBinary<double>(&functions::Multiply<double>, args, result,
                                  status);
    case FCT(FunctionKind::kMultiply, TYPE_NUMERIC):
      return InvokeBinary<NumericValue>(&functions::Multiply<NumericValue>,
                                        args, result, status);
    case FCT(FunctionKind::kMultiply, TYPE_BIGNUMERIC):
      return InvokeBinary<BigNumericValue>(
          &functions::Multiply<BigNumericValue>, args, result, status);

    case FCT(FunctionKind::kMultiply, TYPE_INTERVAL): {
      auto status_interval = args[0].interval_value() * args[1].int64_value();
      if (status_interval.ok()) {
        *result = Value::Interval(*status_interval);
      } else {
        *status = status_interval.status();
      }
      return status->ok();
    }

    case FCT(FunctionKind::kDivide, TYPE_DOUBLE):
      return InvokeBinary<double>(&functions::Divide<double>, args, result,
                                  status);
    case FCT(FunctionKind::kDivide, TYPE_NUMERIC):
      return InvokeBinary<NumericValue>(&functions::Divide<NumericValue>, args,
                                        result, status);
    case FCT(FunctionKind::kDivide, TYPE_BIGNUMERIC):
      return InvokeBinary<BigNumericValue>(&functions::Divide<BigNumericValue>,
                                           args, result, status);
    case FCT(FunctionKind::kDivide, TYPE_INTERVAL): {
      // Sanitize the nanos second part if in micro seconds mode.
      bool round_to_micros = GetTimestampScale(context->GetLanguageOptions()) ==
                             functions::TimestampScale::kMicroseconds;
      auto status_interval = args[0].interval_value().Divide(
          args[1].int64_value(), round_to_micros);
      if (!status_interval.ok()) {
        *status = status_interval.status();
        return false;
      }
      *result = Value::Interval(*status_interval);
      return true;
    }

    case FCT(FunctionKind::kDiv, TYPE_INT64):
      return InvokeBinary<int64_t>(&functions::Divide<int64_t>, args, result,
                                   status);
    case FCT(FunctionKind::kDiv, TYPE_UINT64):
      return InvokeBinary<uint64_t>(&functions::Divide<uint64_t>, args, result,
                                    status);
    case FCT(FunctionKind::kDiv, TYPE_NUMERIC):
      return InvokeBinary<NumericValue>(
          &functions::DivideToIntegralValue<NumericValue>, args, result,
          status);
    case FCT(FunctionKind::kDiv, TYPE_BIGNUMERIC):
      return InvokeBinary<BigNumericValue>(
          &functions::DivideToIntegralValue<BigNumericValue>, args, result,
          status);

    case FCT(FunctionKind::kSafeAdd, TYPE_INT64):
      return SafeInvokeBinary<int64_t>(&functions::Add<int64_t>, args, result,
                                       status);
    case FCT(FunctionKind::kSafeAdd, TYPE_UINT64):
      return SafeInvokeBinary<uint64_t>(&functions::Add<uint64_t>, args, result,
                                        status);
    case FCT(FunctionKind::kSafeAdd, TYPE_DOUBLE):
      return SafeInvokeBinary<double>(&functions::Add<double>, args, result,
                                      status);
    case FCT(FunctionKind::kSafeAdd, TYPE_NUMERIC):
      return SafeInvokeBinary<NumericValue>(&functions::Add<NumericValue>, args,
                                            result, status);
    case FCT(FunctionKind::kSafeAdd, TYPE_BIGNUMERIC):
      return SafeInvokeBinary<BigNumericValue>(&functions::Add<BigNumericValue>,
                                               args, result, status);

    case FCT(FunctionKind::kSafeSubtract, TYPE_INT64):
      return SafeInvokeBinary<int64_t>(&functions::Subtract<int64_t>, args,
                                       result, status);
    case FCT(FunctionKind::kSafeSubtract, TYPE_UINT64):
      return SafeInvokeBinary<int64_t, uint64_t, uint64_t>(
          &functions::Subtract<uint64_t, int64_t>, args, result, status);
    case FCT(FunctionKind::kSafeSubtract, TYPE_DOUBLE):
      return SafeInvokeBinary<double>(&functions::Subtract<double>, args,
                                      result, status);
    case FCT(FunctionKind::kSafeSubtract, TYPE_NUMERIC):
      return SafeInvokeBinary<NumericValue>(&functions::Subtract<NumericValue>,
                                            args, result, status);
    case FCT(FunctionKind::kSafeSubtract, TYPE_BIGNUMERIC):
      return SafeInvokeBinary<BigNumericValue>(
          &functions::Subtract<BigNumericValue>, args, result, status);

    case FCT(FunctionKind::kSafeMultiply, TYPE_INT64):
      return SafeInvokeBinary<int64_t>(&functions::Multiply<int64_t>, args,
                                       result, status);
    case FCT(FunctionKind::kSafeMultiply, TYPE_UINT64):
      return SafeInvokeBinary<uint64_t>(&functions::Multiply<uint64_t>, args,
                                        result, status);
    case FCT(FunctionKind::kSafeMultiply, TYPE_DOUBLE):
      return SafeInvokeBinary<double>(&functions::Multiply<double>, args,
                                      result, status);
    case FCT(FunctionKind::kSafeMultiply, TYPE_NUMERIC):
      return SafeInvokeBinary<NumericValue>(&functions::Multiply<NumericValue>,
                                            args, result, status);
    case FCT(FunctionKind::kSafeMultiply, TYPE_BIGNUMERIC):
      return SafeInvokeBinary<BigNumericValue>(
          &functions::Multiply<BigNumericValue>, args, result, status);

    case FCT(FunctionKind::kSafeDivide, TYPE_DOUBLE):
      return SafeInvokeBinary<double>(&functions::Divide<double>, args, result,
                                      status);
    case FCT(FunctionKind::kSafeDivide, TYPE_NUMERIC):
      return SafeInvokeBinary<NumericValue>(&functions::Divide<NumericValue>,
                                            args, result, status);
    case FCT(FunctionKind::kSafeDivide, TYPE_BIGNUMERIC):
      return SafeInvokeBinary<BigNumericValue>(
          &functions::Divide<BigNumericValue>, args, result, status);

    case FCT(FunctionKind::kMod, TYPE_INT64):
      return InvokeBinary<int64_t>(&functions::Modulo<int64_t>, args, result,
                                   status);
    case FCT(FunctionKind::kMod, TYPE_UINT64):
      return InvokeBinary<uint64_t>(&functions::Modulo<uint64_t>, args, result,
                                    status);
    case FCT(FunctionKind::kMod, TYPE_NUMERIC):
      return InvokeBinary<NumericValue>(&functions::Modulo<NumericValue>, args,
                                        result, status);
    case FCT(FunctionKind::kMod, TYPE_BIGNUMERIC):
      return InvokeBinary<BigNumericValue>(&functions::Modulo<BigNumericValue>,
                                           args, result, status);

    case FCT(FunctionKind::kUnaryMinus, TYPE_INT64):
      return InvokeUnary<int64_t>(&functions::UnaryMinus<int64_t, int64_t>,
                                  args, result, status);
    case FCT(FunctionKind::kUnaryMinus, TYPE_INT32):
      return InvokeUnary<int32_t>(&functions::UnaryMinus<int32_t, int32_t>,
                                  args, result, status);
    case FCT(FunctionKind::kUnaryMinus, TYPE_FLOAT):
      return InvokeUnary<float>(&functions::UnaryMinus<float, float>, args,
                                result, status);
    case FCT(FunctionKind::kUnaryMinus, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::UnaryMinus<double, double>, args,
                                 result, status);
    case FCT(FunctionKind::kUnaryMinus, TYPE_NUMERIC):
      return InvokeUnary<NumericValue>(&functions::UnaryMinus<NumericValue>,
                                       args, result, status);
    case FCT(FunctionKind::kUnaryMinus, TYPE_BIGNUMERIC):
      return InvokeUnary<BigNumericValue>(
          &functions::UnaryMinus<BigNumericValue>, args, result, status);
    case FCT(FunctionKind::kUnaryMinus, TYPE_INTERVAL):
      return InvokeUnary<IntervalValue>(&functions::IntervalUnaryMinus, args,
                                        result, status);
    case FCT(FunctionKind::kSafeNegate, TYPE_INT64):
      return SafeInvokeUnary<int64_t>(&functions::UnaryMinus<int64_t, int64_t>,
                                      args, result, status);
    case FCT(FunctionKind::kSafeNegate, TYPE_INT32):
      return SafeInvokeUnary<int32_t>(&functions::UnaryMinus<int32_t, int32_t>,
                                      args, result, status);
    case FCT(FunctionKind::kSafeNegate, TYPE_FLOAT):
      return SafeInvokeUnary<float>(&functions::UnaryMinus<float, float>, args,
                                    result, status);
    case FCT(FunctionKind::kSafeNegate, TYPE_DOUBLE):
      return SafeInvokeUnary<double>(&functions::UnaryMinus<double, double>,
                                     args, result, status);
    case FCT(FunctionKind::kSafeNegate, TYPE_NUMERIC):
      return SafeInvokeUnary<NumericValue>(&functions::UnaryMinus<NumericValue>,
                                           args, result, status);
    case FCT(FunctionKind::kSafeNegate, TYPE_BIGNUMERIC):
      return SafeInvokeUnary<BigNumericValue>(
          &functions::UnaryMinus<BigNumericValue>, args, result, status);
  }
  *status = ::zetasql_base::UnimplementedErrorBuilder()
            << "Unsupported arithmetic function: " << debug_name();
  return false;
}

static bool IsDistinctFromInt64UInt64(Value int64_value, Value uint64_value) {
  if (int64_value.is_null() || uint64_value.is_null()) {
    return int64_value.is_null() != uint64_value.is_null();
  }

  if (int64_value.int64_value() < 0) {
    return true;  // int64_t value out of range of uint64_t; must be distinct
  }
  return uint64_value.uint64_value() !=
         static_cast<uint64_t>(int64_value.int64_value());
}

static bool IsDistinctFrom(const Value& x, const Value& y) {
  // Special case to handle INT64/UINT64 signatures of $is_distinct_from and
  // $is_not_distinct_from().
  //
  // The reference implementation's function registry does not allow different
  // overloads of the same function name to get different function kinds, so
  // we get here regardless of overload.
  if (x.type()->IsInt64() && y.type()->IsUint64()) {
    return IsDistinctFromInt64UInt64(x, y);
  } else if (x.type()->IsUint64() && y.type()->IsInt64()) {
    return IsDistinctFromInt64UInt64(y, x);
  }

  // The function signatures of $is_distinct_from/$is_not_distinct_from
  // guarantees that, in all other cases, x and y are the same type.
  ABSL_DCHECK(x.type()->Equals(y.type()));
  return !x.Equals(y);
}

static bool IsUncertianArrayEquality(const Value& x, const Value& y) {
  bool involves_undefined_order =
      InternalValue::ContainsArrayWithUncertainOrder(x) ||
      InternalValue::ContainsArrayWithUncertainOrder(y);
  if (!involves_undefined_order) {
    return false;
  }
  // Don't try to do anything too smart with the nested values. Things get very
  // complex and expensive very fast, and complexity and expense is not great
  // in an operator as fundamental as equals, even in the reference
  // implementation.
  const Type* type = x.type();
  bool is_nested_type =
      type->IsStruct() ||
      (type->IsArray() && type->AsArray()->element_type()->IsStruct());
  if (is_nested_type) {
    return true;
  }
  if (!type->IsArray() || x.is_null() || y.is_null()) {
    return false;
  }
  // For simple array types, we can be certain of equality operations when
  // arrays are different lengths, and this is cheap to test.
  return x.num_elements() == y.num_elements();
  // Possibly if we want to narrow this case further in the future, we could
  // scan the elements of 'x' looking for cases where an equal element is not
  // present in 'y'. In that case we could be certain the arrays are not equal.
}

bool ComparisonFunction::Eval(absl::Span<const TupleData* const> params,
                              absl::Span<const Value> args,
                              EvaluationContext* context, Value* result,
                              absl::Status* status) const {
  ABSL_DCHECK_EQ(2, args.size());

  const Value& x = args[0];
  const Value& y = args[1];

  status->Update(ValidateMicrosPrecision(x, context));
  status->Update(ValidateMicrosPrecision(y, context));
  if (!status->ok()) {
    return false;
  }

  if (kind() == FunctionKind::kEqual || kind() == FunctionKind::kIsDistinct ||
      kind() == FunctionKind::kIsNotDistinct) {
    if (kind() == FunctionKind::kEqual) {
      *result = x.SqlEquals(y);
      if (!result->is_valid()) {
        *status = ::zetasql_base::UnimplementedErrorBuilder()
                  << "Unsupported comparison function: " << debug_name()
                  << " with inputs " << TypeKind_Name(x.type_kind()) << " and "
                  << TypeKind_Name(y.type_kind());
        return false;
      }
    } else if (kind() == FunctionKind::kIsDistinct) {
      *result = Value::Bool(IsDistinctFrom(x, y));
    } else if (kind() == FunctionKind::kIsNotDistinct) {
      *result = Value::Bool(!IsDistinctFrom(x, y));
    }
    if (context->IsDeterministicOutput() && IsUncertianArrayEquality(x, y)) {
      context->SetNonDeterministicOutput();
    }
    return true;
  }

  if (context->IsDeterministicOutput() &&
      (InternalValue::ContainsArrayWithUncertainOrder(x) ||
       InternalValue::ContainsArrayWithUncertainOrder(y))) {
    // For inequality, a prefix of the arrays could determine the result, and we
    // don't know which elements are the prefix. We could do better in the
    // future if we need to narrow this by checking if the 'least' value in 'x'
    // is greater than the 'greatest' value in 'y'. In that case there is no
    // possible way to order each array so that x is less than or equal to y.
    context->SetNonDeterministicOutput();
  }

  if (kind() == FunctionKind::kLess) {
    *result = x.SqlLessThan(y);
    if (!result->is_valid()) {
      *status = ::zetasql_base::UnimplementedErrorBuilder()
                << "Unsupported comparison function: " << debug_name()
                << " with inputs " << TypeKind_Name(x.type_kind()) << " and "
                << TypeKind_Name(y.type_kind());
      return false;
    }
    return true;
  }

  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }

  switch (FCT2(kind(), x.type_kind(), y.type_kind())) {
    case FCT2(FunctionKind::kLessOrEqual, TYPE_INT32, TYPE_INT32):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_INT64, TYPE_INT64):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_UINT32, TYPE_UINT32):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_UINT64, TYPE_UINT64):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_BOOL, TYPE_BOOL):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_STRING, TYPE_STRING):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_BYTES, TYPE_BYTES):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_DATE, TYPE_DATE):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_TIMESTAMP, TYPE_TIMESTAMP):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_TIME, TYPE_TIME):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_DATETIME, TYPE_DATETIME):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_INTERVAL, TYPE_INTERVAL):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_ENUM, TYPE_ENUM):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_NUMERIC, TYPE_NUMERIC):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_RANGE, TYPE_RANGE):
      *result = Value::Bool(x.LessThan(y) || x.Equals(y));
      return true;

    case FCT2(FunctionKind::kLessOrEqual, TYPE_FLOAT, TYPE_FLOAT):
      *result =
          Value::Bool(x.float_value() <= y.float_value());  // false if NaN
      return true;
    case FCT2(FunctionKind::kLessOrEqual, TYPE_DOUBLE, TYPE_DOUBLE):
      *result =
          Value::Bool(x.double_value() <= y.double_value());  // false if NaN
      return true;
    case FCT2(FunctionKind::kLessOrEqual, TYPE_INT64, TYPE_UINT64):
      *result = Value::Bool(
          functions::Compare64(x.int64_value(), y.uint64_value()) <= 0);
      return true;
    case FCT2(FunctionKind::kLessOrEqual, TYPE_UINT64, TYPE_INT64):
      *result = Value::Bool(
          functions::Compare64(y.int64_value(), x.uint64_value()) >= 0);
      return true;

    case FCT2(FunctionKind::kLessOrEqual, TYPE_ARRAY, TYPE_ARRAY): {
      const int shorter_array_size =
          (x.num_elements() < y.num_elements() ? x.num_elements()
                                               : y.num_elements());
      ComparisonFunction compare_less(FunctionKind::kLess, types::BoolType());
      // Compare array elements one by one.  If we find that the first array
      // is less or greater than the second, then ignore the remaining
      // elements and return the result.  If we find a NULL element,
      // then the comparison results in NULL.
      for (int i = 0; i < shorter_array_size; ++i) {
        // Evaluate if the element of the first array is less than the element
        // of the second array.
        if (!compare_less.Eval(params, {x.element(i), y.element(i)}, context,
                               result, status)) {
          return false;
        }
        if (result->Equals(values::True())) {
          // Returns the result early if one of the elements compared less.
          return true;
        }
        // If the comparison returned NULL, then return NULL.
        if (result->is_null()) {
          return true;
        }
        // Evaluate if the element of the second array is less than the element
        // of the first array.
        if (!compare_less.Eval(params, {y.element(i), x.element(i)}, context,
                               result, status)) {
          return false;
        }
        if (result->Equals(values::True())) {
          // Returns the result early if the second array element is less
          // than the first array element.
          *result = values::False();
          return true;
        }
        // Otherwise the array elements are not less and not greater, but may
        // not be 'equal' (if one of the elements is NaN, which always
        // compares as false).
        if (IsNaN(x.element(i)) || IsNaN(y.element(i))) {
          *result = values::False();
          return true;
        }
      }

      // If we got here, then the first <shorter_array_size> elements are
      // all equal.  So if the first array is equal in length or shorter
      // than the second array, then it is LessOrEqual.
      if (x.num_elements() <= y.num_elements()) {
        *result = values::True();
        return true;
      }
      *result = values::False();
      return true;
    }
  }
  *status = ::zetasql_base::UnimplementedErrorBuilder()
            << "Unsupported comparison function: " << debug_name()
            << " with inputs " << TypeKind_Name(x.type_kind()) << " and "
            << TypeKind_Name(y.type_kind());
  return false;
}

bool ExistsFunction::Eval(absl::Span<const TupleData* const> params,
                          absl::Span<const Value> args,
                          EvaluationContext* context, Value* result,
                          absl::Status* status) const {
  ABSL_DCHECK_EQ(1, args.size());
  *result = Value::Bool(!args[0].empty());
  return true;
}

bool ArrayConcatFunction::Eval(absl::Span<const TupleData* const> params,
                               absl::Span<const Value> args,
                               EvaluationContext* context, Value* result,
                               absl::Status* status) const {
  ABSL_DCHECK_LE(1, args.size());
  if (HasNulls(args)) {
    Value tracked_value = Value::Null(output_type());
    if (tracked_value.physical_byte_size() >
        context->options().max_value_byte_size) {
      *status = MakeMaxArrayValueByteSizeExceededError(
          context->options().max_value_byte_size,
          zetasql_base::SourceLocation::current());
      return false;
    }
    *result = std::move(tracked_value);
    return true;
  }
  int64_t num_values = 0;
  int64_t bytes_so_far = 0;
  for (const Value& input_array : args) {
    bytes_so_far += input_array.physical_byte_size();
    if (bytes_so_far > context->options().max_value_byte_size) {
      *status = MakeMaxArrayValueByteSizeExceededError(
          context->options().max_value_byte_size,
          zetasql_base::SourceLocation::current());
      return false;
    }
    num_values += input_array.num_elements();
  }
  std::vector<Value> values;
  values.reserve(num_values);
  auto is_ordered = InternalValue::kPreservesOrder;
  for (const Value& input_array : args) {
    if (InternalValue::GetOrderKind(input_array) ==
        InternalValue::kIgnoresOrder) {
      is_ordered = InternalValue::kIgnoresOrder;
    }
    for (int i = 0; i < input_array.num_elements(); ++i) {
      values.push_back(input_array.element(i));
    }
  }
  *result = InternalValue::ArrayNotChecked(output_type()->AsArray(), is_ordered,
                                           std::move(values));
  return true;
}

bool ArrayLengthFunction::Eval(absl::Span<const TupleData* const> params,
                               absl::Span<const Value> args,
                               EvaluationContext* context, Value* result,
                               absl::Status* status) const {
  ABSL_DCHECK_EQ(1, args.size());
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  *result = Value::Int64(args[0].num_elements());
  return true;
}

absl::StatusOr<Value> LambdaEvaluationContext::EvaluateLambda(
    const InlineLambdaExpr* lambda, absl::Span<const Value> args) {
  Value result;
  VirtualTupleSlot lambda_body_slot(&result, &shared_proto_state_);
  absl::Status status;
  if (!lambda->Eval(params_, context_, &lambda_body_slot, &status, args)) {
    ZETASQL_RET_CHECK(!status.ok());
    return status;
  }
  return result;
}

absl::StatusOr<Value> ArrayFilterFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* evaluation_context) const {
  LambdaEvaluationContext context(params, evaluation_context);

  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  ZETASQL_RET_CHECK(args[0].type()->IsArray());
  ZETASQL_RET_CHECK_GE(lambda_->num_args(), 1);
  ZETASQL_RET_CHECK_LE(lambda_->num_args(), 2);
  if (args[0].is_null()) {
    return Value::Null(output_type());
  }

  // Explicitly marking evaluation context to be non-deterministic.
  if (args[0].num_elements() > 1 &&
      InternalValue::GetOrderKind(args[0]) == InternalValue::kIgnoresOrder) {
    evaluation_context->SetNonDeterministicOutput();
  }

  std::vector<Value> filtered_values;
  bool two_argument_lambda = lambda_->num_args() == 2;
  for (int i = 0; i < args[0].num_elements(); ++i) {
    const Value& array_element = args[0].element(i);
    std::vector<Value> lambda_args = {array_element};
    if (two_argument_lambda) {
      // If a two-argument lambda is supplied, the lambda receives an additional
      // parameter specifying the zero-based array index of the array element
      // passed in for the first parameter.
      lambda_args.push_back(Value::Int64(i));
    }
    ZETASQL_ASSIGN_OR_RETURN(Value lambda_result,
                     context.EvaluateLambda(lambda_, lambda_args));
    ZETASQL_RET_CHECK(lambda_result.type()->IsBool());
    if (!lambda_result.is_null() && lambda_result.bool_value()) {
      filtered_values.push_back(std::move(array_element));
    }
  }

  return Value::MakeArray(args[0].type()->AsArray(), filtered_values);
}

absl::StatusOr<Value> ArrayIncludesFunctionWithLambda::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* evaluation_context) const {
  LambdaEvaluationContext context(params, evaluation_context);
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  ZETASQL_RET_CHECK(args[0].type()->IsArray());
  if (args[0].is_null()) {
    return Value::NullBool();
  }

  bool found = false;
  for (int i = 0; i < args[0].num_elements(); ++i) {
    const Value& array_element = args[0].element(i);
    ZETASQL_ASSIGN_OR_RETURN(Value lambda_result,
                     context.EvaluateLambda(lambda_, {array_element}));
    ZETASQL_RET_CHECK(lambda_result.type()->IsBool());
    if (!lambda_result.is_null() && lambda_result.bool_value()) {
      found = true;
      break;
    }
  }

  return Value::Bool(found);
}

absl::StatusOr<Value> ArrayTransformFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* evaluation_context) const {
  LambdaEvaluationContext context(params, evaluation_context);
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  ZETASQL_RET_CHECK(args[0].type()->IsArray());
  ZETASQL_RET_CHECK_GE(lambda_->num_args(), 1);
  ZETASQL_RET_CHECK_LE(lambda_->num_args(), 2);
  if (args[0].is_null()) {
    return Value::Null(output_type());
  }

  // Explicitly marking evaluation context to be non-deterministic.
  if (args[0].num_elements() > 1 &&
      InternalValue::GetOrderKind(args[0]) == InternalValue::kIgnoresOrder) {
    evaluation_context->SetNonDeterministicOutput();
  }

  std::vector<Value> transformed_values;
  bool two_argument_lambda = lambda_->num_args() == 2;
  for (int i = 0; i < args[0].num_elements(); ++i) {
    const Value& array_element = args[0].element(i);
    Value lambda_body_value;
    std::vector<Value> lambda_args = {array_element};
    if (two_argument_lambda) {
      // If a two-argument lambda is supplied, the lambda receives an additional
      // parameter specifying the zero-based array index of the array element
      // passed in for the first parameter.
      lambda_args.push_back(Value::Int64(i));
    }
    ZETASQL_ASSIGN_OR_RETURN(lambda_body_value,
                     context.EvaluateLambda(lambda_, lambda_args));
    transformed_values.push_back(std::move(lambda_body_value));
  }

  return Value::MakeArray(this->output_type()->AsArray(), transformed_values);
}

bool ArrayElementFunction::Eval(absl::Span<const TupleData* const> params,
                                absl::Span<const Value> args,
                                EvaluationContext* context, Value* result,
                                absl::Status* status) const {
  ABSL_DCHECK_EQ(2, args.size());
  const Value& array = args[0];
  // If any of the arguments to the function is NULL, it should return NULL
  // of the element type.
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  int64_t position = args[1].int64_value();
  // Return error upon out-of-bounds access (depending on base).
  if (position < base_) {
    if (safe_) {
      *result = Value::Null(output_type());
      return true;
    } else {
      *status = ::zetasql_base::OutOfRangeErrorBuilder()
                << "Array index " << position << " is out of bounds";
      return false;
    }
  }
  // Return error upon out-of-bounds access (depending on base).
  if (position >= array.num_elements() + base_) {
    if (safe_) {
      *result = Value::Null(output_type());
      return true;
    } else {
      *status = ::zetasql_base::OutOfRangeErrorBuilder()
                << "Array index " << position << " is out of bounds";
      return false;
    }
  }

  MaybeSetNonDeterministicArrayOutput(array, context);
  *result = array.element(position - base_);
  return true;
}

absl::StatusOr<Value> ArrayToStringFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_GE(args.size(), 2);
  ABSL_DCHECK_LE(args.size(), 3);
  if (HasNulls(args)) return Value::Null(output_type());
  std::string delim = args[1].type()->IsString() ? args[1].string_value()
                                                 : args[1].bytes_value();
  std::string result;
  bool first = true;
  const Value& array = args[0];
  for (int i = 0; i < array.num_elements(); ++i) {
    if (array.element(i).is_null() && args.size() == 2) {
      continue;
    }
    if (!first) {
      absl::StrAppend(&result, delim);
    }
    first = false;
    if (array.element(i).is_null()) {
      absl::StrAppend(&result, args[2].type()->IsString()
                                   ? args[2].string_value()
                                   : args[2].bytes_value());
    } else {
      absl::StrAppend(&result, array.element(i).type()->IsString()
                                   ? array.element(i).string_value()
                                   : array.element(i).bytes_value());
    }
  }

  MaybeSetNonDeterministicArrayOutput(array, context);

  return output_type()->IsString() ? Value::String(result)
                                   : Value::Bytes(result);
}

absl::StatusOr<Value> ArrayReverseFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_EQ(args.size(), 1);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }

  MaybeSetNonDeterministicArrayOutput(args[0], context);

  std::vector<Value> elements = args[0].elements();
  std::reverse(elements.begin(), elements.end());
  return Value::Array(output_type()->AsArray(), elements);
}

absl::StatusOr<Value> ArrayIsDistinctFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_EQ(args.size(), 1);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }

  const Value& array = args[0];
  const ArrayType* array_type = array.type()->AsArray();
  ZETASQL_RET_CHECK_NE(array_type, nullptr)
      << "ARRAY_IS_DISTINCT cannot be used on non-array type "
      << array.type()->DebugString();

  ZETASQL_RET_CHECK(array_type->element_type()->SupportsGrouping(
      context->GetLanguageOptions(), nullptr))
      << "ARRAY_IS_DISTINCT cannot be used on argument of type "
      << array.type()->ShortTypeName(
             context->GetLanguageOptions().product_mode())
      << " because the array's element type does not support grouping";

  MaybeSetNonDeterministicArrayOutput(array, context);

  absl::flat_hash_set<Value> values;
  for (int i = 0; i < array.num_elements(); ++i) {
    if (!values.insert(array.element(i)).second) {
      // The insertion did not take place, so the array contains duplicates.
      return Value::Bool(false);
    }
  }

  return Value::Bool(true);
}

absl::StatusOr<Value> ArrayIncludesFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }

  // Find the target.
  const Value& target = args[1];
  for (const Value& element : args[0].elements()) {
    Value equals = element.SqlEquals(target);
    ZETASQL_RET_CHECK(equals.is_valid())
        << "Failed to compare element: " << element.DebugString()
        << " and target: " << target.DebugString();
    if (equals.is_null()) {
      continue;
    }
    if (equals.bool_value()) {
      return Value::Bool(true);
    }
  }
  return Value::Bool(false);
}

absl::StatusOr<Value> ArrayIncludesArrayFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }

  bool require_all = kind() == FunctionKind::kArrayIncludesAll;
  for (const Value& element_2 : args[1].elements()) {
    bool found = false;
    for (const Value& element_1 : args[0].elements()) {
      Value equals = element_1.SqlEquals(element_2);
      ZETASQL_RET_CHECK(equals.is_valid())
          << "Failed to compare element: " << element_1.DebugString()
          << " and element: " << element_2.DebugString();
      if (equals.is_null()) {
        continue;
      }
      if (equals.bool_value()) {
        found = true;
        break;
      }
    }
    if (require_all) {
      if (!found) return Value::Bool(false);
    } else {
      if (found) return Value::Bool(true);
    }
  }
  return require_all ? Value::Bool(true) : Value::Bool(false);
}

absl::StatusOr<Value> ArrayFirstLastFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  ZETASQL_RET_CHECK(kind() == FunctionKind::kArrayFirst ||
            kind() == FunctionKind::kArrayLast);
  if (args[0].is_null()) {
    return Value::Null(output_type());
  }
  if (args[0].is_empty_array()) {
    absl::string_view msg =
        kind() == FunctionKind::kArrayFirst
            ? "ARRAY_FIRST cannot get the first element of an empty array"
            : "ARRAY_LAST cannot get the last element of an empty array";
    return MakeEvalError() << msg;
  }
  if (args[0].num_elements() > 1 &&
      InternalValue::GetOrderKind(args[0]) == InternalValue::kIgnoresOrder) {
    context->SetNonDeterministicOutput();
  }
  if (kind() == FunctionKind::kArrayFirst) {
    const Value& first = args[0].element(0);
    ZETASQL_RET_CHECK(first.is_valid());
    return first;
  }
  const Value& last = args[0].element(args[0].num_elements() - 1);
  ZETASQL_RET_CHECK(last.is_valid());
  return last;
}

absl::StatusOr<Value> ArraySliceFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_GE(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  if (kind() == FunctionKind::kArraySlice) {
    ZETASQL_RET_CHECK_EQ(args.size(), 3);
  } else {
    ZETASQL_RET_CHECK_EQ(args.size(), 2);
    if (args[1].int64_value() < 0) {
      return ::zetasql_base::OutOfRangeErrorBuilder()
             << "The n argument to " << absl::AsciiStrToUpper(debug_name())
             << " must not be negative.";
    }
  }
  int array_length = args[0].num_elements();
  if (array_length > 1 &&
      InternalValue::GetOrderKind(args[0]) == InternalValue::kIgnoresOrder) {
    context->SetNonDeterministicOutput();
  }
  int64_t start = 0;
  int64_t end = array_length;
  switch (kind()) {
    case FunctionKind::kArraySlice:
      start = args[1].int64_value();
      end = args[2].int64_value();
      if (start < 0) {
        start += array_length;
      }
      if (end < 0) {
        end += array_length;
      }
      break;
    case FunctionKind::kArrayFirstN:
      end = args[1].int64_value() - 1;
      break;
    case FunctionKind::kArrayLastN:
      start = array_length - args[1].int64_value();
      break;
    case FunctionKind::kArrayRemoveFirstN:
      start = args[1].int64_value();
      break;
    case FunctionKind::kArrayRemoveLastN:
      end = array_length - args[1].int64_value() - 1;
      break;
    default: {
      ZETASQL_RET_CHECK_FAIL() << "Unexpected function: " << debug_name();
    }
  }
  if (array_length == 0 || end < 0 || start >= array_length || start > end) {
    return Value::EmptyArray(output_type()->AsArray());
  }
  start = std::max(int64_t{0}, start);
  end = std::min(int64_t{array_length - 1}, end);
  const std::vector<Value>& elements = args[0].elements();
  std::vector<Value> result(elements.begin() + start,
                            elements.begin() + end + 1);
  Value output = Value::Array(output_type()->AsArray(), result);
  ZETASQL_RET_CHECK(output.is_valid());
  return output;
}

absl::StatusOr<Value> ArrayMinMaxFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  ZETASQL_RET_CHECK(kind() == FunctionKind::kArrayMin ||
            kind() == FunctionKind::kArrayMax);
  if (args[0].is_null() || args[0].is_empty_array()) {
    return Value::Null(output_type());
  }
  bool has_ties = false;

  Value output_null = Value::Null(output_type());
  bool has_non_null = false;
  const Type* element_type = args[0].type()->AsArray()->element_type();
  switch (FCT(kind(), element_type->kind())) {
    // ARRAY_MIN
    case FCT(FunctionKind::kArrayMin, TYPE_FLOAT):
    case FCT(FunctionKind::kArrayMin, TYPE_DOUBLE): {
      double min_value = std::numeric_limits<double>::infinity();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        if (std::isnan(element.ToDouble()) || std::isnan(min_value)) {
          min_value = std::numeric_limits<double>::quiet_NaN();
        } else {
          min_value = std::min(min_value, element.ToDouble());
        }
      }
      if (!has_non_null) {
        return output_null;
      }
      return output_type()->IsFloat() ? Value::Float(min_value)
                                      : Value::Double(min_value);
    }
    case FCT(FunctionKind::kArrayMin, TYPE_UINT64): {
      uint64_t min_value = std::numeric_limits<uint64_t>::max();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        min_value = std::min(min_value, element.uint64_value());
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::Uint64(min_value);
    }
    case FCT(FunctionKind::kArrayMin, TYPE_INT32):
    case FCT(FunctionKind::kArrayMin, TYPE_INT64):
    case FCT(FunctionKind::kArrayMin, TYPE_UINT32):
    case FCT(FunctionKind::kArrayMin, TYPE_DATE):
    case FCT(FunctionKind::kArrayMin, TYPE_BOOL):
    case FCT(FunctionKind::kArrayMin, TYPE_ENUM): {
      int64_t min_value = std::numeric_limits<int64_t>::max();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        min_value = std::min(min_value, element.ToInt64());
      }
      if (!has_non_null) {
        return output_null;
      }
      switch (element_type->kind()) {
        case TYPE_INT32:
          return Value::Int32(static_cast<int32_t>(min_value));
        case TYPE_INT64:
          return Value::Int64(min_value);
        case TYPE_UINT32:
          return Value::Uint32(static_cast<uint32_t>(min_value));
        case TYPE_DATE:
          return Value::Date(static_cast<int32_t>(min_value));
        case TYPE_BOOL:
          return Value::Bool(min_value > 0);
        case TYPE_ENUM:
          return Value::Enum(output_type()->AsEnum(), min_value,
                             /*allow_unknown_enum_values=*/true);
        default:
          ZETASQL_RET_CHECK_FAIL();
      }
    }
    case FCT(FunctionKind::kArrayMin, TYPE_DATETIME): {
      DatetimeValue min_value = DatetimeValue::FromYMDHMSAndNanos(
          9999, 12, 31, 23, 59, 59, 999999999);
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        min_value = element.LessThan(Value::Datetime(min_value))
                        ? element.datetime_value()
                        : min_value;
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::Datetime(min_value);
    }
    case FCT(FunctionKind::kArrayMin, TYPE_TIMESTAMP): {
      int64_t min_value = std::numeric_limits<int64_t>::max();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        // TODO: Support Value::ToUnixNanos in ARRAY_MIN and MIN.
        min_value = std::min(min_value, element.ToUnixMicros());
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::TimestampFromUnixMicros(min_value);
    }
    case FCT(FunctionKind::kArrayMin, TYPE_TIME): {
      int64_t min_value = std::numeric_limits<int64_t>::max();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        min_value =
            std::min(min_value, element.time_value().Packed64TimeNanos());
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::Time(TimeValue::FromPacked64Nanos(min_value));
    }
    case FCT(FunctionKind::kArrayMin, TYPE_NUMERIC): {
      NumericValue min_value = NumericValue::MaxValue();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        min_value = std::min(min_value, element.numeric_value());
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::Numeric(min_value);
    }
    case FCT(FunctionKind::kArrayMin, TYPE_BIGNUMERIC): {
      BigNumericValue min_value = BigNumericValue::MaxValue();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        min_value = std::min(min_value, element.bignumeric_value());
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::BigNumeric(min_value);
    }
    case FCT(FunctionKind::kArrayMin, TYPE_STRING): {
      std::string min_value;
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        if (!has_non_null) {  // when there isn't already a non-null string
          min_value = element.string_value();
        } else if (!collator_list_.empty()) {  // when there is collation_list
                                               // in ResolvedFunctionCall
          absl::Status status = absl::OkStatus();
          int64_t res = collator_list_[0]->CompareUtf8(element.string_value(),
                                                       min_value, &status);
          ZETASQL_RETURN_IF_ERROR(status);
          if (res < 0) {  // current element orders first
            min_value = element.string_value();
            has_ties = false;
          } else if (res == 0) {
            has_ties = true;
          }
        } else {
          min_value = std::min(min_value, element.string_value());
        }
        has_non_null = true;
      }
      if (!has_non_null) {
        return output_null;
      }
      if (has_ties && InternalValue::GetOrderKind(args[0]) ==
                          InternalValue::kIgnoresOrder) {
        context->SetNonDeterministicOutput();
      }
      return Value::String(min_value);
    }
    case FCT(FunctionKind::kArrayMin, TYPE_BYTES): {
      std::string min_value;
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        if (!has_non_null) {  // there isn't already a non-null string
          min_value = element.bytes_value();
        } else {
          min_value = std::min(min_value, element.bytes_value());
        }
        has_non_null = true;
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::Bytes(min_value);
    }
    case FCT(FunctionKind::kArrayMin, TYPE_INTERVAL): {
      IntervalValue min_value = IntervalValue::MaxValue();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        if (element.interval_value() == min_value) {
          has_ties = true;
        } else if (element.interval_value() < min_value) {
          min_value = element.interval_value();
          has_ties = false;
        }
      }
      if (!has_non_null) {
        return output_null;
      }
      if (has_ties && InternalValue::GetOrderKind(args[0]) ==
                          InternalValue::kIgnoresOrder) {
        context->SetNonDeterministicOutput();
      }
      return Value::Interval(min_value);
    }

    // ARRAY_MAX
    case FCT(FunctionKind::kArrayMax, TYPE_FLOAT):
    case FCT(FunctionKind::kArrayMax, TYPE_DOUBLE): {
      double max_value = -std::numeric_limits<double>::infinity();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        if (std::isnan(element.ToDouble()) || std::isnan(max_value)) {
          max_value = std::numeric_limits<double>::quiet_NaN();
        } else {
          max_value = std::max(max_value, element.ToDouble());
        }
      }
      if (!has_non_null) {
        return output_null;
      }
      return output_type()->IsFloat() ? Value::Float(max_value)
                                      : Value::Double(max_value);
    }
    case FCT(FunctionKind::kArrayMax, TYPE_UINT64): {
      uint64_t max_value = 0;
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        max_value = std::max(max_value, element.uint64_value());
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::Uint64(max_value);
    }
    case FCT(FunctionKind::kArrayMax, TYPE_INT32):
    case FCT(FunctionKind::kArrayMax, TYPE_INT64):
    case FCT(FunctionKind::kArrayMax, TYPE_UINT32):
    case FCT(FunctionKind::kArrayMax, TYPE_DATE):
    case FCT(FunctionKind::kArrayMax, TYPE_BOOL):
    case FCT(FunctionKind::kArrayMax, TYPE_ENUM): {
      int64_t max_value = std::numeric_limits<int64_t>::lowest();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        max_value = std::max(max_value, element.ToInt64());
      }
      if (!has_non_null) {
        return output_null;
      }
      switch (element_type->kind()) {
        case TYPE_INT32:
          return Value::Int32(static_cast<int32_t>(max_value));
        case TYPE_INT64:
          return Value::Int64(max_value);
        case TYPE_UINT32:
          return Value::Uint32(static_cast<uint32_t>(max_value));
        case TYPE_DATE:
          return Value::Date(static_cast<int32_t>(max_value));
        case TYPE_BOOL:
          return Value::Bool(max_value > 0);
        case TYPE_ENUM:
          return Value::Enum(output_type()->AsEnum(), max_value,
                             /*allow_unknown_enum_values=*/true);
        default:
          ZETASQL_RET_CHECK_FAIL();
      }
    }
    case FCT(FunctionKind::kArrayMax, TYPE_DATETIME): {
      DatetimeValue max_value =
          DatetimeValue::FromYMDHMSAndNanos(1, 1, 1, 0, 0, 0, 0);
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        max_value = Value::Datetime(max_value).LessThan(element)
                        ? element.datetime_value()
                        : max_value;
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::Datetime(max_value);
    }
    case FCT(FunctionKind::kArrayMax, TYPE_TIMESTAMP): {
      int64_t max_value = std::numeric_limits<int64_t>::lowest();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        max_value = std::max(max_value, element.ToUnixMicros());
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::TimestampFromUnixMicros(max_value);
    }
    case FCT(FunctionKind::kArrayMax, TYPE_TIME): {
      int64_t max_value = std::numeric_limits<int64_t>::lowest();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        max_value =
            std::max(max_value, element.time_value().Packed64TimeNanos());
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::Time(TimeValue::FromPacked64Nanos(max_value));
    }
    case FCT(FunctionKind::kArrayMax, TYPE_NUMERIC): {
      NumericValue max_value = NumericValue::MinValue();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        max_value = std::max(max_value, element.numeric_value());
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::Numeric(max_value);
    }
    case FCT(FunctionKind::kArrayMax, TYPE_BIGNUMERIC): {
      BigNumericValue max_value = BigNumericValue::MinValue();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        max_value = std::max(max_value, element.bignumeric_value());
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::BigNumeric(max_value);
    }
    case FCT(FunctionKind::kArrayMax, TYPE_STRING): {
      std::string max_value;
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        if (!has_non_null) {  // when there isn't already a non-null string
          max_value = element.string_value();
        } else if (!collator_list_.empty()) {  // when there is collation_list
                                               // in ResolvedFunctionCall
          absl::Status status = absl::OkStatus();
          int64_t res = collator_list_[0]->CompareUtf8(element.string_value(),
                                                       max_value, &status);
          ZETASQL_RETURN_IF_ERROR(status);
          if (res > 0) {  // current element orders first
            max_value = element.string_value();
            has_ties = false;
          } else if (res == 0) {
            has_ties = true;
          }
        } else {
          max_value = std::max(max_value, element.string_value());
        }
        has_non_null = true;
      }
      if (!has_non_null) {
        return output_null;
      }
      if (has_ties && InternalValue::GetOrderKind(args[0]) ==
                          InternalValue::kIgnoresOrder) {
        context->SetNonDeterministicOutput();
      }
      return Value::String(max_value);
    }
    case FCT(FunctionKind::kArrayMax, TYPE_BYTES): {
      std::string max_value;
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        if (!has_non_null) {  // there isn't already a non-null string
          max_value = element.bytes_value();
        } else {
          max_value = std::max(max_value, element.bytes_value());
        }
        has_non_null = true;
      }
      if (!has_non_null) {
        return output_null;
      }
      return Value::Bytes(max_value);
    }
    case FCT(FunctionKind::kArrayMax, TYPE_INTERVAL): {
      IntervalValue max_value = IntervalValue::MinValue();
      for (const Value& element : args[0].elements()) {
        if (element.is_null()) {
          continue;
        }
        has_non_null = true;
        if (element.interval_value() == max_value) {
          has_ties = true;
        } else if (element.interval_value() > max_value) {
          max_value = element.interval_value();
          has_ties = false;
        }
      }
      if (!has_non_null) {
        return output_null;
      }
      if (has_ties && InternalValue::GetOrderKind(args[0]) ==
                          InternalValue::kIgnoresOrder) {
        context->SetNonDeterministicOutput();
      }
      return Value::Interval(max_value);
    }
    default:
      ZETASQL_RET_CHECK_FAIL();
  }
}

static absl::StatusOr<Value> AggregateDoubleArraySumValue(
    const std::vector<Value>& elements, const Type* output_type) {
  zetasql_base::ExactFloat sum = 0;
  bool has_non_null = false;
  for (const Value& element : elements) {
    if (element.is_null()) {
      continue;
    }
    has_non_null = true;
    sum += element.ToDouble();
  }
  if (sum.is_finite() && (sum > std::numeric_limits<double>::max() ||
                          sum < -std::numeric_limits<double>::max())) {
    return ::zetasql_base::OutOfRangeErrorBuilder() << "ARRAY_SUM double overflow";
  }
  if (has_non_null) {
    return Value::Double(sum.ToDouble());
  }
  return Value::Null(output_type);
}

static absl::StatusOr<Value> AggregateInt64ArraySumValue(
    const std::vector<Value>& elements, const Type* output_type) {
  __int128 sum = 0;
  bool has_non_null = false;
  for (const Value& element : elements) {
    if (element.is_null()) {
      continue;
    }
    has_non_null = true;
    sum += element.ToInt64();
  }
  if (sum > std::numeric_limits<int64_t>::max() ||
      sum < std::numeric_limits<int64_t>::min()) {
    return ::zetasql_base::OutOfRangeErrorBuilder() << "ARRAY_SUM int64_t overflow";
  }
  if (has_non_null) {
    return Value::Int64(sum);
  }
  return Value::Null(output_type);
}

static absl::StatusOr<Value> AggregateUint64ArraySumValue(
    const std::vector<Value>& elements, const Type* output_type) {
  unsigned __int128 sum = 0;
  bool has_non_null = false;
  for (const Value& element : elements) {
    if (element.is_null()) {
      continue;
    }
    has_non_null = true;
    sum += element.ToUint64();
  }
  if (sum > std::numeric_limits<uint64_t>::max()) {
    return ::zetasql_base::OutOfRangeErrorBuilder() << "ARRAY_SUM uint64_t overflow";
  }
  if (has_non_null) {
    return Value::Uint64(sum);
  }
  return Value::Null(output_type);
}

static absl::StatusOr<Value> AggregateNumericArraySumValue(
    const std::vector<Value>& elements, const Type* output_type) {
  bool has_non_null = false;
  NumericValue::SumAggregator aggregator = NumericValue::SumAggregator();
  for (const Value& element : elements) {
    if (element.is_null()) {
      continue;
    } else {
      has_non_null = true;
      aggregator.Add(element.numeric_value());
    }
  }

  if (has_non_null) {
    ZETASQL_ASSIGN_OR_RETURN(NumericValue sum, aggregator.GetSum());
    return Value::Numeric(sum);
  }
  return Value::Null(output_type);
}

static absl::StatusOr<Value> AggregateBigNumericArraySumValue(
    const std::vector<Value>& elements, const Type* output_type) {
  bool has_non_null = false;
  BigNumericValue::SumAggregator aggregator = BigNumericValue::SumAggregator();
  for (const Value& element : elements) {
    if (element.is_null()) {
      continue;
    } else {
      has_non_null = true;
      aggregator.Add(element.bignumeric_value());
    }
  }

  if (has_non_null) {
    ZETASQL_ASSIGN_OR_RETURN(BigNumericValue sum, aggregator.GetSum());
    return Value::BigNumeric(sum);
  }
  return Value::Null(output_type);
}

static absl::StatusOr<Value> AggregateIntervalArraySumValue(
    const std::vector<Value>& elements, const Type* output_type) {
  IntervalValue sum;
  bool has_non_null = false;
  IntervalValue::SumAggregator aggregator = IntervalValue::SumAggregator();
  for (const Value& element : elements) {
    if (element.is_null()) {
      continue;
    } else {
      has_non_null = true;
      aggregator.Add(element.interval_value());
    }
  }
  ZETASQL_ASSIGN_OR_RETURN(sum, aggregator.GetSum());
  if (has_non_null) {
    return Value::Interval(sum);
  }
  return Value::Null(output_type);
}

absl::Status ValidateNoDoubleOverflow(long double value) {
  if (value == std::numeric_limits<long double>::infinity() ||
      value == -std::numeric_limits<long double>::infinity()) {
    return absl::OkStatus();
  }

  if (value > std::numeric_limits<double>::max() ||
      value < std::numeric_limits<double>::lowest()) {
    return absl::Status(absl::StatusCode::kOutOfRange, "double overflow");
  }

  return absl::OkStatus();
}

// When the input array contains +/-inf, ARRAY_AVG might return +/-inf or NaN
// which means the output cannot be validated by approximate comparison.
static absl::StatusOr<Value> AggregateDoubleArrayAvgValue(
    const std::vector<Value>& elements) {
  long double avg = 0;
  int64_t non_null_count = 0;
  for (const Value& element : elements) {
    if (element.is_null()) {
      continue;
    }
    non_null_count++;
    long double delta;
    absl::Status status;

    // Use Donald Knuth's iterative running mean algorithm to compute average.
    if (!functions::Subtract(static_cast<long double>(element.ToDouble()), avg,
                             &delta, &status) ||
        !functions::Add(avg, delta / non_null_count, &avg, &status)) {
      return status;
    }
  }

  // Average should never overflow, as it is bound by the elements which are all
  // within bounds - otherwise we would have produced an error already.
  ZETASQL_RET_CHECK_OK(ValidateNoDoubleOverflow(avg));
  if (non_null_count > 0) {
    return Value::Double(static_cast<double>(avg));
  }

  return Value::NullDouble();
}

static absl::StatusOr<Value> AggregateNumericArrayAvgValue(
    const std::vector<Value>& elements) {
  int64_t non_null_count = 0;
  NumericValue::SumAggregator aggregator = NumericValue::SumAggregator();
  for (const Value& element : elements) {
    if (element.is_null()) {
      continue;
    }
    non_null_count++;
    aggregator.Add(element.numeric_value());
  }
  if (non_null_count > 0) {
    ZETASQL_ASSIGN_OR_RETURN(NumericValue avg, aggregator.GetAverage(non_null_count));
    return Value::Numeric(avg);
  }
  return Value::NullNumeric();
}

static absl::StatusOr<Value> AggregateBigNumericArrayAvgValue(
    const std::vector<Value>& elements) {
  int64_t non_null_count = 0;
  BigNumericValue::SumAggregator aggregator = BigNumericValue::SumAggregator();
  for (const Value& element : elements) {
    if (element.is_null()) {
      continue;
    }
    non_null_count++;
    aggregator.Add(element.bignumeric_value());
  }

  if (non_null_count > 0) {
    ZETASQL_ASSIGN_OR_RETURN(BigNumericValue avg,
                     aggregator.GetAverage(non_null_count));
    return Value::BigNumeric(avg);
  }
  return Value::NullBigNumeric();
}

static absl::StatusOr<Value> AggregateIntervalArrayAvgValue(
    const std::vector<Value>& elements, EvaluationContext* context) {
  int64_t non_null_count = 0;
  IntervalValue::SumAggregator aggregator = IntervalValue::SumAggregator();
  for (const Value& element : elements) {
    if (element.is_null()) {
      continue;
    }
    non_null_count++;
    aggregator.Add(element.interval_value());
  }

  if (non_null_count > 0) {
    // Sanitize the nanos second part if in micro seconds mode.
    bool round_to_micros = GetTimestampScale(context->GetLanguageOptions()) ==
                           functions::TimestampScale::kMicroseconds;
    ZETASQL_ASSIGN_OR_RETURN(IntervalValue avg,
                     aggregator.GetAverage(non_null_count, round_to_micros));
    return Value::Interval(avg);
  }
  return Value::NullInterval();
}

absl::StatusOr<Value> ArraySumAvgFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  ZETASQL_RET_CHECK(kind() == FunctionKind::kArraySum ||
            kind() == FunctionKind::kArrayAvg);
  if (args[0].is_null() || args[0].is_empty_array()) {
    return Value::Null(output_type());
  }
  const Type* element_type = args[0].type()->AsArray()->element_type();
  // Indeterminism is only possible when there are multiple elements in the
  // array, as well as:
  // 1. if the input array element type is FLOAT or DOUBLE, both ARRAY_SUM and
  //    ARRAY_AVG are indeterministic; or
  // 2. if the input array element type is INT32, INT64, UINT32 or UINT64,
  //    only ARRAY_AVG is indeterministic.
  if (args[0].num_elements() > 1) {
    if (element_type->IsFloatingPoint() ||
        (kind() == FunctionKind::kArrayAvg &&
         (element_type->IsSignedInteger() ||
          element_type->IsUnsignedInteger()))) {
      context->SetNonDeterministicOutput();
    }
  }
  Value output = Value::Null(output_type());

  switch (FCT(kind(), element_type->kind())) {
    // ARRAY_SUM
    case FCT(FunctionKind::kArraySum, TYPE_INT32):
    case FCT(FunctionKind::kArraySum, TYPE_INT64): {
      return AggregateInt64ArraySumValue(args[0].elements(), output_type());
    }
    case FCT(FunctionKind::kArraySum, TYPE_UINT32):
    case FCT(FunctionKind::kArraySum, TYPE_UINT64): {
      return AggregateUint64ArraySumValue(args[0].elements(), output_type());
    }
    case FCT(FunctionKind::kArraySum, TYPE_FLOAT):
    case FCT(FunctionKind::kArraySum, TYPE_DOUBLE): {
      return AggregateDoubleArraySumValue(args[0].elements(), output_type());
    }
    case FCT(FunctionKind::kArraySum, TYPE_NUMERIC): {
      return AggregateNumericArraySumValue(args[0].elements(), output_type());
    }
    case FCT(FunctionKind::kArraySum, TYPE_BIGNUMERIC): {
      return AggregateBigNumericArraySumValue(args[0].elements(),
                                              output_type());
    }
    case FCT(FunctionKind::kArraySum, TYPE_INTERVAL): {
      return AggregateIntervalArraySumValue(args[0].elements(), output_type());
    }

    // ARRAY_AVG
    case FCT(FunctionKind::kArrayAvg, TYPE_INT32):
    case FCT(FunctionKind::kArrayAvg, TYPE_INT64):
    case FCT(FunctionKind::kArrayAvg, TYPE_UINT32):
    case FCT(FunctionKind::kArrayAvg, TYPE_UINT64):
    case FCT(FunctionKind::kArrayAvg, TYPE_FLOAT):
    case FCT(FunctionKind::kArrayAvg, TYPE_DOUBLE): {
      return AggregateDoubleArrayAvgValue(args[0].elements());
    }
    case FCT(FunctionKind::kArrayAvg, TYPE_NUMERIC): {
      return AggregateNumericArrayAvgValue(args[0].elements());
    }
    case FCT(FunctionKind::kArrayAvg, TYPE_BIGNUMERIC): {
      return AggregateBigNumericArrayAvgValue(args[0].elements());
    }
    case FCT(FunctionKind::kArrayAvg, TYPE_INTERVAL): {
      return AggregateIntervalArrayAvgValue(args[0].elements(), context);
    }
    default:
      ZETASQL_RET_CHECK_FAIL();
  }
}

static bool IsFindSingletonFunction(FunctionKind kind) {
  return kind == FunctionKind::kArrayOffset || kind == FunctionKind::kArrayFind;
}

static absl::StatusOr<bool> IsEqualToTarget(const Value& element,
                                            const Value& target,
                                            const CollatorList& collator_list) {
  // Use collation_list that is defined in the resolved function call.
  if (!collator_list.empty()) {
    ZETASQL_RET_CHECK(element.type()->kind() == TYPE_STRING);
    absl::Status status = absl::OkStatus();
    if (element.is_null() || target.is_null()) {
      return element.is_null() && target.is_null();
    }
    int64_t res = collator_list[0]->CompareUtf8(element.string_value(),
                                                target.string_value(), &status);
    ZETASQL_RETURN_IF_ERROR(status);
    return res == 0;
  } else {
    Value equals = element.SqlEquals(target);
    ZETASQL_RET_CHECK(equals.is_valid())
        << "Failed to compare element: " << element.DebugString()
        << " and target: " << target.DebugString();
    return !equals.is_null() && equals.bool_value();
  }
}

static absl::StatusOr<bool> SatisfiesCondition(
    const Value& element, const InlineLambdaExpr* lambda,
    LambdaEvaluationContext& lambda_context) {
  bool found = false;
  ZETASQL_ASSIGN_OR_RETURN(Value lambda_result,
                   lambda_context.EvaluateLambda(lambda, {element}));
  ZETASQL_RET_CHECK(lambda_result.type()->IsBool());
  if (!lambda_result.is_null() && lambda_result.bool_value()) {
    found = true;
  }
  return found;
}

absl::StatusOr<Value> ArrayFindFunctions::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  LambdaEvaluationContext lambda_context(params, context);
  bool using_lambda_arg = lambda_ != nullptr;
  if (using_lambda_arg) {
    ZETASQL_RET_CHECK_EQ(lambda_->num_args(), 1);
  }

  ZETASQL_RET_CHECK(kind() == FunctionKind::kArrayOffset ||
            kind() == FunctionKind::kArrayFind ||
            kind() == FunctionKind::kArrayOffsets ||
            kind() == FunctionKind::kArrayFindAll);
  if (IsFindSingletonFunction(kind())) {
    ZETASQL_RET_CHECK_EQ(args.size(), using_lambda_arg ? 2 : 3);
  } else {
    ZETASQL_RET_CHECK_EQ(args.size(), using_lambda_arg ? 1 : 2);
  }

  if (HasNulls(args)) {
    return Value::Null(output_type());
  }

  const Value& input_array = args[0];
  if (input_array.is_empty_array()) {
    if (IsFindSingletonFunction(kind())) {
      return Value::Null(output_type());
    }
    return Value::EmptyArray(output_type()->AsArray());
  }

  auto order_kind = InternalValue::GetOrderKind(input_array);
  int input_array_length = input_array.num_elements();
  std::vector<Value> found_offsets;
  std::vector<Value> found_values;
  for (int i = 0; i < input_array_length; ++i) {
    bool found = false;
    if (using_lambda_arg) {
      ZETASQL_ASSIGN_OR_RETURN(found, SatisfiesCondition(input_array.element(i),
                                                 lambda_, lambda_context));
    } else {
      ZETASQL_ASSIGN_OR_RETURN(
          found, IsEqualToTarget(input_array.element(i), /*target=*/args[1],
                                 collator_list_));
    }
    if (found) {
      found_offsets.push_back(Value::Int64(i));
      found_values.push_back(input_array.element(i));
    }
  }

  // Empty result
  if (found_offsets.empty()) {
    ZETASQL_RET_CHECK(found_values.empty());
    return IsFindSingletonFunction(kind())
               ? Value::Null(output_type())
               : Value::EmptyArray(output_type()->AsArray());
  }

  if ((kind() == FunctionKind::kArrayOffset ||
       kind() == FunctionKind::kArrayOffsets) &&
      input_array_length > 1 && order_kind == InternalValue::kIgnoresOrder) {
    context->SetNonDeterministicOutput();
  }

  bool are_ties_distinguishable = IsTypeWithDistinguishableTies(
      input_array.type()->AsArray()->element_type(), collator_list_);
  if (kind() == FunctionKind::kArrayFind &&
      order_kind == InternalValue::kIgnoresOrder && found_offsets.size() > 1 &&
      (are_ties_distinguishable || using_lambda_arg)) {
    context->SetNonDeterministicOutput();
  }
  if (IsFindSingletonFunction(kind())) {
    // If find mode equals to 1, get the "FIRST" found element, otherwise get
    // the "LAST" found element.
    int mode_idx = using_lambda_arg ? 1 : 2;
    const int32_t mode = args[mode_idx].enum_value();
    if (kind() == FunctionKind::kArrayOffset) {  // ARRAY_OFFSET
      return mode == 1 ? found_offsets.front() : found_offsets.back();
    }
    // ARRAY_FIND
    return mode == 1 ? found_values.front() : found_values.back();
  }
  // ARRAY_OFFSETS
  if (kind() == FunctionKind::kArrayOffsets) {
    return InternalValue::Array(output_type()->AsArray(), found_offsets,
                                order_kind);
  }
  // ARRAY_FIND_ALL
  return InternalValue::Array(output_type()->AsArray(), found_values,
                              order_kind);
}

bool IsFunction::Eval(absl::Span<const TupleData* const> params,
                      absl::Span<const Value> args, EvaluationContext* context,
                      Value* result, absl::Status* status) const {
  ABSL_DCHECK_EQ(1, args.size());
  const Value& val = args[0];
  switch (kind()) {
    case FunctionKind::kIsNull:
      *result = Value::Bool(val.is_null());
      return true;
    case FunctionKind::kIsTrue:
      *result = Value::Bool(!val.is_null() && val.bool_value() == true);
      return true;
    case FunctionKind::kIsFalse:
      *result = Value::Bool(!val.is_null() && val.bool_value() == false);
      return true;
    default:
      *status = ::zetasql_base::UnimplementedErrorBuilder()
                << "Unexpected function: " << debug_name();
      return false;
  }
}

absl::StatusOr<Value> CastFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_GE(args.size(), 1);
  const Value& v = args[0];
  const bool return_null_on_error =
      args.size() >= 2 ? args[1].bool_value() : false;

  std::optional<std::string> format;
  if (args.size() >= 3) {
    // Returns NULL if format is null.
    if (args[2].is_null()) {
      return absl::StatusOr<Value>(Value::Null(output_type()));
    }

    format = args[2].string_value();
  }

  std::optional<std::string> time_zone;
  if (args.size() >= 4) {
    // Returns NULL if time_zone is null.
    if (args[3].is_null()) {
      return absl::StatusOr<Value>(Value::Null(output_type()));
    }

    time_zone = args[3].string_value();
  }

  absl::StatusOr<Value> status_or = internal::CastValueWithoutTypeValidation(
      v, context->GetDefaultTimeZone(),
      absl::FromUnixMicros(context->GetCurrentTimestamp()),
      context->GetLanguageOptions(), output_type(), format, time_zone,
      extended_cast_evaluator_.get(), /*canonicalize_zero=*/true);
  if (!status_or.ok() && return_null_on_error) {
    // TODO: check that failure is not due to absence of
    // extended_type_function. In this case we still probably wants to fail the
    // whole query.
    return absl::StatusOr<Value>(Value::Null(output_type()));
  }
  if (HasFloatingPoint(v.type()) && !HasFloatingPoint(output_type())) {
    context->SetNonDeterministicOutput();
  }
  if (!type_params_.IsEmpty() && status_or.ok()) {
    Value casted_value = status_or.value();
    ZETASQL_RETURN_IF_ERROR(ApplyConstraints(
        type_params_, context->GetLanguageOptions().product_mode(),
        casted_value));
    return casted_value;
  }
  // Provide more helpful error message in the case of cast from string to enum.
  if (!status_or.ok() && !v.is_null() && output_type()->IsEnum() &&
      v.type()->IsString()) {
    std::string suggestion =
        SuggestEnumValue(output_type()->AsEnum(), v.string_value());

    if (!suggestion.empty()) {
      zetasql_base::StatusBuilder builder(status_or.status());
      builder << "Did you mean '" << suggestion << "'?";
      if (zetasql_base::CaseEqual(suggestion, v.string_value())) {
        // If the actual value only differs by case, add a reminder.
        builder << " (Note: ENUM values are case sensitive)";
      }
      return builder;
    }
  }
  return status_or;
}

bool BitCastFunction::Eval(absl::Span<const TupleData* const> params,
                           absl::Span<const Value> args,
                           EvaluationContext* context, Value* result,
                           absl::Status* status) const {
  ABSL_DCHECK_EQ(args.size(), 1);
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  const Value& val = args[0];
  switch (FCT(kind(), val.type_kind())) {
    case FCT(FunctionKind::kBitCastToInt32, TYPE_UINT32):
      return InvokeUnary<int32_t, uint32_t>(
          &functions::BitCast<uint32_t, int32_t>, args, result, status);
    case FCT(FunctionKind::kBitCastToInt64, TYPE_UINT64):
      return InvokeUnary<int64_t, uint64_t>(
          &functions::BitCast<uint64_t, int64_t>, args, result, status);
    case FCT(FunctionKind::kBitCastToUint32, TYPE_INT32):
      return InvokeUnary<uint32_t, int32_t>(
          &functions::BitCast<int32_t, uint32_t>, args, result, status);
    case FCT(FunctionKind::kBitCastToUint64, TYPE_INT64):
      return InvokeUnary<uint64_t, int64_t>(
          &functions::BitCast<int64_t, uint64_t>, args, result, status);
    case FCT(FunctionKind::kBitCastToInt32, TYPE_INT32):
    case FCT(FunctionKind::kBitCastToInt64, TYPE_INT64):
    case FCT(FunctionKind::kBitCastToUint32, TYPE_UINT32):
    case FCT(FunctionKind::kBitCastToUint64, TYPE_UINT64):
      *result = val;
      return true;
    default:
      *status = ::zetasql_base::UnimplementedErrorBuilder()
                << "Unsupported argument or output type for bit_cast.";
      return false;
  }
}

bool LogicalFunction::Eval(absl::Span<const TupleData* const> params,
                           absl::Span<const Value> args,
                           EvaluationContext* context, Value* result,
                           absl::Status* status) const {
  switch (FCT(kind(), args[0].type_kind())) {
    case FCT(FunctionKind::kAnd, TYPE_BOOL): {
      // Assume true and downgrade appropriately.
      bool known_true = true;
      bool known_false = false;
      for (int i = 0; i < args.size(); ++i) {
        if (args[i].is_null()) {
          known_true = false;
        } else if (!args[i].bool_value()) {
          known_true = false;
          known_false = true;
        }
      }
      ABSL_DCHECK(!(known_true && known_false));
      *result = known_true
                    ? Value::Bool(true)
                    : (known_false ? Value::Bool(false) : Value::NullBool());
      return true;
    }
    case FCT(FunctionKind::kOr, TYPE_BOOL): {
      // Assume false and upgrade appropriately.
      bool known_true = false;
      bool known_false = true;
      for (int i = 0; i < args.size(); ++i) {
        if (args[i].is_null()) {
          known_false = false;
        } else if (args[i].bool_value()) {
          known_true = true;
          known_false = false;
        }
      }
      ABSL_DCHECK(!(known_true && known_false));
      *result = known_true
                    ? Value::Bool(true)
                    : (known_false ? Value::Bool(false) : Value::NullBool());
      return true;
    }
    case FCT(FunctionKind::kNot, TYPE_BOOL): {
      ABSL_DCHECK_EQ(1, args.size());
      *result =
          args[0].is_null() ? args[0] : Value::Bool(!args[0].bool_value());
      return true;
    }
  }
  *status = ::zetasql_base::UnimplementedErrorBuilder()
            << "Unsupported logical function: " << debug_name();
  return false;
}

std::string BuiltinAggregateFunction::debug_name() const {
  return BuiltinFunctionCatalog::GetDebugNameByKind(kind());
}

namespace {
// kOrAgg is an aggregate function used internally to execute IN subqueries and
// later ANY(SELECT ...) subqueries, once supported in ZetaSQL. The function
// does an OR of all input values including NULLs and returns false for empty
// input.

// Accumulator implementation for BuiltinAggregateFunction.
class BuiltinAggregateAccumulator : public AggregateAccumulator {
 public:
  static absl::StatusOr<std::unique_ptr<BuiltinAggregateAccumulator>> Create(
      const BuiltinAggregateFunction* function, const Type* input_type,
      absl::Span<const Value> args, CollatorList collator_list,
      EvaluationContext* context) {
    auto accumulator = absl::WrapUnique(new BuiltinAggregateAccumulator(
        function, input_type, args, std::move(collator_list), context));

    ZETASQL_RETURN_IF_ERROR(accumulator->Reset());
    return accumulator;
  }

  BuiltinAggregateAccumulator(const BuiltinAggregateAccumulator&) = delete;
  BuiltinAggregateAccumulator& operator=(const BuiltinAggregateAccumulator&) =
      delete;

  ~BuiltinAggregateAccumulator() override {
    accountant()->ReturnBytes(requested_bytes_);
  }

  // 'input_type' is the type of the Value argument to Accumulate().  'args'
  // contains any arguments to the aggregation function other than the argument
  // being aggregated. For example, 'args' contains the delimiter for
  // kStringAgg.
  absl::Status Reset() final;

  bool Accumulate(const Value& value, bool* stop_accumulation,
                  absl::Status* status) override;

  absl::StatusOr<Value> GetFinalResult(bool inputs_in_defined_order) override;

 private:

  BuiltinAggregateAccumulator(const BuiltinAggregateFunction* function,
                              const Type* input_type,
                              absl::Span<const Value> args,
                              CollatorList collator_list,
                              EvaluationContext* context)
      : function_(function),
        input_type_(input_type),
        args_(args.begin(), args.end()),
        collator_list_(std::move(collator_list)),
        context_(context) {}

  absl::StatusOr<Value> GetFinalResultInternal(bool inputs_in_defined_order);

  MemoryAccountant* accountant() { return context_->memory_accountant(); }

  const BuiltinAggregateFunction* function_;
  const Type* input_type_;
  const std::vector<Value> args_;
  // The collators used for aggregate functions with collations.
  CollatorList collator_list_;
  EvaluationContext* context_;

  // The number of bytes currently requested from 'accountant()'.
  int64_t requested_bytes_ = 0;

  // AnyValue
  Value any_value_;
  // Count of non-null values.
  int64_t count_ = 0;
  int64_t countif_ = 0;
  long double out_double_ = 0;         // Max, Min, Avg, CovarPop, CovarSamp
  zetasql_base::ExactFloat out_exact_float_ = 0;     // Sum
  long double avg_ = 0;                // VarPop, VarSamp, StddevPop, StddevSamp
  long double variance_ = 0;           // VarPop, VarSamp, StddevPop, StddevSamp
  int64_t out_int64_ = 0;              // Max, Min
  uint64_t out_uint64_ = 0;            // Max, Min
  DatetimeValue out_datetime_;         // Max, Min
  IntervalValue out_interval_;         // Max, Min
  __int128 out_int128_ = 0;            // Sum
  unsigned __int128 out_uint128_ = 0;  // Sum
  NumericValue out_numeric_;           // Min, Max
  BigNumericValue out_bignumeric_;     // Min, Max
  Value out_range_;                    // Min, Max
  NumericValue::SumAggregator numeric_aggregator_;                // Avg, Sum
  BigNumericValue::SumAggregator bignumeric_aggregator_;          // Avg, Sum
  IntervalValue::SumAggregator interval_aggregator_;              // Sum
  NumericValue::VarianceAggregator numeric_variance_aggregator_;  // Var, Stddev
  BigNumericValue::VarianceAggregator
      bignumeric_variance_aggregator_;  // Var, Stddev
  std::string out_string_ = "";         // Max, Min, StringAgg
  std::string delimiter_ = ",";         // StringAgg
  // OrAgg, AndAgg, LogicalOr, LogicalAnd.
  bool has_null_ = false;
  bool has_true_ = false;
  bool has_false_ = false;
  // Bitwise aggregates.
  int32_t bit_int32_ = 0;
  int64_t bit_int64_ = 0;
  uint32_t bit_uint32_ = 0;
  uint64_t bit_uint64_ = 0;
  // ArrayAgg and ArrayConcatAgg
  std::vector<Value> array_agg_;
  // Percentile.
  Value percentile_;
  std::vector<Value> percentile_population_;
  // Used for ANON_* functions from (broken link).
  std::unique_ptr<::differential_privacy::Algorithm<double>> anon_double_;
  std::unique_ptr<::differential_privacy::Algorithm<int64_t>> anon_int64_;
  // Used for WITH DIFFERENTIAL_PRIVACY functions from
  // (broken link)
  std::unique_ptr<::differential_privacy::Algorithm<double>> dp_double_;
  std::unique_ptr<::differential_privacy::Algorithm<int64_t>> dp_int64_;
  // An output array for Min, Max.
  Value min_max_out_array_;
};

// Accumulator implementation for user defined aggregates
class UserDefinedAggregateAccumulator : public AggregateAccumulator {
 public:
  static absl::StatusOr<std::unique_ptr<UserDefinedAggregateAccumulator>>
  Create(std::unique_ptr<AggregateFunctionEvaluator> aggregator,
         const Type* output_type, int num_input_fields) {
    auto accumulator = absl::WrapUnique(new UserDefinedAggregateAccumulator(
        std::move(aggregator), output_type, num_input_fields));

    ZETASQL_RETURN_IF_ERROR(accumulator->Reset());
    return accumulator;
  }

  absl::Status Reset() final { return aggregator_->Reset(); }

  bool Accumulate(const Value& value, bool* stop_accumulation,
                  absl::Status* status) override {
    ABSL_DCHECK(status != nullptr);
    // The format of 'value' depends on the number of aggregate function
    // arguments as follows:
    // -Nullary functions: 'value' is an empty struct, so we will pass in an
    // empty span.
    // -Unary functions: 'value' is the same type as the function argument
    // -N-ary functions: 'value' is a struct with n fields for n function
    // arguments and the type of each field corresponds to the types of the
    // function arguments.
    std::vector<const Value*> args;
    switch (num_input_fields_) {
      case 0: {
        break;
      }
      case 1: {
        args.push_back(&value);
        break;
      }
      default: {
        for (const Value& arg : value.fields()) {
          args.push_back(&arg);
        }
        break;
      }
    }

    absl::Status accumulate_status =
        aggregator_->Accumulate(absl::MakeSpan(args), stop_accumulation);
    if (!accumulate_status.ok()) {
      if (status != nullptr) {
        status->Update(accumulate_status);
      }
      return false;
    }
    return true;
  }

  absl::StatusOr<Value> GetFinalResult(bool inputs_in_defined_order) override {
    // inputs_in_defined_order is only relevant to compliance testing and
    // RQG therefore we can omit this in the public interface.
    ZETASQL_ASSIGN_OR_RETURN(Value result, aggregator_->GetFinalResult());

    // Check that the final result value is valid and consistent with the
    // expected output type. Since equality check on types can be expensive,
    // the type equality check is performed in debug mode only.
    bool invalid = !result.is_valid() ||
                   (ZETASQL_DEBUG_MODE && !output_type_->Equals(result.type()));
    ZETASQL_RET_CHECK(!invalid) << "User-defined aggregate function "
                        << "returned a bad result: " << result.DebugString(true)
                        << "\n"
                        << "Expected value of type: "
                        << output_type_->DebugString();
    return result;
  }

 private:
  explicit UserDefinedAggregateAccumulator(
      std::unique_ptr<AggregateFunctionEvaluator> aggregator,
      const Type* output_type, int num_input_fields)
      : aggregator_(std::move(aggregator)),
        output_type_(output_type),
        num_input_fields_(num_input_fields) {}

  std::unique_ptr<AggregateFunctionEvaluator> aggregator_;
  const Type* output_type_;
  int num_input_fields_;
};

class UserDefinedAggregateFunction : public AggregateFunctionBody {
 public:
  UserDefinedAggregateFunction(
      AggregateFunctionEvaluatorFactory evaluator_factory,
      const FunctionSignature& function_signature, const Type* output_type,
      int num_input_fields, const Type* input_type,
      absl::string_view function_name, bool ignores_null = true)
      : AggregateFunctionBody(output_type, num_input_fields, input_type,
                              ignores_null),
        evaluator_factory_(evaluator_factory),
        function_signature_(function_signature),
        function_name_(function_name),
        output_type_(output_type) {}

  UserDefinedAggregateFunction(const UserDefinedAggregateFunction&) = delete;
  UserDefinedAggregateFunction& operator=(const UserDefinedAggregateFunction&) =
      delete;

  std::string debug_name() const override { return function_name_; }

  absl::StatusOr<std::unique_ptr<AggregateAccumulator>> CreateAccumulator(
      absl::Span<const Value> args, CollatorList collator_list,
      EvaluationContext* context) const override {
    auto status_or_evaluator = evaluator_factory_(function_signature_);
    ZETASQL_RETURN_IF_ERROR(status_or_evaluator.status());
    std::unique_ptr<AggregateFunctionEvaluator> evaluator =
        std::move(status_or_evaluator.value());
    // This should never happen because we already check for null evaluator
    // in the algebrizer.
    ZETASQL_RET_CHECK(evaluator != nullptr);
    return UserDefinedAggregateAccumulator::Create(
        std::move(evaluator), output_type_, num_input_fields());
  }

 private:
  AggregateFunctionEvaluatorFactory evaluator_factory_;
  const FunctionSignature& function_signature_;
  const std::string function_name_;
  const Type* output_type_;
};

template <typename T>
static absl::Status SetAnonBuilderEpsilon(const Value& arg, T* builder) {
  if (arg.is_null()) {
    // We don't currently have a way to distinguish unspecified vs. explicitly
    // specified NULL value, so we always produce an error in this case.
    return ::zetasql_base::OutOfRangeErrorBuilder() << "Epsilon cannot be NULL";
  } else {
    // We check for NaN epsilon here because it will cause the privacy
    // libraries to ABSL_DCHECK fail.  The privacy libraries should also probably
    // not allow non-positive or non-finite values, but we don't check that
    // here for the reference implementation.
    if (std::isnan(arg.double_value())) {
      return ::zetasql_base::OutOfRangeErrorBuilder() << "Epsilon cannot be NaN";
    }
    builder->SetEpsilon(arg.double_value());
  }
  return absl::OkStatus();
}

template <typename T>
static absl::Status InitializeAnonBuilder(const std::vector<Value>& args,
                                          T* builder) {
  // The last two args represent 'delta' and 'epsilon'.  If clamping
  // bounds are explicitly set, then there will be two additional args
  // that are args 0 and 1.
  // TODO: Remove the delta argument.  When delta is set, we
  // compute k_threshold from delta/epsilon/kappa, and the delta value
  // is no longer relevant to the function itself.
  ZETASQL_RET_CHECK(args.size() == 2 || args.size() == 4) << args.size();

  int epsilon_offset = 1;

  if (args.size() == 4) {
    if (args[0].type()->IsDouble()) {
      ZETASQL_RET_CHECK(args[1].type()->IsDouble()) << args[1].type()->DebugString();
      builder->SetLower(args[0].double_value())
          .SetUpper(args[1].double_value());
    } else {
      ZETASQL_RET_CHECK(args[0].type()->IsInt64()) << args[0].type()->DebugString();
      ZETASQL_RET_CHECK(args[1].type()->IsInt64()) << args[1].type()->DebugString();
      builder->SetLower(args[0].int64_value()).SetUpper(args[1].int64_value());
    }
    epsilon_offset = 3;
  }

  return SetAnonBuilderEpsilon<T>(args[epsilon_offset], builder);
}

template <>
absl::Status
InitializeAnonBuilder<::differential_privacy::Quantiles<double>::Builder>(
    const std::vector<Value>& args,
    ::differential_privacy::Quantiles<double>::Builder* builder) {
  // The current implementation always expects 5 arguments (until b/205277450 is
  // fixed and optional clamping bounds is supported):
  //   * args[0] is the input value to quantiles
  //   * args[1] is the lower clamped bound
  //   * args[2] is the upper clamped bound
  //   * args[3] is delta
  //   * args[4] is epsilon
  // TODO: Remove the delta argument.  When delta is set, we
  // compute k_threshold from delta/epsilon/kappa, and the delta value
  // is no longer relevant to the function itself.

  ZETASQL_RET_CHECK(args.size() == 5) << args.size();

  // Create a vector of n+1 quantile boundaries, representing the requested n
  // quantiles.
  double number_of_quantile_boundaries = args[0].int64_value() + 1;
  std::vector<double> quantiles;
  for (double i = 0; i < number_of_quantile_boundaries; ++i) {
    quantiles.push_back(i / number_of_quantile_boundaries);
  }

  builder->SetQuantiles(quantiles);

  int epsilon_offset = 2;

  // TODO: Add "if (args.size() == 5)" clause for these four lines
  //   when optional lower and upper bounds can be supported, since then there
  //   may only be three args, instead of assuming there will always be five
  //   args.
  ZETASQL_RET_CHECK(args[1].type()->IsDouble()) << args[1].type()->DebugString();
  ZETASQL_RET_CHECK(args[2].type()->IsDouble()) << args[2].type()->DebugString();
  builder->SetLower(args[1].double_value()).SetUpper(args[2].double_value());
  epsilon_offset = 4;

  return SetAnonBuilderEpsilon<
      ::differential_privacy::Quantiles<double>::Builder>(args[epsilon_offset],
                                                          builder);
}

// For functions which return an array of elements from their per-user
// aggregation, we need to set MaxContributionsPerPartition to the length of the
// array.
// TODO: Plumb the value through rather than relying on the
// constant.
template <typename T>
absl::Status InitializeAnonBuilderForArrayFunction(
    const std::vector<Value>& args, T* builder) {
  builder->SetMaxContributionsPerPartition(
      anonymization::kPerUserArrayAggLimit);
  return InitializeAnonBuilder(args, builder);
}

// Returns the type of first argument to the member function.
template <class ObjectType, class ArgType, class Result>
constexpr ArgType GetMemberFunctionFirstArgumentType(
    Result (ObjectType::*)(ArgType));

// Checks if contribution bounds have correct types and sets them on the
// builder.
template <class Builder>
absl::Status CheckAndSetContributionBounds(const Value& value,
                                           Builder* builder) {
  if (value.is_null()) {
    return absl::OkStatus();
  }
  ZETASQL_RET_CHECK(value.type()->IsStruct()) << value.type()->DebugString();
  ZETASQL_RET_CHECK(value.num_fields() == 2) << value.DebugString();
  // TODO: Add support for other DP signatures here.
  if (!value.field(0).type()->IsDouble() && !value.field(0).type()->IsInt64()) {
    return absl::OutOfRangeError(
        absl::StrCat("Contribution bounds can only be INT64 or DOUBLE but is: ",
                     value.field(0).type()->DebugString()));
  }
  if (value.field(0).type()->IsDouble()) {
    ZETASQL_RET_CHECK(value.field(1).type()->IsDouble())
        << value.field(1).type()->DebugString();
    // Getting the arg type is necessary to silence compiler warning that we are
    // implicitly converting double to int64_t. We use static cast to make it
    // explicit.
    using BuilderArgType =
        std::decay_t<decltype(GetMemberFunctionFirstArgumentType(
            &Builder::SetLower))>;
    builder
        ->SetLower(static_cast<BuilderArgType>(value.field(0).double_value()))
        .SetUpper(static_cast<BuilderArgType>(value.field(1).double_value()));
    return absl::OkStatus();
  }
  ZETASQL_RET_CHECK(value.field(0).type()->IsInt64())
      << value.field(0).type()->DebugString();
  ZETASQL_RET_CHECK(value.field(1).type()->IsInt64())
      << value.field(1).type()->DebugString();
  builder->SetLower(value.field(0).int64_value())
      .SetUpper(value.field(1).int64_value());

  return absl::OkStatus();
}

bool IsDPProtoReportFunction(const Type* output_type) {
  return output_type != nullptr && output_type->IsProto() &&
         output_type->AsProto()->descriptor()->full_name() ==
             zetasql::functions::DifferentialPrivacyOutputWithReport::
                 GetDescriptor()
                     ->full_name();
}
bool IsDPJsonReportFunction(const Type* output_type) {
  return output_type != nullptr && output_type->IsJson();
}

bool IsDPReportFunction(const Type* output_type) {
  return IsDPProtoReportFunction(output_type) ||
         IsDPJsonReportFunction(output_type);
}

// Returns true if both class templates are the same. e.g.
template <template <class...> class T1, template <class...> class T2>
inline constexpr bool is_same_template_v = false;

template <template <class...> class T>
inline constexpr bool is_same_template_v<T, T> = true;

static_assert(is_same_template_v<::differential_privacy::BoundedVariance,
                                 ::differential_privacy::BoundedVariance>);
static_assert(
    !is_same_template_v<::differential_privacy::BoundedVariance,
                        ::differential_privacy::BoundedStandardDeviation>);

template <template <class...> class T>
inline constexpr bool kIsDPArrayAlgorithm =
    is_same_template_v<T, ::differential_privacy::BoundedVariance> ||
    is_same_template_v<T, ::differential_privacy::BoundedStandardDeviation> ||
    is_same_template_v<T, ::differential_privacy::Quantiles>;

template <template <class> class AlgorithmType, class ValueType>
absl::StatusOr<std::unique_ptr<::differential_privacy::Algorithm<ValueType>>>
BuildDPAlgorithm(bool is_report_function, const std::vector<Value>& args,
                 int expected_base_arg_count,
                 int contribution_bounds_base_offset,
                 typename AlgorithmType<ValueType>::Builder&& builder) {
  // For functions which return an array of elements from their per-user
  // aggregation, we need to set MaxContributionsPerPartition to the length
  // of the array.
  // TODO: Plumb the value through rather than relying on the
  // constant.
  if constexpr (kIsDPArrayAlgorithm<AlgorithmType>) {
    builder.SetMaxContributionsPerPartition(
        anonymization::kPerUserArrayAggLimit);
  }
  // The last two args represent 'delta' and 'epsilon'.  If clamping
  // bounds are explicitly set, then there will be one additional arg at 0.
  // TODO: Remove the delta argument. When delta is set, we compute
  // group_selection_threshold from delta/epsilon/max_groups_contributed, and
  // the delta value is no longer relevant to the function itself.

  const int report_offset = is_report_function ? 1 : 0;
  const int expected_arg_count_report =
      expected_base_arg_count + (is_report_function ? 1 : 0);

  ZETASQL_RET_CHECK(args.size() == expected_arg_count_report ||
            args.size() == (expected_arg_count_report + 1))
      << args.size();
  const size_t epsilon_offset = args.size() - 1;
  if (args.size() == (expected_arg_count_report + 1)) {
    const int contribution_bounds_offset =
        contribution_bounds_base_offset + report_offset;
    ZETASQL_RETURN_IF_ERROR(CheckAndSetContributionBounds(
        args[contribution_bounds_offset], &builder));
  }
  ZETASQL_RETURN_IF_ERROR(SetAnonBuilderEpsilon(args[epsilon_offset], &builder));
  return builder.Build();
}

template <template <class> class AlgorithmType, class ValueType>
absl::StatusOr<std::unique_ptr<::differential_privacy::Algorithm<ValueType>>>
BuildDPAlgorithm(bool is_report_function, const std::vector<Value>& args) {
  return BuildDPAlgorithm<AlgorithmType, ValueType>(
      is_report_function, args,
      /*expected_base_arg_count=*/2,
      /*contribution_bounds_base_offset=*/0,
      typename AlgorithmType<ValueType>::Builder());
}

template <>
absl::StatusOr<std::unique_ptr<::differential_privacy::Algorithm<double>>>
BuildDPAlgorithm<::differential_privacy::Quantiles, double>(
    bool is_report_function, const std::vector<Value>& args) {
  ::differential_privacy::Quantiles<double>::Builder builder;
  // The current implementation always expects 4 arguments (until b/205277450 is
  // fixed and optional contribution bounds are supported).
  const int report_offset = is_report_function ? 1 : 0;
  ZETASQL_RET_CHECK(args.size() == (4 + report_offset)) << args.size();
  // Create a vector of n+1 quantile boundaries, representing the requested n
  // quantiles.
  double number_of_quantile_boundaries = args[0].int64_value() + 1;
  std::vector<double> quantiles;
  for (double i = 0; i < number_of_quantile_boundaries; ++i) {
    quantiles.push_back(i / number_of_quantile_boundaries);
  }
  builder.SetQuantiles(quantiles);
  return BuildDPAlgorithm<::differential_privacy::Quantiles, double>(
      is_report_function, args,
      /*expected_base_arg_count=*/3,
      /*contribution_bounds_base_offset=*/1, std::move(builder));
}

absl::Status BuiltinAggregateAccumulator::Reset() {
  accountant()->ReturnBytes(requested_bytes_);
  requested_bytes_ = sizeof(*this);
  absl::Status status;
  if (!accountant()->RequestBytes(requested_bytes_, &status)) {
    requested_bytes_ = 0;
    return status;
  }

  // For performance, we only initialize the member values that are needed by
  // the other methods. This is important when evaluating queries like:
  //   SELECT ARRAY_AGG(foo HAVING MAX bar) FROM Table
  // because the aggregation will be reset every time we see a new MAX value of
  // bar.
  count_ = 0;
  has_null_ = false;

  switch (function_->kind()) {
    case FunctionKind::kAnyValue:
      any_value_ = Value::Invalid();
      break;
    case FunctionKind::kArrayAgg:
    case FunctionKind::kArrayConcatAgg:
      array_agg_.clear();
      break;
    case FunctionKind::kStringAgg: {
      out_string_.clear();
      if (!args_.empty()) {
        if (args_[0].is_null()) {
          return ::zetasql_base::InvalidArgumentErrorBuilder()
                 << "Illegal NULL separator in STRING_AGG";
        }
        delimiter_ = (function_->output_type()->kind() == TYPE_STRING)
                         ? args_[0].string_value()
                         : args_[0].bytes_value();
      }
      break;
    }
    case FunctionKind::kBitOr:
    case FunctionKind::kBitXor:
      bit_int32_ = 0;
      bit_int64_ = 0;
      bit_uint32_ = 0;
      bit_uint64_ = 0;
      break;
    case FunctionKind::kBitAnd:
      // Initialize the bit variables to all ones.
      bit_int32_ = bit_uint32_ = 0xffffffff;
      bit_int64_ = bit_uint64_ = 0xffffffffffffffff;
      break;
    case FunctionKind::kOrAgg:
    case FunctionKind::kLogicalOr:
      has_true_ = false;
      break;
    case FunctionKind::kAndAgg:
    case FunctionKind::kLogicalAnd:
      has_false_ = false;
      break;
    default:
      break;
  }

  switch (FCT(function_->kind(), input_type_->kind())) {
    case FCT(FunctionKind::kCountIf, TYPE_BOOL):
      countif_ = 0;
      break;

      // Max
    case FCT(FunctionKind::kMax, TYPE_FLOAT):
    case FCT(FunctionKind::kMax, TYPE_DOUBLE):
      out_double_ = -std::numeric_limits<long double>::infinity();
      break;
    case FCT(FunctionKind::kMax, TYPE_UINT64):
      out_uint64_ = 0;
      break;
    case FCT(FunctionKind::kMax, TYPE_INT32):
    case FCT(FunctionKind::kMax, TYPE_INT64):
    case FCT(FunctionKind::kMax, TYPE_UINT32):
    case FCT(FunctionKind::kMax, TYPE_DATE):
    case FCT(FunctionKind::kMax, TYPE_BOOL):
    case FCT(FunctionKind::kMax, TYPE_ENUM):
    case FCT(FunctionKind::kMax, TYPE_TIMESTAMP):
    case FCT(FunctionKind::kMax, TYPE_TIME):
      out_int64_ = std::numeric_limits<int64_t>::lowest();
      break;
    case FCT(FunctionKind::kMax, TYPE_NUMERIC):
      out_numeric_ = NumericValue::MinValue();
      break;
    case FCT(FunctionKind::kMax, TYPE_BIGNUMERIC):
      out_bignumeric_ = BigNumericValue::MinValue();
      break;
    case FCT(FunctionKind::kMax, TYPE_DATETIME):
      out_datetime_ = DatetimeValue::FromYMDHMSAndNanos(1, 1, 1, 0, 0, 0, 0);
      break;
    case FCT(FunctionKind::kMax, TYPE_INTERVAL):
      out_interval_ = IntervalValue::MinValue();
      break;
    case FCT(FunctionKind::kMax, TYPE_STRING):
    case FCT(FunctionKind::kMax, TYPE_BYTES):
      out_string_.clear();
      break;
    case FCT(FunctionKind::kMax, TYPE_ARRAY):
      min_max_out_array_ = Value::Invalid();
      break;
    case FCT(FunctionKind::kMax, TYPE_RANGE):
      out_range_ = Value::Invalid();
      break;

      // Min
    case FCT(FunctionKind::kMin, TYPE_FLOAT):
    case FCT(FunctionKind::kMin, TYPE_DOUBLE):
      out_double_ = std::numeric_limits<long double>::infinity();
      break;
    case FCT(FunctionKind::kMin, TYPE_UINT64):
      out_uint64_ = std::numeric_limits<uint64_t>::max();
      break;
    case FCT(FunctionKind::kMin, TYPE_INT32):
    case FCT(FunctionKind::kMin, TYPE_INT64):
    case FCT(FunctionKind::kMin, TYPE_UINT32):
    case FCT(FunctionKind::kMin, TYPE_DATE):
    case FCT(FunctionKind::kMin, TYPE_BOOL):
    case FCT(FunctionKind::kMin, TYPE_ENUM):
    case FCT(FunctionKind::kMin, TYPE_TIMESTAMP):
    case FCT(FunctionKind::kMin, TYPE_TIME):
      out_int64_ = std::numeric_limits<int64_t>::max();
      break;
    case FCT(FunctionKind::kMin, TYPE_NUMERIC):
      out_numeric_ = NumericValue::MaxValue();
      break;
    case FCT(FunctionKind::kMin, TYPE_BIGNUMERIC):
      out_bignumeric_ = BigNumericValue::MaxValue();
      break;
    case FCT(FunctionKind::kMin, TYPE_DATETIME):
      out_datetime_ = DatetimeValue::FromYMDHMSAndNanos(9999, 12, 31, 23, 59,
                                                        59, 999999999);
      break;
    case FCT(FunctionKind::kMin, TYPE_INTERVAL):
      out_interval_ = IntervalValue::MaxValue();
      break;
    case FCT(FunctionKind::kMin, TYPE_STRING):
    case FCT(FunctionKind::kMin, TYPE_BYTES):
      out_string_.clear();
      break;
    case FCT(FunctionKind::kMin, TYPE_ARRAY):
      min_max_out_array_ = Value::Invalid();
      break;
    case FCT(FunctionKind::kMin, TYPE_RANGE):
      out_range_ = Value::Invalid();
      break;

      // Avg and Sum
    case FCT(FunctionKind::kAvg, TYPE_INT64):
    case FCT(FunctionKind::kAvg, TYPE_UINT64):
    case FCT(FunctionKind::kAvg, TYPE_DOUBLE):
      out_double_ = 0;
      break;
    case FCT(FunctionKind::kAvg, TYPE_NUMERIC):
      numeric_aggregator_ = NumericValue::SumAggregator();
      break;
    case FCT(FunctionKind::kAvg, TYPE_BIGNUMERIC):
      bignumeric_aggregator_ = BigNumericValue::SumAggregator();
      break;
    case FCT(FunctionKind::kAvg, TYPE_INTERVAL):
      interval_aggregator_ = IntervalValue::SumAggregator();
      break;

    // Sum
    case FCT(FunctionKind::kSum, TYPE_DOUBLE):
      out_exact_float_ = 0;
      break;
    case FCT(FunctionKind::kSum, TYPE_INT64):
      out_int128_ = 0;
      break;
    case FCT(FunctionKind::kSum, TYPE_UINT64):
      out_uint128_ = 0;
      break;
    case FCT(FunctionKind::kSum, TYPE_NUMERIC):
      numeric_aggregator_ = NumericValue::SumAggregator();
      break;
    case FCT(FunctionKind::kSum, TYPE_BIGNUMERIC):
      bignumeric_aggregator_ = BigNumericValue::SumAggregator();
      break;
    case FCT(FunctionKind::kSum, TYPE_INTERVAL):
      interval_aggregator_ = IntervalValue::SumAggregator();
      break;

    // Variance and standard deviation.
    case FCT(FunctionKind::kStddevPop, TYPE_DOUBLE):
    case FCT(FunctionKind::kStddevSamp, TYPE_DOUBLE):
    case FCT(FunctionKind::kVarPop, TYPE_DOUBLE):
    case FCT(FunctionKind::kVarSamp, TYPE_DOUBLE):
      avg_ = 0;
      variance_ = 0;
      break;
    case FCT(FunctionKind::kStddevPop, TYPE_NUMERIC):
    case FCT(FunctionKind::kStddevSamp, TYPE_NUMERIC):
    case FCT(FunctionKind::kVarPop, TYPE_NUMERIC):
    case FCT(FunctionKind::kVarSamp, TYPE_NUMERIC):
      numeric_variance_aggregator_ = NumericValue::VarianceAggregator();
      break;
    case FCT(FunctionKind::kStddevPop, TYPE_BIGNUMERIC):
    case FCT(FunctionKind::kStddevSamp, TYPE_BIGNUMERIC):
    case FCT(FunctionKind::kVarPop, TYPE_BIGNUMERIC):
    case FCT(FunctionKind::kVarSamp, TYPE_BIGNUMERIC):
      bignumeric_variance_aggregator_ = BigNumericValue::VarianceAggregator();
      break;

    // PERCENTILE_CONT.
    case FCT(FunctionKind::kPercentileCont, TYPE_DOUBLE):
      ZETASQL_RET_CHECK_EQ(args_.size(), 1);
      ZETASQL_RET_CHECK(args_[0].type()->IsDouble());
      percentile_ = args_[0];
      break;
    case FCT(FunctionKind::kPercentileCont, TYPE_NUMERIC):
      ZETASQL_RET_CHECK_EQ(args_.size(), 1);
      ZETASQL_RET_CHECK(args_[0].type()->IsNumericType());
      percentile_ = args_[0];
      break;
    case FCT(FunctionKind::kPercentileCont, TYPE_BIGNUMERIC):
      ZETASQL_RET_CHECK_EQ(args_.size(), 1);
      ZETASQL_RET_CHECK(args_[0].type()->IsBigNumericType());
      percentile_ = args_[0];
      break;

    // Anonymization functions.
    case FCT(FunctionKind::kAnonSumWithReportProto, TYPE_DOUBLE):
    case FCT(FunctionKind::kAnonSumWithReportJson, TYPE_DOUBLE):
    case FCT(FunctionKind::kAnonSum, TYPE_DOUBLE): {
      ::differential_privacy::BoundedSum<double>::Builder builder;
      ZETASQL_RETURN_IF_ERROR(InitializeAnonBuilder<>(args_, &builder));
      ZETASQL_ASSIGN_OR_RETURN(anon_double_, builder.Build());
      break;
    }
    case FCT(FunctionKind::kAnonSumWithReportProto, TYPE_INT64):
    case FCT(FunctionKind::kAnonSumWithReportJson, TYPE_INT64):
    case FCT(FunctionKind::kAnonSum, TYPE_INT64): {
      ::differential_privacy::BoundedSum<int64_t>::Builder builder;
      ZETASQL_RETURN_IF_ERROR(InitializeAnonBuilder<>(args_, &builder));
      ZETASQL_ASSIGN_OR_RETURN(anon_int64_, builder.Build());
      break;
    }
    case FCT(FunctionKind::kAnonAvgWithReportProto, TYPE_DOUBLE):
    case FCT(FunctionKind::kAnonAvgWithReportJson, TYPE_DOUBLE):
    case FCT(FunctionKind::kAnonAvg, TYPE_DOUBLE): {
      ::differential_privacy::BoundedMean<double>::Builder builder;
      ZETASQL_RETURN_IF_ERROR(InitializeAnonBuilder<>(args_, &builder));
      ZETASQL_ASSIGN_OR_RETURN(anon_double_, builder.Build());
      break;
    }
    case FCT(FunctionKind::kAnonVarPop, TYPE_ARRAY): {
      ZETASQL_RET_CHECK(input_type_->AsArray()->element_type()->IsDouble());
      ::differential_privacy::BoundedVariance<double>::Builder builder;
      ZETASQL_RETURN_IF_ERROR(InitializeAnonBuilderForArrayFunction<>(args_, &builder));
      ZETASQL_ASSIGN_OR_RETURN(anon_double_, builder.Build());
      break;
    }
    case FCT(FunctionKind::kAnonStddevPop, TYPE_ARRAY): {
      ZETASQL_RET_CHECK(input_type_->AsArray()->element_type()->IsDouble());
      ::differential_privacy::BoundedStandardDeviation<double>::Builder builder;
      ZETASQL_RETURN_IF_ERROR(InitializeAnonBuilderForArrayFunction<>(args_, &builder));
      ZETASQL_ASSIGN_OR_RETURN(anon_double_, builder.Build());
      break;
    }
    case FCT(FunctionKind::kAnonQuantilesWithReportProto, TYPE_ARRAY):
    case FCT(FunctionKind::kAnonQuantilesWithReportJson, TYPE_ARRAY):
    case FCT(FunctionKind::kAnonQuantiles, TYPE_ARRAY): {
      ::differential_privacy::Quantiles<double>::Builder builder;
      ZETASQL_RETURN_IF_ERROR(InitializeAnonBuilderForArrayFunction<>(args_, &builder));
      ZETASQL_ASSIGN_OR_RETURN(anon_double_, builder.Build());
      break;
    }

    // DifferentialPrivacy functions.
    case FCT(FunctionKind::kDifferentialPrivacySum, TYPE_DOUBLE): {
      ZETASQL_ASSIGN_OR_RETURN(
          dp_double_,
          (BuildDPAlgorithm<::differential_privacy::BoundedSum, double>(
              IsDPReportFunction(function_->output_type()), args_)));
      break;
    }
    case FCT(FunctionKind::kDifferentialPrivacySum, TYPE_INT64): {
      ZETASQL_ASSIGN_OR_RETURN(
          dp_int64_,
          (BuildDPAlgorithm<::differential_privacy::BoundedSum, int64_t>(
              IsDPReportFunction(function_->output_type()), args_)));
      break;
    }

    case FCT(FunctionKind::kDifferentialPrivacyAvg, TYPE_DOUBLE): {
      ZETASQL_ASSIGN_OR_RETURN(
          dp_double_,
          (BuildDPAlgorithm<::differential_privacy::BoundedMean, double>(
              IsDPReportFunction(function_->output_type()), args_)));
      break;
    }

    case FCT(FunctionKind::kDifferentialPrivacyVarPop, TYPE_ARRAY): {
      ZETASQL_RET_CHECK(input_type_->AsArray()->element_type()->IsDouble());
      ZETASQL_ASSIGN_OR_RETURN(
          dp_double_,
          (BuildDPAlgorithm<::differential_privacy::BoundedVariance, double>(
              IsDPReportFunction(function_->output_type()), args_)));
      break;
    }
    case FCT(FunctionKind::kDifferentialPrivacyStddevPop, TYPE_ARRAY): {
      ZETASQL_RET_CHECK(input_type_->AsArray()->element_type()->IsDouble());
      ZETASQL_ASSIGN_OR_RETURN(
          dp_double_,
          (BuildDPAlgorithm<::differential_privacy::BoundedStandardDeviation,
                            double>(
              IsDPReportFunction(function_->output_type()), args_)));
      break;
    }
    case FCT(FunctionKind::kDifferentialPrivacyQuantiles, TYPE_ARRAY): {
      ZETASQL_ASSIGN_OR_RETURN(
          dp_double_,
          (BuildDPAlgorithm<::differential_privacy::Quantiles, double>(
              IsDPReportFunction(function_->output_type()), args_)));
      break;
    }
  }

  return absl::OkStatus();
}

bool BuiltinAggregateAccumulator::Accumulate(const Value& value,
                                             bool* stop_accumulation,
                                             absl::Status* status) {
  *stop_accumulation = false;

  int64_t bytes_to_return = 0;
  int64_t additional_bytes_to_request = 0;

  switch (function_->kind()) {
    case FunctionKind::kAnyValue: {
      if (!any_value_.is_valid()) {
        any_value_ = value;  // Take the first value, possibly NULL.
        additional_bytes_to_request = value.physical_byte_size();
      } else if (!any_value_.Equals(value)) {
        // At least two distinct values in the input.
        context_->SetNonDeterministicOutput();
        *stop_accumulation = true;
      }
      break;
    }
    case FunctionKind::kArrayAgg:
    case FunctionKind::kArrayConcatAgg: {
      additional_bytes_to_request = value.physical_byte_size();
      array_agg_.push_back(value);
      break;
    }
    case FunctionKind::kPercentileDisc:
    case FunctionKind::kPercentileCont:
      percentile_population_.push_back(value);
      break;
    default:
      break;
  }

  if (value.is_null()) {
    has_null_ = true;
    accountant()->ReturnBytes(bytes_to_return);
    requested_bytes_ -= bytes_to_return;
    if (!accountant()->RequestBytes(additional_bytes_to_request, status)) {
      return false;
    }
    requested_bytes_ += additional_bytes_to_request;
    return true;
  }

  ++count_;
  switch (FCT(function_->kind(), input_type_->kind())) {
    // Avg
    case FCT(FunctionKind::kAvg, TYPE_INT64):
    case FCT(FunctionKind::kAvg, TYPE_UINT64):
    case FCT(FunctionKind::kAvg, TYPE_DOUBLE): {
      // Iterative algorithm that is less likely to overflow in the common
      // case (lots of values of similar magnitude), and is supposedly
      // attributed to Knuth.
      long double delta;
      if (!functions::Subtract((long double)value.ToDouble(), out_double_,
                               &delta, status) ||
          !functions::Add(out_double_, delta / count_, &out_double_, status)) {
        return false;
      }
      break;
    }
    case FCT(FunctionKind::kAvg, TYPE_NUMERIC): {
      // For Numeric type the sum is accumulated in numeric_aggregator, then
      // divided by count at the end.
      numeric_aggregator_.Add(value.numeric_value());
      break;
    }
    case FCT(FunctionKind::kAvg, TYPE_BIGNUMERIC): {
      // For BigNumeric type the sum is accumulated in bignumeric_aggregator,
      // then divided by count at the end.
      bignumeric_aggregator_.Add(value.bignumeric_value());
      break;
    }
    case FCT(FunctionKind::kAvg, TYPE_INTERVAL): {
      // For Interval type the sum is accumulated in interval_aggregator, then
      // divided by count at the end.
      interval_aggregator_.Add(value.interval_value());
      break;
    }
    // Variance and Stddev
    case FCT(FunctionKind::kStddevPop, TYPE_DOUBLE):
    case FCT(FunctionKind::kStddevSamp, TYPE_DOUBLE):
    case FCT(FunctionKind::kVarPop, TYPE_DOUBLE):
    case FCT(FunctionKind::kVarSamp, TYPE_DOUBLE): {
      *status =
          UpdateMeanAndVariance(value.ToDouble(), count_, &avg_, &variance_);
      if (!status->ok()) return false;
      break;
    }
    // Variance and Stddev for NumericValue
    case FCT(FunctionKind::kStddevPop, TYPE_NUMERIC):
    case FCT(FunctionKind::kStddevSamp, TYPE_NUMERIC):
    case FCT(FunctionKind::kVarPop, TYPE_NUMERIC):
    case FCT(FunctionKind::kVarSamp, TYPE_NUMERIC): {
      numeric_variance_aggregator_.Add(value.numeric_value());
      break;
    }
    // Variance and Stddev for BigNumericValue
    case FCT(FunctionKind::kStddevPop, TYPE_BIGNUMERIC):
    case FCT(FunctionKind::kStddevSamp, TYPE_BIGNUMERIC):
    case FCT(FunctionKind::kVarPop, TYPE_BIGNUMERIC):
    case FCT(FunctionKind::kVarSamp, TYPE_BIGNUMERIC): {
      bignumeric_variance_aggregator_.Add(value.bignumeric_value());
      break;
    }
    // Bitwise aggregates.
    case FCT(FunctionKind::kBitAnd, TYPE_INT32): {
      if (!functions::BitwiseAnd(bit_int32_, value.int32_value(), &bit_int32_,
                                 status)) {
        return false;
      }
      break;
    }
    case FCT(FunctionKind::kBitAnd, TYPE_INT64): {
      if (!functions::BitwiseAnd(bit_int64_, value.int64_value(), &bit_int64_,
                                 status)) {
        return false;
      }
      break;
    }
    case FCT(FunctionKind::kBitAnd, TYPE_UINT32): {
      if (!functions::BitwiseAnd(bit_uint32_, value.uint32_value(),
                                 &bit_uint32_, status)) {
        return false;
      }
      break;
    }
    case FCT(FunctionKind::kBitAnd, TYPE_UINT64): {
      if (!functions::BitwiseAnd(bit_uint64_, value.uint64_value(),
                                 &bit_uint64_, status)) {
        return false;
      }
      break;
    }
    case FCT(FunctionKind::kBitOr, TYPE_INT32): {
      if (!functions::BitwiseOr(bit_int32_, value.int32_value(), &bit_int32_,
                                status)) {
        return false;
      }
      break;
    }
    case FCT(FunctionKind::kBitOr, TYPE_INT64): {
      if (!functions::BitwiseOr(bit_int64_, value.int64_value(), &bit_int64_,
                                status)) {
        return false;
      }
      break;
    }
    case FCT(FunctionKind::kBitOr, TYPE_UINT32): {
      if (!functions::BitwiseOr(bit_uint32_, value.uint32_value(), &bit_uint32_,
                                status)) {
        return false;
      }
      break;
    }
    case FCT(FunctionKind::kBitOr, TYPE_UINT64): {
      if (!functions::BitwiseOr(bit_uint64_, value.uint64_value(), &bit_uint64_,
                                status)) {
        return false;
      }
      break;
    }
    case FCT(FunctionKind::kBitXor, TYPE_INT32): {
      if (count_ == 1) {
        bit_int32_ = value.int32_value();
      } else if (!functions::BitwiseXor(bit_int32_, value.int32_value(),
                                        &bit_int32_, status)) {
        return false;
      }
      break;
    }
    case FCT(FunctionKind::kBitXor, TYPE_INT64): {
      if (count_ == 1) {
        bit_int64_ = value.int64_value();
      } else if (!functions::BitwiseXor(bit_int64_, value.int64_value(),
                                        &bit_int64_, status)) {
        return false;
      }
      break;
    }
    case FCT(FunctionKind::kBitXor, TYPE_UINT32): {
      if (count_ == 1) {
        bit_uint32_ = value.uint32_value();
      } else if (!functions::BitwiseXor(bit_uint32_, value.uint32_value(),
                                        &bit_uint32_, status)) {
        return false;
      }
      break;
    }
    case FCT(FunctionKind::kBitXor, TYPE_UINT64): {
      if (count_ == 1) {
        bit_uint64_ = value.uint64_value();
      } else if (!functions::BitwiseXor(bit_uint64_, value.uint64_value(),
                                        &bit_uint64_, status)) {
        return false;
      }
      break;
    }
    case FCT(FunctionKind::kCountIf, TYPE_BOOL): {
      countif_ += (value.bool_value() ? 1 : 0);
      break;
    }
      // Max
    case FCT(FunctionKind::kMax, TYPE_FLOAT):
    case FCT(FunctionKind::kMax, TYPE_DOUBLE): {
      if (std::isnan(value.ToDouble()) || std::isnan(out_double_)) {
        out_double_ = std::numeric_limits<long double>::quiet_NaN();
      } else {
        out_double_ = std::max(out_double_, (long double)value.ToDouble());
      }
      break;
    }
    case FCT(FunctionKind::kMax, TYPE_INT32):
    case FCT(FunctionKind::kMax, TYPE_INT64):
    case FCT(FunctionKind::kMax, TYPE_UINT32):
    case FCT(FunctionKind::kMax, TYPE_DATE):
    case FCT(FunctionKind::kMax, TYPE_BOOL):
    case FCT(FunctionKind::kMax, TYPE_ENUM): {
      out_int64_ = std::max(out_int64_, value.ToInt64());
      break;
    }
    case FCT(FunctionKind::kMax, TYPE_NUMERIC): {
      out_numeric_ = std::max(value.numeric_value(), out_numeric_);
      break;
    }
    case FCT(FunctionKind::kMax, TYPE_BIGNUMERIC): {
      out_bignumeric_ = std::max(value.bignumeric_value(), out_bignumeric_);
      break;
    }
    case FCT(FunctionKind::kMax, TYPE_TIMESTAMP): {
      out_int64_ = std::max(out_int64_, value.ToUnixMicros());
      break;
    }
    case FCT(FunctionKind::kMax, TYPE_DATETIME): {
      out_datetime_ = Value::Datetime(out_datetime_).LessThan(value)
                          ? value.datetime_value()
                          : out_datetime_;
      break;
    }
    case FCT(FunctionKind::kMax, TYPE_TIME): {
      out_int64_ = std::max(out_int64_, value.time_value().Packed64TimeNanos());
      break;
    }
    case FCT(FunctionKind::kMax, TYPE_INTERVAL): {
      out_interval_ = std::max(out_interval_, value.interval_value());
      break;
    }
    case FCT(FunctionKind::kMax, TYPE_UINT64): {
      out_uint64_ = std::max(out_uint64_, value.uint64_value());
      break;
    }
    case FCT(FunctionKind::kMax, TYPE_STRING): {
      bytes_to_return = out_string_.size();
      if (count_ <= 1) {
        out_string_ = value.string_value();
      } else if (collator_list_.empty()) {
        out_string_ = std::max(out_string_, value.string_value());
      } else {
        int64_t result = collator_list_[0]->CompareUtf8(value.string_value(),
                                                        out_string_, status);
        if (!status->ok()) {
          return false;
        }
        if (result > 0) {
          out_string_ = value.string_value();
        }
      }
      additional_bytes_to_request = out_string_.size();
      break;
    }
    case FCT(FunctionKind::kMax, TYPE_BYTES): {
      bytes_to_return = out_string_.size();
      out_string_ = std::max(out_string_, value.bytes_value());
      additional_bytes_to_request = out_string_.size();
      break;
    }
    case FCT(FunctionKind::kMax, TYPE_ARRAY): {
      if (count_ == 1) {
        min_max_out_array_ = value;
        additional_bytes_to_request = value.physical_byte_size();
      } else {
        if (min_max_out_array_.LessThan(value)) {
          bytes_to_return = min_max_out_array_.physical_byte_size();
          min_max_out_array_ = value;
          additional_bytes_to_request = min_max_out_array_.physical_byte_size();
        }
      }
      break;
    }
    case FCT(FunctionKind::kMax, TYPE_RANGE): {
      if (!out_range_.is_valid()) {
        out_range_ = value;
      } else {
        const Value comparison_result = out_range_.SqlLessThan(value);
        if (!comparison_result.is_valid() || comparison_result.is_null()) {
          return false;
        }
        out_range_ = comparison_result.bool_value() ? value : out_range_;
      }
      break;
    }

      // Min
    case FCT(FunctionKind::kMin, TYPE_FLOAT):
    case FCT(FunctionKind::kMin, TYPE_DOUBLE): {
      if (std::isnan(value.ToDouble()) || std::isnan(out_double_)) {
        out_double_ = std::numeric_limits<long double>::quiet_NaN();
      } else {
        out_double_ = std::min(out_double_, (long double)value.ToDouble());
      }
      break;
    }
    case FCT(FunctionKind::kMin, TYPE_UINT64): {
      out_uint64_ = std::min(out_uint64_, value.uint64_value());
      break;
    }
    case FCT(FunctionKind::kMin, TYPE_INT32):
    case FCT(FunctionKind::kMin, TYPE_INT64):
    case FCT(FunctionKind::kMin, TYPE_UINT32):
    case FCT(FunctionKind::kMin, TYPE_DATE):
    case FCT(FunctionKind::kMin, TYPE_BOOL):
    case FCT(FunctionKind::kMin, TYPE_ENUM): {
      out_int64_ = std::min(out_int64_, value.ToInt64());
      break;
    }
    case FCT(FunctionKind::kMin, TYPE_NUMERIC): {
      out_numeric_ = std::min(value.numeric_value(), out_numeric_);
      break;
    }
    case FCT(FunctionKind::kMin, TYPE_BIGNUMERIC): {
      out_bignumeric_ = std::min(value.bignumeric_value(), out_bignumeric_);
      break;
    }
    case FCT(FunctionKind::kMin, TYPE_TIMESTAMP): {
      // TODO: Support Value::ToUnixNanos in ARRAY_MIN and MIN.
      out_int64_ = std::min(out_int64_, value.ToUnixMicros());
      break;
    }
    case FCT(FunctionKind::kMin, TYPE_DATETIME): {
      out_datetime_ = value.LessThan(Value::Datetime(out_datetime_))
                          ? value.datetime_value()
                          : out_datetime_;
      break;
    }
    case FCT(FunctionKind::kMin, TYPE_TIME): {
      out_int64_ = std::min(out_int64_, value.time_value().Packed64TimeNanos());
      break;
    }
    case FCT(FunctionKind::kMin, TYPE_INTERVAL): {
      out_interval_ = std::min(out_interval_, value.interval_value());
      break;
    }
    case FCT(FunctionKind::kMin, TYPE_STRING): {
      bytes_to_return = out_string_.size();
      if (count_ <= 1) {
        out_string_ = value.string_value();
      } else if (collator_list_.empty()) {
        out_string_ = std::min(out_string_, value.string_value());
      } else {
        int64_t result = collator_list_[0]->CompareUtf8(value.string_value(),
                                                        out_string_, status);
        if (!status->ok()) {
          return false;
        }
        if (result < 0) {
          out_string_ = value.string_value();
        }
      }
      additional_bytes_to_request = out_string_.size();
      break;
    }
    case FCT(FunctionKind::kMin, TYPE_BYTES): {
      bytes_to_return = out_string_.size();
      out_string_ = (count_ > 1) ? std::min(out_string_, value.bytes_value())
                                 : value.bytes_value();
      additional_bytes_to_request = out_string_.size();
      break;
    }
    case FCT(FunctionKind::kMin, TYPE_ARRAY): {
      if (count_ == 1) {
        min_max_out_array_ = value;
      } else {
        if (value.LessThan(min_max_out_array_)) {
          bytes_to_return = out_string_.size();
          min_max_out_array_ = value;
          additional_bytes_to_request = min_max_out_array_.physical_byte_size();
        }
      }
      break;
    }
    case FCT(FunctionKind::kMin, TYPE_RANGE): {
      if (!out_range_.is_valid()) {
        out_range_ = value;
      } else {
        const Value comparison_result = value.SqlLessThan(out_range_);
        if (!comparison_result.is_valid() || comparison_result.is_null()) {
          return false;
        }
        out_range_ = comparison_result.bool_value() ? value : out_range_;
      }
      break;
    }

      // Sum
    case FCT(FunctionKind::kSum, TYPE_INT64): {
      out_int128_ += value.int64_value();
      break;
    }
    case FCT(FunctionKind::kSum, TYPE_UINT64): {
      out_uint128_ += value.uint64_value();
      break;
    }
    case FCT(FunctionKind::kSum, TYPE_DOUBLE): {
      out_exact_float_ += value.double_value();
      break;
    }
    case FCT(FunctionKind::kSum, TYPE_NUMERIC): {
      numeric_aggregator_.Add(value.numeric_value());
      break;
    }
    case FCT(FunctionKind::kSum, TYPE_BIGNUMERIC): {
      bignumeric_aggregator_.Add(value.bignumeric_value());
      break;
    }
    case FCT(FunctionKind::kSum, TYPE_INTERVAL): {
      interval_aggregator_.Add(value.interval_value());
      break;
    }
    case FCT(FunctionKind::kStringAgg, TYPE_STRING): {
      if (count_ > 1) {
        additional_bytes_to_request = delimiter_.size();
        absl::StrAppend(&out_string_, delimiter_);
      }
      additional_bytes_to_request += value.string_value().size();
      absl::StrAppend(&out_string_, value.string_value());
      break;
    }
    case FCT(FunctionKind::kStringAgg, TYPE_BYTES): {
      if (count_ > 1) {
        additional_bytes_to_request = delimiter_.size();
        absl::StrAppend(&out_string_, delimiter_);
      }
      additional_bytes_to_request = value.bytes_value().size();
      absl::StrAppend(&out_string_, value.bytes_value());
      break;
    }
    case FCT(FunctionKind::kOrAgg, TYPE_BOOL):
    case FCT(FunctionKind::kLogicalOr, TYPE_BOOL): {
      if (value.bool_value() == true) {
        has_true_ = true;
        *stop_accumulation = true;
      }
      break;
    }
    case FCT(FunctionKind::kAndAgg, TYPE_BOOL):
    case FCT(FunctionKind::kLogicalAnd, TYPE_BOOL): {
      if (value.bool_value() == false) {
        has_false_ = true;
        *stop_accumulation = true;
      }
      break;
    }
    case FCT(FunctionKind::kAnonSum, TYPE_DOUBLE):
    case FCT(FunctionKind::kAnonSumWithReportProto, TYPE_DOUBLE):
    case FCT(FunctionKind::kAnonSumWithReportJson, TYPE_DOUBLE):
    case FCT(FunctionKind::kAnonAvg, TYPE_DOUBLE):
    case FCT(FunctionKind::kAnonAvgWithReportProto, TYPE_DOUBLE):
    case FCT(FunctionKind::kAnonAvgWithReportJson, TYPE_DOUBLE):
      anon_double_->AddEntry(value.double_value());
      break;
    case FCT(FunctionKind::kAnonSum, TYPE_INT64):
    case FCT(FunctionKind::kAnonSumWithReportProto, TYPE_INT64):
    case FCT(FunctionKind::kAnonSumWithReportJson, TYPE_INT64):
      anon_int64_->AddEntry(value.int64_value());
      break;
    case FCT(FunctionKind::kAnonQuantiles, TYPE_ARRAY):
    case FCT(FunctionKind::kAnonQuantilesWithReportProto, TYPE_ARRAY):
    case FCT(FunctionKind::kAnonQuantilesWithReportJson, TYPE_ARRAY):
    case FCT(FunctionKind::kAnonVarPop, TYPE_ARRAY):
    case FCT(FunctionKind::kAnonStddevPop, TYPE_ARRAY): {
      for (const Value& value_element : value.elements()) {
        if (!value_element.is_null()) {
          if (!value_element.type()->IsDouble()) {
            *status = ::zetasql_base::InternalErrorBuilder()
                      << "Each element must be a double for ";
            return false;
          }
          anon_double_->AddEntry(value_element.double_value());
        }
      }
    } break;

    case FCT(FunctionKind::kDifferentialPrivacySum, TYPE_DOUBLE):
    case FCT(FunctionKind::kDifferentialPrivacyAvg, TYPE_DOUBLE):
      dp_double_->AddEntry(value.double_value());
      break;
    case FCT(FunctionKind::kDifferentialPrivacySum, TYPE_INT64):
      dp_int64_->AddEntry(value.int64_value());
      break;
    case FCT(FunctionKind::kDifferentialPrivacyQuantiles, TYPE_ARRAY):
    case FCT(FunctionKind::kDifferentialPrivacyVarPop, TYPE_ARRAY):
    case FCT(FunctionKind::kDifferentialPrivacyStddevPop, TYPE_ARRAY): {
      for (const Value& value_element : value.elements()) {
        if (!value_element.is_null()) {
          if (!value_element.type()->IsDouble()) {
            *status = ::zetasql_base::InternalErrorBuilder()
                      << "Each element must be a double for ";
            return false;
          }
          dp_double_->AddEntry(value_element.double_value());
        }
      }
    } break;
  }

  accountant()->ReturnBytes(bytes_to_return);
  requested_bytes_ -= bytes_to_return;

  if (!accountant()->RequestBytes(additional_bytes_to_request, status)) {
    return false;
  }
  requested_bytes_ += additional_bytes_to_request;

  return true;
}  // NOLINT(readability/fn_size)

absl::StatusOr<Value> BuiltinAggregateAccumulator::GetFinalResult(
    bool inputs_in_defined_order) {
  ZETASQL_ASSIGN_OR_RETURN(const Value result,
                   GetFinalResultInternal(inputs_in_defined_order));
  if (result.physical_byte_size() > context_->options().max_value_byte_size) {
    return ::zetasql_base::ResourceExhaustedErrorBuilder()
           << "Aggregate values are limited to "
           << context_->options().max_value_byte_size << " bytes";
  }
  return result;
}

template <typename T>
absl::StatusOr<Value> ComputePercentileCont(
    const std::vector<Value>& values_arg, T percentile, bool ignore_nulls) {
  ZETASQL_ASSIGN_OR_RETURN(PercentileEvaluator<T> percentile_evalutor,
                   PercentileEvaluator<T>::Create(percentile));

  std::vector<T> normal_values;
  normal_values.reserve(values_arg.size());
  size_t num_nulls = 0;
  for (const Value& value_arg : values_arg) {
    if constexpr (std::is_same_v<T, double>) {
      ZETASQL_RET_CHECK(value_arg.type()->IsDouble());
    } else if constexpr (std::is_same_v<T, NumericValue>) {
      ZETASQL_RET_CHECK(value_arg.type()->IsNumericType());
    }
    if (value_arg.is_null()) {
      ++num_nulls;
    } else {
      normal_values.push_back(value_arg.Get<T>());
    }
  }

  T result_value;
  bool result_is_not_null =
      percentile_evalutor.template ComputePercentileCont<false>(
          normal_values.begin(), normal_values.end(),
          (ignore_nulls ? 0 : num_nulls), &result_value);
  return result_is_not_null ? Value::Make<T>(result_value)
                            : Value::MakeNull<T>();
}

template <typename T, typename PercentileType, typename V = T,
          typename ValueCreationFn = Value (*)(T)>
absl::StatusOr<Value> ComputePercentileDisc(
    const PercentileEvaluator<PercentileType>& percentile_evalutor,
    const std::vector<Value>& values_arg, const Type* type,
    V (Value::*extract_value_fn)() const /* e.g., &Value::double_value */,
    const ValueCreationFn& value_creation_fn /* e.g., &Value::Double */,
    bool ignore_nulls, const zetasql::ZetaSqlCollator* collator = nullptr) {
  std::vector<T> normal_values;
  normal_values.reserve(values_arg.size());
  size_t num_nulls = 0;
  for (const Value& value_arg : values_arg) {
    if (value_arg.is_null()) {
      ++num_nulls;
    } else {
      normal_values.push_back((value_arg.*extract_value_fn)());
    }
  }

  if constexpr (std::is_same_v<T, absl::string_view>) {
    if (collator != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(auto itr,
                       percentile_evalutor
                           .template ComputePercentileDiscWithCollation<false>(
                               normal_values.begin(), normal_values.end(),
                               (ignore_nulls ? 0 : num_nulls), collator));
      return itr == normal_values.end() ? Value::Null(type)
                                        : value_creation_fn(*itr);
    }
  }
  auto itr = percentile_evalutor.template ComputePercentileDisc<T, false>(
      normal_values.begin(), normal_values.end(),
      (ignore_nulls ? 0 : num_nulls));
  return itr == normal_values.end() ? Value::Null(type)
                                    : value_creation_fn(*itr);
}

template <typename PercentileType>
absl::StatusOr<Value> ComputePercentileDisc(
    const std::vector<Value>& values_arg, const Type* type,
    PercentileType percentile, bool ignore_nulls,
    const zetasql::ZetaSqlCollator* collator) {
  ZETASQL_ASSIGN_OR_RETURN(PercentileEvaluator<PercentileType> percentile_evalutor,
                   PercentileEvaluator<PercentileType>::Create(percentile));
  switch (type->kind()) {
    case TYPE_INT64:
      return ComputePercentileDisc<int64_t>(percentile_evalutor, values_arg,
                                            type, &Value::int64_value,
                                            &Value::Int64, ignore_nulls);
    case TYPE_INT32:
      return ComputePercentileDisc<int32_t>(percentile_evalutor, values_arg,
                                            type, &Value::int32_value,
                                            &Value::Int32, ignore_nulls);
    case TYPE_UINT64:
      return ComputePercentileDisc<uint64_t>(percentile_evalutor, values_arg,
                                             type, &Value::uint64_value,
                                             &Value::Uint64, ignore_nulls);
    case TYPE_UINT32:
      return ComputePercentileDisc<uint32_t>(percentile_evalutor, values_arg,
                                             type, &Value::uint32_value,
                                             &Value::Uint32, ignore_nulls);
    case TYPE_DOUBLE:
      return ComputePercentileDisc<double>(percentile_evalutor, values_arg,
                                           type, &Value::double_value,
                                           &Value::Double, ignore_nulls);
    case TYPE_FLOAT:
      return ComputePercentileDisc<float>(percentile_evalutor, values_arg, type,
                                          &Value::float_value, &Value::Float,
                                          ignore_nulls);
    case TYPE_NUMERIC:
      return ComputePercentileDisc<NumericValue>(
          percentile_evalutor, values_arg, type, &Value::numeric_value,
          &Value::Numeric, ignore_nulls);
    case TYPE_BIGNUMERIC:
      return ComputePercentileDisc<BigNumericValue>(
          percentile_evalutor, values_arg, type, &Value::bignumeric_value,
          &Value::BigNumeric, ignore_nulls);
    case TYPE_BYTES:
      return ComputePercentileDisc<absl::string_view, PercentileType,
                                   const std::string&>(
          percentile_evalutor, values_arg, type, &Value::bytes_value,
          &Value::Bytes, ignore_nulls);
    case TYPE_STRING:
      return ComputePercentileDisc<absl::string_view, PercentileType,
                                   const std::string&>(
          percentile_evalutor, values_arg, type, &Value::string_value,
          &Value::String, ignore_nulls, collator);
    case TYPE_BOOL:
      return ComputePercentileDisc<bool>(percentile_evalutor, values_arg, type,
                                         &Value::bool_value, &Value::Bool,
                                         ignore_nulls);
    case TYPE_DATE:
      return ComputePercentileDisc<int32_t>(percentile_evalutor, values_arg,
                                            type, &Value::date_value,
                                            &Value::Date, ignore_nulls);
    case TYPE_DATETIME:
      return ComputePercentileDisc<int64_t>(
          percentile_evalutor, values_arg, type,
          &Value::ToPacked64DatetimeMicros, &Value::DatetimeFromPacked64Micros,
          ignore_nulls);
    case TYPE_TIME:
      return ComputePercentileDisc<int64_t>(
          percentile_evalutor, values_arg, type, &Value::ToPacked64TimeMicros,
          &Value::TimeFromPacked64Micros, ignore_nulls);
    case TYPE_TIMESTAMP:
      return ComputePercentileDisc<int64_t>(
          percentile_evalutor, values_arg, type, &Value::ToUnixMicros,
          &Value::TimestampFromUnixMicros, ignore_nulls);
    case TYPE_ENUM:
      return ComputePercentileDisc<int32_t, PercentileType, int32_t,
                                   std::function<Value(int32_t)>>(
          percentile_evalutor, values_arg, type, &Value::enum_value,
          [type](int32_t value) -> Value {
            return Value::Enum(type->AsEnum(), value,
                               /*allow_unknown_enum_values=*/true);
          },
          ignore_nulls);
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported argument type for percentile_disc.";
  }
}

absl::Status ConvertDifferentPrivacyOutputToAnonOutputProto(
    const ::differential_privacy::Output& input,
    zetasql::AnonOutputWithReport* anon_output_proto) {
  ZETASQL_RET_CHECK_GT(input.elements_size(), 0);
  AnonOutputValues anon_output_values;
  for (const ::differential_privacy::Output::Element& element :
       input.elements()) {
    AnonOutputValue* anon_output_value = anon_output_values.add_values();

    ZETASQL_RET_CHECK(element.has_noise_confidence_interval());
    anon_output_value->mutable_noise_confidence_interval()
        ->set_confidence_level(
            element.noise_confidence_interval().confidence_level());
    anon_output_value->mutable_noise_confidence_interval()->set_lower_bound(
        element.noise_confidence_interval().lower_bound());
    anon_output_value->mutable_noise_confidence_interval()->set_upper_bound(
        element.noise_confidence_interval().upper_bound());

    ZETASQL_RET_CHECK(element.has_value());
    switch (element.value().value_case()) {
      case ::differential_privacy::ValueType::kIntValue:
        anon_output_value->set_int_value(element.value().int_value());
        break;
      case ::differential_privacy::ValueType::kFloatValue:
        anon_output_value->set_float_value(element.value().float_value());
        break;
      case ::differential_privacy::ValueType::kStringValue:
        anon_output_value->set_string_value(element.value().string_value());
        break;
      case ::differential_privacy::ValueType::VALUE_NOT_SET:
        break;
      default:
        ZETASQL_RET_CHECK_FAIL() << "Invalid element value type";
    }
  }

  if (input.elements_size() == 1) {
    *anon_output_proto->mutable_value() = anon_output_values.values(0);
  } else {
    *anon_output_proto->mutable_values() = anon_output_values;
  }

  if (input.has_error_report() && input.error_report().has_bounding_report()) {
    const ::differential_privacy::BoundingReport& input_bounding_report =
        input.error_report().bounding_report();
    BoundingReport anon_bounding_report;
    anon_bounding_report.set_num_inputs(input_bounding_report.num_inputs());
    anon_bounding_report.set_num_outside(input_bounding_report.num_outside());

    // Lower bound.
    ZETASQL_RET_CHECK(input_bounding_report.has_lower_bound());

    switch (input_bounding_report.lower_bound().value_case()) {
      case ::differential_privacy::ValueType::kIntValue:
        anon_bounding_report.mutable_lower_bound()->set_int_value(
            input_bounding_report.lower_bound().int_value());
        break;
      case ::differential_privacy::ValueType::kFloatValue:
        anon_bounding_report.mutable_lower_bound()->set_float_value(
            input_bounding_report.lower_bound().float_value());
        break;
      case ::differential_privacy::ValueType::kStringValue:
        anon_bounding_report.mutable_lower_bound()->set_string_value(
            input_bounding_report.lower_bound().string_value());
        break;
      case ::differential_privacy::ValueType::VALUE_NOT_SET:
        break;
      default:
        ZETASQL_RET_CHECK_FAIL()
            << "Invalid input_bounding_report lower_bound value type";
    }

    // Upper bound.
    ZETASQL_RET_CHECK(input_bounding_report.has_upper_bound());

    switch (input_bounding_report.upper_bound().value_case()) {
      case ::differential_privacy::ValueType::kIntValue:
        anon_bounding_report.mutable_upper_bound()->set_int_value(
            input_bounding_report.upper_bound().int_value());
        break;
      case ::differential_privacy::ValueType::kFloatValue:
        anon_bounding_report.mutable_upper_bound()->set_float_value(
            input_bounding_report.upper_bound().float_value());
        break;
      case ::differential_privacy::ValueType::kStringValue:
        anon_bounding_report.mutable_upper_bound()->set_string_value(
            input_bounding_report.upper_bound().string_value());
        break;
      case ::differential_privacy::ValueType::VALUE_NOT_SET:
        break;
      default:
        ZETASQL_RET_CHECK_FAIL()
            << "Invalid input_bounding_report upper_bound value type";
    }

    *anon_output_proto->mutable_bounding_report() = anon_bounding_report;
  }

  return absl::OkStatus();
}

absl::Status ConvertDifferentPrivacyOutputToDifferentialPrivacyOutputProto(
    const ::differential_privacy::Output& input,
    zetasql::functions::DifferentialPrivacyOutputWithReport*
        dp_output_proto) {
  ZETASQL_RET_CHECK_GT(input.elements_size(), 0);
  zetasql::functions::DifferentialPrivacyOutputValues dp_output_values;
  for (const ::differential_privacy::Output::Element& element :
       input.elements()) {
    zetasql::functions::DifferentialPrivacyOutputValue* dp_output_value =
        dp_output_values.add_values();

    ZETASQL_RET_CHECK(element.has_noise_confidence_interval());
    dp_output_value->mutable_noise_confidence_interval()->set_confidence_level(
        element.noise_confidence_interval().confidence_level());
    dp_output_value->mutable_noise_confidence_interval()->set_lower_bound(
        element.noise_confidence_interval().lower_bound());
    dp_output_value->mutable_noise_confidence_interval()->set_upper_bound(
        element.noise_confidence_interval().upper_bound());

    ZETASQL_RET_CHECK(element.has_value());
    switch (element.value().value_case()) {
      case ::differential_privacy::ValueType::kIntValue:
        dp_output_value->set_int_value(element.value().int_value());
        break;
      case ::differential_privacy::ValueType::kFloatValue:
        dp_output_value->set_float_value(element.value().float_value());
        break;
      case ::differential_privacy::ValueType::kStringValue:
        dp_output_value->set_string_value(element.value().string_value());
        break;
      case ::differential_privacy::ValueType::VALUE_NOT_SET:
        break;
      default:
        ZETASQL_RET_CHECK_FAIL() << "Invalid element value type";
    }
  }

  if (input.elements_size() == 1) {
    *dp_output_proto->mutable_value() = dp_output_values.values(0);
  } else {
    *dp_output_proto->mutable_values() = dp_output_values;
  }

  if (input.has_error_report() && input.error_report().has_bounding_report()) {
    const ::differential_privacy::BoundingReport& input_bounding_report =
        input.error_report().bounding_report();
    zetasql::functions::DifferentialPrivacyBoundingReport dp_bounding_report;
    dp_bounding_report.set_num_inputs(input_bounding_report.num_inputs());
    dp_bounding_report.set_num_outside(input_bounding_report.num_outside());

    // Lower bound.
    ZETASQL_RET_CHECK(input_bounding_report.has_lower_bound());

    switch (input_bounding_report.lower_bound().value_case()) {
      case ::differential_privacy::ValueType::kIntValue:
        dp_bounding_report.mutable_lower_bound()->set_int_value(
            input_bounding_report.lower_bound().int_value());
        break;
      case ::differential_privacy::ValueType::kFloatValue:
        dp_bounding_report.mutable_lower_bound()->set_float_value(
            input_bounding_report.lower_bound().float_value());
        break;
      case ::differential_privacy::ValueType::kStringValue:
        dp_bounding_report.mutable_lower_bound()->set_string_value(
            input_bounding_report.lower_bound().string_value());
        break;
      case ::differential_privacy::ValueType::VALUE_NOT_SET:
        break;
      default:
        ZETASQL_RET_CHECK_FAIL()
            << "Invalid input_bounding_report lower_bound value type";
    }

    // Upper bound.
    ZETASQL_RET_CHECK(input_bounding_report.has_upper_bound());

    switch (input_bounding_report.upper_bound().value_case()) {
      case ::differential_privacy::ValueType::kIntValue:
        dp_bounding_report.mutable_upper_bound()->set_int_value(
            input_bounding_report.upper_bound().int_value());
        break;
      case ::differential_privacy::ValueType::kFloatValue:
        dp_bounding_report.mutable_upper_bound()->set_float_value(
            input_bounding_report.upper_bound().float_value());
        break;
      case ::differential_privacy::ValueType::kStringValue:
        dp_bounding_report.mutable_upper_bound()->set_string_value(
            input_bounding_report.upper_bound().string_value());
        break;
      case ::differential_privacy::ValueType::VALUE_NOT_SET:
        break;
      default:
        ZETASQL_RET_CHECK_FAIL()
            << "Invalid input_bounding_report upper_bound value type";
    }

    *dp_output_proto->mutable_bounding_report() = dp_bounding_report;
  }

  return absl::OkStatus();
}

absl::Status ConvertDifferentPrivacyOutputToOutputJson(
    const ::differential_privacy::Output& input, JSONValue* anon_output_json) {
  ZETASQL_RET_CHECK_GT(input.elements_size(), 0);

  JSONValueRef json_ref = anon_output_json->GetRef();
  auto insert_value_to_json = [](JSONValueRef ref,
                                 ::differential_privacy::ValueType value) {
    switch (value.value_case()) {
      case ::differential_privacy::ValueType::ValueCase::kFloatValue:
        ref.SetDouble(value.float_value());
        return absl::OkStatus();
      case ::differential_privacy::ValueType::ValueCase::kIntValue:
        ref.SetInt64(value.int_value());
        return absl::OkStatus();
      default:
        return absl::InternalError(
            absl::StrCat("Invalid value type for anon output json: ",
                         static_cast<int>(value.value_case())));
    }
  };
  auto insert_element_into_json =
      [&insert_value_to_json](
          JSONValueRef ref,
          const ::differential_privacy::Output::Element& element)
      -> absl::Status {
    ZETASQL_RETURN_IF_ERROR(
        insert_value_to_json(ref.GetMember("value"), element.value()));
    if (element.has_noise_confidence_interval()) {
      auto noise_confidence_interval_ref =
          ref.GetMember("noise_confidence_interval");
      noise_confidence_interval_ref.GetMember("upper_bound")
          .SetDouble(element.noise_confidence_interval().upper_bound());
      noise_confidence_interval_ref.GetMember("lower_bound")
          .SetDouble(element.noise_confidence_interval().lower_bound());
      noise_confidence_interval_ref.GetMember("confidence_level")
          .SetDouble(element.noise_confidence_interval().confidence_level());
    }
    return absl::OkStatus();
  };

  if (input.elements_size() == 1) {
    ZETASQL_RETURN_IF_ERROR(insert_element_into_json(json_ref.GetMember("result"),
                                             input.elements(0)));
  } else {
    for (int i = 0; i < input.elements_size(); ++i) {
      ZETASQL_RETURN_IF_ERROR(insert_element_into_json(
          json_ref.GetMember("result").GetArrayElement(i), input.elements(i)));
    }
  }

  if (input.has_error_report() && input.error_report().has_bounding_report()) {
    const auto& bounding_report = input.error_report().bounding_report();
    auto bounding_report_ref = json_ref.GetMember("bounding_report");

    if (bounding_report.has_lower_bound()) {
      ZETASQL_RETURN_IF_ERROR(
          insert_value_to_json(bounding_report_ref.GetMember("lower_bound"),
                               bounding_report.lower_bound()));
    }
    if (bounding_report.has_upper_bound()) {
      ZETASQL_RETURN_IF_ERROR(
          insert_value_to_json(bounding_report_ref.GetMember("upper_bound"),
                               bounding_report.upper_bound()));
    }
    if (bounding_report.has_num_inputs()) {
      bounding_report_ref.GetMember("num_inputs")
          .SetDouble(bounding_report.num_inputs());
    }
    if (bounding_report.has_num_outside()) {
      bounding_report_ref.GetMember("num_outside")
          .SetDouble(bounding_report.num_outside());
    }
  }
  return absl::OkStatus();
}

// Removes the payload for identifying when approx bounds failed because it had
// not enough data.  This step is required for the migration.
//
// TODO: Remove this function after the migration and change the
// logic to return a null value in case the payload is present.
absl::StatusOr<differential_privacy::Output> IgnoreDifferentialPrivacyPayload(
    const absl::StatusOr<differential_privacy::Output>& output) {
  if (output.ok()) {
    return output;
  }
  absl::Status result = output.status();
  result.ErasePayload(
      "type.googleapis.com/differential_privacy.ApproxBoundsNotEnoughData");
  return result;
}

template <class T>
absl::StatusOr<Value> GetAnonProtoReturnValue(
    std::unique_ptr<::differential_privacy::Algorithm<T>>& algorithm) {
  zetasql::TypeFactory type_factory;
  const ProtoType* anon_output_proto_type = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory.MakeProtoType(
      zetasql::AnonOutputWithReport::descriptor(), &anon_output_proto_type));
  zetasql::AnonOutputWithReport anon_output_proto;
  if (algorithm != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(auto result, IgnoreDifferentialPrivacyPayload(
                                      algorithm->PartialResult()));
    ZETASQL_RETURN_IF_ERROR(ConvertDifferentPrivacyOutputToAnonOutputProto(
        result, &anon_output_proto));
  }
  return Value::Proto(anon_output_proto_type,
                      absl::Cord(anon_output_proto.SerializeAsString()));
}

template <class T>
absl::StatusOr<Value> GetAnonJsonReturnValue(
    std::unique_ptr<::differential_privacy::Algorithm<T>>& algorithm) {
  JSONValue output_json;
  if (algorithm != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(auto result, IgnoreDifferentialPrivacyPayload(
                                      algorithm->PartialResult()));
    ZETASQL_RETURN_IF_ERROR(
        ConvertDifferentPrivacyOutputToOutputJson(result, &output_json));
  }
  return Value::Json(JSONValue::CopyFrom(output_json.GetConstRef()));
}

template <class T>
absl::StatusOr<Value> GetAnonReturnValue(
    std::unique_ptr<::differential_privacy::Algorithm<T>>& algorithm) {
  if (algorithm == nullptr) {
    return Value::MakeNull<T>();
  }
  ZETASQL_ASSIGN_OR_RETURN(auto result, IgnoreDifferentialPrivacyPayload(
                                    algorithm->PartialResult()));
  return Value::Make<T>(::differential_privacy::GetValue<T>(result));
}

template <class T>
absl::StatusOr<Value> GetDPReturnValue(
    std::unique_ptr<::differential_privacy::Algorithm<T>>& algorithm) {
  if (algorithm == nullptr) {
    return Value::MakeNull<T>();
  }
  ZETASQL_ASSIGN_OR_RETURN(auto result, IgnoreDifferentialPrivacyPayload(
                                    algorithm->PartialResult()));
  return Value::Make<T>(::differential_privacy::GetValue<T>(result));
}

template <class T>
absl::StatusOr<Value> GetDPJsonReturnValue(
    std::unique_ptr<::differential_privacy::Algorithm<T>>& algorithm) {
  if (algorithm == nullptr) {
    return Value::MakeNull<T>();
  }
  JSONValue output_json;
  ZETASQL_ASSIGN_OR_RETURN(auto result, IgnoreDifferentialPrivacyPayload(
                                    algorithm->PartialResult()));
  ZETASQL_RETURN_IF_ERROR(
      ConvertDifferentPrivacyOutputToOutputJson(result, &output_json));
  return Value::Json(JSONValue::CopyFrom(output_json.GetConstRef()));
}

template <class T>
absl::StatusOr<Value> GetDPProtoReturnValue(
    std::unique_ptr<::differential_privacy::Algorithm<T>>& algorithm) {
  if (algorithm == nullptr) {
    return Value::MakeNull<T>();
  }
  zetasql::TypeFactory type_factory;
  const ProtoType* dp_output_proto_type = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory.MakeProtoType(
      zetasql::functions::DifferentialPrivacyOutputWithReport::descriptor(),
      &dp_output_proto_type));
  zetasql::functions::DifferentialPrivacyOutputWithReport dp_output_proto;
  ZETASQL_ASSIGN_OR_RETURN(auto result, IgnoreDifferentialPrivacyPayload(
                                    algorithm->PartialResult()));
  ZETASQL_RETURN_IF_ERROR(ConvertDifferentPrivacyOutputToDifferentialPrivacyOutputProto(
      result, &dp_output_proto));
  return Value::Proto(dp_output_proto_type,
                      absl::Cord(dp_output_proto.SerializeAsString()));
}

absl::StatusOr<Value> GetDPQuantilesReturnValue(
    std::unique_ptr<::differential_privacy::Algorithm<double>>& algorithm) {
  if (algorithm == nullptr) {
    return Value::NullDouble();
  } else {
    ZETASQL_ASSIGN_OR_RETURN(auto value, IgnoreDifferentialPrivacyPayload(
                                     algorithm->PartialResult()));
    std::vector<Value> values;
    for (const ::differential_privacy::Output::Element& element :
         value.elements()) {
      values.push_back(Value::Double(element.value().float_value()));
    }
    return Value::MakeArray(types::DoubleArrayType(),
                            absl::Span<const Value>(values));
  }
}

template <class T>
absl::StatusOr<Value> GetDPReturnValue(
    std::unique_ptr<::differential_privacy::Algorithm<T>>& algorithm,
    const Type* output_type) {
  if (IsDPJsonReportFunction(output_type)) {
    return GetDPJsonReturnValue(algorithm);
  } else if (IsDPProtoReportFunction(output_type)) {
    return GetDPProtoReturnValue(algorithm);
  } else {
    return GetDPReturnValue<T>(algorithm);
  }
}

absl::StatusOr<Value> BuiltinAggregateAccumulator::GetFinalResultInternal(
    bool inputs_in_defined_order) {
  const Type* output_type = function_->output_type();
  absl::Status error;
  switch (function_->kind()) {
    case FunctionKind::kArrayAgg:
      // ARRAY_AGG returns NULL over empty input.
      return array_agg_.empty()
                 ? Value::Null(function_->output_type())
                 : InternalValue::ArrayNotChecked(output_type->AsArray(),
                                                  inputs_in_defined_order,
                                                  std::move(array_agg_));
    case FunctionKind::kArrayConcatAgg: {
      std::vector<Value> values;
      bool found_non_null_inputs = false;
      for (const Value& input_array : array_agg_) {
        // ARRAY_CONCAT_AGG ignores NULLs.
        if (input_array.is_null()) continue;
        found_non_null_inputs = true;
        for (int i = 0; i < input_array.num_elements(); ++i) {
          values.push_back(input_array.element(i));
        }
      }
      // ARRAY_CONCAT_AGG returns NULL over empty input, or if all the inputs
      // are NULLs.
      if (!found_non_null_inputs) {
        return Value::Null(output_type);
      }
      return InternalValue::ArrayNotChecked(output_type->AsArray(),
                                            InternalValue::kIgnoresOrder,
                                            std::move(values));
    }
    case FunctionKind::kAnyValue:
      return any_value_.is_valid() ? any_value_ : Value::Null(output_type);
    case FunctionKind::kCount:
      return Value::Int64(count_);
    case FunctionKind::kCountIf:
      return Value::Int64(countif_);
    default:
      break;
  }
  switch (FCT(function_->kind(), input_type_->kind())) {
    // Avg
    case FCT(FunctionKind::kAvg, TYPE_INT64):
    case FCT(FunctionKind::kAvg, TYPE_UINT64):
    case FCT(FunctionKind::kAvg, TYPE_DOUBLE): {
      ZETASQL_RET_CHECK_GE(count_, 0);
      if (count_ < 0) {
        return Value::NullDouble();
      }

      // AVG() should never overflow, since all the elements are by definition
      // part of the domain and average will be between them.
      // If we ever hit this, it's an internal error and a bug.
      ZETASQL_RET_CHECK_OK(ValidateNoDoubleOverflow(out_double_));
      return count_ > 0 ? Value::Double(out_double_) : Value::NullDouble();
    }
    case FCT(FunctionKind::kAvg, TYPE_NUMERIC): {
      if (count_ == 0) {
        return Value::NullNumeric();
      }
      ZETASQL_ASSIGN_OR_RETURN(out_numeric_, numeric_aggregator_.GetAverage(count_));
      return Value::Numeric(out_numeric_);
    }
    case FCT(FunctionKind::kAvg, TYPE_BIGNUMERIC): {
      if (count_ == 0) {
        return Value::NullBigNumeric();
      }
      ZETASQL_ASSIGN_OR_RETURN(out_bignumeric_,
                       bignumeric_aggregator_.GetAverage(count_));
      return Value::BigNumeric(out_bignumeric_);
    }
    case FCT(FunctionKind::kAvg, TYPE_INTERVAL): {
      if (count_ == 0) {
        return Value::NullInterval();
      }
      // Round nanos second part towards zero if in micro seconds mode.
      bool round_to_micros =
          GetTimestampScale(context_->GetLanguageOptions()) ==
          functions::TimestampScale::kMicroseconds;
      ZETASQL_ASSIGN_OR_RETURN(out_interval_, interval_aggregator_.GetAverage(
                                          count_, round_to_micros));
      return Value::Interval(out_interval_);
    }
    // Sum
    case FCT(FunctionKind::kSum, TYPE_DOUBLE): {
      if (count_ == 0) {
        return Value::NullDouble();
      }
      if (out_exact_float_.is_finite() &&
          (out_exact_float_ > std::numeric_limits<double>::max() ||
           out_exact_float_ < -std::numeric_limits<double>::max())) {
        return ::zetasql_base::OutOfRangeErrorBuilder() << "double overflow";
      }
      return Value::Double(out_exact_float_.ToDouble());
    }
    case FCT(FunctionKind::kSum, TYPE_INT64): {
      if (count_ == 0) {
        return Value::NullInt64();
      }
      if (out_int128_ > std::numeric_limits<int64_t>::max() ||
          out_int128_ < std::numeric_limits<int64_t>::min()) {
        return ::zetasql_base::OutOfRangeErrorBuilder() << "int64 overflow";
      }
      return Value::Int64(static_cast<int64_t>(out_int128_));
    }
    case FCT(FunctionKind::kSum, TYPE_UINT64): {
      if (count_ == 0) {
        return Value::NullUint64();
      }
      if (out_uint128_ > std::numeric_limits<uint64_t>::max()) {
        return ::zetasql_base::OutOfRangeErrorBuilder() << "uint64 overflow";
      }
      return Value::Uint64(static_cast<uint64_t>(out_uint128_));
    }
    case FCT(FunctionKind::kSum, TYPE_NUMERIC): {
      if (count_ == 0) {
        return Value::NullNumeric();
      }
      ZETASQL_ASSIGN_OR_RETURN(out_numeric_, numeric_aggregator_.GetSum());
      return Value::Numeric(out_numeric_);
    }
    case FCT(FunctionKind::kSum, TYPE_BIGNUMERIC): {
      if (count_ == 0) {
        return Value::NullBigNumeric();
      }
      ZETASQL_ASSIGN_OR_RETURN(out_bignumeric_, bignumeric_aggregator_.GetSum());
      return Value::BigNumeric(out_bignumeric_);
    }
    case FCT(FunctionKind::kSum, TYPE_INTERVAL): {
      if (count_ == 0) {
        return Value::NullInterval();
      }
      ZETASQL_ASSIGN_OR_RETURN(out_interval_, interval_aggregator_.GetSum());
      return Value::Interval(out_interval_);
    }
    // Variance and Stddev
    case FCT(FunctionKind::kStddevPop, TYPE_DOUBLE): {
      if (count_ == 0) return Value::NullDouble();
      ZETASQL_RET_CHECK(functions::Sqrt<long double>(variance_, &variance_, &error));
      ZETASQL_RET_CHECK_OK(error);
      return Value::Double(variance_);
    }
    case FCT(FunctionKind::kStddevSamp, TYPE_DOUBLE): {
      if (count_ <= 1) return Value::NullDouble();
      // stddev_samp = sqrt(variance * count / (count - 1))
      long double tmp;
      ZETASQL_RET_CHECK(functions::Divide(static_cast<long double>(count_),
                                  static_cast<long double>(count_ - 1), &tmp,
                                  &error));
      ZETASQL_RET_CHECK_OK(error);
      ZETASQL_RET_CHECK(functions::Multiply(variance_, tmp, &variance_, &error));
      ZETASQL_RET_CHECK_OK(error);
      ZETASQL_RET_CHECK(functions::Sqrt(variance_, &variance_, &error));
      ZETASQL_RET_CHECK_OK(error);
      return Value::Double(variance_);
    }
    case FCT(FunctionKind::kVarPop, TYPE_DOUBLE):
      return count_ > 0 ? Value::Double(variance_) : Value::NullDouble();
    case FCT(FunctionKind::kVarSamp, TYPE_DOUBLE): {
      if (count_ <= 1) return Value::NullDouble();
      // var_samp = variance * count / (count - 1)
      long double tmp;
      ZETASQL_RET_CHECK(functions::Divide(static_cast<long double>(count_),
                                  static_cast<long double>(count_ - 1), &tmp,
                                  &error));
      ZETASQL_RET_CHECK_OK(error);
      ZETASQL_RET_CHECK(functions::Multiply(variance_, tmp, &variance_, &error));
      ZETASQL_RET_CHECK_OK(error);
      return Value::Double(variance_);
    }
    // Variance and Stddev for NumericValue
    case FCT(FunctionKind::kStddevPop, TYPE_NUMERIC): {
      return CreateValueFromOptional(
          numeric_variance_aggregator_.GetPopulationStdDev(count_));
    }
    case FCT(FunctionKind::kStddevSamp, TYPE_NUMERIC): {
      return CreateValueFromOptional(
          numeric_variance_aggregator_.GetSamplingStdDev(count_));
    }
    case FCT(FunctionKind::kVarPop, TYPE_NUMERIC): {
      return CreateValueFromOptional(
          numeric_variance_aggregator_.GetPopulationVariance(count_));
    }
    case FCT(FunctionKind::kVarSamp, TYPE_NUMERIC): {
      return CreateValueFromOptional(
          numeric_variance_aggregator_.GetSamplingVariance(count_));
    }
    // Variance and Stddev for BigNumericValue
    case FCT(FunctionKind::kStddevPop, TYPE_BIGNUMERIC): {
      return CreateValueFromOptional(
          bignumeric_variance_aggregator_.GetPopulationStdDev(count_));
    }
    case FCT(FunctionKind::kStddevSamp, TYPE_BIGNUMERIC): {
      return CreateValueFromOptional(
          bignumeric_variance_aggregator_.GetSamplingStdDev(count_));
    }
    case FCT(FunctionKind::kVarPop, TYPE_BIGNUMERIC): {
      return CreateValueFromOptional(
          bignumeric_variance_aggregator_.GetPopulationVariance(count_));
    }
    case FCT(FunctionKind::kVarSamp, TYPE_BIGNUMERIC): {
      return CreateValueFromOptional(
          bignumeric_variance_aggregator_.GetSamplingVariance(count_));
    }

    // Max, Min
    case FCT(FunctionKind::kMax, TYPE_BOOL):
    case FCT(FunctionKind::kMin, TYPE_BOOL):
      return count_ > 0 ? Value::Bool(out_int64_ > 0) : Value::NullBool();
    case FCT(FunctionKind::kMax, TYPE_INT32):
    case FCT(FunctionKind::kMin, TYPE_INT32):
      return count_ > 0 ? Value::Int32(out_int64_) : Value::NullInt32();
    case FCT(FunctionKind::kMax, TYPE_INT64):
    case FCT(FunctionKind::kMin, TYPE_INT64):
      return count_ > 0 ? Value::Int64(out_int64_) : Value::NullInt64();
    case FCT(FunctionKind::kMax, TYPE_UINT32):
    case FCT(FunctionKind::kMin, TYPE_UINT32):
      return count_ > 0 ? Value::Uint32(out_int64_) : Value::NullUint32();
    case FCT(FunctionKind::kMax, TYPE_UINT64):
    case FCT(FunctionKind::kMin, TYPE_UINT64):
      return count_ > 0 ? Value::Uint64(out_uint64_) : Value::NullUint64();
    case FCT(FunctionKind::kMax, TYPE_FLOAT):
    case FCT(FunctionKind::kMin, TYPE_FLOAT):
      return count_ > 0 ? Value::Float(out_double_) : Value::NullFloat();
    case FCT(FunctionKind::kMax, TYPE_DOUBLE):
    case FCT(FunctionKind::kMin, TYPE_DOUBLE):
      return count_ > 0 ? Value::Double(out_double_) : Value::NullDouble();
    case FCT(FunctionKind::kMax, TYPE_NUMERIC):
    case FCT(FunctionKind::kMin, TYPE_NUMERIC):
      return count_ > 0 ? Value::Numeric(out_numeric_) : Value::NullNumeric();
    case FCT(FunctionKind::kMax, TYPE_BIGNUMERIC):
    case FCT(FunctionKind::kMin, TYPE_BIGNUMERIC):
      return count_ > 0 ? Value::BigNumeric(out_bignumeric_)
                        : Value::NullBigNumeric();
    case FCT(FunctionKind::kMax, TYPE_STRING):
    case FCT(FunctionKind::kMin, TYPE_STRING):
      return count_ > 0 ? Value::String(out_string_) : Value::NullString();
    case FCT(FunctionKind::kStringAgg, TYPE_STRING):
      if (count_ > 1) {
        context_->SetNonDeterministicOutput();
      }
      return count_ > 0 ? Value::String(out_string_) : Value::NullString();
    case FCT(FunctionKind::kMax, TYPE_BYTES):
    case FCT(FunctionKind::kMin, TYPE_BYTES):
      return count_ > 0 ? Value::Bytes(out_string_) : Value::NullBytes();
    case FCT(FunctionKind::kStringAgg, TYPE_BYTES):
      if (count_ > 1) {
        context_->SetNonDeterministicOutput();
      }
      return count_ > 0 ? Value::Bytes(out_string_) : Value::NullBytes();
    case FCT(FunctionKind::kMax, TYPE_DATE):
    case FCT(FunctionKind::kMin, TYPE_DATE):
      return count_ > 0 ? Value::Date(out_int64_) : Value::NullDate();
    case FCT(FunctionKind::kMax, TYPE_TIMESTAMP):
    case FCT(FunctionKind::kMin, TYPE_TIMESTAMP):
      return count_ > 0 ? Value::TimestampFromUnixMicros(out_int64_)
                        : Value::NullTimestamp();
    case FCT(FunctionKind::kMax, TYPE_DATETIME):
    case FCT(FunctionKind::kMin, TYPE_DATETIME):
      return count_ > 0 ? Value::Datetime(out_datetime_)
                        : Value::NullDatetime();
    case FCT(FunctionKind::kMax, TYPE_TIME):
    case FCT(FunctionKind::kMin, TYPE_TIME):
      return count_ > 0 ? Value::Time(TimeValue::FromPacked64Nanos(out_int64_))
                        : Value::NullTime();
    case FCT(FunctionKind::kMax, TYPE_INTERVAL):
    case FCT(FunctionKind::kMin, TYPE_INTERVAL):
      return count_ > 0 ? Value::Interval(out_interval_)
                        : Value::NullInterval();
    case FCT(FunctionKind::kMax, TYPE_ENUM):
    case FCT(FunctionKind::kMin, TYPE_ENUM):
      return count_ > 0 ? Value::Enum(output_type->AsEnum(), out_int64_,
                                      /*allow_unknown_enum_values=*/true)
                        : Value::Null(output_type);

    case FCT(FunctionKind::kMax, TYPE_ARRAY):
    case FCT(FunctionKind::kMin, TYPE_ARRAY): {
      if (count_ > 0) {
        return min_max_out_array_;
      }
      return Value::Null(output_type);
    }
    case FCT(FunctionKind::kMax, TYPE_RANGE):
    case FCT(FunctionKind::kMin, TYPE_RANGE): {
      if (count_ > 0) {
        return out_range_;
      }
      return Value::Null(output_type);
    }

    // Logical aggregates.
    case FCT(FunctionKind::kOrAgg, TYPE_BOOL):
      return has_true_ ? Value::Bool(true)
                       : (has_null_ ? Value::NullBool() : Value::Bool(false));
    case FCT(FunctionKind::kLogicalOr, TYPE_BOOL):
      return has_true_ ? Value::Bool(true)
                       : (count_ == 0 ? Value::NullBool() : Value::Bool(false));
    case FCT(FunctionKind::kAndAgg, TYPE_BOOL):
      return has_false_ ? Value::Bool(false)
                        : (has_null_ ? Value::NullBool() : Value::Bool(true));
    case FCT(FunctionKind::kLogicalAnd, TYPE_BOOL):
      return has_false_ ? Value::Bool(false)
                        : (count_ == 0 ? Value::NullBool() : Value::Bool(true));
    // Bitwise aggregates.
    case FCT(FunctionKind::kBitAnd, TYPE_INT32):
    case FCT(FunctionKind::kBitOr, TYPE_INT32):
    case FCT(FunctionKind::kBitXor, TYPE_INT32):
      return count_ > 0 ? Value::Int32(bit_int32_) : Value::NullInt32();
    case FCT(FunctionKind::kBitAnd, TYPE_INT64):
    case FCT(FunctionKind::kBitOr, TYPE_INT64):
    case FCT(FunctionKind::kBitXor, TYPE_INT64):
      return count_ > 0 ? Value::Int64(bit_int64_) : Value::NullInt64();
    case FCT(FunctionKind::kBitAnd, TYPE_UINT32):
    case FCT(FunctionKind::kBitOr, TYPE_UINT32):
    case FCT(FunctionKind::kBitXor, TYPE_UINT32):
      return count_ > 0 ? Value::Uint32(bit_uint32_) : Value::NullUint32();
    case FCT(FunctionKind::kBitAnd, TYPE_UINT64):
    case FCT(FunctionKind::kBitOr, TYPE_UINT64):
    case FCT(FunctionKind::kBitXor, TYPE_UINT64):
      return count_ > 0 ? Value::Uint64(bit_uint64_) : Value::NullUint64();
    case FCT(FunctionKind::kPercentileCont, TYPE_DOUBLE):
      return ComputePercentileCont<>(percentile_population_,
                                     percentile_.double_value(),
                                     function_->ignores_null());
    case FCT(FunctionKind::kPercentileCont, TYPE_NUMERIC):
      return ComputePercentileCont<>(percentile_population_,
                                     percentile_.numeric_value(),
                                     function_->ignores_null());
    case FCT(FunctionKind::kPercentileCont, TYPE_BIGNUMERIC):
      return ComputePercentileCont<>(percentile_population_,
                                     percentile_.bignumeric_value(),
                                     function_->ignores_null());
    case FCT(FunctionKind::kAnonSum, TYPE_DOUBLE):
    case FCT(FunctionKind::kAnonAvg, TYPE_DOUBLE):
    case FCT(FunctionKind::kAnonVarPop, TYPE_ARRAY):
    case FCT(FunctionKind::kAnonStddevPop, TYPE_ARRAY):
      return GetAnonReturnValue(anon_double_);
    case FCT(FunctionKind::kAnonQuantiles, TYPE_ARRAY):
      return GetDPQuantilesReturnValue(anon_double_);
    case FCT(FunctionKind::kAnonQuantilesWithReportProto, TYPE_ARRAY):
      return GetAnonProtoReturnValue(anon_double_);
    case FCT(FunctionKind::kAnonQuantilesWithReportJson, TYPE_ARRAY):
      return GetAnonJsonReturnValue(anon_double_);
    case FCT(FunctionKind::kAnonSum, TYPE_INT64):
      return GetAnonReturnValue(anon_int64_);
    case FCT(FunctionKind::kAnonSumWithReportProto, TYPE_INT64):
      return GetAnonProtoReturnValue(anon_int64_);
    case FCT(FunctionKind::kAnonAvgWithReportProto, TYPE_DOUBLE):
    case FCT(FunctionKind::kAnonSumWithReportProto, TYPE_DOUBLE):
      return GetAnonProtoReturnValue(anon_double_);
    case FCT(FunctionKind::kAnonSumWithReportJson, TYPE_INT64):
      return GetAnonJsonReturnValue(anon_int64_);
    case FCT(FunctionKind::kAnonAvgWithReportJson, TYPE_DOUBLE):
    case FCT(FunctionKind::kAnonSumWithReportJson, TYPE_DOUBLE):
      return GetAnonJsonReturnValue(anon_double_);

    // $differential_privacy_* functions.
    case FCT(FunctionKind::kDifferentialPrivacySum, TYPE_INT64):
      return GetDPReturnValue(dp_int64_, function_->output_type());
    case FCT(FunctionKind::kDifferentialPrivacyAvg, TYPE_DOUBLE):
    case FCT(FunctionKind::kDifferentialPrivacySum, TYPE_DOUBLE):
      return GetDPReturnValue(dp_double_, function_->output_type());
    case FCT(FunctionKind::kDifferentialPrivacyVarPop, TYPE_ARRAY):
    case FCT(FunctionKind::kDifferentialPrivacyStddevPop, TYPE_ARRAY):
      return GetDPReturnValue(dp_double_);

    case FCT(FunctionKind::kDifferentialPrivacyQuantiles, TYPE_ARRAY):
      if (IsDPJsonReportFunction(function_->output_type())) {
        return GetDPJsonReturnValue(dp_double_);
      } else if (IsDPProtoReportFunction(function_->output_type())) {
        return GetDPProtoReturnValue(dp_double_);
      } else {
        return GetDPQuantilesReturnValue(dp_double_);
      }
  }
  return ::zetasql_base::UnimplementedErrorBuilder()
         << "Unsupported aggregate function: " << function_->debug_name() << "("
         << input_type_->DebugString() << ")";
}

}  // namespace

absl::StatusOr<std::unique_ptr<AggregateAccumulator>>
BuiltinAggregateFunction::CreateAccumulator(absl::Span<const Value> args,
                                            CollatorList collator_list,
                                            EvaluationContext* context) const {
  return BuiltinAggregateAccumulator::Create(this, input_type(), args,
                                             std::move(collator_list), context);
}

namespace {

bool IsDeletableSketchInitFunction(FunctionKind kind) {
  return false;
}

// Accumulator implementation for BinaryStatFunction.
class BinaryStatAccumulator : public AggregateAccumulator {
 public:
  static absl::StatusOr<std::unique_ptr<BinaryStatAccumulator>> Create(
      const BinaryStatFunction* function, const Type* input_type,
      absl::Span<const Value> args, EvaluationContext* context) {
    auto accumulator = absl::WrapUnique(
        new BinaryStatAccumulator(function, input_type, args, context));
    ZETASQL_RETURN_IF_ERROR(accumulator->Reset());
    return accumulator;
  }

  BinaryStatAccumulator(const BinaryStatAccumulator&) = delete;
  BinaryStatAccumulator& operator=(const BinaryStatAccumulator&) = delete;

  ~BinaryStatAccumulator() override {
    context_->memory_accountant()->ReturnBytes(requested_bytes_);
  }

  absl::Status Reset() final;

  bool Accumulate(const Value& value, bool* stop_accumulation,
                  absl::Status* status) override;

  absl::StatusOr<Value> GetFinalResult(bool inputs_in_defined_order) override;

 private:
  BinaryStatAccumulator(const BinaryStatFunction* function,
                        const Type* input_type, absl::Span<const Value> args,
                        EvaluationContext* context)
      : function_(function),
        input_type_(input_type),
        min_required_pair_count_(
            function_->kind() == FunctionKind::kCovarPop ||
                    IsDeletableSketchInitFunction(function_->kind())
                ? 1
                : 2),
        args_(args.begin(), args.end()),
        context_(context) {}

  const BinaryStatFunction* function_;
  const Type* input_type_;
  const int64_t min_required_pair_count_;
  const std::vector<Value> args_;
  EvaluationContext* context_;

  int64_t requested_bytes_ = 0;

  int64_t pair_count_ = 0;
  long double mean_x_ = 0;
  long double variance_x_ = 0;
  long double mean_y_ = 0;
  long double variance_y_ = 0;
  long double covar_ = 0;
  bool input_has_nan_or_inf_ = false;
  NumericValue::CovarianceAggregator numeric_covariance_aggregator_;    // Covar
  NumericValue::CorrelationAggregator numeric_correlation_aggregator_;  // Corr
  BigNumericValue::CovarianceAggregator
      bignumeric_covariance_aggregator_;  // Covar
  BigNumericValue::CorrelationAggregator
      bignumeric_correlation_aggregator_;  // Corr
};

absl::Status BinaryStatAccumulator::Reset() {
  context_->memory_accountant()->ReturnBytes(requested_bytes_);

  requested_bytes_ = sizeof(*this);

  absl::Status status;
  if (!context_->memory_accountant()->RequestBytes(requested_bytes_, &status)) {
    return status;
  }

  switch (FCT2(function_->kind(),
               input_type_->AsStruct()->field(0).type->kind(),
               input_type_->AsStruct()->field(1).type->kind())) {
    case FCT2(FunctionKind::kCovarPop, TYPE_NUMERIC, TYPE_NUMERIC):
    case FCT2(FunctionKind::kCovarSamp, TYPE_NUMERIC, TYPE_NUMERIC):
      numeric_covariance_aggregator_ = NumericValue::CovarianceAggregator();
      pair_count_ = 0;
      break;
    case FCT2(FunctionKind::kCorr, TYPE_NUMERIC, TYPE_NUMERIC):
      numeric_correlation_aggregator_ = NumericValue::CorrelationAggregator();
      pair_count_ = 0;
      break;
    case FCT2(FunctionKind::kCovarPop, TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
    case FCT2(FunctionKind::kCovarSamp, TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
      bignumeric_covariance_aggregator_ =
          BigNumericValue::CovarianceAggregator();
      pair_count_ = 0;
      break;
    case FCT2(FunctionKind::kCorr, TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
      bignumeric_correlation_aggregator_ =
          BigNumericValue::CorrelationAggregator();
      pair_count_ = 0;
      break;
    case FCT2(FunctionKind::kCovarPop, TYPE_DOUBLE, TYPE_DOUBLE):
    case FCT2(FunctionKind::kCovarSamp, TYPE_DOUBLE, TYPE_DOUBLE):
    case FCT2(FunctionKind::kCorr, TYPE_DOUBLE, TYPE_DOUBLE):
      pair_count_ = 0;
      mean_x_ = 0;
      variance_x_ = 0;
      mean_y_ = 0;
      variance_y_ = 0;
      covar_ = 0;
      input_has_nan_or_inf_ = false;
      break;
  }

  return absl::OkStatus();
}

bool BinaryStatAccumulator::Accumulate(const Value& value,
                                       bool* stop_accumulation,
                                       absl::Status* status) {
  *stop_accumulation = false;

  if (value.type_kind() != TYPE_STRUCT || value.num_fields() != 2) {
    *status = zetasql_base::InternalErrorBuilder().LogError().EmitStackTrace()
              << "Unexpected value type in BinaryStatAccumulator::Accumulate: "
              << value.DebugString();
    return false;
  }

  const Value& arg_y = value.field(0);
  const Value& arg_x = value.field(1);

  if (arg_x.is_null() || arg_y.is_null()) {
    return true;
  }

  ++pair_count_;

  switch (FCT2(function_->kind(),
               input_type_->AsStruct()->field(0).type->kind(),
               input_type_->AsStruct()->field(1).type->kind())) {
    case FCT2(FunctionKind::kCovarPop, TYPE_NUMERIC, TYPE_NUMERIC):
    case FCT2(FunctionKind::kCovarSamp, TYPE_NUMERIC, TYPE_NUMERIC):
      numeric_covariance_aggregator_.Add(arg_x.numeric_value(),
                                         arg_y.numeric_value());
      break;
    case FCT2(FunctionKind::kCorr, TYPE_NUMERIC, TYPE_NUMERIC):
      numeric_correlation_aggregator_.Add(arg_x.numeric_value(),
                                          arg_y.numeric_value());
      break;
    case FCT2(FunctionKind::kCovarPop, TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
    case FCT2(FunctionKind::kCovarSamp, TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
      bignumeric_covariance_aggregator_.Add(arg_x.bignumeric_value(),
                                            arg_y.bignumeric_value());
      break;
    case FCT2(FunctionKind::kCorr, TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
      bignumeric_correlation_aggregator_.Add(arg_x.bignumeric_value(),
                                             arg_y.bignumeric_value());
      break;
    case FCT2(FunctionKind::kCovarPop, TYPE_DOUBLE, TYPE_DOUBLE):
    case FCT2(FunctionKind::kCovarSamp, TYPE_DOUBLE, TYPE_DOUBLE):
    case FCT2(FunctionKind::kCorr, TYPE_DOUBLE, TYPE_DOUBLE): {
      const double x = arg_x.ToDouble();
      const double y = arg_y.ToDouble();
      if (!std::isfinite(x) || !std::isfinite(y)) {
        input_has_nan_or_inf_ = true;
      }
      if (input_has_nan_or_inf_) {
        if (pair_count_ >= min_required_pair_count_) {
          *stop_accumulation = true;
        }
        return true;
      }

      *status = UpdateCovariance(x, y, mean_x_, mean_y_, pair_count_, &covar_);
      if (!status->ok()) return false;

      *status = UpdateMeanAndVariance(x, pair_count_, &mean_x_, &variance_x_);
      if (!status->ok()) return false;

      *status = UpdateMeanAndVariance(y, pair_count_, &mean_y_, &variance_y_);
      break;
    }
  }

  return status->ok();
}

absl::StatusOr<Value> BinaryStatAccumulator::GetFinalResult(
    bool /* inputs_in_defined_order */) {
  if (pair_count_ < min_required_pair_count_) {
    return Value::Null(function_->output_type());
  }

  if (input_has_nan_or_inf_) {
    return Value::Double(std::numeric_limits<double>::quiet_NaN());
  }

  absl::Status error;
  double out_double = std::numeric_limits<double>::quiet_NaN();
  switch (FCT2(function_->kind(),
               input_type_->AsStruct()->field(0).type->kind(),
               input_type_->AsStruct()->field(1).type->kind())) {
    case FCT2(FunctionKind::kCovarPop, TYPE_NUMERIC, TYPE_NUMERIC):
      return CreateValueFromOptional(
          numeric_covariance_aggregator_.GetPopulationCovariance(pair_count_));
    case FCT2(FunctionKind::kCovarSamp, TYPE_NUMERIC, TYPE_NUMERIC):
      return CreateValueFromOptional(
          numeric_covariance_aggregator_.GetSamplingCovariance(pair_count_));
    case FCT2(FunctionKind::kCorr, TYPE_NUMERIC, TYPE_NUMERIC):
      return CreateValueFromOptional(
          numeric_correlation_aggregator_.GetCorrelation(pair_count_));
    case FCT2(FunctionKind::kCovarPop, TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
      return CreateValueFromOptional(
          bignumeric_covariance_aggregator_.GetPopulationCovariance(
              pair_count_));
    case FCT2(FunctionKind::kCovarSamp, TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
      return CreateValueFromOptional(
          bignumeric_covariance_aggregator_.GetSamplingCovariance(pair_count_));
    case FCT2(FunctionKind::kCorr, TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
      return CreateValueFromOptional(
          bignumeric_correlation_aggregator_.GetCorrelation(pair_count_));
    case FCT2(FunctionKind::kCovarPop, TYPE_DOUBLE, TYPE_DOUBLE): {
      out_double = static_cast<double>(covar_);
      break;
    }
    case FCT2(FunctionKind::kCovarSamp, TYPE_DOUBLE, TYPE_DOUBLE): {
      // out_double = covar * pair_count / (pair_count - 1)
      long double tmp;
      ZETASQL_RET_CHECK(functions::Multiply(
          covar_, static_cast<long double>(pair_count_), &tmp, &error));
      ZETASQL_RET_CHECK_OK(error);
      ZETASQL_RET_CHECK(functions::Divide(
          tmp, static_cast<long double>(pair_count_ - 1), &tmp, &error));
      ZETASQL_RET_CHECK_OK(error);
      out_double = static_cast<double>(tmp);
      break;
    }
    case FCT2(FunctionKind::kCorr, TYPE_DOUBLE, TYPE_DOUBLE): {
      // out_double = covar / sqrt(variance_x * variance_y)
      long double denominator;
      ZETASQL_RET_CHECK(
          functions::Multiply(variance_x_, variance_y_, &denominator, &error));
      ZETASQL_RET_CHECK_OK(error);

      if (std::fpclassify(denominator) == FP_ZERO &&
          std::fpclassify(covar_) == FP_ZERO) {
        return Value::Double(std::numeric_limits<double>::quiet_NaN());
      }

      long double tmp;
      ZETASQL_RET_CHECK(functions::Sqrt(denominator, &denominator, &error));
      ZETASQL_RET_CHECK_OK(error);
      ZETASQL_RET_CHECK(functions::Divide(covar_, denominator, &tmp, &error));
      ZETASQL_RET_CHECK_OK(error);
      out_double = static_cast<double>(tmp);
      break;
    }
    default: {
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported function: " << function_->debug_name() << "("
             << input_type_->DebugString() << ")";
    }
  }

  return Value::Double(out_double);
}

}  // namespace

absl::StatusOr<std::unique_ptr<AggregateAccumulator>>
BinaryStatFunction::CreateAccumulator(absl::Span<const Value> args,
                                      CollatorList collator_list,
                                      EvaluationContext* context) const {
  // <collator_list> should be empty for bivariate stats functions.
  ZETASQL_RET_CHECK(collator_list.empty());
  return BinaryStatAccumulator::Create(this, input_type(), args, context);
}

namespace {
absl::StatusOr<Value> LikeImpl(const Value& lhs, const Value& rhs,
                               const RE2* regexp) {
  if (lhs.is_null() || rhs.is_null()) {
    return Value::Null(types::BoolType());
  }

  const std::string& text =
      lhs.type_kind() == TYPE_STRING ? lhs.string_value() : lhs.bytes_value();

  if (regexp != nullptr) {
    // Regexp is precompiled
    return Value::Bool(RE2::FullMatch(text, *regexp));
  } else {
    // Regexp is not precompiled, compile it on the fly
    const std::string& pattern =
        rhs.type_kind() == TYPE_STRING ? rhs.string_value() : rhs.bytes_value();
    std::unique_ptr<RE2> regexp;
    ZETASQL_RETURN_IF_ERROR(
        functions::CreateLikeRegexp(pattern, lhs.type_kind(), &regexp));
    return Value::Bool(RE2::FullMatch(text, *regexp));
  }
}

bool IsTrue(const Value& value) {
  return !value.is_null() && value.bool_value();
}

bool IsFalse(const Value& value) {
  return !value.is_null() && !value.bool_value();
}

}  // namespace

absl::StatusOr<Value> LikeFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_CHECK_EQ(2, args.size());
  return LikeImpl(args[0], args[1], regexp_.get());
}

absl::StatusOr<Value> LikeAnyFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_CHECK_LE(1, args.size());
  ABSL_CHECK_EQ(regexp_.size(), args.size() - 1);

  if (args[0].is_null()) {
    return Value::Null(output_type());
  }

  Value result = Value::Bool(false);

  for (int i = 1; i < args.size(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(Value local_result,
                     LikeImpl(args[0], args[i], regexp_[i - 1].get()));
    if (IsTrue(local_result)) {
      return local_result;
    } else if (!IsTrue(result) && !IsFalse(local_result)) {
      result = local_result;
    }
  }
  return result;
}

absl::StatusOr<Value> LikeAllFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_CHECK_LE(1, args.size());
  ABSL_CHECK_EQ(regexp_.size(), args.size() - 1);

  if (args[0].is_null()) {
    return Value::Null(output_type());
  }

  Value result = Value::Bool(true);

  for (int i = 1; i < args.size(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(Value local_result,
                     LikeImpl(args[0], args[i], regexp_[i - 1].get()));
    if (!IsFalse(result) && !IsTrue(local_result)) {
      result = local_result;
    }
  }
  return result;
}

absl::StatusOr<Value> LikeAnyArrayFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2)
      << "LIKE ANY with UNNEST has exactly 2 arguments";

  // Return FALSE if the patterns array is NULL or empty and NULL if the search
  // input is NULL
  if (args[1].is_null() || args[1].is_empty_array()) {
    return Value::Bool(false);
  }
  if (args[0].is_null()) {
    return Value::Null(output_type());
  }

  // For cases with the rhs is a subquery expression creating an ARRAY, the
  // number of regexps will be less than the number of elements and the regexp
  // for each element will be generated during execution
  ZETASQL_RET_CHECK_LE(regexp_.size(), args[1].num_elements())
      << "The number of regular expressions should be less than or equal to"
         "the number of arguments in the pattern list";

  Value result = Value::Bool(false);

  for (int i = 0; i < args[1].num_elements(); ++i) {
    const RE2* current_regexp = i < regexp_.size() ? regexp_[i].get() : nullptr;
    ZETASQL_ASSIGN_OR_RETURN(Value local_result,
                     LikeImpl(args[0], args[1].element(i), current_regexp));
    if (IsTrue(local_result)) {
      return local_result;
    } else if (!IsTrue(result) && !IsFalse(local_result)) {
      result = local_result;
    }
  }
  return result;
}

absl::StatusOr<Value> LikeAllArrayFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2)
      << "LIKE ANY with UNNEST has exactly 2 arguments";

  // Return TRUE if the patterns array is NULL or empty and NULL if the search
  // input is NULL
  if (args[1].is_null() || args[1].is_empty_array()) {
    return Value::Bool(true);
  }
  if (args[0].is_null()) {
    return Value::Null(output_type());
  }

  // For cases with the rhs is a subquery expression creating an ARRAY, the
  // number of regexps will be less than the number of elements and the regexp
  // for each element will be generated during execution
  ZETASQL_RET_CHECK_LE(regexp_.size(), args[1].num_elements())
      << "The number of regular expressions should be less than or equal to"
         "the number of arguments in the pattern list";

  Value result = Value::Bool(true);

  for (int i = 0; i < args[1].num_elements(); ++i) {
    // If there is not a precomputed regexp for a pattern, then a nullptr can
    // be passed to LikeImpl() to compute the regexp during execution
    const RE2* current_regexp = i < regexp_.size() ? regexp_[i].get() : nullptr;
    ZETASQL_ASSIGN_OR_RETURN(Value local_result,
                     LikeImpl(args[0], args[1].element(i), current_regexp));
    if (!IsFalse(result) && !IsTrue(local_result)) {
      result = local_result;
    }
  }
  return result;
}

bool BitwiseFunction::Eval(absl::Span<const TupleData* const> params,
                           absl::Span<const Value> args,
                           EvaluationContext* context, Value* result,
                           absl::Status* status) const {
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  switch (FCT(kind(), args[0].type_kind())) {
    case FCT(FunctionKind::kBitwiseNot, TYPE_INT32):
      return InvokeUnary<int32_t>(&functions::BitwiseNot<int32_t>, args, result,
                                  status);
    case FCT(FunctionKind::kBitwiseNot, TYPE_INT64):
      return InvokeUnary<int64_t>(&functions::BitwiseNot<int64_t>, args, result,
                                  status);
    case FCT(FunctionKind::kBitwiseNot, TYPE_UINT32):
      return InvokeUnary<uint32_t>(&functions::BitwiseNot<uint32_t>, args,
                                   result, status);
    case FCT(FunctionKind::kBitwiseNot, TYPE_UINT64):
      return InvokeUnary<uint64_t>(&functions::BitwiseNot<uint64_t>, args,
                                   result, status);
    case FCT(FunctionKind::kBitwiseNot, TYPE_BYTES):
      return InvokeBytes<std::string>(&functions::BitwiseNotBytes, result,
                                      status, args[0].bytes_value());

    case FCT(FunctionKind::kBitwiseOr, TYPE_INT32):
      return InvokeBinary<int32_t>(&functions::BitwiseOr<int32_t>, args, result,
                                   status);
    case FCT(FunctionKind::kBitwiseOr, TYPE_INT64):
      return InvokeBinary<int64_t>(&functions::BitwiseOr<int64_t>, args, result,
                                   status);
    case FCT(FunctionKind::kBitwiseOr, TYPE_UINT32):
      return InvokeBinary<uint32_t>(&functions::BitwiseOr<uint32_t>, args,
                                    result, status);
    case FCT(FunctionKind::kBitwiseOr, TYPE_UINT64):
      return InvokeBinary<uint64_t>(&functions::BitwiseOr<uint64_t>, args,
                                    result, status);
    case FCT(FunctionKind::kBitwiseOr, TYPE_BYTES):
      return InvokeBytes<std::string>(
          &functions::BitwiseBinaryOpBytes<std::bit_or>, result, status,
          args[0].bytes_value(), args[1].bytes_value());

    case FCT(FunctionKind::kBitwiseXor, TYPE_INT32):
      return InvokeBinary<int32_t>(&functions::BitwiseXor<int32_t>, args,
                                   result, status);
    case FCT(FunctionKind::kBitwiseXor, TYPE_INT64):
      return InvokeBinary<int64_t>(&functions::BitwiseXor<int64_t>, args,
                                   result, status);
    case FCT(FunctionKind::kBitwiseXor, TYPE_UINT32):
      return InvokeBinary<uint32_t>(&functions::BitwiseXor<uint32_t>, args,
                                    result, status);
    case FCT(FunctionKind::kBitwiseXor, TYPE_UINT64):
      return InvokeBinary<uint64_t>(&functions::BitwiseXor<uint64_t>, args,
                                    result, status);
    case FCT(FunctionKind::kBitwiseXor, TYPE_BYTES):
      return InvokeBytes<std::string>(
          &functions::BitwiseBinaryOpBytes<std::bit_xor>, result, status,
          args[0].bytes_value(), args[1].bytes_value());

    case FCT(FunctionKind::kBitwiseAnd, TYPE_INT32):
      return InvokeBinary<int32_t>(&functions::BitwiseAnd<int32_t>, args,
                                   result, status);
    case FCT(FunctionKind::kBitwiseAnd, TYPE_INT64):
      return InvokeBinary<int64_t>(&functions::BitwiseAnd<int64_t>, args,
                                   result, status);
    case FCT(FunctionKind::kBitwiseAnd, TYPE_UINT32):
      return InvokeBinary<uint32_t>(&functions::BitwiseAnd<uint32_t>, args,
                                    result, status);
    case FCT(FunctionKind::kBitwiseAnd, TYPE_UINT64):
      return InvokeBinary<uint64_t>(&functions::BitwiseAnd<uint64_t>, args,
                                    result, status);
    case FCT(FunctionKind::kBitwiseAnd, TYPE_BYTES):
      return InvokeBytes<std::string>(
          &functions::BitwiseBinaryOpBytes<std::bit_and>, result, status,
          args[0].bytes_value(), args[1].bytes_value());

    case FCT(FunctionKind::kBitwiseLeftShift, TYPE_INT32):
      return InvokeBinary<int32_t, int32_t, int64_t>(
          &functions::BitwiseLeftShift<int32_t>, args, result, status);
    case FCT(FunctionKind::kBitwiseLeftShift, TYPE_INT64):
      return InvokeBinary<int64_t, int64_t, int64_t>(
          &functions::BitwiseLeftShift<int64_t>, args, result, status);
    case FCT(FunctionKind::kBitwiseLeftShift, TYPE_UINT32):
      return InvokeBinary<uint32_t, uint32_t, int64_t>(
          &functions::BitwiseLeftShift<uint32_t>, args, result, status);
    case FCT(FunctionKind::kBitwiseLeftShift, TYPE_UINT64):
      return InvokeBinary<uint64_t, uint64_t, int64_t>(
          &functions::BitwiseLeftShift<uint64_t>, args, result, status);
    case FCT(FunctionKind::kBitwiseLeftShift, TYPE_BYTES):
      return InvokeBytes<std::string>(&functions::BitwiseLeftShiftBytes, result,
                                      status, args[0].bytes_value(),
                                      args[1].int64_value());

    case FCT(FunctionKind::kBitwiseRightShift, TYPE_INT32):
      return InvokeBinary<int32_t, int32_t, int64_t>(
          &functions::BitwiseRightShift<int32_t>, args, result, status);
    case FCT(FunctionKind::kBitwiseRightShift, TYPE_INT64):
      return InvokeBinary<int64_t, int64_t, int64_t>(
          &functions::BitwiseRightShift<int64_t>, args, result, status);
    case FCT(FunctionKind::kBitwiseRightShift, TYPE_UINT32):
      return InvokeBinary<uint32_t, uint32_t, int64_t>(
          &functions::BitwiseRightShift<uint32_t>, args, result, status);
    case FCT(FunctionKind::kBitwiseRightShift, TYPE_UINT64):
      return InvokeBinary<uint64_t, uint64_t, int64_t>(
          &functions::BitwiseRightShift<uint64_t>, args, result, status);
    case FCT(FunctionKind::kBitwiseRightShift, TYPE_BYTES):
      return InvokeBytes<std::string>(&functions::BitwiseRightShiftBytes,
                                      result, status, args[0].bytes_value(),
                                      args[1].int64_value());
  }
  *status = ::zetasql_base::UnimplementedErrorBuilder()
            << "Unsupported bitwise function: " << debug_name();
  return false;
}

bool BitCountFunction::Eval(absl::Span<const TupleData* const> params,
                            absl::Span<const Value> args,
                            EvaluationContext* context, Value* result,
                            absl::Status* status) const {
  ABSL_CHECK_EQ(1, args.size());
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  switch (args[0].type_kind()) {
    case TYPE_INT32:
      *result = Value::Make(functions::BitCount(args[0].int32_value()));
      return true;
    case TYPE_INT64:
      *result = Value::Make(functions::BitCount(args[0].int64_value()));
      return true;
    case TYPE_UINT64:
      *result = Value::Make(functions::BitCount(args[0].uint64_value()));
      return true;
    case TYPE_BYTES:
      *result = Value::Make(functions::BitCount(args[0].bytes_value()));
      return true;
    default:
      break;
  }
  *status = ::zetasql_base::UnimplementedErrorBuilder()
            << "Unsupported BitCount function: " << debug_name();
  return false;
}

bool MathFunction::Eval(absl::Span<const TupleData* const> params,
                        absl::Span<const Value> args,
                        EvaluationContext* context, Value* result,
                        absl::Status* status) const {
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  switch (FCT(kind(), output_type()->kind())) {
    case FCT(FunctionKind::kAbs, TYPE_INT32):
      return InvokeUnary<int32_t>(&functions::Abs<int32_t>, args, result,
                                  status);
    case FCT(FunctionKind::kAbs, TYPE_INT64):
      return InvokeUnary<int64_t>(&functions::Abs<int64_t>, args, result,
                                  status);
    case FCT(FunctionKind::kAbs, TYPE_UINT32):
      return InvokeUnary<uint32_t>(&functions::Abs<uint32_t>, args, result,
                                   status);
    case FCT(FunctionKind::kAbs, TYPE_UINT64):
      return InvokeUnary<uint64_t>(&functions::Abs<uint64_t>, args, result,
                                   status);
    case FCT(FunctionKind::kAbs, TYPE_FLOAT):
      return InvokeUnary<float>(&functions::Abs<float>, args, result, status);
    case FCT(FunctionKind::kAbs, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Abs<double>, args, result, status);
    case FCT(FunctionKind::kAbs, TYPE_NUMERIC):
      return InvokeUnary<NumericValue>(&functions::Abs<NumericValue>, args,
                                       result, status);
    case FCT(FunctionKind::kAbs, TYPE_BIGNUMERIC):
      return InvokeUnary<BigNumericValue>(&functions::Abs<BigNumericValue>,
                                          args, result, status);

    case FCT(FunctionKind::kSign, TYPE_INT32):
      return InvokeUnary<int32_t>(&functions::Sign<int32_t>, args, result,
                                  status);
    case FCT(FunctionKind::kSign, TYPE_INT64):
      return InvokeUnary<int64_t>(&functions::Sign<int64_t>, args, result,
                                  status);
    case FCT(FunctionKind::kSign, TYPE_UINT32):
      return InvokeUnary<uint32_t>(&functions::Sign<uint32_t>, args, result,
                                   status);
    case FCT(FunctionKind::kSign, TYPE_UINT64):
      return InvokeUnary<uint64_t>(&functions::Sign<uint64_t>, args, result,
                                   status);
    case FCT(FunctionKind::kSign, TYPE_FLOAT):
      return InvokeUnary<float>(&functions::Sign<float>, args, result, status);
    case FCT(FunctionKind::kSign, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Sign<double>, args, result,
                                 status);
    case FCT(FunctionKind::kSign, TYPE_NUMERIC):
      return InvokeUnary<NumericValue>(&functions::Sign<NumericValue>, args,
                                       result, status);
    case FCT(FunctionKind::kSign, TYPE_BIGNUMERIC):
      return InvokeUnary<BigNumericValue>(&functions::Sign<BigNumericValue>,
                                          args, result, status);

    case FCT(FunctionKind::kIsInf, TYPE_BOOL):
      return InvokeUnary<bool, double>(&functions::IsInf<double>, args, result,
                                       status);
    case FCT(FunctionKind::kIsNan, TYPE_BOOL):
      return InvokeUnary<bool, double>(&functions::IsNan<double>, args, result,
                                       status);

    case FCT(FunctionKind::kIeeeDivide, TYPE_DOUBLE):
      return InvokeBinary<double>(&functions::IeeeDivide<double>, args, result,
                                  status);
    case FCT(FunctionKind::kIeeeDivide, TYPE_FLOAT):
      return InvokeBinary<float>(&functions::IeeeDivide<float>, args, result,
                                 status);

    case FCT(FunctionKind::kSqrt, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Sqrt<double>, args, result,
                                 status);
    case FCT(FunctionKind::kSqrt, TYPE_NUMERIC):
      return InvokeUnary<NumericValue>(&functions::Sqrt<NumericValue>, args,
                                       result, status);
    case FCT(FunctionKind::kSqrt, TYPE_BIGNUMERIC):
      return InvokeUnary<BigNumericValue>(&functions::Sqrt<BigNumericValue>,
                                          args, result, status);
    case FCT(FunctionKind::kCbrt, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Cbrt<double>, args, result,
                                 status);
    case FCT(FunctionKind::kCbrt, TYPE_NUMERIC):
      return InvokeUnary<NumericValue>(&functions::Cbrt<NumericValue>, args,
                                       result, status);
    case FCT(FunctionKind::kCbrt, TYPE_BIGNUMERIC):
      return InvokeUnary<BigNumericValue>(&functions::Cbrt<BigNumericValue>,
                                          args, result, status);
    case FCT(FunctionKind::kPow, TYPE_DOUBLE):
      return InvokeBinary<double>(&functions::Pow<double>, args, result,
                                  status);
    case FCT(FunctionKind::kPow, TYPE_NUMERIC):
      return InvokeBinary<NumericValue>(&functions::Pow<NumericValue>, args,
                                        result, status);
    case FCT(FunctionKind::kPow, TYPE_BIGNUMERIC):
      return InvokeBinary<BigNumericValue>(&functions::Pow<BigNumericValue>,
                                           args, result, status);
    case FCT(FunctionKind::kExp, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Exp<double>, args, result, status);
    case FCT(FunctionKind::kExp, TYPE_NUMERIC):
      return InvokeUnary<NumericValue>(&functions::Exp<NumericValue>, args,
                                       result, status);
    case FCT(FunctionKind::kExp, TYPE_BIGNUMERIC):
      return InvokeUnary<BigNumericValue>(&functions::Exp<BigNumericValue>,
                                          args, result, status);
    case FCT(FunctionKind::kNaturalLogarithm, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::NaturalLogarithm<double>, args,
                                 result, status);
    case FCT(FunctionKind::kNaturalLogarithm, TYPE_NUMERIC):
      return InvokeUnary<NumericValue>(
          &functions::NaturalLogarithm<NumericValue>, args, result, status);
    case FCT(FunctionKind::kNaturalLogarithm, TYPE_BIGNUMERIC):
      return InvokeUnary<BigNumericValue>(
          &functions::NaturalLogarithm<BigNumericValue>, args, result, status);
    case FCT(FunctionKind::kDecimalLogarithm, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::DecimalLogarithm<double>, args,
                                 result, status);
    case FCT(FunctionKind::kDecimalLogarithm, TYPE_NUMERIC):
      return InvokeUnary<NumericValue>(
          &functions::DecimalLogarithm<NumericValue>, args, result, status);
    case FCT(FunctionKind::kDecimalLogarithm, TYPE_BIGNUMERIC):
      return InvokeUnary<BigNumericValue>(
          &functions::DecimalLogarithm<BigNumericValue>, args, result, status);
    case FCT(FunctionKind::kLogarithm, TYPE_DOUBLE):
      if (args.size() == 1) {
        return InvokeUnary<double>(&functions::NaturalLogarithm<double>, args,
                                   result, status);
      } else {
        return InvokeBinary<double>(&functions::Logarithm<double>, args, result,
                                    status);
      }
    case FCT(FunctionKind::kLogarithm, TYPE_NUMERIC):
      if (args.size() == 1) {
        return InvokeUnary<NumericValue>(
            &functions::NaturalLogarithm<NumericValue>, args, result, status);
      } else {
        return InvokeBinary<NumericValue>(&functions::Logarithm<NumericValue>,
                                          args, result, status);
      }
    case FCT(FunctionKind::kLogarithm, TYPE_BIGNUMERIC):
      if (args.size() == 1) {
        return InvokeUnary<BigNumericValue>(
            &functions::NaturalLogarithm<BigNumericValue>, args, result,
            status);
      } else {
        return InvokeBinary<BigNumericValue>(
            &functions::Logarithm<BigNumericValue>, args, result, status);
      }

    case FCT(FunctionKind::kCos, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Cos<double>, args, result, status);
    case FCT(FunctionKind::kCosh, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Cosh<double>, args, result,
                                 status);
    case FCT(FunctionKind::kAcos, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Acos<double>, args, result,
                                 status);
    case FCT(FunctionKind::kAcosh, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Acosh<double>, args, result,
                                 status);
    case FCT(FunctionKind::kSin, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Sin<double>, args, result, status);
    case FCT(FunctionKind::kSinh, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Sinh<double>, args, result,
                                 status);
    case FCT(FunctionKind::kAsin, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Asin<double>, args, result,
                                 status);
    case FCT(FunctionKind::kAsinh, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Asinh<double>, args, result,
                                 status);
    case FCT(FunctionKind::kTan, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Tan<double>, args, result, status);
    case FCT(FunctionKind::kTanh, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Tanh<double>, args, result,
                                 status);
    case FCT(FunctionKind::kAtan, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Atan<double>, args, result,
                                 status);
    case FCT(FunctionKind::kAtanh, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Atanh<double>, args, result,
                                 status);
    case FCT(FunctionKind::kAtan2, TYPE_DOUBLE):
      return InvokeBinary<double>(&functions::Atan2<double>, args, result,
                                  status);
    case FCT(FunctionKind::kCsc, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Csc<double>, args, result, status);

    case FCT(FunctionKind::kSec, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Sec<double>, args, result, status);

    case FCT(FunctionKind::kCot, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Cot<double>, args, result, status);

    case FCT(FunctionKind::kCsch, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Csch<double>, args, result,
                                 status);

    case FCT(FunctionKind::kSech, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Sech<double>, args, result,
                                 status);

    case FCT(FunctionKind::kCoth, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Coth<double>, args, result,
                                 status);

    case FCT(FunctionKind::kRound, TYPE_DOUBLE):
      if (args.size() == 1) {
        return InvokeUnary<double>(&functions::Round<double>, args, result,
                                   status);
      } else if (args.size() == 2) {
        return InvokeBinary<double, double, int64_t>(
            &functions::RoundDecimal<double>, args, result, status);
      } else if (args.size() == 3 &&
                 context->GetLanguageOptions().LanguageFeatureEnabled(
                     FEATURE_ROUND_WITH_ROUNDING_MODE)) {
        *status = ::zetasql_base::UnimplementedErrorBuilder()
                  << "ROUND with a rounding_mode is only allowed for NUMERIC "
                     "or BIGNUMERIC values: "
                  << debug_name();
        return false;
      }
      break;
    case FCT(FunctionKind::kRound, TYPE_FLOAT):
      if (args.size() == 1) {
        return InvokeUnary<float>(&functions::Round<float>, args, result,
                                  status);
      } else if (args.size() == 2) {
        return InvokeBinary<float, float, int64_t>(
            &functions::RoundDecimal<float>, args, result, status);
      } else if (args.size() == 3 &&
                 context->GetLanguageOptions().LanguageFeatureEnabled(
                     FEATURE_ROUND_WITH_ROUNDING_MODE)) {
        *status = ::zetasql_base::UnimplementedErrorBuilder()
                  << "ROUND with a rounding_mode is only allowed for NUMERIC "
                     "or BIGNUMERIC values: "
                  << debug_name();
        return false;
      }
      break;
    case FCT(FunctionKind::kRound, TYPE_NUMERIC):
      if (args.size() == 1) {
        return InvokeUnary<NumericValue>(&functions::Round<NumericValue>, args,
                                         result, status);
      } else if (args.size() == 2) {
        return InvokeBinary<NumericValue, NumericValue, int64_t>(
            &functions::RoundDecimal<NumericValue>, args, result, status);
      } else if (args.size() == 3 &&
                 context->GetLanguageOptions().LanguageFeatureEnabled(
                     FEATURE_ROUND_WITH_ROUNDING_MODE)) {
        return InvokeRoundTernary<NumericValue, NumericValue, int64_t,
                                  functions::RoundingMode>(
            &functions::RoundDecimalWithRoundingMode<NumericValue>, args,
            result, status);
      }
      break;
    case FCT(FunctionKind::kRound, TYPE_BIGNUMERIC):
      if (args.size() == 1) {
        return InvokeUnary<BigNumericValue>(&functions::Round<BigNumericValue>,
                                            args, result, status);
      } else if (args.size() == 2) {
        return InvokeBinary<BigNumericValue, BigNumericValue, int64_t>(
            &functions::RoundDecimal<BigNumericValue>, args, result, status);
      } else if (args.size() == 3 &&
                 context->GetLanguageOptions().LanguageFeatureEnabled(
                     FEATURE_ROUND_WITH_ROUNDING_MODE)) {
        return InvokeRoundTernary<BigNumericValue, BigNumericValue, int64_t,
                                  functions::RoundingMode>(
            &functions::RoundDecimalWithRoundingMode<BigNumericValue>, args,
            result, status);
      }
      break;
    case FCT(FunctionKind::kTrunc, TYPE_DOUBLE):
      if (args.size() == 1) {
        return InvokeUnary<double>(&functions::Trunc<double>, args, result,
                                   status);
      } else {
        return InvokeBinary<double, double, int64_t>(
            &functions::TruncDecimal<double>, args, result, status);
      }
    case FCT(FunctionKind::kTrunc, TYPE_FLOAT):
      if (args.size() == 1) {
        return InvokeUnary<float>(&functions::Trunc<float>, args, result,
                                  status);
      } else {
        return InvokeBinary<float, float, int64_t>(
            &functions::TruncDecimal<float>, args, result, status);
      }
    case FCT(FunctionKind::kTrunc, TYPE_NUMERIC):
      if (args.size() == 1) {
        return InvokeUnary<NumericValue>(&functions::Trunc<NumericValue>, args,
                                         result, status);
      } else {
        return InvokeBinary<NumericValue, NumericValue, int64_t>(
            &functions::TruncDecimal<NumericValue>, args, result, status);
      }
    case FCT(FunctionKind::kTrunc, TYPE_BIGNUMERIC):
      if (args.size() == 1) {
        return InvokeUnary<BigNumericValue>(&functions::Trunc<BigNumericValue>,
                                            args, result, status);
      } else {
        return InvokeBinary<BigNumericValue, BigNumericValue, int64_t>(
            &functions::TruncDecimal<BigNumericValue>, args, result, status);
      }
    case FCT(FunctionKind::kCeil, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Ceil<double>, args, result,
                                 status);
    case FCT(FunctionKind::kCeil, TYPE_FLOAT):
      return InvokeUnary<float>(&functions::Ceil<float>, args, result, status);
    case FCT(FunctionKind::kCeil, TYPE_NUMERIC):
      return InvokeUnary<NumericValue>(&functions::Ceil<NumericValue>, args,
                                       result, status);
    case FCT(FunctionKind::kCeil, TYPE_BIGNUMERIC):
      return InvokeUnary<BigNumericValue>(&functions::Ceil<BigNumericValue>,
                                          args, result, status);
    case FCT(FunctionKind::kFloor, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Floor<double>, args, result,
                                 status);
    case FCT(FunctionKind::kFloor, TYPE_FLOAT):
      return InvokeUnary<float>(&functions::Floor<float>, args, result, status);
    case FCT(FunctionKind::kFloor, TYPE_NUMERIC):
      return InvokeUnary<NumericValue>(&functions::Floor<NumericValue>, args,
                                       result, status);
    case FCT(FunctionKind::kFloor, TYPE_BIGNUMERIC):
      return InvokeUnary<BigNumericValue>(&functions::Floor<BigNumericValue>,
                                          args, result, status);
  }
  *status = ::zetasql_base::UnimplementedErrorBuilder()
            << "Unsupported math function: " << debug_name();
  return false;
}

bool NetFunction::Eval(absl::Span<const TupleData* const> params,
                       absl::Span<const Value> args, EvaluationContext* context,
                       Value* result, absl::Status* status) const {
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  switch (FCT_TYPE_ARITY(kind(), args[0].type_kind(), args.size())) {
    case FCT_TYPE_ARITY(FunctionKind::kNetFormatIP, TYPE_INT64, 1):
      return InvokeString<std::string>(&functions::net::FormatIP, result,
                                       status, args[0].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetParseIP, TYPE_STRING, 1):
      return Invoke<int64_t>(&functions::net::ParseIP, result, status,
                             args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetFormatPackedIP, TYPE_BYTES, 1):
      return InvokeString<std::string>(&functions::net::FormatPackedIP, result,
                                       status, args[0].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetParsePackedIP, TYPE_STRING, 1):
      return InvokeBytes<std::string>(&functions::net::ParsePackedIP, result,
                                      status, args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetIPInNet, TYPE_STRING, 2):
      return Invoke<bool>(&functions::net::IPInNet, result, status,
                          args[0].string_value(), args[1].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetMakeNet, TYPE_STRING, 2):
      return InvokeString<std::string>(&functions::net::MakeNet, result, status,
                                       args[0].string_value(),
                                       args[1].int32_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetHost, TYPE_STRING, 1):
      return InvokeNullableString<absl::string_view>(
          &functions::net::Host, result, status, args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetRegDomain, TYPE_STRING, 1):
      return InvokeNullableString<absl::string_view>(
          &functions::net::RegDomain, result, status, args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetPublicSuffix, TYPE_STRING, 1):
      return InvokeNullableString<absl::string_view>(
          &functions::net::PublicSuffix, result, status,
          args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetIPFromString, TYPE_STRING, 1):
      return InvokeBytes<std::string>(&functions::net::IPFromString, result,
                                      status, args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetSafeIPFromString, TYPE_STRING, 1):
      return InvokeNullableBytes<std::string>(&functions::net::SafeIPFromString,
                                              result, status,
                                              args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetIPToString, TYPE_BYTES, 1):
      return InvokeString<std::string>(&functions::net::IPToString, result,
                                       status, args[0].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetIPNetMask, TYPE_INT64, 2):
      return InvokeBytes<std::string>(&functions::net::IPNetMask, result,
                                      status, args[0].int64_value(),
                                      args[1].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetIPTrunc, TYPE_BYTES, 2):
      return InvokeBytes<std::string>(&functions::net::IPTrunc, result, status,
                                      args[0].bytes_value(),
                                      args[1].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetIPv4FromInt64, TYPE_INT64, 1):
      return InvokeBytes<std::string>(&functions::net::IPv4FromInt64, result,
                                      status, args[0].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kNetIPv4ToInt64, TYPE_BYTES, 1):
      return Invoke<int64_t>(&functions::net::IPv4ToInt64, result, status,
                             args[0].bytes_value());
  }
  *status = ::zetasql_base::UnimplementedErrorBuilder()
            << "Unsupported net function: " << debug_name();
  return false;
}

bool StringFunction::Eval(absl::Span<const TupleData* const> params,
                          absl::Span<const Value> args,
                          EvaluationContext* context, Value* result,
                          absl::Status* status) const {
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  switch (FCT_TYPE_ARITY(kind(), args[0].type_kind(), args.size())) {
    case FCT_TYPE_ARITY(FunctionKind::kStrpos, TYPE_STRING, 2):
    case FCT_TYPE_ARITY(FunctionKind::kInstr, TYPE_STRING, 2):
      return Invoke<int64_t>(&functions::StrPosOccurrenceUtf8, result, status,
                             args[0].string_value(), args[1].string_value(),
                             /*pos=*/1, /*occurrence=*/1);
    case FCT_TYPE_ARITY(FunctionKind::kStrpos, TYPE_BYTES, 2):
    case FCT_TYPE_ARITY(FunctionKind::kInstr, TYPE_BYTES, 2):
      return Invoke<int64_t>(&functions::StrPosOccurrenceBytes, result, status,
                             args[0].bytes_value(), args[1].bytes_value(),
                             /*pos=*/1, /*occurrence=*/1);
    case FCT_TYPE_ARITY(FunctionKind::kInstr, TYPE_STRING, 3):
      return Invoke<int64_t>(&functions::StrPosOccurrenceUtf8, result, status,
                             args[0].string_value(), args[1].string_value(),
                             /*pos=*/args[2].int64_value(), /*occurrence=*/1);
    case FCT_TYPE_ARITY(FunctionKind::kInstr, TYPE_STRING, 4):
      return Invoke<int64_t>(&functions::StrPosOccurrenceUtf8, result, status,
                             args[0].string_value(), args[1].string_value(),
                             /*pos=*/args[2].int64_value(),
                             /*occurrence=*/args[3].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kInstr, TYPE_BYTES, 3):
      return Invoke<int64_t>(&functions::StrPosOccurrenceBytes, result, status,
                             args[0].bytes_value(), args[1].bytes_value(),
                             /*pos=*/args[2].int64_value(), /*occurrence=*/1);
    case FCT_TYPE_ARITY(FunctionKind::kInstr, TYPE_BYTES, 4):
      return Invoke<int64_t>(&functions::StrPosOccurrenceBytes, result, status,
                             args[0].bytes_value(), args[1].bytes_value(),
                             /*pos=*/args[2].int64_value(),
                             /*occurrence=*/args[3].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kLength, TYPE_STRING, 1):
      return Invoke<int64_t>(&functions::LengthUtf8, result, status,
                             args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kLength, TYPE_BYTES, 1):
      return Invoke<int64_t>(&functions::LengthBytes, result, status,
                             args[0].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kByteLength, TYPE_STRING, 1):
      return Invoke<int64_t>(&functions::LengthBytes, result, status,
                             args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kByteLength, TYPE_BYTES, 1):
      return Invoke<int64_t>(&functions::LengthBytes, result, status,
                             args[0].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kCharLength, TYPE_STRING, 1):
      return Invoke<int64_t>(&functions::LengthUtf8, result, status,
                             args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kAscii, TYPE_STRING, 1):
      return Invoke<int64_t>(&functions::FirstCharOfStringToASCII, result,
                             status, args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kAscii, TYPE_BYTES, 1):
      return Invoke<int64_t>(&functions::FirstByteOfBytesToASCII, result,
                             status, args[0].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kUnicode, TYPE_STRING, 1):
      return Invoke<int64_t>(&functions::FirstCharToCodePoint, result, status,
                             args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kStartsWith, TYPE_STRING, 2):
      return Invoke<bool>(&functions::StartsWithUtf8, result, status,
                          args[0].string_value(), args[1].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kStartsWith, TYPE_BYTES, 2):
      return Invoke<bool>(&functions::StartsWithBytes, result, status,
                          args[0].bytes_value(), args[1].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kEndsWith, TYPE_STRING, 2):
      return Invoke<bool>(&functions::EndsWithUtf8, result, status,
                          args[0].string_value(), args[1].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kEndsWith, TYPE_BYTES, 2):
      return Invoke<bool>(&functions::EndsWithBytes, result, status,
                          args[0].bytes_value(), args[1].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kChr, TYPE_INT64, 1):
      return InvokeString<std::string>(&functions::CodePointToString, result,
                                       status, args[0].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kSubstr, TYPE_STRING, 2):
      return InvokeString<absl::string_view>(&functions::SubstrUtf8, result,
                                             status, args[0].string_value(),
                                             args[1].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kSubstr, TYPE_STRING, 3):
      return InvokeString<absl::string_view>(
          &functions::SubstrWithLengthUtf8, result, status,
          args[0].string_value(), args[1].int64_value(), args[2].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kSubstr, TYPE_BYTES, 2):
      return InvokeBytes<absl::string_view>(&functions::SubstrBytes, result,
                                            status, args[0].bytes_value(),
                                            args[1].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kSubstr, TYPE_BYTES, 3):
      return InvokeBytes<absl::string_view>(
          &functions::SubstrWithLengthBytes, result, status,
          args[0].bytes_value(), args[1].int64_value(), args[2].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kTrim, TYPE_STRING, 1):
      return InvokeString<absl::string_view>(&functions::TrimSpacesUtf8, result,
                                             status, args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kTrim, TYPE_STRING, 2):
      return InvokeString<absl::string_view>(&functions::TrimUtf8, result,
                                             status, args[0].string_value(),
                                             args[1].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kTrim, TYPE_BYTES, 2):
      return InvokeBytes<absl::string_view>(&functions::TrimBytes, result,
                                            status, args[0].bytes_value(),
                                            args[1].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kLtrim, TYPE_STRING, 1):
      return InvokeString<absl::string_view>(&functions::LeftTrimSpacesUtf8,
                                             result, status,
                                             args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kLtrim, TYPE_STRING, 2):
      return InvokeString<absl::string_view>(&functions::LeftTrimUtf8, result,
                                             status, args[0].string_value(),
                                             args[1].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kLtrim, TYPE_BYTES, 2):
      return InvokeBytes<absl::string_view>(&functions::LeftTrimBytes, result,
                                            status, args[0].bytes_value(),
                                            args[1].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kRtrim, TYPE_STRING, 1):
      return InvokeString<absl::string_view>(&functions::RightTrimSpacesUtf8,
                                             result, status,
                                             args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kRtrim, TYPE_STRING, 2):
      return InvokeString<absl::string_view>(&functions::RightTrimUtf8, result,
                                             status, args[0].string_value(),
                                             args[1].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kRtrim, TYPE_BYTES, 2):
      return InvokeBytes<absl::string_view>(&functions::RightTrimBytes, result,
                                            status, args[0].bytes_value(),
                                            args[1].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kLeft, TYPE_STRING, 2):
      return InvokeString<absl::string_view>(&functions::LeftUtf8, result,
                                             status, args[0].string_value(),
                                             args[1].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kLeft, TYPE_BYTES, 2):
      return InvokeBytes<absl::string_view>(&functions::LeftBytes, result,
                                            status, args[0].bytes_value(),
                                            args[1].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kRight, TYPE_STRING, 2):
      return InvokeString<absl::string_view>(&functions::RightUtf8, result,
                                             status, args[0].string_value(),
                                             args[1].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kRight, TYPE_BYTES, 2):
      return InvokeBytes<absl::string_view>(&functions::RightBytes, result,
                                            status, args[0].bytes_value(),
                                            args[1].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kReplace, TYPE_BYTES, 3):
      return InvokeBytes<std::string>(
          &functions::ReplaceBytes, result, status, args[0].bytes_value(),
          args[1].bytes_value(), args[2].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kReplace, TYPE_STRING, 3):
      return InvokeString<std::string>(
          &functions::ReplaceUtf8, result, status, args[0].string_value(),
          args[1].string_value(), args[2].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kRepeat, TYPE_BYTES, 2):
      return InvokeBytes<std::string>(&functions::Repeat, result, status,
                                      args[0].bytes_value(),
                                      args[1].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kRepeat, TYPE_STRING, 2):
      return InvokeString<std::string>(&functions::Repeat, result, status,
                                       args[0].string_value(),
                                       args[1].int64_value());

    case FCT_TYPE_ARITY(FunctionKind::kReverse, TYPE_BYTES, 1):
      return InvokeBytes<std::string>(&functions::ReverseBytes, result, status,
                                      args[0].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kReverse, TYPE_STRING, 1):
      return InvokeString<std::string>(&functions::ReverseUtf8, result, status,
                                       args[0].string_value());

    case FCT_TYPE_ARITY(FunctionKind::kLpad, TYPE_BYTES, 2):
      return InvokeBytes<std::string>(&functions::LeftPadBytesDefault, result,
                                      status, args[0].bytes_value(),
                                      args[1].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kLpad, TYPE_STRING, 2):
      return InvokeString<std::string>(&functions::LeftPadUtf8Default, result,
                                       status, args[0].string_value(),
                                       args[1].int64_value());

    case FCT_TYPE_ARITY(FunctionKind::kLpad, TYPE_BYTES, 3):
      return InvokeBytes<std::string>(
          &functions::LeftPadBytes, result, status, args[0].bytes_value(),
          args[1].int64_value(), args[2].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kLpad, TYPE_STRING, 3):
      return InvokeString<std::string>(
          &functions::LeftPadUtf8, result, status, args[0].string_value(),
          args[1].int64_value(), args[2].string_value());

    case FCT_TYPE_ARITY(FunctionKind::kRpad, TYPE_BYTES, 2):
      return InvokeBytes<std::string>(&functions::RightPadBytesDefault, result,
                                      status, args[0].bytes_value(),
                                      args[1].int64_value());
    case FCT_TYPE_ARITY(FunctionKind::kRpad, TYPE_STRING, 2):
      return InvokeString<std::string>(&functions::RightPadUtf8Default, result,
                                       status, args[0].string_value(),
                                       args[1].int64_value());

    case FCT_TYPE_ARITY(FunctionKind::kRpad, TYPE_BYTES, 3):
      return InvokeBytes<std::string>(
          &functions::RightPadBytes, result, status, args[0].bytes_value(),
          args[1].int64_value(), args[2].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kRpad, TYPE_STRING, 3):
      return InvokeString<std::string>(
          &functions::RightPadUtf8, result, status, args[0].string_value(),
          args[1].int64_value(), args[2].string_value());

    case FCT_TYPE_ARITY(FunctionKind::kSafeConvertBytesToString, TYPE_BYTES, 1):
      return InvokeString<std::string>(&functions::SafeConvertBytes, result,
                                       status, args[0].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kNormalize, TYPE_STRING, 1):
    case FCT_TYPE_ARITY(FunctionKind::kNormalize, TYPE_STRING, 2):
      return InvokeString<std::string>(
          &functions::Normalize, result, status, args[0].string_value(),
          args.size() == 2
              ? static_cast<functions::NormalizeMode>(args[1].enum_value())
              : functions::NormalizeMode::NFC,
          false /* is_casefold */);
    case FCT_TYPE_ARITY(FunctionKind::kNormalizeAndCasefold, TYPE_STRING, 1):
    case FCT_TYPE_ARITY(FunctionKind::kNormalizeAndCasefold, TYPE_STRING, 2):
      return InvokeString<std::string>(
          &functions::Normalize, result, status, args[0].string_value(),
          args.size() == 2
              ? static_cast<functions::NormalizeMode>(args[1].enum_value())
              : functions::NormalizeMode::NFC,
          true /* is_casefold */);
    case FCT_TYPE_ARITY(FunctionKind::kToBase64, TYPE_BYTES, 1):
      return InvokeString<std::string>(&functions::ToBase64, result, status,
                                       args[0].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kFromBase64, TYPE_STRING, 1):
      return InvokeBytes<std::string>(&functions::FromBase64, result, status,
                                      args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kToHex, TYPE_BYTES, 1):
      return InvokeString<std::string>(&functions::ToHex, result, status,
                                       args[0].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kFromHex, TYPE_STRING, 1):
      return InvokeBytes<std::string>(&functions::FromHex, result, status,
                                      args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kSoundex, TYPE_STRING, 1):
      return InvokeString<std::string>(&functions::Soundex, result, status,
                                       args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kTranslate, TYPE_STRING, 3):
      return InvokeString<std::string>(
          &functions::TranslateUtf8, result, status, args[0].string_value(),
          args[1].string_value(), args[2].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kTranslate, TYPE_BYTES, 3):
      return InvokeBytes<std::string>(
          &functions::TranslateBytes, result, status, args[0].bytes_value(),
          args[1].bytes_value(), args[2].bytes_value());
    case FCT_TYPE_ARITY(FunctionKind::kInitCap, TYPE_STRING, 1):
      return InvokeString<std::string>(&functions::InitialCapitalizeDefault,
                                       result, status, args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kInitCap, TYPE_STRING, 2):
      return InvokeString<std::string>(&functions::InitialCapitalize, result,
                                       status, args[0].string_value(),
                                       args[1].string_value());
  }
  *status = ::zetasql_base::UnimplementedErrorBuilder()
            << "Unsupported string function: " << debug_name();
  return false;
}

bool NumericFunction::Eval(absl::Span<const TupleData* const> params,
                           absl::Span<const Value> args,
                           EvaluationContext* context, Value* result,
                           absl::Status* status) const {
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  switch (FCT_TYPE_ARITY(kind(), args[0].type_kind(), args.size())) {
    case FCT_TYPE_ARITY(FunctionKind::kParseNumeric, TYPE_STRING, 1):
      return Invoke<NumericValue>(&functions::ParseNumeric, result, status,
                                  args[0].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kParseBignumeric, TYPE_STRING, 1):
      return Invoke<BigNumericValue>(&functions::ParseBigNumeric, result,
                                     status, args[0].string_value());
  }
  *status = ::zetasql_base::UnimplementedErrorBuilder()
            << "Unsupported numeric function: " << debug_name();
  return false;
}

absl::StatusOr<Value> RegexpFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (HasNulls(args)) return Value::Null(output_type());
  std::unique_ptr<const functions::RegExp> runtime_regexp;
  const functions::RegExp* regexp = const_regexp_.get();
  if (regexp == nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(runtime_regexp, CreateRegexp(args[1]));
    regexp = runtime_regexp.get();
  }
  switch (FCT(kind(), args[0].type_kind())) {
    case FCT(FunctionKind::kRegexpContains, TYPE_STRING): {
      return Contains<TYPE_STRING>(args, *regexp);
    }
    case FCT(FunctionKind::kRegexpContains, TYPE_BYTES): {
      return Contains<TYPE_BYTES>(args, *regexp);
    }
    case FCT(FunctionKind::kRegexpMatch, TYPE_STRING): {
      return Match<TYPE_STRING>(args, *regexp);
    }
    case FCT(FunctionKind::kRegexpMatch, TYPE_BYTES): {
      return Match<TYPE_BYTES>(args, *regexp);
    }
    case FCT(FunctionKind::kRegexpExtract, TYPE_STRING): {
      return Extract<TYPE_STRING>(args, *regexp);
    }
    case FCT(FunctionKind::kRegexpExtract, TYPE_BYTES): {
      return Extract<TYPE_BYTES>(args, *regexp);
    }
    case FCT(FunctionKind::kRegexpInstr, TYPE_STRING): {
      return Instr<TYPE_STRING>(args, *regexp);
    }
    case FCT(FunctionKind::kRegexpInstr, TYPE_BYTES): {
      return Instr<TYPE_BYTES>(args, *regexp);
    }
    case FCT(FunctionKind::kRegexpExtractAll, TYPE_STRING): {
      return ExtractAll<TYPE_STRING>(args, *regexp);
    }
    case FCT(FunctionKind::kRegexpExtractAll, TYPE_BYTES): {
      return ExtractAll<TYPE_BYTES>(args, *regexp);
    }
    case FCT(FunctionKind::kRegexpReplace, TYPE_STRING): {
      return Replace<TYPE_STRING>(args, *regexp);
    }
    case FCT(FunctionKind::kRegexpReplace, TYPE_BYTES): {
      return Replace<TYPE_BYTES>(args, *regexp);
    }
  }
  return ::zetasql_base::UnimplementedErrorBuilder()
         << "Unsupported regexp function: " << debug_name();
}

absl::StatusOr<Value> SplitFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (HasNulls(args)) return Value::Null(output_type());
  absl::Status status;
  std::vector<std::string> parts;
  std::vector<Value> values;
  if (args[0].type()->kind() == TYPE_STRING) {
    const std::string& delimiter =
        (args.size() == 1) ? "," : args[1].string_value();
    if (!functions::SplitUtf8(args[0].string_value(), delimiter, &parts,
                              &status)) {
      return status;
    }
    for (const std::string& s : parts) {
      values.push_back(Value::String(s));
    }
    return Value::Array(types::StringArrayType(), values);
  } else {
    absl::Status status;
    if (!functions::SplitBytes(args[0].bytes_value(), args[1].bytes_value(),
                               &parts, &status)) {
      return status;
    }
    for (const std::string& s : parts) {
      values.push_back(Value::Bytes(s));
    }
    return Value::Array(types::BytesArrayType(), values);
  }
}

absl::StatusOr<Value> ConcatFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (HasNulls(args)) return Value::Null(output_type());

  int64_t output_size = 0;
  if (output_type()->kind() == TYPE_STRING) {
    std::string result;
    for (const Value& in : args) {
      output_size += in.string_value().size();
      if (output_size > context->options().max_value_byte_size) {
        return ConcatError(context->options().max_value_byte_size,
                           zetasql_base::SourceLocation::current());
      }
    }
    for (const Value& in : args) {
      result.append(in.string_value());
    }
    return Value::String(result);
  } else {
    std::string result;
    for (const Value& in : args) {
      output_size += in.bytes_value().size();
      if (output_size > context->options().max_value_byte_size) {
        return ConcatError(context->options().max_value_byte_size,
                           zetasql_base::SourceLocation::current());
      }
    }
    for (const Value& in : args) {
      result.append(in.bytes_value());
    }
    return Value::Bytes(result);
  }
}

absl::StatusOr<Value> CaseConverterFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (HasNulls(args)) return Value::Null(output_type());
  std::string result;
  absl::Status error;
  switch (FCT(kind(), output_type()->kind())) {
    case FCT(FunctionKind::kUpper, TYPE_STRING):
      if (!functions::UpperUtf8(args[0].string_value(), &result, &error)) {
        return error;
      } else {
        return Value::String(result);
      }
    case FCT(FunctionKind::kLower, TYPE_STRING):
      if (!functions::LowerUtf8(args[0].string_value(), &result, &error)) {
        return error;
      } else {
        return Value::String(result);
      }
    case FCT(FunctionKind::kUpper, TYPE_BYTES):
      if (!functions::UpperBytes(args[0].bytes_value(), &result, &error)) {
        return error;
      } else {
        return Value::Bytes(result);
      }
    case FCT(FunctionKind::kLower, TYPE_BYTES):
      if (!functions::LowerBytes(args[0].bytes_value(), &result, &error)) {
        return error;
      } else {
        return Value::Bytes(result);
      }
  }
  return ::zetasql_base::UnimplementedErrorBuilder()
         << "Unsupported function: " << debug_name();
}

absl::StatusOr<Value> MakeProtoFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_CHECK_EQ(args.size(), fields_.size());
  absl::Cord proto_cord;
  google::protobuf::io::CordOutputStream cord_output;
  {
    ProtoUtil::WriteFieldOptions write_field_options{
        .allow_null_map_keys =
            !context->GetLanguageOptions().LanguageFeatureEnabled(
                LanguageFeature::FEATURE_V_1_3_PROTO_MAPS)};

    google::protobuf::io::CodedOutputStream coded_output(&cord_output);
    for (int i = 0; i < args.size(); i++) {
      bool nondeterministic = false;
      ZETASQL_RETURN_IF_ERROR(ProtoUtil::WriteField(
          write_field_options, fields_[i].first, fields_[i].second, args[i],
          &nondeterministic, &coded_output));
      if (nondeterministic) {
        context->SetNonDeterministicOutput();
      }
    }
  }
  proto_cord = cord_output.Consume();
  return Value::Proto(output_type()->AsProto(), proto_cord);
}

struct FilterFieldsFunction::FieldPathTrieNode {
  // nullptr for the root node.
  const google::protobuf::FieldDescriptor* const field_descriptor;
  // Indicates whether this node is include or exclude.
  bool include;
  // Child nodes which are keyed by proto field tag numbers.
  absl::flat_hash_map<int, std::unique_ptr<FieldPathTrieNode>> children;
};

FilterFieldsFunction::FilterFieldsFunction(const Type* output_type,
                                           bool reset_cleared_required_fields)
    : SimpleBuiltinScalarFunction(FunctionKind::kFilterFields, output_type),
      reset_cleared_required_fields_(reset_cleared_required_fields) {}

FilterFieldsFunction::~FilterFieldsFunction() = default;

absl::Status FilterFieldsFunction::RecursivelyPrune(
    const FieldPathTrieNode* node, google::protobuf::Message* message) const {
  ZETASQL_RET_CHECK(node) << "FilterFieldsFunction is uninitialized!";
  ZETASQL_RET_CHECK(!node->children.empty());
  if (node->include) {
    return HandleIncludedMessage(node->children, message);
  } else {
    return HandleExcludedMessage(node->children, message);
  }
  return absl::OkStatus();
}

absl::Status FilterFieldsFunction::PruneOnMessageField(
    const google::protobuf::Reflection& reflection, const FieldPathTrieNode* child,
    const google::protobuf::FieldDescriptor* field_descriptor,
    google::protobuf::Message* message) const {
  if (!field_descriptor->is_repeated()) {
    ZETASQL_RETURN_IF_ERROR(RecursivelyPrune(
        child, reflection.MutableMessage(message, field_descriptor)));
  } else {
    int field_size = reflection.FieldSize(*message, field_descriptor);
    for (int i = 0; i < field_size; ++i) {
      ZETASQL_RETURN_IF_ERROR(RecursivelyPrune(
          child,
          reflection.MutableRepeatedMessage(message, field_descriptor, i)));
    }
  }
  return absl::OkStatus();
}

absl::Status FilterFieldsFunction::HandleIncludedMessage(
    const TagToNodeMap& child_nodes, google::protobuf::Message* message) const {
  const google::protobuf::Reflection& reflection = *message->GetReflection();
  // In an inclusive node, fields in children is either:
  // * fully exclusive, to be cleared
  // * partially exclusive, to be pruned recursively
  for (const auto& [tag, child_node] : child_nodes) {
    const google::protobuf::FieldDescriptor* child_descriptor =
        child_node->field_descriptor;
    if (child_node->children.empty()) {
      ZETASQL_RET_CHECK(!child_node->include);
      reflection.ClearField(message, child_descriptor);
      continue;
    }
    ZETASQL_RET_CHECK(child_descriptor->message_type())
        << child_descriptor->DebugString();

    // Prune recursively.
    ZETASQL_RETURN_IF_ERROR(PruneOnMessageField(reflection, child_node.get(),
                                        child_descriptor, message));
  }
  return absl::OkStatus();
}

absl::Status FilterFieldsFunction::HandleExcludedMessage(
    const TagToNodeMap& child_nodes, google::protobuf::Message* message) const {
  const google::protobuf::Reflection& reflection = *message->GetReflection();
  std::vector<const google::protobuf::FieldDescriptor*> fields;
  reflection.ListFields(*message, &fields);
  // In an exclusive node, clear all fields except for those who have a child
  // node which we'll process accordingly.
  for (const google::protobuf::FieldDescriptor* field_descriptor : fields) {
    const auto child_it = child_nodes.find(field_descriptor->number());
    if (child_it == child_nodes.end()) {
      reflection.ClearField(message, field_descriptor);
      continue;
    }
    if (child_it->second->children.empty()) {
      ZETASQL_RET_CHECK(child_it->second->include);
      continue;
    }

    // Prune recursively.
    ZETASQL_RETURN_IF_ERROR(PruneOnMessageField(reflection, child_it->second.get(),
                                        field_descriptor, message));
  }
  return absl::OkStatus();
}

absl::Status FilterFieldsFunction::AddFieldPath(
    bool include,
    const std::vector<const google::protobuf::FieldDescriptor*>& field_path) {
  if (root_node_ == nullptr) {
    // Root node has the reverse inclusive/exclusive status with first inserted
    // field path.
    root_node_ = absl::WrapUnique<FieldPathTrieNode>(
        new FieldPathTrieNode{nullptr, !include, {}});
  }
  FieldPathTrieNode* node = root_node_.get();
  for (int i = 0; i < field_path.size(); ++i) {
    const google::protobuf::FieldDescriptor* field_descriptor = field_path[i];
    std::unique_ptr<FieldPathTrieNode>& child_node =
        node->children[field_descriptor->number()];
    if (child_node != nullptr) {
      ZETASQL_RET_CHECK_NE(i, field_path.size() - 1);
    } else {
      ZETASQL_RET_CHECK_NE(node->include, include);
      child_node = absl::WrapUnique<FieldPathTrieNode>(
          new FieldPathTrieNode{field_descriptor, node->include, {}});
    }
    node = child_node.get();
  }
  // Override inclusion/exclusion status inherited from parent node.
  node->include = include;
  return absl::OkStatus();
}

absl::StatusOr<Value> FilterFieldsFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK(args[0].type()->IsProto());
  if (args[0].is_null()) {
    return Value::Null(args[0].type());
  }
  google::protobuf::DynamicMessageFactory factory;
  std::unique_ptr<google::protobuf::Message> mutable_root_message =
      absl::WrapUnique(args[0].ToMessage(&factory));
  ZETASQL_RETURN_IF_ERROR(
      RecursivelyPrune(root_node_.get(), mutable_root_message.get()));
  if (reset_cleared_required_fields_) {
    InitializeRequiredFields(mutable_root_message.get());
  }
  return Value::Proto(args[0].type()->AsProto(),
                      mutable_root_message->SerializeAsCord());
}

// Sets the proto field denoted by <path> to <new_field_value>. The first proto
// field in <path> is looked up with regards to <parent_proto>.
static absl::StatusOr<Value> ReplaceProtoFields(
    const Value parent_proto,
    const std::vector<const google::protobuf::FieldDescriptor*>& path,
    const Value new_field_value, EvaluationContext* context) {
  // TODO: Refactor this to code to modify the serialized proto
  // instead of using reflection
  ZETASQL_RET_CHECK(parent_proto.type()->IsProto());
  if (parent_proto.is_null()) {
    return MakeEvalError() << "REPLACE_FIELDS() cannot be used to modify the "
                              "fields of a NULL valued proto";
  }
  if (new_field_value.is_null() && path.back()->is_required()) {
    return MakeEvalError()
           << "REPLACE_FIELDS() cannot be used to clear required fields";
  }
  if (new_field_value.is_null() &&
      path.back()->containing_type()->options().map_entry() &&
      context->GetLanguageOptions().LanguageFeatureEnabled(
          LanguageFeature::FEATURE_V_1_3_PROTO_MAPS)) {
    return MakeEvalError() << "REPLACE_FIELDS() cannot be used to clear a "
                              "field of a map entry";
  }
  google::protobuf::DynamicMessageFactory factory;
  auto mutable_root_message =
      absl::WrapUnique(parent_proto.ToMessage(&factory));
  if (!mutable_root_message->IsInitialized()) {
    return MakeEvalError()
           << "REPLACE_FIELDS() cannot be used on a proto with missing fields: "
           << mutable_root_message->InitializationErrorString();
  }
  google::protobuf::Message* message_to_modify = mutable_root_message.get();
  const google::protobuf::Reflection* reflection = message_to_modify->GetReflection();
  // Get the Reflection object until the second-to-last path element as this
  // message contains the field that we want to modify (i.e., the last element).
  for (auto iter = path.begin(); iter != path.end() - 1; ++iter) {
    if (!reflection->HasField(*message_to_modify, *iter)) {
      return MakeEvalError() << "REPLACE_FIELDS() cannot be used to modify the "
                                "fields of an unset proto";
    }
    message_to_modify =
        reflection->MutableMessage(message_to_modify, *iter, &factory);
    reflection = message_to_modify->GetReflection();
  }
  if (new_field_value.is_null()) {
    reflection->ClearField(message_to_modify, path.back());
  } else {
    if (path.back()->is_repeated()) {
      // Clear repeated fields, so MergeValueToProtoField does not append to
      // them.
      reflection->ClearField(message_to_modify, path.back());

      // There is a bug with verification of Proto repeated fields which are
      // set to unordered values (via new_field_value). Verification assumes
      // that the repeated field value is ordered leading to false negatives. If
      // new_field_value contains an unordered array value for a repeated field,
      // result from ZetaSQL reference driver is marked non-deterministic and
      // is ignored.
      // TODO : Fix the ordering issue in Proto repeated field,
      // after which below safeguard can be removed.
      if (InternalValue::GetOrderKind(new_field_value) !=
          InternalValue::kPreservesOrder) {
        context->SetNonDeterministicOutput();
      }
    }
    ZETASQL_RETURN_IF_ERROR(MergeValueToProtoField(
        new_field_value, path.back(), /*use_wire_format_annotations=*/false,
        &factory, message_to_modify));
  }

  // We should not be able to deinitialize, but defensively verify and return
  // here or else we will `ABSL_CHECK` and crash the process.
  if (!mutable_root_message->IsInitialized()) {
    return MakeEvalError()
           << "REPLACE_FIELDS() cannot be used to make an uninitialized proto: "
           << mutable_root_message->InitializationErrorString();
  }

  return Value::Proto(parent_proto.type()->AsProto(),
                      mutable_root_message->SerializeAsCord());
}

// Sets the field denoted by <path> to <new_field_value>. <path_index>
// indicates which Struct field in <path> should be extracted from
// <parent_struct>.
static absl::StatusOr<Value> ReplaceStructFields(
    const Value parent_struct,
    const ReplaceFieldsFunction::StructAndProtoPath& path, int path_index,
    const Value new_field_value, EvaluationContext* context) {
  ZETASQL_RET_CHECK(parent_struct.type()->IsStruct());
  std::vector<Value> new_struct_values = parent_struct.fields();
  Value new_field = new_field_value;
  if (path_index != path.struct_index_path.size() - 1) {
    ZETASQL_ASSIGN_OR_RETURN(
        new_field,
        ReplaceStructFields(
            parent_struct.fields()[path.struct_index_path[path_index]], path,
            path_index + 1, new_field_value, context));
  } else if (!path.field_descriptor_path.empty()) {
    // We have found the last Struct field in <path>. If <path> then traverses
    // into a nested proto, write <new_field_value> to the final proto field.
    ZETASQL_RET_CHECK(parent_struct.fields()[path.struct_index_path.back()]
                  .type()
                  ->IsProto());
    ZETASQL_ASSIGN_OR_RETURN(new_field,
                     ReplaceProtoFields(
                         parent_struct.fields()[path.struct_index_path.back()],
                         path.field_descriptor_path, new_field_value, context));
  }
  new_struct_values[path.struct_index_path[path_index]] = new_field;
  return Value::Struct(parent_struct.type()->AsStruct(), new_struct_values);
}

absl::StatusOr<Value> ReplaceFieldsFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(
      field_paths_.size(),
      args.size() - 1 /*The first argument is the root proto or struct*/);
  ZETASQL_RET_CHECK(args[0].type()->IsStructOrProto());
  Value output = args[0];
  for (int i = 0; i < field_paths_.size(); ++i) {
    if (output_type()->IsStruct()) {
      ZETASQL_ASSIGN_OR_RETURN(
          output, ReplaceStructFields(output, field_paths_[i],
                                      /*path_index=*/0, args[i + 1], context));
    } else {
      ZETASQL_ASSIGN_OR_RETURN(
          output,
          ReplaceProtoFields(output, field_paths_[i].field_descriptor_path,
                             args[i + 1], context));
    }
  }
  return output;
}

absl::StatusOr<Value> NullaryFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  switch (kind()) {
    case FunctionKind::kCurrentDate: {
      return Value::Date(context->GetCurrentDateInDefaultTimezone());
    }
    case FunctionKind::kCurrentTimestamp: {
      int64_t timestamp = context->GetCurrentTimestamp();
      return Value::TimestampFromUnixMicros(timestamp);
    }
    case FunctionKind::kCurrentDatetime: {
      return Value::Datetime(context->GetCurrentDatetimeInDefaultTimezone());
    }
    case FunctionKind::kCurrentTime: {
      return Value::Time(context->GetCurrentTimeInDefaultTimezone());
    }
    case FunctionKind::kPi: {
      return Value::Double(functions::Pi());
    }
    case FunctionKind::kPiNumeric: {
      return Value::Numeric(functions::Pi_Numeric());
    }
    case FunctionKind::kPiBigNumeric: {
      return Value::BigNumeric(functions::Pi_BigNumeric());
    }
    default:
      break;
  }
  return ::zetasql_base::UnimplementedErrorBuilder()
         << "Unsupported nullary function: " << debug_name();
}

absl::StatusOr<Value> DateTimeUnaryFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (args[0].is_null()) {
    return Value::Null(output_type());
  }
  switch (FCT(kind(), args[0].type_kind())) {
    case FCT(FunctionKind::kCurrentDate, TYPE_STRING): {
      int32_t date;
      ZETASQL_RETURN_IF_ERROR(functions::ExtractFromTimestamp(
          functions::DATE, context->GetCurrentTimestamp(),
          functions::kMicroseconds, args[0].string_value(), &date));
      return Value::Date(date);
    }
    case FCT(FunctionKind::kCurrentDatetime, TYPE_STRING): {
      DatetimeValue datetime;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToDatetime(
          functions::MakeTime(context->GetCurrentTimestamp(),
                              functions::kMicroseconds),
          args[0].string_value(), &datetime));
      return Value::Datetime(datetime);
    }
    case FCT(FunctionKind::kCurrentTime, TYPE_STRING): {
      TimeValue time;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToTime(
          functions::MakeTime(context->GetCurrentTimestamp(),
                              functions::kMicroseconds),
          args[0].string_value(), &time));
      return Value::Time(time);
    }
    case FCT(FunctionKind::kDateFromUnixDate, TYPE_INT64):
      if (args[0].int64_value() < types::kDateMin ||
          args[0].int64_value() > types::kDateMax) {
        return ::zetasql_base::OutOfRangeErrorBuilder()
               << "DATE_FROM_UNIX_DATE range is " << types::kDateMin << " to "
               << types::kDateMax << " but saw " << args[0].int64_value();
      }
      return Value::Date(args[0].int64_value());
    case FCT(FunctionKind::kUnixDate, TYPE_DATE):
      return output_type()->IsInt64() ? Value::Int64(args[0].date_value())
                                      : Value::Int32(args[0].date_value());
  }
  return ::zetasql_base::UnimplementedErrorBuilder()
         << "Unsupported function: " << debug_name();
}

absl::StatusOr<Value> FormatDateDatetimeTimestampFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_GE(args.size(), 2);
  ABSL_DCHECK_LE(args.size(), 3);
  if (HasNulls(args)) return Value::Null(output_type());
  std::string result_string;
  switch (args[1].type_kind()) {
    case TYPE_DATE:
      ZETASQL_RETURN_IF_ERROR(functions::FormatDateToString(
          args[0].string_value(), args[1].date_value(),
          {.expand_Q = true, .expand_J = true}, &result_string));
      break;
    case TYPE_DATETIME:
      ZETASQL_RETURN_IF_ERROR(functions::FormatDatetimeToStringWithOptions(
          args[0].string_value(), args[1].datetime_value(),
          {.expand_Q = true, .expand_J = true}, &result_string));
      break;
    case TYPE_TIMESTAMP: {
      if (args.size() == 2) {
        ZETASQL_RETURN_IF_ERROR(functions::FormatTimestampToString(
            args[0].string_value(),
            context->GetLanguageOptions().LanguageFeatureEnabled(
                FEATURE_TIMESTAMP_NANOS)
                ? args[1].ToTime()
                : absl::FromUnixMicros(args[1].ToUnixMicros()),
            context->GetDefaultTimeZone(), {.expand_Q = true, .expand_J = true},
            &result_string));
      } else {
        ZETASQL_RETURN_IF_ERROR(functions::FormatTimestampToString(
            args[0].string_value(),
            context->GetLanguageOptions().LanguageFeatureEnabled(
                FEATURE_TIMESTAMP_NANOS)
                ? args[1].ToTime()
                : absl::FromUnixMicros(args[1].ToUnixMicros()),
            args[2].string_value(), {.expand_Q = true, .expand_J = true},
            &result_string));
      }
      break;
    }
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported type " << args[1].type()->DebugString()
             << " in function " << debug_name();
  }
  return Value::String(result_string);
}

absl::StatusOr<Value> FormatTimeFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (HasNulls(args)) return Value::Null(output_type());
  std::string result_string;
  ZETASQL_RETURN_IF_ERROR(functions::FormatTimeToString(
      args[0].string_value(), args[1].time_value(), &result_string));
  return Value::String(result_string);
}

absl::StatusOr<Value> TimestampConversionFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (HasNulls(args)) return Value::Null(output_type());
  if (!args.empty() && args[0].type()->IsDatetime()) {
    absl::Time timestamp;
    ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
    if (args.size() == 2 && args[1].type()->IsString()) {
      ZETASQL_RETURN_IF_ERROR(functions::ConvertDatetimeToTimestamp(
          args[0].datetime_value(), args[1].string_value(), &timestamp));
    } else if (args.size() == 1) {
      ZETASQL_RETURN_IF_ERROR(functions::ConvertDatetimeToTimestamp(
          args[0].datetime_value(), context->GetDefaultTimeZone(), &timestamp));
    } else {
      return MakeEvalError() << "Unsupported function: " << debug_name();
    }
    return Value::Timestamp(timestamp);
  } else if (!args.empty() && args[0].type()->IsString()) {
    absl::Time timestamp;
    if (args.size() == 2 && args[1].type()->IsString()) {
      ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTimestamp(
          args[0].string_value(), args[1].string_value(),
          GetTimestampScale(context->GetLanguageOptions()), false, &timestamp));
    } else if (args.size() == 1) {
      ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTimestamp(
          args[0].string_value(), context->GetDefaultTimeZone(),
          GetTimestampScale(context->GetLanguageOptions()), true, &timestamp));
    } else {
      return MakeEvalError() << "Unsupported function: " << debug_name();
    }
    const Value result = Value::Timestamp(timestamp);
    ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(result, context));
    return result;
  } else if (!args.empty() && args[0].type()->IsDate()) {
    int64_t timestamp_micros;
    if (args.size() == 2 && args[1].type()->IsString()) {
      ZETASQL_RETURN_IF_ERROR(functions::ConvertDateToTimestamp(
          args[0].date_value(), functions::kMicroseconds,
          args[1].string_value(), &timestamp_micros));
    } else if (args.size() == 1) {
      ZETASQL_RETURN_IF_ERROR(functions::ConvertDateToTimestamp(
          args[0].date_value(), functions::kMicroseconds,
          context->GetDefaultTimeZone(), &timestamp_micros));
    } else {
      return MakeEvalError() << "Unsupported function: " << debug_name();
    }
    return Value::TimestampFromUnixMicros(timestamp_micros);
  } else if (args.size() == 1 && args[0].type()->IsTimestamp()) {
    return args[0];
  } else {
    return ::zetasql_base::UnimplementedErrorBuilder()
           << "Unsupported function: " << debug_name();
  }
}

static bool Int64Only(absl::Span<const Value> args,
                      EvaluationContext* context) {
  bool int64_only = true;
  for (const auto& each : args) {
    if (!each.type()->IsInt64()) {
      int64_only = false;
      break;
    }
  }
  return int64_only;
}

absl::StatusOr<Value> CivilTimeConstructionAndConversionFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (HasNulls(args)) return Value::Null(output_type());
  switch (kind()) {
    case FunctionKind::kDate: {
      int32_t date;
      if (args.size() == 3 && Int64Only(args, context)) {
        ZETASQL_RETURN_IF_ERROR(functions::ConstructDate(args[0].int64_value(),
                                                 args[1].int64_value(),
                                                 args[2].int64_value(), &date));
      } else if (args.size() == 1 && args[0].type()->IsDatetime()) {
        ZETASQL_RETURN_IF_ERROR(functions::ExtractFromDatetime(
            functions::DATE, args[0].datetime_value(), &date));
      } else if (!args.empty() && args[0].type()->IsTimestamp()) {
        if (args.size() == 2 && args[1].type()->IsString()) {
          ZETASQL_RETURN_IF_ERROR(
              functions::ExtractFromTimestamp(functions::DATE, args[0].ToTime(),
                                              args[1].string_value(), &date));
        } else if (args.size() == 1) {
          ZETASQL_RETURN_IF_ERROR(functions::ExtractFromTimestamp(
              functions::DATE, args[0].ToTime(), context->GetDefaultTimeZone(),
              &date));
        } else {
          return MakeEvalError() << "Unsupported function: " << debug_name();
        }
      } else if (args.size() == 1 && args[0].type()->IsDate()) {
        return args[0];
      } else if (args.size() == 1 && args[0].type()->IsString()) {
        int64_t timestamp_micros;
        ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTimestamp(
            args[0].string_value(), context->GetDefaultTimeZone(),
            functions::kMicroseconds, true, &timestamp_micros));
        ZETASQL_RETURN_IF_ERROR(functions::ExtractFromTimestamp(
            functions::DATE, timestamp_micros, functions::kMicroseconds,
            context->GetDefaultTimeZone(), &date));
      } else {
        return ::zetasql_base::UnimplementedErrorBuilder()
               << "Unsupported function: " << debug_name();
      }
      return Value::Date(date);
    } break;
    case FunctionKind::kTime: {
      TimeValue time;
      if (args.size() == 3 && Int64Only(args, context)) {
        ZETASQL_RETURN_IF_ERROR(functions::ConstructTime(args[0].int64_value(),
                                                 args[1].int64_value(),
                                                 args[2].int64_value(), &time));
        return Value::Time(time);
      } else if (args.size() == 1 && args[0].type()->IsDatetime()) {
        ZETASQL_RETURN_IF_ERROR(functions::ExtractTimeFromDatetime(
            args[0].datetime_value(), &time));
      } else if (!args.empty() && args[0].type()->IsTimestamp()) {
        ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
        if (args.size() == 2 && args[1].type()->IsString()) {
          ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToTime(
              args[0].ToTime(), args[1].string_value(), &time));
        } else if (args.size() == 1) {
          ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToTime(
              args[0].ToTime(), context->GetDefaultTimeZone(), &time));
        } else {
          return MakeEvalError() << "Unsupported function: " << debug_name();
        }
      } else if (args.size() == 1 && args[0].type()->IsTime()) {
        return args[0];
      } else {
        ZETASQL_RET_CHECK_FAIL() << "Unexpected function call for " << debug_name();
      }
      return Value::Time(time);
    } break;
    case FunctionKind::kDatetime: {
      DatetimeValue datetime;
      if (args.size() == 6 && Int64Only(args, context)) {
        ZETASQL_RETURN_IF_ERROR(functions::ConstructDatetime(
            args[0].int64_value(), args[1].int64_value(), args[2].int64_value(),
            args[3].int64_value(), args[4].int64_value(), args[5].int64_value(),
            &datetime));
      } else if (args.size() == 2 && args[0].type()->IsDate() &&
                 args[1].type()->IsTime()) {
        ZETASQL_RETURN_IF_ERROR(functions::ConstructDatetime(
            args[0].date_value(), args[1].time_value(), &datetime));
      } else if (args.size() == 1 && args[0].type()->IsDate()) {
        ZETASQL_RETURN_IF_ERROR(functions::ConstructDatetime(args[0].date_value(),
                                                     TimeValue(), &datetime));
      } else if (!args.empty() && args[0].type()->IsTimestamp()) {
        ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
        if (args.size() == 2 && args[1].type()->IsString()) {
          ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToDatetime(
              args[0].ToTime(), args[1].string_value(), &datetime));
        } else if (args.size() == 1) {
          ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToDatetime(
              args[0].ToTime(), context->GetDefaultTimeZone(), &datetime));
        } else {
          return MakeEvalError() << "Unsupported function: " << debug_name();
        }
      } else if (args.size() == 1 && args[0].type()->IsDatetime()) {
        return args[0];
      } else if (args.size() == 1 && args[0].type()->IsString()) {
        ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToDatetime(
            args[0].string_value(), functions::kMicroseconds, &datetime));
      } else {
        ZETASQL_RET_CHECK_FAIL() << "Unexpected function call for " << debug_name();
      }
      return Value::Datetime(datetime);
    } break;
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unexpected function kind";
  }
}

// This function converts INT64 to TIMESTAMP, and also support identity
// "conversion" from TIMESTAMP to TIMESTAMP.
absl::StatusOr<Value> TimestampFromIntFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  // In the LEGACY mode, other types such as TYPE_STRING can match
  // the signature, but reference implementation doesn't support them.
  if ((args[0].type_kind() != TYPE_INT64 && !args[0].type()->IsTimestamp()) ||
      !output_type()->IsTimestamp()) {
    ZETASQL_RET_CHECK_FAIL() << "Unsupported function: " << debug_name();
  }
  if (HasNulls(args)) return Value::Null(output_type());
  if (args[0].type()->IsTimestamp()) {
    return args[0];
  }

  int64_t scale;
  switch (kind()) {
    case FunctionKind::kTimestampSeconds:
    case FunctionKind::kTimestampFromUnixSeconds:
      scale = 1000000;
      break;
    case FunctionKind::kTimestampMillis:
    case FunctionKind::kTimestampFromUnixMillis:
      scale = 1000;
      break;
    case FunctionKind::kTimestampMicros:
    case FunctionKind::kTimestampFromUnixMicros:
      scale = 1;
      break;
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unexpected function kind";
  }

  const int64_t value = args[0].int64_value();
  if (value < types::kTimestampMin / scale ||
      value > types::kTimestampMax / scale) {
    return MakeEvalError() << "Input value " << args[0].int64_value()
                           << " cannot be converted into a TIMESTAMP, because"
                           << " it would be out of the allowed range between "
                           << types::kTimestampMin << " to "
                           << types::kTimestampMax << " (microseconds)";
  }

  return Value::TimestampFromUnixMicros(value * scale);
}

absl::StatusOr<Value> IntFromTimestampFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  // In the LEGACY mode, other types such as TYPE_STRING can match
  // the signature, but reference implementation doesn't support them.
  if (args[0].type_kind() != TYPE_TIMESTAMP) {
    return ::zetasql_base::UnimplementedErrorBuilder()
           << "Unsupported function: " << debug_name();
  }
  if (HasNulls(args)) return Value::Null(output_type());
  int scale;
  // TODO: UNIX_NANOS will need to do something different.
  switch (kind()) {
    case FunctionKind::kSecondsFromTimestamp:
      scale = 1000000;
      break;
    case FunctionKind::kMillisFromTimestamp:
      scale = 1000;
      break;
    case FunctionKind::kMicrosFromTimestamp:
      scale = 1;
      break;
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unexpected function kind";
  }

  // No overflows possible with division, result truncated downwards.
  int64_t micros = static_cast<int64_t>(args[0].ToUnixMicros());
  int64_t unix_time = micros / scale;
  if (micros < 0 && micros % scale != 0) {
    unix_time--;
  }
  return Value::Int64(unix_time);
}

absl::StatusOr<Value> StringConversionFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (args[0].is_null() || (args.size() == 2 && args[1].is_null())) {
    return Value::Null(output_type());
  }

  std::string result_string;
  switch (args[0].type_kind()) {
    case TYPE_TIMESTAMP: {
      absl::TimeZone timezone;
      if (args.size() == 1) {
        timezone = context->GetDefaultTimeZone();
      } else {
        ZETASQL_RETURN_IF_ERROR(
            functions::MakeTimeZone(args[1].string_value(), &timezone));
      }
      if (context->GetLanguageOptions().LanguageFeatureEnabled(
              FEATURE_TIMESTAMP_NANOS)) {
        ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToString(
            args[0].ToTime(), functions::kNanoseconds, timezone,
            &result_string));
      } else {
        ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToStringWithTruncation(
            args[0].ToUnixMicros(), functions::kMicroseconds, timezone,
            &result_string));
      }
      break;
    }
    case TYPE_DATE:
      ZETASQL_RETURN_IF_ERROR(
          functions::ConvertDateToString(args[0].date_value(), &result_string));
      break;
    case TYPE_TIME:
      ZETASQL_RETURN_IF_ERROR(functions::ConvertTimeToString(
          args[0].time_value(),
          GetTimestampScale(context->GetLanguageOptions()), &result_string));
      break;
    case TYPE_DATETIME:
      ZETASQL_RETURN_IF_ERROR(functions::ConvertDatetimeToString(
          args[0].datetime_value(),
          GetTimestampScale(context->GetLanguageOptions()), &result_string));
      break;
    case TYPE_JSON: {
      ZETASQL_RET_CHECK_EQ(args.size(), 1);
      JSONValue json_storage;
      const LanguageOptions& language_options = context->GetLanguageOptions();
      ZETASQL_ASSIGN_OR_RETURN(
          JSONValueConstRef json_value_const_ref,
          GetJSONValueConstRef(
              args[0],
              JSONParsingOptions{
                  .wide_number_mode =
                      (language_options.LanguageFeatureEnabled(
                           FEATURE_JSON_STRICT_NUMBER_PARSING)
                           ? JSONParsingOptions::WideNumberMode::kExact
                           : JSONParsingOptions::WideNumberMode::kRound)},
              json_storage));
      ZETASQL_ASSIGN_OR_RETURN(result_string,
                       functions::ConvertJsonToString(json_value_const_ref));
    } break;
    default:
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Unsupported type " << args[0].type()->DebugString()
             << " for String function";
  }
  return Value::String(result_string);
}

absl::StatusOr<Value> ParseDateFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (HasNulls(args)) return Value::Null(output_type());
  int32_t date;
  ZETASQL_RETURN_IF_ERROR(functions::ParseStringToDate(args[0].string_value(),
                                               args[1].string_value(),
                                               /*parse_version2=*/true, &date));
  return Value::Date(date);
}

absl::StatusOr<Value> ParseDatetimeFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (HasNulls(args)) return Value::Null(output_type());
  DatetimeValue datetime;
  ZETASQL_RETURN_IF_ERROR(functions::ParseStringToDatetime(
      args[0].string_value(), args[1].string_value(),
      GetTimestampScale(context->GetLanguageOptions()), /*parse_version2=*/true,
      &datetime));
  return Value::Datetime(datetime);
}

absl::StatusOr<Value> ParseTimeFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (HasNulls(args)) return Value::Null(output_type());
  TimeValue time;
  ZETASQL_RETURN_IF_ERROR(functions::ParseStringToTime(
      args[0].string_value(), args[1].string_value(),
      GetTimestampScale(context->GetLanguageOptions()), &time));
  return Value::Time(time);
}

absl::StatusOr<Value> ParseTimestampFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK(args.size() == 2 || args.size() == 3);
  if (HasNulls(args)) return Value::Null(output_type());
  if (context->GetLanguageOptions().LanguageFeatureEnabled(
          FEATURE_TIMESTAMP_NANOS)) {
    absl::Time timestamp;
    if (args.size() == 2) {
      ZETASQL_RETURN_IF_ERROR(functions::ParseStringToTimestamp(
          args[0].string_value(), args[1].string_value(),
          context->GetDefaultTimeZone(), /*parse_version2=*/true, &timestamp));
    } else {
      ZETASQL_RETURN_IF_ERROR(functions::ParseStringToTimestamp(
          args[0].string_value(), args[1].string_value(),
          args[2].string_value(), /*parse_version2=*/true, &timestamp));
    }
    return Value::Timestamp(timestamp);
  } else {
    int64_t timestamp;
    if (args.size() == 2) {
      ZETASQL_RETURN_IF_ERROR(functions::ParseStringToTimestamp(
          args[0].string_value(), args[1].string_value(),
          context->GetDefaultTimeZone(), /*parse_version2=*/true, &timestamp));
    } else {
      ZETASQL_RETURN_IF_ERROR(functions::ParseStringToTimestamp(
          args[0].string_value(), args[1].string_value(),
          args[2].string_value(), /*parse_version2=*/true, &timestamp));
    }
    return Value::TimestampFromUnixMicros(timestamp);
  }
}

absl::StatusOr<Value> DateTimeTruncFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  // The signature arguments are (<expr>, <datepart>).
  ZETASQL_RET_CHECK(args.size() == 2 || args.size() == 3);
  if (args[0].is_null() || args[1].is_null() ||
      (args.size() == 3 && args[2].is_null())) {
    return Value::Null(output_type());
  }
  functions::DateTimestampPart part =
      static_cast<functions::DateTimestampPart>(args[1].enum_value());
  switch (args[0].type_kind()) {
    case TYPE_DATE: {
      int32_t date;
      ZETASQL_RETURN_IF_ERROR(
          functions::TruncateDate(args[0].date_value(), part, &date));
      return values::Date(date);
    }
    case TYPE_TIMESTAMP: {
      int64_t int64_timestamp;
      if (args.size() == 2) {
        ZETASQL_RETURN_IF_ERROR(functions::TimestampTrunc(args[0].ToUnixMicros(),
                                                  context->GetDefaultTimeZone(),
                                                  part, &int64_timestamp));
      } else {
        ZETASQL_RETURN_IF_ERROR(functions::TimestampTrunc(args[0].ToUnixMicros(),
                                                  args[2].string_value(), part,
                                                  &int64_timestamp));
      }
      return Value::TimestampFromUnixMicros(int64_timestamp);
    }
    case TYPE_DATETIME: {
      DatetimeValue datetime;
      ZETASQL_RETURN_IF_ERROR(functions::TruncateDatetime(args[0].datetime_value(),
                                                  part, &datetime));
      return Value::Datetime(datetime);
    }
    case TYPE_TIME: {
      TimeValue time;
      ZETASQL_RETURN_IF_ERROR(
          functions::TruncateTime(args[0].time_value(), part, &time));
      return Value::Time(time);
    }
    default:
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Unsupported type " << args[0].type()->DebugString()
             << " for datetime TRUNC function";
  }
}

absl::StatusOr<Value> LastDayFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  // The signature arguments are (<date> or <datetime>, <datepart> optional).
  ZETASQL_RET_CHECK_LE(args.size(), 2);
  ZETASQL_RET_CHECK_GE(args.size(), 1);
  if (args[0].is_null()) {
    return Value::Null(output_type());
  }
  functions::DateTimestampPart part;
  if (args.size() == 2) {
    if (args[1].is_null()) {
      return Value::Null(output_type());
    }
    part = static_cast<functions::DateTimestampPart>(args[1].enum_value());
  } else {
    part = functions::DateTimestampPart::MONTH;
  }
  int32_t date;
  if (args[0].type_kind() == TYPE_DATE) {
    ZETASQL_RETURN_IF_ERROR(
        functions::LastDayOfDate(args[0].date_value(), part, &date));
  } else {
    ZETASQL_RETURN_IF_ERROR(
        functions::LastDayOfDatetime(args[0].datetime_value(), part, &date));
  }
  return values::Date(date);
}

absl::StatusOr<Value> FromProtoFunction::Eval(
    absl::Span<const TupleData* const> params,
    const absl::Span<const Value> args, EvaluationContext* context) const {
  if (HasNulls(args)) return Value::Null(output_type());

  // This function is idempotent, therefore we just return the input value if
  // its type matches the output type.
  if (output_type()->kind() == args[0].type_kind()) {
    return args[0];
  }
  google::protobuf::DynamicMessageFactory factory;
  std::unique_ptr<google::protobuf::Message> message;
  message.reset(args[0].ToMessage(&factory));
  switch (output_type()->kind()) {
    case TYPE_TIMESTAMP: {
      google::protobuf::Timestamp proto_timestamp;
      functions::TimestampScale scale =
          GetTimestampScale(context->GetLanguageOptions());
      proto_timestamp.CopyFrom(*message);

      if (scale == functions::TimestampScale::kMicroseconds) {
        int64_t timestamp;
        ZETASQL_RETURN_IF_ERROR(functions::ConvertProto3TimestampToTimestamp(
            proto_timestamp, scale, &timestamp));
        return Value::TimestampFromUnixMicros(timestamp);
      }

      absl::Time timestamp;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertProto3TimestampToTimestamp(
          proto_timestamp, &timestamp));
      return Value::Timestamp(timestamp);
    }
    case TYPE_DATE: {
      int32_t date;
      google::type::Date proto_date;
      proto_date.CopyFrom(*message);
      ZETASQL_RETURN_IF_ERROR(functions::ConvertProto3DateToDate(proto_date, &date));
      return Value::Date(date);
      break;
    }
    case TYPE_TIME: {
      TimeValue time;
      google::type::TimeOfDay proto_time_of_day;
      proto_time_of_day.CopyFrom(*message);
      ZETASQL_RETURN_IF_ERROR(functions::ConvertProto3TimeOfDayToTime(
          proto_time_of_day, GetTimestampScale(context->GetLanguageOptions()),
          &time));
      return Value::Time(time);
      break;
    }
    case TYPE_DOUBLE: {
      double double_value;
      google::protobuf::DoubleValue proto_double_wrapper;
      proto_double_wrapper.CopyFrom(*message);
      ZETASQL_RETURN_IF_ERROR(
          functions::ConvertProto3WrapperToType<google::protobuf::DoubleValue>(
              proto_double_wrapper, &double_value));
      return Value::Double(double_value);
      break;
    }
    case TYPE_FLOAT: {
      float float_value;
      google::protobuf::FloatValue proto_float_wrapper;
      proto_float_wrapper.CopyFrom(*message);
      ZETASQL_RETURN_IF_ERROR(
          functions::ConvertProto3WrapperToType<google::protobuf::FloatValue>(
              proto_float_wrapper, &float_value));
      return Value::Float(float_value);
      break;
    }
    case TYPE_INT64: {
      int64_t int64_value;
      google::protobuf::Int64Value proto_int64_wrapper;
      proto_int64_wrapper.CopyFrom(*message);
      ZETASQL_RETURN_IF_ERROR(
          functions::ConvertProto3WrapperToType<google::protobuf::Int64Value>(
              proto_int64_wrapper, &int64_value));
      return Value::Int64(int64_value);
      break;
    }
    case TYPE_UINT64: {
      uint64_t uint64_value;
      google::protobuf::UInt64Value proto_uint64_wrapper;
      proto_uint64_wrapper.CopyFrom(*message);
      ZETASQL_RETURN_IF_ERROR(
          functions::ConvertProto3WrapperToType<google::protobuf::UInt64Value>(
              proto_uint64_wrapper, &uint64_value));
      return Value::Uint64(uint64_value);
      break;
    }
    case TYPE_INT32: {
      int32_t int32_value;
      google::protobuf::Int32Value proto_int32_wrapper;
      proto_int32_wrapper.CopyFrom(*message);
      ZETASQL_RETURN_IF_ERROR(
          functions::ConvertProto3WrapperToType<google::protobuf::Int32Value>(
              proto_int32_wrapper, &int32_value));
      return Value::Int32(int32_value);
      break;
    }
    case TYPE_UINT32: {
      uint32_t uint32_value;
      google::protobuf::UInt32Value proto_uint32_wrapper;
      proto_uint32_wrapper.CopyFrom(*message);
      ZETASQL_RETURN_IF_ERROR(
          functions::ConvertProto3WrapperToType<google::protobuf::UInt32Value>(
              proto_uint32_wrapper, &uint32_value));
      return Value::Uint32(uint32_value);
      break;
    }
    case TYPE_BOOL: {
      bool bool_value;
      google::protobuf::BoolValue proto_bool_wrapper;
      proto_bool_wrapper.CopyFrom(*message);
      ZETASQL_RETURN_IF_ERROR(
          functions::ConvertProto3WrapperToType<google::protobuf::BoolValue>(
              proto_bool_wrapper, &bool_value));
      return Value::Bool(bool_value);
      break;
    }
    case TYPE_BYTES: {
      absl::Cord bytes_value;
      google::protobuf::BytesValue proto_bytes_wrapper;
      proto_bytes_wrapper.CopyFrom(*message);
      ZETASQL_RETURN_IF_ERROR(
          functions::ConvertProto3WrapperToType<google::protobuf::BytesValue>(
              proto_bytes_wrapper, &bytes_value));
      return Value::Bytes(bytes_value);
      break;
    }
    case TYPE_STRING: {
      std::string string_value;
      google::protobuf::StringValue proto_string_wrapper;
      proto_string_wrapper.CopyFrom(*message);
      ZETASQL_RETURN_IF_ERROR(
          functions::ConvertProto3WrapperToType<google::protobuf::StringValue>(
              proto_string_wrapper, &string_value));
      return Value::String(string_value);
      break;
    }
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported function: " << debug_name();
  }
}

absl::StatusOr<Value> ToProtoFunction::Eval(
    absl::Span<const TupleData* const> params,
    const absl::Span<const Value> args, EvaluationContext* context) const {
  if (HasNulls(args)) return Value::Null(output_type());

  // This function is idempotent if the input argument is a proto.
  if (args[0].type()->IsProto()) {
    return args[0];
  }
  switch (args[0].type_kind()) {
    case TYPE_TIMESTAMP: {
      google::protobuf::Timestamp proto_timestamp;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToProto3Timestamp(
          args[0].ToTime(), &proto_timestamp));
      return zetasql::values::Proto(output_type()->AsProto(),
                                      proto_timestamp);
      break;
    }
    case TYPE_DATE: {
      google::type::Date proto_date;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertDateToProto3Date(args[0].date_value(),
                                                         &proto_date));
      return zetasql::values::Proto(output_type()->AsProto(), proto_date);
      break;
    }
    case TYPE_TIME: {
      google::type::TimeOfDay proto_time_of_day;
      ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
      ZETASQL_RETURN_IF_ERROR(functions::ConvertTimeToProto3TimeOfDay(
          args[0].time_value(), &proto_time_of_day));
      return zetasql::values::Proto(output_type()->AsProto(),
                                      proto_time_of_day);
      break;
    }
    case TYPE_DOUBLE: {
      google::protobuf::DoubleValue proto_double_wrapper;
      functions::ConvertTypeToProto3Wrapper<google::protobuf::DoubleValue>(
          args[0].double_value(), &proto_double_wrapper);
      return zetasql::values::Proto(output_type()->AsProto(),
                                      proto_double_wrapper);
      break;
    }
    case TYPE_FLOAT: {
      google::protobuf::FloatValue proto_float_wrapper;
      functions::ConvertTypeToProto3Wrapper<google::protobuf::FloatValue>(
          args[0].float_value(), &proto_float_wrapper);
      return zetasql::values::Proto(output_type()->AsProto(),
                                      proto_float_wrapper);
      break;
    }
    case TYPE_INT64: {
      google::protobuf::Int64Value proto_int64_wrapper;
      functions::ConvertTypeToProto3Wrapper<google::protobuf::Int64Value>(
          args[0].int64_value(), &proto_int64_wrapper);
      return zetasql::values::Proto(output_type()->AsProto(),
                                      proto_int64_wrapper);
      break;
    }
    case TYPE_UINT64: {
      google::protobuf::UInt64Value proto_uint64_wrapper;
      functions::ConvertTypeToProto3Wrapper<google::protobuf::UInt64Value>(
          args[0].uint64_value(), &proto_uint64_wrapper);
      return zetasql::values::Proto(output_type()->AsProto(),
                                      proto_uint64_wrapper);
      break;
    }
    case TYPE_INT32: {
      google::protobuf::Int32Value proto_int32_wrapper;
      functions::ConvertTypeToProto3Wrapper<google::protobuf::Int32Value>(
          args[0].int32_value(), &proto_int32_wrapper);
      return zetasql::values::Proto(output_type()->AsProto(),
                                      proto_int32_wrapper);
      break;
    }
    case TYPE_UINT32: {
      google::protobuf::UInt32Value proto_uint32_wrapper;
      functions::ConvertTypeToProto3Wrapper<google::protobuf::UInt32Value>(
          args[0].uint32_value(), &proto_uint32_wrapper);
      return zetasql::values::Proto(output_type()->AsProto(),
                                      proto_uint32_wrapper);
      break;
    }
    case TYPE_BOOL: {
      google::protobuf::BoolValue proto_bool_wrapper;
      functions::ConvertTypeToProto3Wrapper<google::protobuf::BoolValue>(
          args[0].bool_value(), &proto_bool_wrapper);
      return zetasql::values::Proto(output_type()->AsProto(),
                                      proto_bool_wrapper);
      break;
    }
    case TYPE_BYTES: {
      google::protobuf::BytesValue proto_bytes_wrapper;
      functions::ConvertTypeToProto3Wrapper<google::protobuf::BytesValue>(
          absl::Cord(args[0].bytes_value()), &proto_bytes_wrapper);
      return zetasql::values::Proto(output_type()->AsProto(),
                                      proto_bytes_wrapper);
      break;
    }
    case TYPE_STRING: {
      google::protobuf::StringValue proto_string_wrapper;
      functions::ConvertTypeToProto3Wrapper<google::protobuf::StringValue>(
          args[0].string_value(), &proto_string_wrapper);
      return zetasql::values::Proto(output_type()->AsProto(),
                                      proto_string_wrapper);
      break;
    }
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported function: " << debug_name()
             << " for input: " << args[0];
  }
}

absl::StatusOr<Value> EnumValueDescriptorProtoFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  if (HasNulls(args)) return Value::Null(output_type());

  const google::protobuf::EnumValueDescriptor* arg_value_desc =
      args[0].type()->AsEnum()->enum_descriptor()->FindValueByNumber(
          args[0].enum_value());
  if (arg_value_desc == nullptr) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "Invalid enum value: " << args[0].enum_value()
           << " for enum type: " << args[0].type()->DebugString();
  }
  ZETASQL_RET_CHECK_EQ(arg_value_desc->type(),
               args[0].type()->AsEnum()->enum_descriptor());

  google::protobuf::EnumValueDescriptorProto enum_value_desc_proto;
  arg_value_desc->CopyTo(&enum_value_desc_proto);
  return zetasql::values::Proto(output_type()->AsProto(),
                                  enum_value_desc_proto);
}

absl::StatusOr<Value> DateTimeDiffFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (args[0].is_null() || args[1].is_null()) {
    return Value::Null(output_type());
  }
  int32_t value32;
  int64_t value64;
  functions::DateTimestampPart part =
      static_cast<functions::DateTimestampPart>(args[2].enum_value());
  switch (FCT(kind(), args[0].type_kind())) {
    case FCT(FunctionKind::kDateAdd, TYPE_DATE):
      ZETASQL_RETURN_IF_ERROR(functions::AddDate(args[0].date_value(), part,
                                         args[1].int64_value(), &value32));
      return Value::Date(value32);
    case FCT(FunctionKind::kDateSub, TYPE_DATE):
      ZETASQL_RETURN_IF_ERROR(functions::SubDate(args[0].date_value(), part,
                                         args[1].int64_value(), &value32));
      return Value::Date(value32);
    case FCT(FunctionKind::kDateAdd, TYPE_DATETIME):
    case FCT(FunctionKind::kTimestampAdd, TYPE_DATETIME):
    case FCT(FunctionKind::kDatetimeAdd, TYPE_DATETIME): {
      DatetimeValue datetime;
      ZETASQL_RETURN_IF_ERROR(functions::AddDatetime(args[0].datetime_value(), part,
                                             args[1].int64_value(), &datetime));
      return Value::Datetime(datetime);
    }
    case FCT(FunctionKind::kDateSub, TYPE_DATETIME):
    case FCT(FunctionKind::kTimestampSub, TYPE_DATETIME):
    case FCT(FunctionKind::kDatetimeSub, TYPE_DATETIME): {
      DatetimeValue datetime;
      ZETASQL_RETURN_IF_ERROR(functions::SubDatetime(args[0].datetime_value(), part,
                                             args[1].int64_value(), &datetime));
      return Value::Datetime(datetime);
    }
    case FCT(FunctionKind::kDateDiff, TYPE_DATETIME):
    case FCT(FunctionKind::kTimestampDiff, TYPE_DATETIME):
    case FCT(FunctionKind::kDatetimeDiff, TYPE_DATETIME):
      ZETASQL_RETURN_IF_ERROR(functions::DiffDatetimes(
          args[0].datetime_value(), args[1].datetime_value(), part, &value64));
      return Value::Int64(value64);
    case FCT(FunctionKind::kDateDiff, TYPE_DATE):
      ZETASQL_RETURN_IF_ERROR(functions::DiffDates(
          args[0].date_value(), args[1].date_value(), part, &value32));
      return output_type()->IsInt64() ? Value::Int64(value32)
                                      : Value::Int32(value32);
    case FCT(FunctionKind::kDateDiff, TYPE_TIMESTAMP):
    case FCT(FunctionKind::kDatetimeDiff, TYPE_TIMESTAMP):
    case FCT(FunctionKind::kTimestampDiff, TYPE_TIMESTAMP):
      ZETASQL_RETURN_IF_ERROR(functions::TimestampDiff(
          args[0].ToUnixMicros(), args[1].ToUnixMicros(),
          functions::kMicroseconds, part, &value64));
      return Value::Int64(value64);
    case FCT(FunctionKind::kTimeAdd, TYPE_TIME): {
      TimeValue time;
      ZETASQL_RETURN_IF_ERROR(functions::AddTime(args[0].time_value(), part,
                                         args[1].int64_value(), &time));
      return Value::Time(time);
    }
    case FCT(FunctionKind::kTimeSub, TYPE_TIME): {
      TimeValue time;
      ZETASQL_RETURN_IF_ERROR(functions::SubTime(args[0].time_value(), part,
                                         args[1].int64_value(), &time));
      return Value::Time(time);
    }
    case FCT(FunctionKind::kTimeDiff, TYPE_TIME):
      ZETASQL_RETURN_IF_ERROR(functions::DiffTimes(
          args[0].time_value(), args[1].time_value(), part, &value64));
      return Value::Int64(value64);
    case FCT(FunctionKind::kDateAdd, TYPE_TIMESTAMP):
    case FCT(FunctionKind::kDatetimeAdd, TYPE_TIMESTAMP):
    case FCT(FunctionKind::kTimestampAdd, TYPE_TIMESTAMP): {
      // We can hardcode the time zone to the default because it is only
      // used for error messaging.
      ZETASQL_RETURN_IF_ERROR(functions::AddTimestamp(
          args[0].ToUnixMicros(), functions::kMicroseconds,
          context->GetDefaultTimeZone(), part, args[1].int64_value(),
          &value64));
      return Value::TimestampFromUnixMicros(value64);
    }
    case FCT(FunctionKind::kDateSub, TYPE_TIMESTAMP):
    case FCT(FunctionKind::kDatetimeSub, TYPE_TIMESTAMP):
    case FCT(FunctionKind::kTimestampSub, TYPE_TIMESTAMP): {
      // We can hardcode the time zone to the default because it is only
      // used for error messaging.
      ZETASQL_RETURN_IF_ERROR(functions::SubTimestamp(
          args[0].ToUnixMicros(), functions::kMicroseconds,
          context->GetDefaultTimeZone(), part, args[1].int64_value(),
          &value64));
      return Value::TimestampFromUnixMicros(value64);
    }
  }
  return ::zetasql_base::UnimplementedErrorBuilder()
         << "Unsupported function: " << debug_name();
}

absl::StatusOr<Value> ExtractFromFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (args[0].is_null() || args[1].is_null() ||
      (args.size() == 3 && args[2].is_null())) {
    return Value::Null(output_type());
  }
  functions::DateTimestampPart part =
      static_cast<functions::DateTimestampPart>(args[1].enum_value());
  int32_t value32;
  switch (args[0].type_kind()) {
    case TYPE_DATE:
      ZETASQL_RETURN_IF_ERROR(
          functions::ExtractFromDate(part, args[0].date_value(), &value32));
      return output_type()->IsInt64() ? Value::Int64(value32)
                                      : Value::Int32(value32);
    case TYPE_TIMESTAMP:
      if (args.size() == 2) {
        ZETASQL_RETURN_IF_ERROR(functions::ExtractFromTimestamp(
            part, args[0].ToUnixMicros(), functions::kMicroseconds,
            context->GetDefaultTimeZone(), &value32));
      } else {
        ZETASQL_RETURN_IF_ERROR(functions::ExtractFromTimestamp(
            part, args[0].ToUnixMicros(), functions::kMicroseconds,
            args[2].string_value(), &value32));
      }
      return output_type()->IsInt64() ? Value::Int64(value32)
                                      : Value::Int32(value32);
    case TYPE_DATETIME:
      ZETASQL_RETURN_IF_ERROR(functions::ExtractFromDatetime(
          part, args[0].datetime_value(), &value32));
      return output_type()->IsInt64() ? Value::Int64(value32)
                                      : Value::Int32(value32);
    case TYPE_TIME:
      ZETASQL_RETURN_IF_ERROR(
          functions::ExtractFromTime(part, args[0].time_value(), &value32));
      return output_type()->IsInt64() ? Value::Int64(value32)
                                      : Value::Int32(value32);
    case TYPE_INTERVAL: {
      ZETASQL_ASSIGN_OR_RETURN(int64_t result, args[0].interval_value().Extract(part));
      return Value::Int64(result);
    }
    default: {
    }
  }
  return ::zetasql_base::UnimplementedErrorBuilder()
         << "Unsupported function: " << debug_name();
}

absl::StatusOr<Value> ExtractDateFromFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (args[0].is_null() || (args.size() == 2 && args[1].is_null())) {
    return Value::NullDate();
  }
  int32_t value32;
  switch (args[0].type_kind()) {
    case TYPE_TIMESTAMP: {
      if (args.size() == 1) {
        ZETASQL_RETURN_IF_ERROR(functions::ExtractFromTimestamp(
            functions::DATE, args[0].ToUnixMicros(), functions::kMicroseconds,
            context->GetDefaultTimeZone(), &value32));
      } else {
        ZETASQL_RETURN_IF_ERROR(functions::ExtractFromTimestamp(
            functions::DATE, args[0].ToUnixMicros(), functions::kMicroseconds,
            args[1].string_value(), &value32));
      }
      break;
    }
    case TYPE_DATETIME: {
      ZETASQL_RETURN_IF_ERROR(functions::ExtractFromDatetime(
          functions::DATE, args[0].datetime_value(), &value32));
      break;
    }
    default:
      // In the LEGACY mode, other types such as TYPE_TIMESTAMP_SECONDS can
      // match the signature, but reference implementation doesn't support them.
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported type in $extract_date function";
  }
  return Value::Date(value32);
}

absl::StatusOr<Value> ExtractTimeFromFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (args[0].is_null() || (args.size() == 2 && args[1].is_null())) {
    return Value::NullTime();
  }
  TimeValue time;
  switch (args[0].type_kind()) {
    case TYPE_DATETIME:
      ZETASQL_RETURN_IF_ERROR(
          functions::ExtractTimeFromDatetime(args[0].datetime_value(), &time));
      break;
    case TYPE_TIMESTAMP: {
      if (args.size() == 1) {
        ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToTime(
            args[0].ToTime(), context->GetDefaultTimeZone(), &time));
      } else {
        ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToTime(
            args[0].ToTime(), args[1].string_value(), &time));
      }
      break;
    }
    default:
      // Should not reach here: no other types TIME date part could be extracted
      // from.
      ZETASQL_RET_CHECK_FAIL() << "Unsupported type in $extract_time function";
  }
  return Value::Time(time);
}

absl::StatusOr<Value> ExtractDatetimeFromFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (args[0].is_null() || (args.size() == 2 && args[1].is_null())) {
    return Value::NullDatetime();
  }
  DatetimeValue datetime;
  if (args.size() == 1) {
    ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToDatetime(
        args[0].ToTime(), context->GetDefaultTimeZone(), &datetime));
  } else {
    ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToDatetime(
        args[0].ToTime(), args[1].string_value(), &datetime));
  }
  return Value::Datetime(datetime);
}

absl::StatusOr<Value> IntervalFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (HasNulls(args)) {
    return Value::NullInterval();
  }

  IntervalValue interval;
  switch (kind()) {
    case FunctionKind::kIntervalCtor: {
      ZETASQL_ASSIGN_OR_RETURN(
          interval,
          IntervalValue::FromInteger(
              args[0].int64_value(),
              static_cast<functions::DateTimestampPart>(args[1].enum_value())));
      break;
    }
    case FunctionKind::kMakeInterval: {
      ZETASQL_RET_CHECK_EQ(6, args.size());
      ZETASQL_ASSIGN_OR_RETURN(interval,
                       IntervalValue::FromYMDHMS(
                           args[0].int64_value(), args[1].int64_value(),
                           args[2].int64_value(), args[3].int64_value(),
                           args[4].int64_value(), args[5].int64_value()));
      break;
    }
    case FunctionKind::kJustifyHours: {
      ZETASQL_ASSIGN_OR_RETURN(interval, JustifyHours(args[0].interval_value()));
      break;
    }
    case FunctionKind::kJustifyDays: {
      ZETASQL_ASSIGN_OR_RETURN(interval, JustifyDays(args[0].interval_value()));
      break;
    }
    case FunctionKind::kJustifyInterval: {
      ZETASQL_ASSIGN_OR_RETURN(interval, JustifyInterval(args[0].interval_value()));
      break;
    }
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unexpected function: " << debug_name();
  }
  return Value::Interval(interval);
}

absl::StatusOr<Value> CollateFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);
  if (args[1].is_null()) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "The second argument of COLLATE() must not be NULL";
  }

  return args[0];
}

absl::StatusOr<Value> ExtractOneofCaseFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (args[0].is_null()) {
    return Value::NullString();
  }

  google::protobuf::DynamicMessageFactory factory;
  auto root_message = absl::WrapUnique(args[0].ToMessage(&factory));
  const google::protobuf::Reflection* reflection = root_message->GetReflection();
  const google::protobuf::FieldDescriptor* set_oneof_field =
      reflection->GetOneofFieldDescriptor(*root_message, oneof_desc_);
  if (set_oneof_field == nullptr) {
    return Value::String("");
  }
  return Value::String(set_oneof_field->name());
}

absl::StatusOr<Value> RandFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK(args.empty());
  return Value::Double(
      absl::Uniform<double>(*context->GetRandomNumberGenerator(), 0, 1));
}

absl::StatusOr<Value> ErrorFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(1, args.size());
  if (args[0].is_null()) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "ERROR function called with NULL value";
  }
  return ::zetasql_base::OutOfRangeErrorBuilder() << args[0].string_value();
}

std::string UserDefinedScalarFunction::debug_name() const {
  return absl::StrCat("UDF[", function_name_, "]");
}

bool UserDefinedScalarFunction::Eval(absl::Span<const TupleData* const> params,
                                     absl::Span<const Value> args,
                                     EvaluationContext* context, Value* result,
                                     absl::Status* status) const {
  auto status_or_result = evaluator_(args);
  if (!status_or_result.ok()) {
    *status = status_or_result.status();
    return false;
  }
  *result = std::move(status_or_result.value());
  // Type equality checking can be expensive. Do it in debug mode only.
  bool invalid = !result->is_valid() ||
                 (ZETASQL_DEBUG_MODE && !output_type()->Equals(result->type()));
  if (invalid) {
    *status = ::zetasql_base::InternalErrorBuilder()
              << "User-defined function " << function_name_
              << " returned a bad result: " << result->DebugString(true) << "\n"
              << "Expected value of type: " << output_type()->DebugString();
    return false;
  }
  return true;
}

std::string BuiltinAnalyticFunction::debug_name() const {
  return BuiltinFunctionCatalog::GetDebugNameByKind(kind_);
}

absl::Status DenseRankFunction::Eval(
    const TupleSchema& schema, const absl::Span<const TupleData* const>& tuples,
    const absl::Span<const std::vector<Value>>& args,
    const absl::Span<const AnalyticWindow>& windows,
    const TupleComparator* comparator,
    ResolvedFunctionCallBase::ErrorMode error_mode, EvaluationContext* context,
    std::vector<Value>* result) const {
  ZETASQL_RET_CHECK(args.empty());
  ZETASQL_RET_CHECK(windows.empty());
  ZETASQL_RET_CHECK(comparator != nullptr);

  if (tuples.empty()) {
    return absl::OkStatus();
  }

  const TupleData* prev_tuple = tuples.front();
  int64_t dense_rank = 1;

  // The rank number for the first row.
  result->emplace_back(Value::Int64(1));
  for (int tuple_id = 1; tuple_id < tuples.size(); ++tuple_id) {
    if ((*comparator)(prev_tuple, tuples[tuple_id])) {
      prev_tuple = tuples[tuple_id];
      ++dense_rank;
    }
    result->emplace_back(Value::Int64(dense_rank));
  }

  return absl::OkStatus();
}

absl::Status RankFunction::Eval(
    const TupleSchema& schema, const absl::Span<const TupleData* const>& tuples,
    const absl::Span<const std::vector<Value>>& args,
    const absl::Span<const AnalyticWindow>& windows,
    const TupleComparator* comparator,
    ResolvedFunctionCallBase::ErrorMode error_mode, EvaluationContext* context,
    std::vector<Value>* result) const {
  ZETASQL_RET_CHECK(args.empty());
  ZETASQL_RET_CHECK(windows.empty());
  ZETASQL_RET_CHECK(comparator != nullptr);

  if (tuples.empty()) {
    return absl::OkStatus();
  }

  const TupleData* prev_tuple = tuples.front();
  int64_t rank = 1;
  int64_t num_peers = 1;

  // The rank number for the first row.
  result->emplace_back(Value::Int64(1));
  for (int tuple_id = 1; tuple_id < tuples.size(); ++tuple_id) {
    if ((*comparator)(prev_tuple, tuples[tuple_id])) {
      prev_tuple = tuples[tuple_id];
      rank += num_peers;
      num_peers = 1;
    } else {
      ++num_peers;
    }
    result->emplace_back(Value::Int64(rank));
  }

  return absl::OkStatus();
}

absl::Status RowNumberFunction::Eval(
    const TupleSchema& schema, const absl::Span<const TupleData* const>& tuples,
    const absl::Span<const std::vector<Value>>& args,
    const absl::Span<const AnalyticWindow>& windows,
    const TupleComparator* comparator,
    ResolvedFunctionCallBase::ErrorMode error_mode, EvaluationContext* context,
    std::vector<Value>* result) const {
  ZETASQL_RET_CHECK(args.empty());
  ZETASQL_RET_CHECK(windows.empty());
  ZETASQL_RET_CHECK(comparator == nullptr);

  for (int64_t row_number = 1; row_number <= tuples.size(); ++row_number) {
    result->emplace_back(Value::Int64(row_number));
  }

  if (tuples.size() > 1) {
    // ROW_NUMBER generates non-deterministic results if there is no order by
    // or the order by is not a total order. We cannot check order by here, so
    // just mark the result as non-deterministic if there are more than one
    // tuples.
    context->SetNonDeterministicOutput();
  }

  return absl::OkStatus();
}

absl::Status PercentRankFunction::Eval(
    const TupleSchema& schema, const absl::Span<const TupleData* const>& tuples,
    const absl::Span<const std::vector<Value>>& args,
    const absl::Span<const AnalyticWindow>& windows,
    const TupleComparator* comparator,
    ResolvedFunctionCallBase::ErrorMode error_mode, EvaluationContext* context,
    std::vector<Value>* result) const {
  ZETASQL_RET_CHECK(args.empty());
  ZETASQL_RET_CHECK(windows.empty());
  ZETASQL_RET_CHECK(comparator != nullptr);

  if (tuples.empty()) {
    return absl::OkStatus();
  }

  // Return 0 if there is only one tuple.
  if (tuples.size() == 1) {
    result->emplace_back(Value::Double(0));
    return absl::OkStatus();
  }

  RankFunction rank_function;
  ZETASQL_RETURN_IF_ERROR(rank_function.Eval(schema, tuples, args, windows, comparator,
                                     error_mode, context, result));

  const double num_tuples_minus_one = tuples.size() - 1;
  for (Value& rank_value : *result) {
    rank_value =
        Value::Double((rank_value.int64_value() - 1) / num_tuples_minus_one);
  }

  return absl::OkStatus();
}

absl::Status CumeDistFunction::Eval(
    const TupleSchema& schema, const absl::Span<const TupleData* const>& tuples,
    const absl::Span<const std::vector<Value>>& args,
    const absl::Span<const AnalyticWindow>& windows,
    const TupleComparator* comparator,
    ResolvedFunctionCallBase::ErrorMode error_mode, EvaluationContext* context,
    std::vector<Value>* result) const {
  ZETASQL_RET_CHECK(args.empty());
  ZETASQL_RET_CHECK(windows.empty());
  ZETASQL_RET_CHECK(comparator != nullptr);

  if (tuples.empty()) {
    return absl::OkStatus();
  }

  int tuple_id = 0;
  int num_peers;
  while (tuple_id < tuples.size()) {
    const TupleData* order_key_tuple = tuples[tuple_id];

    ++tuple_id;
    num_peers = 1;

    // Find the index of the start tuple of the next ordering group, which is
    // also the number of tuples preceding or peer with the tuples in
    // the current ordering group.
    while (tuple_id < tuples.size() &&
           !(*comparator)(order_key_tuple, tuples[tuple_id])) {
      ++num_peers;
      ++tuple_id;
    }

    result->insert(
        result->end(), num_peers,
        Value::Double(static_cast<double>(tuple_id) / tuples.size()));
  }

  return absl::OkStatus();
}

// Returns true if it finds a tuple in 'tuples' such that a) it is a peer with
// the tuple at 'current_tuple_id', b) it is within the range 'analytic_window',
// and c) 'predicate' evaluates to true on the peer tuple. The found tuple
// can be the current tuple itself. Requires that the current tuple is
// within 'analytic_window'.
//
// The signature of Predicate is
//    bool Predicate(int peer_tuple_id);
template <class Predicate>
static bool ApplyToEachPeerTuple(const TupleSchema& schema,
                                 int current_tuple_id,
                                 absl::Span<const TupleData* const> tuples,
                                 const AnalyticWindow& analytic_window,
                                 const TupleComparator& comparator,
                                 Predicate predicate) {
  // Apply the predicate to the current tuple and every peer tuple preceding it
  // until the predicate evaluates to true.
  for (int tuple_id = current_tuple_id;
       tuple_id >= analytic_window.start_tuple_id; --tuple_id) {
    if (comparator(tuples[tuple_id], tuples[current_tuple_id])) break;
    if (predicate(tuple_id)) return true;
  }

  // Apply the predicate to every peer tuple following the current tuple until
  // the predicate evaluates to true.
  for (int tuple_id = current_tuple_id + 1;
       tuple_id < analytic_window.start_tuple_id + analytic_window.num_tuples;
       ++tuple_id) {
    if (comparator(tuples[current_tuple_id], tuples[tuple_id])) break;
    if (predicate(tuple_id)) return true;
  }

  return false;
}

// 'schema' is useful for debug logging (to print tuples).
bool NtileFunction::OrderingPeersAreNotEqual(
    const TupleSchema& schema, int key_tuple_id,
    absl::Span<const TupleData* const> tuples,
    const TupleComparator& comparator) {
  AnalyticWindow partition(0 /* start_tuple_id */, tuples.size());
  // Returns true if the tuple at 'peer_tuple_id' is not equal to the key tuple
  // at 'key_tuple_id'.
  const auto not_equal_to_key = [key_tuple_id, &tuples](int peer_tuple_id) {
    return !tuples[peer_tuple_id]->Equals(*tuples[key_tuple_id]);
  };
  return ApplyToEachPeerTuple(schema, key_tuple_id, tuples, partition,
                              comparator, not_equal_to_key);
}

absl::Status NtileFunction::Eval(
    const TupleSchema& schema, const absl::Span<const TupleData* const>& tuples,
    const absl::Span<const std::vector<Value>>& args,
    const absl::Span<const AnalyticWindow>& windows,
    const TupleComparator* comparator,
    ResolvedFunctionCallBase::ErrorMode error_mode, EvaluationContext* context,
    std::vector<Value>* result) const {
  ZETASQL_RET_CHECK(windows.empty());
  ZETASQL_RET_CHECK(comparator != nullptr);

  // Get the first argument value, which gives the number of buckets.
  ZETASQL_RET_CHECK_EQ(1, args.size());
  ZETASQL_RET_CHECK_EQ(1, args[0].size());
  ZETASQL_RET_CHECK(args[0][0].type()->IsInt64());

  if (args[0][0].is_null()) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "The N value (number of buckets) for the NTILE function "
              "must not be NULL";
  }

  const int64_t bucket_count_argument = args[0][0].int64_value();
  if (bucket_count_argument <= 0) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "The N value (number of buckets) for the NTILE function "
              "must be positive";
  }

  if (tuples.empty()) {
    return absl::OkStatus();
  }

  // If the bucket count argument value is larger than the number of tuples,
  // the number of buckets used is effectively the number of tuples.
  const int num_buckets =
      std::min<int64_t>(tuples.size(), bucket_count_argument);
  // 'num_buckets' must be larger than zero, so the division is safe.
  const int small_bucket_size = tuples.size() / num_buckets;
  const int large_bucket_size = small_bucket_size + 1;

  // The first 'num_buckets_with_one_more_tuple' buckets have
  // 'large_bucket_size' tuples, and the remaining buckets have
  // 'small_bucket_size' tuples.
  const int num_buckets_with_one_more_tuple = tuples.size() % num_buckets;

  // NTILE returns non-deterministic results if there exists an ordering group
  // where peer tuples (tied in the window ordering) are not in a single
  // bucket and the peers are not equivalent to one another.
  for (int bucket_id = 1; bucket_id <= num_buckets_with_one_more_tuple;
       ++bucket_id) {
    const int64_t first_tuple_id_of_current_bucket =
        (bucket_id - 1) * large_bucket_size;
    ZETASQL_RET_CHECK_LT(first_tuple_id_of_current_bucket, tuples.size());
    if (bucket_id > 1 &&
        (!(*comparator)(tuples[first_tuple_id_of_current_bucket - 1],
                        tuples[first_tuple_id_of_current_bucket]) &&
         OrderingPeersAreNotEqual(schema, first_tuple_id_of_current_bucket,
                                  tuples, *comparator))) {
      context->SetNonDeterministicOutput();
    }

    result->insert(result->end(), large_bucket_size, Value::Int64(bucket_id));
  }

  const int num_tuples_in_large_buckets =
      large_bucket_size * num_buckets_with_one_more_tuple;
  for (int bucket_id = num_buckets_with_one_more_tuple + 1;
       bucket_id <= num_buckets; ++bucket_id) {
    const int first_tuple_id_of_current_bucket =
        num_tuples_in_large_buckets +
        (bucket_id - num_buckets_with_one_more_tuple - 1) * small_bucket_size;
    if (bucket_id > 1 &&
        (!(*comparator)(tuples[first_tuple_id_of_current_bucket - 1],
                        tuples[first_tuple_id_of_current_bucket]) &&
         OrderingPeersAreNotEqual(schema, first_tuple_id_of_current_bucket,
                                  tuples, *comparator))) {
      context->SetNonDeterministicOutput();
    }

    result->insert(result->end(), small_bucket_size, Value::Int64(bucket_id));
  }

  return absl::OkStatus();
}

// Returns true if there is a tuple in 'tuples' such that
// a) it is a peer with the current tuple at 'current_tuple_id',
// b) it is not the tuple at 'excluded_tuple_id',
// c) it is within the range 'analytic_window',
// d) it has a different value in 'values' with the value for the current, and
// e) it has a non-null value or 'ignore_nulls' is false.
// 'values' matches 1:1 with 'tuples'.
// Requires that the current tuple is within 'analytic_window'.
static bool CurrentTupleHasPeerWithDifferentRespectedValues(
    const TupleSchema& schema, int current_tuple_id, int excluded_tuple_id,
    absl::Span<const TupleData* const> tuples, absl::Span<const Value> values,
    const AnalyticWindow& analytic_window, const TupleComparator& comparator,
    bool ignore_nulls) {
  const auto has_different_value = [&values, current_tuple_id,
                                    excluded_tuple_id,
                                    ignore_nulls](int peer_tuple_id) {
    return excluded_tuple_id != peer_tuple_id &&
           !values[peer_tuple_id].Equals(values[current_tuple_id]) &&
           (!ignore_nulls || !values[peer_tuple_id].is_null());
  };
  return ApplyToEachPeerTuple(schema, current_tuple_id, tuples, analytic_window,
                              comparator, has_different_value);
}

absl::Status FirstValueFunction::Eval(
    const TupleSchema& schema, const absl::Span<const TupleData* const>& tuples,
    const absl::Span<const std::vector<Value>>& args,
    const absl::Span<const AnalyticWindow>& windows,
    const TupleComparator* comparator,
    ResolvedFunctionCallBase::ErrorMode error_mode, EvaluationContext* context,
    std::vector<Value>* result) const {
  ZETASQL_RET_CHECK_EQ(1, args.size());
  ZETASQL_RET_CHECK_EQ(tuples.size(), args[0].size());
  ZETASQL_RET_CHECK_EQ(tuples.size(), windows.size());
  ZETASQL_RET_CHECK(comparator != nullptr);

  const std::vector<Value>& values = args[0];
  const Value null_value = Value::Null(output_type());
  for (const AnalyticWindow& window : windows) {
    int offset = 0;
    if (ignore_nulls_) {
      while (offset < window.num_tuples &&
             values[window.start_tuple_id + offset].is_null()) {
        ++offset;
      }
    }
    if (window.num_tuples == offset) {
      result->emplace_back(null_value);
      continue;
    }

    const int first_value_tuple_id = window.start_tuple_id + offset;
    result->emplace_back(values[first_value_tuple_id]);
    // FIRST_VALUE is not deterministic if the FIRST_VALUE argument can evaluate
    // to different values for the first ordering group of the window.
    //
    // For example, consider FIRST_VALUE(b) OVER (ORDER BY a).
    // If the window for an input tuple contains three tuples (a, b):
    // (1, 2), (1, 3), (2, 4). The return value is non-deterministic, because
    // the first ordering group which includes the first and
    // second tuple has two different b values (2 and 3), and either of
    // the two can be the output.
    if (CurrentTupleHasPeerWithDifferentRespectedValues(
            schema, first_value_tuple_id /* current_tuple_id */,
            -1 /* excluded_tuple_id */, tuples, values, window, *comparator,
            ignore_nulls_)) {
      context->SetNonDeterministicOutput();
    }
  }

  return absl::OkStatus();
}

absl::Status LastValueFunction::Eval(
    const TupleSchema& schema, const absl::Span<const TupleData* const>& tuples,
    const absl::Span<const std::vector<Value>>& args,
    const absl::Span<const AnalyticWindow>& windows,
    const TupleComparator* comparator,
    ResolvedFunctionCallBase::ErrorMode error_mode, EvaluationContext* context,
    std::vector<Value>* result) const {
  ZETASQL_RET_CHECK_EQ(1, args.size());
  ZETASQL_RET_CHECK_EQ(tuples.size(), args[0].size());
  ZETASQL_RET_CHECK_EQ(tuples.size(), windows.size());
  ZETASQL_RET_CHECK(comparator != nullptr);

  const std::vector<Value>& values = args[0];
  const Value null_value = Value::Null(output_type());
  for (const AnalyticWindow& window : windows) {
    int offset = window.num_tuples - 1;
    if (ignore_nulls_) {
      while (offset >= 0 && values[window.start_tuple_id + offset].is_null()) {
        --offset;
      }
    }
    if (offset < 0) {
      result->emplace_back(null_value);
      continue;
    }

    const int last_value_tuple_id = window.start_tuple_id + offset;

    // LAST_VALUE is not deterministic if the LAST_VALUE argument can evaluate
    // to different values for the last ordering group of the window.
    //
    // For example, consider LAST_VALUE(b) OVER (ORDER BY a).
    // If the window for an input tuple contains three tuples (a, b):
    // (1, 2), (2, 3), (2, 4). The return value is non-deterministic, because
    // the last ordering group which includes the second and
    // third tuple has two different b values (3 and 4), and either of the two
    // can be the output.
    if (CurrentTupleHasPeerWithDifferentRespectedValues(
            schema, last_value_tuple_id /* current_tuple_id */,
            -1 /* excluded_tuple_id */, tuples, values, window, *comparator,
            ignore_nulls_)) {
      context->SetNonDeterministicOutput();
    }
    result->emplace_back(values[last_value_tuple_id]);
  }

  return absl::OkStatus();
}

absl::Status NthValueFunction::Eval(
    const TupleSchema& schema, const absl::Span<const TupleData* const>& tuples,
    const absl::Span<const std::vector<Value>>& args,
    const absl::Span<const AnalyticWindow>& windows,
    const TupleComparator* comparator,
    ResolvedFunctionCallBase::ErrorMode error_mode, EvaluationContext* context,
    std::vector<Value>* result) const {
  ZETASQL_RET_CHECK_EQ(2, args.size());
  ZETASQL_RET_CHECK_EQ(tuples.size(), args[0].size());
  ZETASQL_RET_CHECK_EQ(tuples.size(), windows.size());
  ZETASQL_RET_CHECK_EQ(1, args[1].size());
  ZETASQL_RET_CHECK(args[1][0].type()->IsInt64());
  ZETASQL_RET_CHECK(comparator != nullptr);

  if (args[1][0].is_null()) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "The N value for the NthValue function must not be NULL";
  }

  int64_t n_value = args[1][0].int64_value();
  if (n_value <= 0) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "The N value for the NthValue function must be positive";
  }

  // Convert to 0-based.
  --n_value;

  const std::vector<Value>& values = args[0];
  const Value null_value = Value::Null(output_type());
  for (const AnalyticWindow& window : windows) {
    int64_t offset = n_value;
    if (ignore_nulls_) {
      int num_non_nulls = 0;
      for (offset = 0; offset < window.num_tuples; ++offset) {
        if (!values[window.start_tuple_id + offset].is_null() &&
            ++num_non_nulls > n_value) {
          break;
        }
      }
    }
    if (offset >= window.num_tuples) {
      result->emplace_back(null_value);
      continue;
    }

    const int64_t nth_value_tuple_id = window.start_tuple_id + offset;
    result->emplace_back(values[nth_value_tuple_id]);
    // NTH_VALUE is not deterministic if the NTH_VALUE argument can evaluate
    // to different values for the tuples that are peers with the n-th tuple
    // in the window.
    //
    // For example, consider NTH_VALUE(b, 2) OVER (ORDER BY a).
    // If the window for an input tuple contains three tuples (a, b):
    // (1, 2), (2, 3), (2, 4). The return value is non-deterministic, because
    // if we switch the second and the third tuples that have the same 'a'
    // value, the return value changes from 3 to 4.
    if (CurrentTupleHasPeerWithDifferentRespectedValues(
            schema, nth_value_tuple_id /* current_tuple_id */,
            -1 /* excluded_tuple_id */, tuples, values, window, *comparator,
            ignore_nulls_)) {
      context->SetNonDeterministicOutput();
    }
  }

  return absl::OkStatus();
}

// Returns the value at 'offset' in 'arg_values' if the offset is within
// the bound, otherwise returns 'default_value'.
static Value GetOutputAtOffset(int offset, const std::vector<Value>& arg_values,
                               const Value& default_value) {
  ABSL_DCHECK(!arg_values.empty());
  if (offset < 0 || offset >= arg_values.size()) {
    return default_value;
  }

  return arg_values[offset];
}

// Returns the output determinism of LEAD or LAG for the tuple at
// 'current_tuple_id'. For LAG, 'offset' is the negation of the offset argument
// value. For LEAD, it is the offset argument value. 'arg_values' contains
// the values of the LEAD/LAG argument expression and matches 1:1 with 'tuples'.
// 'default_value' is the value of the default expression argument to LEAD/LAG.
static bool LeadLagOutputIsNonDeterministic(
    const TupleSchema& schema, int current_tuple_id, int offset,
    absl::Span<const TupleData* const> tuples,
    const std::vector<Value>& arg_values, const Value& default_value,
    const TupleComparator& comparator) {
  const AnalyticWindow partition(0 /* start_tuple_id */, tuples.size());
  if (offset == 0) return false;

  // The output is deterministic if
  // 1) the output does not change when the current tuple is switched with
  //    any tuple that is a peer with the current tuple, and
  // 2) the output does not change when the offset tuple is switched with
  //    any tuple that is a peer with the offset tuple.

  const int offset_tuple_id = offset + current_tuple_id;
  const Value current_output =
      GetOutputAtOffset(offset + current_tuple_id, arg_values, default_value);

  // Check the condition 1).

  // Returns true if the output changes when the current tuple is
  // at 'alternative_tuple_id' instead of 'current_tuple_id'.
  const auto changing_current_position_changes_output =
      [current_tuple_id, offset, &tuples, &arg_values, &default_value,
       &current_output](int alternative_tuple_id) {
        // Switch the current tuple and the tuple at 'alternative_tuple_id',
        // and then check if the output remains the same.

        // No need to switch the two tuples if they are equal, because
        // the two are not distinguishable and can have different outputs.
        if (tuples[alternative_tuple_id]->Equals(*tuples[current_tuple_id])) {
          return false;
        }

        std::vector<const TupleData*> tuples_with_new_order(tuples.begin(),
                                                            tuples.end());
        std::vector<Value> arg_values_with_new_order = arg_values;
        std::swap(tuples_with_new_order[current_tuple_id],
                  tuples_with_new_order[alternative_tuple_id]);
        std::swap(arg_values_with_new_order[current_tuple_id],
                  arg_values_with_new_order[alternative_tuple_id]);

        const int new_offset_tuple_id = offset + alternative_tuple_id;
        const Value output_at_alternative_position = GetOutputAtOffset(
            new_offset_tuple_id, arg_values_with_new_order, default_value);
        return !output_at_alternative_position.Equals(current_output);
      };
  if (ApplyToEachPeerTuple(schema, current_tuple_id, tuples, partition,
                           comparator,
                           changing_current_position_changes_output)) {
    return true;
  }

  // Check the condition 2).
  // Returns true if the offset tuple has any peers tuples that are different,
  // since any of those peers could be used to compute the output for the
  // current tuple.
  if (offset_tuple_id >= 0 && offset_tuple_id < tuples.size() &&
      CurrentTupleHasPeerWithDifferentRespectedValues(
          schema, offset + current_tuple_id, current_tuple_id, tuples,
          arg_values, partition, comparator, false /* ignore_nulls */)) {
    return true;
  }

  return false;
}

// Helper function to compute LEAD/LAG values.
// For every tuple at i in 'tuples', insert into 'result' a value in 'values'
// at a position at i+normalized_offset if i+normalized_offset is within
// the bound of 'values', or 'default value' otherwise.
// 'tuples' matches 1:1 with 'values'. 'comparator' is used to determine
// whether two tuples are peers in the window ordering associated with LEAD/LAG.
static void ComputeLeadLagValues(
    const TupleSchema& schema, int normalized_offset,
    absl::Span<const TupleData* const> tuples, const std::vector<Value>& values,
    const Value& default_value, const TupleComparator* comparator,
    EvaluationContext* context, std::vector<Value>* result) {
  for (int tuple_id = 0; tuple_id < tuples.size(); ++tuple_id) {
    result->emplace_back(
        GetOutputAtOffset(normalized_offset + tuple_id, values, default_value));
    if (LeadLagOutputIsNonDeterministic(schema, tuple_id, normalized_offset,
                                        tuples, values, default_value,
                                        *comparator)) {
      context->SetNonDeterministicOutput();
    }
  }
}

absl::Status LeadFunction::Eval(
    const TupleSchema& schema, const absl::Span<const TupleData* const>& tuples,
    const absl::Span<const std::vector<Value>>& args,
    const absl::Span<const AnalyticWindow>& windows,
    const TupleComparator* comparator,
    ResolvedFunctionCallBase::ErrorMode error_mode, EvaluationContext* context,
    std::vector<Value>* result) const {
  ZETASQL_RET_CHECK(windows.empty());
  ZETASQL_RET_CHECK(comparator != nullptr);

  // The optional arguments <offset> and <default expression> must have
  // been populated with default expressions by the algebrizer if they are not
  // specified.
  ZETASQL_RET_CHECK_EQ(3, args.size());
  // First argument <value expression> must have been evaluated on each
  // input tuple.
  ZETASQL_RET_CHECK_EQ(tuples.size(), args[0].size());
  // Second argument <offset> must be a constant and of type int64_t.
  ZETASQL_RET_CHECK_EQ(1, args[1].size());
  ZETASQL_RET_CHECK(args[1][0].type()->IsInt64());
  // Third argument <default expression> must be a constant.
  ZETASQL_RET_CHECK_EQ(1, args[2].size());

  if (args[1][0].is_null()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "The offset to the function LEAD must not be null";
  }

  const int64_t offset = args[1][0].int64_value();
  if (offset < 0) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "The offset to the function LEAD must not be negative";
  }

  // The first (tuples.size() - offset) tuples have lead values starting from
  // the offset to the last in args[0].
  const std::vector<Value>& values = args[0];
  const Value& default_value = args[2][0];
  ZETASQL_RET_CHECK(default_value.type()->Equals(output_type()));
  const int normalized_offset = std::min<int64_t>(offset, tuples.size());
  ComputeLeadLagValues(schema, normalized_offset, tuples, values, default_value,
                       comparator, context, result);

  return absl::OkStatus();
}

absl::Status LagFunction::Eval(const TupleSchema& schema,
                               const absl::Span<const TupleData* const>& tuples,
                               const absl::Span<const std::vector<Value>>& args,
                               const absl::Span<const AnalyticWindow>& windows,
                               const TupleComparator* comparator,
                               ResolvedFunctionCallBase::ErrorMode error_mode,
                               EvaluationContext* context,
                               std::vector<Value>* result) const {
  ZETASQL_RET_CHECK(windows.empty());
  ZETASQL_RET_CHECK(comparator != nullptr);

  // The optional arguments <offset> and <default expression> must have
  // been populated with default expressions by the algebrizer if they are not
  // specified.
  ZETASQL_RET_CHECK_EQ(3, args.size());
  // First argument <value expression> must have been evaluated on each
  // input tuple.
  ZETASQL_RET_CHECK_EQ(tuples.size(), args[0].size());
  // Second argument <offset> must be a constant and of type int64_t.
  ZETASQL_RET_CHECK_EQ(1, args[1].size());
  ZETASQL_RET_CHECK(args[1][0].type()->IsInt64());
  // Third argument <default expression> must be a constant.
  ZETASQL_RET_CHECK_EQ(1, args[2].size());

  if (args[1][0].is_null()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "The offset to the function LAG must not be null";
  }

  const int64_t offset = args[1][0].int64_value();
  if (offset < 0) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "The offset to the function LAG must not be negative";
  }

  const std::vector<Value>& values = args[0];
  const Value& default_value = args[2][0];
  ZETASQL_RET_CHECK(default_value.type()->Equals(output_type()));
  const int normalized_offset = -1 * std::min<int64_t>(offset, tuples.size());
  ComputeLeadLagValues(schema, normalized_offset, tuples, values, default_value,
                       comparator, context, result);

  return absl::OkStatus();
}

absl::Status PercentileContFunction::Eval(
    const TupleSchema& schema, const absl::Span<const TupleData* const>& tuples,
    const absl::Span<const std::vector<Value>>& args,
    const absl::Span<const AnalyticWindow>& windows,
    const TupleComparator* comparator,
    ResolvedFunctionCallBase::ErrorMode error_mode, EvaluationContext* context,
    std::vector<Value>* result) const {
  ZETASQL_RET_CHECK(windows.empty());
  ZETASQL_RET_CHECK(comparator == nullptr);

  ZETASQL_RET_CHECK_EQ(2, args.size());
  const std::vector<Value>& values_arg = args[0];
  // First argument <value expression> must have been evaluated on each
  // input tuple.
  ZETASQL_RET_CHECK_EQ(tuples.size(), args[0].size());
  // Second argument <offset> must be a constant and of type double.
  ZETASQL_RET_CHECK_EQ(1, args[1].size());
  const Value& percentile_arg = args[1][0];
  if (percentile_arg.is_null()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "The second argument to the function PERCENTILE_CONT must not be"
              " null";
  }
  Value output_value;
  switch (percentile_arg.type_kind()) {
    case TYPE_DOUBLE: {
      ZETASQL_ASSIGN_OR_RETURN(
          output_value,
          ComputePercentileCont(values_arg, percentile_arg.double_value(),
                                ignore_nulls_));
      break;
    }
    case TYPE_NUMERIC: {
      ZETASQL_ASSIGN_OR_RETURN(
          output_value,
          ComputePercentileCont(values_arg, percentile_arg.numeric_value(),
                                ignore_nulls_));
      break;
    }
    case TYPE_BIGNUMERIC: {
      ZETASQL_ASSIGN_OR_RETURN(
          output_value,
          ComputePercentileCont(values_arg, percentile_arg.bignumeric_value(),
                                ignore_nulls_));
      break;
    }
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported argument type for percentile_disc.";
  }
  result->resize(values_arg.size(), output_value);
  return absl::OkStatus();
}

absl::Status PercentileDiscFunction::Eval(
    const TupleSchema& schema, const absl::Span<const TupleData* const>& tuples,
    const absl::Span<const std::vector<Value>>& args,
    const absl::Span<const AnalyticWindow>& windows,
    const TupleComparator* comparator,
    ResolvedFunctionCallBase::ErrorMode error_mode, EvaluationContext* context,
    std::vector<Value>* result) const {
  ZETASQL_RET_CHECK(windows.empty());
  ZETASQL_RET_CHECK(comparator == nullptr);

  ZETASQL_RET_CHECK_EQ(2, args.size());
  const std::vector<Value>& values_arg = args[0];
  // First argument <value expression> must have been evaluated on each
  // input tuple.
  ZETASQL_RET_CHECK_EQ(tuples.size(), args[0].size());
  // Second argument <offset> must be a constant and of type double.
  ZETASQL_RET_CHECK_EQ(1, args[1].size());
  const Value& percentile_arg = args[1][0];
  if (percentile_arg.is_null()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "The second argument to the function PERCENTILE_DISC must not be"
              " null";
  }
  Value output_value;
  switch (percentile_arg.type_kind()) {
    case TYPE_DOUBLE: {
      ZETASQL_ASSIGN_OR_RETURN(output_value,
                       ComputePercentileDisc(values_arg, output_type(),
                                             percentile_arg.double_value(),
                                             ignore_nulls_, collator_.get()));
      break;
    }
    case TYPE_NUMERIC: {
      ZETASQL_ASSIGN_OR_RETURN(output_value,
                       ComputePercentileDisc(values_arg, output_type(),
                                             percentile_arg.numeric_value(),
                                             ignore_nulls_, collator_.get()));
      break;
    }
    case TYPE_BIGNUMERIC: {
      ZETASQL_ASSIGN_OR_RETURN(output_value,
                       ComputePercentileDisc(values_arg, output_type(),
                                             percentile_arg.bignumeric_value(),
                                             ignore_nulls_, collator_.get()));
      break;
    }
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported argument type for percentile_disc.";
  }
  result->resize(values_arg.size(), output_value);
  return absl::OkStatus();
}

absl::StatusOr<Value> DateTimeBucketFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK(args.size() == 2 || args.size() == 3);
  if (args[0].is_null() || args[1].is_null() ||
      (args.size() == 3 && args[2].is_null())) {
    return Value::Null(output_type());
  }

  switch (FCT(kind(), args[0].type_kind())) {
    case FCT(FunctionKind::kTimestampBucket, TYPE_TIMESTAMP):
    case FCT(FunctionKind::kDateTimeBucket, TYPE_TIMESTAMP):
    case FCT(FunctionKind::kDateBucket, TYPE_TIMESTAMP): {
      ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
      ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[1], context));
      absl::Time result;
      absl::Time origin;
      if (args.size() == 3) {
        ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[2], context));
        origin = args[2].ToTime();
      } else {
        origin = absl::FromCivil(functions::kDefaultTimeBucketOrigin,
                                 context->GetDefaultTimeZone());
      }
      ZETASQL_RETURN_IF_ERROR(functions::TimestampBucket(
          args[0].ToTime(), args[1].interval_value(), origin,
          context->GetDefaultTimeZone(),
          GetTimestampScale(context->GetLanguageOptions()), &result));
      return Value::Timestamp(result);
    }
    case FCT(FunctionKind::kTimestampBucket, TYPE_DATETIME):
    case FCT(FunctionKind::kDateTimeBucket, TYPE_DATETIME):
    case FCT(FunctionKind::kDateBucket, TYPE_DATETIME): {
      ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
      ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[1], context));
      DatetimeValue result;
      DatetimeValue origin;
      if (args.size() == 3) {
        ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[2], context));
        origin = args[2].datetime_value();
      } else {
        origin = DatetimeValue::FromCivilSecondAndMicros(
            functions::kDefaultTimeBucketOrigin, 0);
      }
      ZETASQL_RETURN_IF_ERROR(functions::DatetimeBucket(
          args[0].datetime_value(), args[1].interval_value(), origin,
          GetTimestampScale(context->GetLanguageOptions()), &result));
      return Value::Datetime(result);
    }
    case FCT(FunctionKind::kTimestampBucket, TYPE_DATE):
    case FCT(FunctionKind::kDateTimeBucket, TYPE_DATE):
    case FCT(FunctionKind::kDateBucket, TYPE_DATE): {
      ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
      ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[1], context));
      int32_t result;
      int32_t origin;
      if (args.size() == 3) {
        ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[2], context));
        origin = args[2].date_value();
      } else {
        ZETASQL_ASSIGN_OR_RETURN(
            origin, functions::ConvertCivilDayToDate(
                        absl::CivilDay(functions::kDefaultTimeBucketOrigin)));
      }
      ZETASQL_RETURN_IF_ERROR(functions::DateBucket(
          args[0].date_value(), args[1].interval_value(), origin, &result));
      return Value::Date(result);
    }
    default:
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Unsupported type " << args[0].type()->DebugString()
             << " for datetime BUCKET function";
  }
}

absl::Status ValidateMicrosPrecision(const Value& value,
                                     EvaluationContext* context) {
  if (value.is_null()) {
    return absl::OkStatus();
  }
  functions::TimestampScale scale =
      GetTimestampScale(context->GetLanguageOptions());
  if (scale == functions::TimestampScale::kNanoseconds) {
    return absl::OkStatus();
  }
  if (value.type()->IsTimestamp()) {
    if (absl::GetFlag(
            FLAGS_zetasql_reference_impl_validate_timestamp_precision)) {
      absl::Duration dnanos = value.ToTime() - absl::UnixEpoch();
      absl::Duration dmicros = absl::Floor(dnanos, absl::Microseconds(1));
      ZETASQL_RET_CHECK_EQ(dnanos, dmicros);
    }
    return absl::OkStatus();
  }
  if (value.type()->IsInterval()) {
    ZETASQL_RET_CHECK_EQ(value.interval_value().get_nano_fractions(), 0);
    return absl::OkStatus();
  }
  if (value.type()->IsDatetime()) {
    DatetimeValue dv = value.datetime_value();
    ZETASQL_RET_CHECK_EQ(dv.Microseconds() * 1000, dv.Nanoseconds());
    return absl::OkStatus();
  }
  if (value.type()->IsTime()) {
    TimeValue tv = value.time_value();
    ZETASQL_RET_CHECK_EQ(tv.Microseconds() * 1000, tv.Nanoseconds());
    return absl::OkStatus();
  }
  if (value.type()->IsArray()) {
    const Type* element_type = value.type()->AsArray()->element_type();

    if (element_type->IsTimestamp() || element_type->IsInterval() ||
        element_type->IsDatetime() || element_type->IsTime()) {
      for (const Value& element : value.elements()) {
        ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(element, context));
      }
    }
  }
  if (value.type()->IsRange()) {
    ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(value.start(), context));
    ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(value.end(), context));
    return absl::OkStatus();
  }
  // TODO: Validate struct fields and range endpoints.
  //    Maybe refactor this into a generic visitor which collects refs to the
  //    the interesting values and a then a separate checking pass.
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<AggregateFunctionBody>>
MakeUserDefinedAggregateFunction(
    AggregateFunctionEvaluatorFactory evaluator_factory,
    const FunctionSignature& function_signature, TypeFactory* type_factory,
    absl::string_view function_name, bool ignores_null) {
  ZETASQL_RET_CHECK(function_signature.result_type().IsConcrete());
  int num_input_fields =
      static_cast<int>(function_signature.arguments().size());
  ZETASQL_RET_CHECK_EQ(function_signature.NumConcreteArguments(), num_input_fields);
  const Type* input_type;
  switch (num_input_fields) {
    case 0:
      input_type = types::EmptyStructType();
      break;
    case 1:
      input_type = function_signature.ConcreteArgumentType(0);
      break;
    default: {
      std::vector<StructType::StructField> fields;
      fields.reserve(num_input_fields);
      for (int i = 0; i < num_input_fields; ++i) {
        fields.push_back({"", function_signature.ConcreteArgumentType(i)});
      }
      const StructType* struct_type;
      ZETASQL_RET_CHECK_OK(type_factory->MakeStructType(fields, &struct_type));
      input_type = struct_type;
      break;
    }
  }
  return std::make_unique<UserDefinedAggregateFunction>(
      evaluator_factory, function_signature,
      function_signature.result_type().type(), num_input_fields, input_type,
      function_name, ignores_null);
}

static ::zetasql_base::StatusBuilder DistanceFunctionResultConverter(
    const absl::Status& original_status) {
  if (!original_status.ok()) {
    return ::zetasql_base::OutOfRangeErrorBuilder() << original_status.message();
  }
  return ::zetasql_base::StatusBuilder(original_status);
}

absl::StatusOr<Value> CosineDistanceFunctionDense::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  ZETASQL_ASSIGN_OR_RETURN(Value result,
                   functions::CosineDistanceDense(args[0], args[1]),
                   _.With(&DistanceFunctionResultConverter));
  return result;
}

absl::StatusOr<Value> CosineDistanceFunctionSparseInt64Key::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  ZETASQL_ASSIGN_OR_RETURN(Value result,
                   functions::CosineDistanceSparseInt64Key(args[0], args[1]),
                   _.With(&DistanceFunctionResultConverter));
  return result;
}

absl::StatusOr<Value> CosineDistanceFunctionSparseStringKey::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  ZETASQL_ASSIGN_OR_RETURN(Value result,
                   functions::CosineDistanceSparseStringKey(args[0], args[1]),
                   _.With(&DistanceFunctionResultConverter));
  return result;
}

absl::StatusOr<Value> EuclideanDistanceFunctionDense::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  ZETASQL_ASSIGN_OR_RETURN(Value result,
                   functions::EuclideanDistanceDense(args[0], args[1]),
                   _.With(&DistanceFunctionResultConverter));
  return result;
}

absl::StatusOr<Value> EuclideanDistanceFunctionSparseInt64Key::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  ZETASQL_ASSIGN_OR_RETURN(Value result,
                   functions::EuclideanDistanceSparseInt64Key(args[0], args[1]),
                   _.With(&DistanceFunctionResultConverter));
  return result;
}

absl::StatusOr<Value> EuclideanDistanceFunctionSparseStringKey::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  ZETASQL_ASSIGN_OR_RETURN(
      Value result,
      functions::EuclideanDistanceSparseStringKey(args[0], args[1]),
      _.With(&DistanceFunctionResultConverter));
  return result;
}

absl::StatusOr<Value> EditDistanceFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_GE(args.size(), 2);
  ZETASQL_RET_CHECK_LE(args.size(), 3);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  ZETASQL_RET_CHECK((args[0].type()->IsString() && args[1].type()->IsString()) ||
            (args[0].type()->IsBytes() && args[1].type()->IsBytes()));
  bool is_string = args[0].type()->IsString();
  absl::string_view s0 = args[0].type()->IsBytes() ? args[0].bytes_value()
                                                   : args[0].string_value();
  absl::string_view s1 = args[1].type()->IsBytes() ? args[1].bytes_value()
                                                   : args[1].string_value();

  int64_t result = 0;
  if (is_string) {
    ZETASQL_ASSIGN_OR_RETURN(
        result,
        functions::EditDistance(s0, s1,
                                args.size() > 2
                                    ? std::make_optional(args[2].int64_value())
                                    : std::nullopt),
        _.With(&DistanceFunctionResultConverter));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(
        result,
        functions::EditDistanceBytes(
            s0, s1,
            args.size() > 2 ? std::make_optional(args[2].int64_value())
                            : std::nullopt),
        _.With(&DistanceFunctionResultConverter));
  }

  return Value::Int64(result);
}

}  // namespace zetasql
