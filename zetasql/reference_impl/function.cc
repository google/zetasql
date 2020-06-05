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

#include "zetasql/reference_impl/function.h"

#include <stddef.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <map>
#include <string>
#include <type_traits>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/timestamp.pb.h"
#include "google/protobuf/wrappers.pb.h"
#include "google/type/date.pb.h"
#include "google/type/latlng.pb.h"
#include "google/type/timeofday.pb.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/extension_set.h"
#include "google/protobuf/message.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/internal_value.h"
#include "zetasql/public/cast.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/function.h"
#include "zetasql/public/functions/arithmetics.h"
#include "zetasql/public/functions/bitcast.h"
#include "zetasql/public/functions/bitwise.h"
#include "zetasql/public/functions/common_proto.h"
#include "zetasql/public/functions/comparison.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/functions/generate_array.h"
#include "zetasql/public/functions/like.h"
#include "zetasql/public/functions/math.h"
#include "zetasql/public/functions/normalize_mode.pb.h"
#include "zetasql/public/functions/parse_date_time.h"
#include "zetasql/public/functions/percentile.h"
#include "zetasql/public/functions/regexp.h"
#include "zetasql/public/functions/string.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/proto_value_conversion.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/common.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/proto_util.h"
#include "zetasql/reference_impl/tuple_comparator.h"
#include <cstdint>
#include "absl/base/optimization.h"
#include "absl/memory/memory.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/exactfloat.h"
#include "re2/re2.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

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
  typedef bool(*ptr)(InType, OutType*, absl::Status* error);
};

template <typename OutType, typename InType = OutType>
bool InvokeUnary(typename UnaryExecutor<OutType, InType>::ptr function,
                 absl::Span<const Value> args, Value* result,
                 absl::Status* status) {
  CHECK_EQ(1, args.size());
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
    *result = Value::MakeNull<OutType>();
  }
  return true;
}

template <typename OutType, typename InType1, typename InType2>
struct BinaryExecutor {
  typedef bool(*ptr)(InType1, InType2, OutType*, absl::Status* error);
};

template <typename OutType, typename InType1 = OutType,
          typename InType2 = OutType>
bool InvokeBinary(
    typename BinaryExecutor<OutType, InType1, InType2>::ptr function,
    absl::Span<const Value> args, Value* result, absl::Status* status) {
  CHECK_EQ(2, args.size());
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
    *result = Value::MakeNull<OutType>();
  }
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

absl::Status MakeMaxArrayValueByteSizeExceededError(
    int64_t max_value_byte_size, const zetasql_base::SourceLocation& source_loc) {
  return zetasql_base::OutOfRangeErrorBuilder(source_loc)
         << "Arrays are limited to " << max_value_byte_size << " bytes";
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
          context->options().max_value_byte_size, ZETASQL_LOC);
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
Value CreateValueFromOptional(absl::optional<T> opt) {
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
  const std::map<FunctionKind, std::string>& function_debug_name_by_kind()
      const {
    return function_debug_name_by_kind_;
  }

  const std::map<std::string, FunctionKind>& function_kind_by_name() const {
    return function_kind_by_name_;
  }

 private:
  void RegisterFunction(FunctionKind kind, const std::string& name,
                        const std::string& debug_name) {
    CHECK(zetasql_base::InsertIfNotPresent(&function_debug_name_by_kind_, kind,
                                  debug_name))
        << "Duplicate function debug_name: " << debug_name;
    if (!name.empty()) {
      CHECK(zetasql_base::InsertIfNotPresent(&function_kind_by_name_, name, kind))
          << "Duplicate function name: " << name;
    }
  }

  std::map<FunctionKind, std::string> function_debug_name_by_kind_;
  std::map<std::string, FunctionKind> function_kind_by_name_;
};

FunctionMap::FunctionMap() {
  static const std::string kPrivate = "";  // for private functions
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
  RegisterFunction(FunctionKind::kSafeArrayAtOrdinal, "$safe_array_at_ordinal",
                   "SafeArrayAtOrdinal");
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
  RegisterFunction(FunctionKind::kDatetimeAdd, "datetime_add", "Datetime_add");
  RegisterFunction(FunctionKind::kDatetimeSub, "datetime_sub", "Datetime_sub");
  RegisterFunction(FunctionKind::kDatetimeDiff, "datetime_diff",
                   "Datetime_diff");
  RegisterFunction(FunctionKind::kDateTrunc, "date_trunc", "Date_trunc");
  RegisterFunction(FunctionKind::kDatetimeTrunc, "datetime_trunc",
                   "Datetime_trunc");
  RegisterFunction(FunctionKind::kDateDiff, "date_diff", "Date_diff");
  RegisterFunction(FunctionKind::kDivide, "$divide", "Divide");
  RegisterFunction(FunctionKind::kSafeDivide, "safe_divide", "SafeDivide");
  RegisterFunction(FunctionKind::kDiv, "div", "Div");
  RegisterFunction(FunctionKind::kEqual, "$equal", "Equal");
  RegisterFunction(FunctionKind::kExists, "exists", "Exists");
  RegisterFunction(FunctionKind::kGenerateArray, "generate_array",
                   "GenerateArray");
  RegisterFunction(FunctionKind::kGenerateDateArray, "generate_date_array",
                   "GenerateDateArray");
  RegisterFunction(FunctionKind::kGenerateTimestampArray,
                   "generate_timestamp_array", "GenerateTimestampArray");
  RegisterFunction(FunctionKind::kRangeBucket, "range_bucket", "RangeBucket");
  RegisterFunction(FunctionKind::kJsonExtract, "json_extract", "JsonExtract");
  RegisterFunction(FunctionKind::kJsonExtractScalar, "json_extract_scalar",
                   "JsonExtractScalar");
  RegisterFunction(FunctionKind::kJsonExtractArray, "json_extract_array",
                   "JsonExtractArray");
  RegisterFunction(FunctionKind::kJsonQuery, "json_query", "JsonQuery");
  RegisterFunction(FunctionKind::kJsonValue, "json_value", "JsonValue");
  RegisterFunction(FunctionKind::kGreatest, "greatest", "Greatest");
  RegisterFunction(FunctionKind::kIsNull, "$is_null", "IsNull");
  RegisterFunction(FunctionKind::kIsTrue, "$is_true", "IsTrue");
  RegisterFunction(FunctionKind::kIsFalse, "$is_false", "IsFalse");
  RegisterFunction(FunctionKind::kLeast, "least", "Least");
  RegisterFunction(FunctionKind::kLess, "$less", "Less");
  RegisterFunction(FunctionKind::kLessOrEqual, "$less_or_equal", "LessOrEqual");
  RegisterFunction(FunctionKind::kLike, "$like", "Like");
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
  RegisterFunction(FunctionKind::kCorr, "corr", "Corr");
  RegisterFunction(FunctionKind::kCovarPop, "covar_pop", "Covar_pop");
  RegisterFunction(FunctionKind::kCovarSamp, "covar_samp", "Covar_samp");
  RegisterFunction(FunctionKind::kVarPop, "var_pop", "Var_pop");
  RegisterFunction(FunctionKind::kVarSamp, "var_samp", "Var_samp");
  RegisterFunction(FunctionKind::kByteLength, "byte_length", "ByteLength");
  RegisterFunction(FunctionKind::kCharLength, "char_length", "CharLength");
  RegisterFunction(FunctionKind::kConcat, "concat", "Concat");
  RegisterFunction(FunctionKind::kEndsWith, "ends_with", "EndsWith");
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
  RegisterFunction(FunctionKind::kRegexpReplace, "regexp_replace",
                   "RegexpReplace");
  RegisterFunction(FunctionKind::kReplace, "replace", "Replace");
  RegisterFunction(FunctionKind::kRtrim, "rtrim", "Rtrim");
  RegisterFunction(FunctionKind::kSplit, "split", "Split");
  RegisterFunction(FunctionKind::kStartsWith, "starts_with", "StartsWith");
  RegisterFunction(FunctionKind::kStrpos, "strpos", "Strpos");
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
                   "safe_convert_bytes_to_string", "SafeConvertBytesToString");
  RegisterFunction(FunctionKind::kNormalize,
                   "normalize", "Normalize");
  RegisterFunction(FunctionKind::kNormalizeAndCasefold,
                   "normalize_and_casefold", "NormalizeAndCasefold");
  RegisterFunction(FunctionKind::kToBase64, "to_base64", "ToBase64");
  RegisterFunction(FunctionKind::kFromBase64, "from_base64", "FromBase64");
  RegisterFunction(FunctionKind::kToHex, "to_hex", "ToHex");
  RegisterFunction(FunctionKind::kFromHex, "from_hex", "FromHex");
  RegisterFunction(FunctionKind::kToCodePoints, "to_code_points",
                   "ToCodePoints");
  RegisterFunction(FunctionKind::kCodePointsToString, "code_points_to_string",
                   "CodePointsToString");
  RegisterFunction(FunctionKind::kCodePointsToBytes, "code_points_to_bytes",
                   "CodePointsToBytes");
  RegisterFunction(FunctionKind::kCurrentDate, "current_date", "Current_date");
  RegisterFunction(FunctionKind::kCurrentDatetime, "current_datetime",
                   "Current_datetime");
  RegisterFunction(FunctionKind::kCurrentTime, "current_time", "Current_time");
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
  RegisterFunction(FunctionKind::kFormatDate, "format_date",
                   "Format_date");
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
  RegisterFunction(FunctionKind::kStringFromTimestamp, "string",
                   "String");
  RegisterFunction(FunctionKind::kParseDate, "parse_date",
                   "Parse_date");
  RegisterFunction(FunctionKind::kParseDatetime, "parse_datetime",
                   "Parse_datetime");
  RegisterFunction(FunctionKind::kParseTime, "parse_time",
                   "Parse_time");
  RegisterFunction(FunctionKind::kParseTimestamp, "parse_timestamp",
                   "Parse_timestamp");
  RegisterFunction(FunctionKind::kFromProto, "from_proto", "From_proto");
  RegisterFunction(FunctionKind::kToProto, "to_proto", "To_proto");
  RegisterFunction(FunctionKind::kEnumValueDescriptorProto,
                   "enum_value_descriptor_proto",
                   "Enum_value_descriptor_proto");
  RegisterFunction(FunctionKind::kDate, "date", "Date");
  RegisterFunction(FunctionKind::kTimestamp, "timestamp", "Timestamp");
  RegisterFunction(FunctionKind::kTime, "time", "Time");
  RegisterFunction(FunctionKind::kDatetime, "datetime", "Datetime");
  RegisterFunction(FunctionKind::kDenseRank, "dense_rank", "Dense_rank");
  RegisterFunction(FunctionKind::kRank, "rank", "Rank");
  RegisterFunction(FunctionKind::kRowNumber, "row_number", "Row_number");
  RegisterFunction(FunctionKind::kPercentRank, "percent_rank", "Percent_rank");
  RegisterFunction(FunctionKind::kCumeDist, "cume_dist", "Cume_dist");
  RegisterFunction(FunctionKind::kNtile, "ntile", "Ntile");
  RegisterFunction(FunctionKind::kFirstValue, "first_value", "First_value");
  RegisterFunction(FunctionKind::kLastValue, "last_value", "Last_value");
  RegisterFunction(FunctionKind::kNthValue, "nth_value", "Nth_value");
  RegisterFunction(FunctionKind::kLead, "lead", "Lead");
  RegisterFunction(FunctionKind::kLag, "lag", "Lag");
  RegisterFunction(FunctionKind::kPercentileCont, "percentile_cont",
                   "Percentile_cont");
  RegisterFunction(FunctionKind::kRand, "rand", "Rand");
  RegisterFunction(FunctionKind::kGenerateUuid, "generate_uuid",
                   "Generate_Uuid");
  RegisterFunction(FunctionKind::kMd5, "md5", "Md5");
  RegisterFunction(FunctionKind::kSha1, "sha1", "Sha1");
  RegisterFunction(FunctionKind::kSha256, "sha256", "Sha256");
  RegisterFunction(FunctionKind::kSha512, "sha512", "Sha512");
  RegisterFunction(FunctionKind::kFarmFingerprint, "farm_fingerprint",
                   "FarmFingerprint");
}

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
  static absl::Status InitializePattern(const Value& pattern,
                                        functions::RegExp* regexp) {
    absl::Status status;
    regexp->InitializePatternUtf8(FromValue(pattern), &status);
    return status;
  }

  static const std::string& FromValue(const Value& value) {
    return value.string_value();
  }

  static Value ToValue(absl::string_view out) { return Value::String(out); }

  static Value ToArray(absl::Span<const Value> values) {
    return Value::Array(types::StringArrayType(), values);
  }

  static Value NullValue() { return Value::NullString(); }

  static absl::string_view DebugString() { return "STRING"; }
};

// Traits for zetasql bytes
template <>
struct ValueTraits<TYPE_BYTES> {
  static absl::Status InitializePattern(const Value& pattern,
                                        functions::RegExp* regexp) {
    absl::Status status;
    regexp->InitializePatternBytes(FromValue(pattern), &status);
    return status;
  }

  static const std::string& FromValue(const Value& value) {
    return value.bytes_value();
  }

  static Value ToValue(absl::string_view out) { return Value::Bytes(out); }

  static Value ToArray(absl::Span<const Value> values) {
    return Value::Array(types::BytesArrayType(), values);
  }

  static Value NullValue() { return Value::NullBytes(); }

  static absl::string_view DebugString() { return "BYTES"; }
};

// Wrap the function in a function which initializes the RE2 expr (if the
// RE2 pattern is not known at Prepare time) or initialize the RE2 expression
// and directly pass back the function.
template <TypeKind type>
static RegexpFunction::EvalFunction WrapOrInitRegexpFunction(
    RegexpFunction::EvalFunction func, const ConstExpr* pattern,
    functions::RegExp* regexp, absl::Status* status) {
  if (pattern && !pattern->value().is_null()) {
    *status = ValueTraits<type>::InitializePattern(pattern->value(), regexp);
    return func;
  } else {
    return [func](const absl::Span<const Value>& x,
                  functions::RegExp* regexp) -> zetasql_base::StatusOr<Value> {
      ZETASQL_RETURN_IF_ERROR(ValueTraits<type>::InitializePattern(x[1], regexp));
      return func(x, regexp);
    };
  }
}

// Helper function for regexp_contains.
template <TypeKind type>
static zetasql_base::StatusOr<Value> Contains(absl::Span<const Value> x,
                                      functions::RegExp* regexp) {
  absl::Status status;
  bool out;
  if (!regexp->Contains(ValueTraits<type>::FromValue(x[0]), &out, &status)) {
    return status;
  }
  return Value::Bool(out);
}

// Helper function for regexp_match.
template <TypeKind type>
static zetasql_base::StatusOr<Value> Match(absl::Span<const Value> x,
                                   functions::RegExp* regexp) {
  absl::Status status;
  bool out;
  if (!regexp->Match(ValueTraits<type>::FromValue(x[0]), &out, &status)) {
    return status;
  }
  return Value::Bool(out);
}

// Helper function for regexp_extract.
template <TypeKind type>
static zetasql_base::StatusOr<Value> Extract(absl::Span<const Value> x,
                                     functions::RegExp* regexp) {
  absl::Status status;
  absl::string_view out;
  bool is_null;
  std::string in_str = ValueTraits<type>::FromValue(x[0]);
  if (!regexp->Extract(in_str, &out, &is_null, &status)) {
    return status;
  }
  if (is_null) {
    return ValueTraits<type>::NullValue();
  } else {
    return ValueTraits<type>::ToValue(out);
  }
}

// Helper function for regexp_replace.
template <TypeKind type>
static zetasql_base::StatusOr<Value> Replace(absl::Span<const Value> x,
                                     functions::RegExp* regexp) {
  absl::Status status;
  std::string out;
  if (!regexp->Replace(ValueTraits<type>::FromValue(x[0]),
                       ValueTraits<type>::FromValue(x[2]), &out, &status)) {
    return status;
  }
  return ValueTraits<type>::ToValue(out);
}

// Helper function for regexp_extractall.
template <TypeKind type>
static zetasql_base::StatusOr<Value> ExtractAll(absl::Span<const Value> x,
                                        functions::RegExp* regexp) {
  absl::Status status;
  std::vector<Value> values;
  std::string in_str = ValueTraits<type>::FromValue(x[0]);
  absl::string_view in = in_str;
  regexp->ExtractAllReset(in);
  while (true) {
    absl::string_view out;
    if (!regexp->ExtractAllNext(&out, &status)) {
      break;
    }
    values.push_back(ValueTraits<type>::ToValue(out));
  }
  if (!status.ok()) {
    return status;
  }
  return ValueTraits<type>::ToArray(values);
}

absl::Status UpdateCovariance(
    double x, double y,
    double mean_x, double mean_y,
    double pair_count, double* covar) {
  absl::Status error;
  double old_pair_count, delta_x, delta_y, tmp;

  // Stable one-pass covariance algorithm per
  // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Covariance
  // covar = ((covar * old_pair_count) +
  //     ((x - mean_x) * (y - mean_y) * old_pair_count) / pair_count) /
  //     pair_count;
  if (
      // pair_count - 1
      !functions::Subtract(pair_count, 1.0, &old_pair_count, &error) ||
      // x - mean_x
      !functions::Subtract(x, mean_x, &delta_x, &error) ||
      // y - mean_y
      !functions::Subtract(y, mean_y, &delta_y, &error) ||
      // (x - mean_x) * (y - mean_y)
      !functions::Multiply(delta_x, delta_y, &tmp, &error) ||
      // (x - mean_x) * (y - mean_y) * old_pair_count
      !functions::Multiply(tmp, old_pair_count, &tmp, &error) ||
      // (x - mean_x) * (y - mean_y) * old_pair_count / pair_count
      !functions::Divide(tmp, pair_count, &tmp, &error) ||
      // covar * old_pair_count
      !functions::Multiply(*covar, old_pair_count, covar, &error) ||
      // covar * old_pair_count +
      //     ((x - mean_x) * (y - mean_y) * old_pair_count) / pair_count
      !functions::Add(*covar, tmp, covar, &error) ||
      // (covar * old_pair_count +
      //     ((x - mean_x) * (y - mean_y) * old_pair_count) / pair_count) /
      //     pair_count
      !functions::Divide(*covar, pair_count, covar, &error)) {
    return error;
  }

  return absl::OkStatus();
}

absl::Status UpdateMeanAndVariance(
    double arg,
    double count,
    double* mean,
    double* variance) {
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

  const double frac = 1.0 / count;

  // delta = x - mean
  double delta;
  if (!functions::Subtract(arg, *mean, &delta, &error)) {
    return error;
  }

  // mean += delta * frac
  double tmp;
  if (!functions::Multiply(delta, frac, &tmp, &error) ||
      !functions::Add(*mean, tmp, mean, &error)) {
    return error;
  }

  if (
      // (1-frac) * delta
      !functions::Multiply(1 - frac, delta, &tmp, &error) ||
      // (1-frac) * delta ^ 2
      !functions::Multiply(tmp, delta, &tmp, &error) ||
      // (1-frac) * delta ^ 2 - variance
      !functions::Subtract(tmp, *variance, &tmp, &error) ||
      // frac * ((1-frac) * delta ^ 2 - variance)
      !functions::Multiply(frac, tmp, &tmp, &error) ||
      // variance += frac * ((1-frac) * delta ^ 2 - variance)
      !functions::Add(*variance, tmp, variance, &error)) {
    return error;
  }

  return absl::OkStatus();
}

absl::Status ConcatError(int64_t max_output_size, zetasql_base::SourceLocation src) {
  return zetasql_base::OutOfRangeErrorBuilder(src)
         << absl::StrCat("Output of CONCAT exceeds max allowed output size of ",
                         max_output_size, " bytes");
}

}  // namespace

ABSL_CONST_INIT absl::Mutex BuiltinFunctionRegistry::mu_(absl::kConstInit);

/* static */ zetasql_base::StatusOr<BuiltinScalarFunction*>
BuiltinFunctionRegistry::GetScalarFunction(FunctionKind kind,
                                           const Type* output_type) {
  absl::MutexLock lock(&mu_);
  auto it = GetFunctionMap().find(kind);
  if (it != GetFunctionMap().end()) {
    return it->second(output_type);
  } else {
    return zetasql_base::UnimplementedErrorBuilder(ZETASQL_LOC)
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
  DCHECK(array.type()->IsArray());
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

// Function used to switch on a (function kind, output type) pair.
static constexpr uint64_t FCT(FunctionKind function_kind, TypeKind type_kind) {
  return (static_cast<uint64_t>(function_kind) << 32) + type_kind;
}

// Function used to switch on a (function kind, output type, output type)
// triple.
static constexpr uint64_t FCT2(
    FunctionKind function_kind, TypeKind type_kind1, TypeKind type_kind2) {
  return (static_cast<uint64_t>(function_kind) << 32) +
         (static_cast<uint64_t>(type_kind1) << 16) +
         type_kind2;
}

// Function used to switch on a (function kind, output type, args.size())
// triple.
static constexpr uint64_t FCT_TYPE_ARITY(
    FunctionKind function_kind, TypeKind type_kind, size_t arity) {
  return (static_cast<uint64_t>(function_kind) << 32) +
         (type_kind << 16) +
         arity;
}

zetasql_base::StatusOr<FunctionKind> BuiltinFunctionCatalog::GetKindByName(
    const absl::string_view& name) {
  const FunctionKind* kind = zetasql_base::FindOrNull(
      GetFunctionMap().function_kind_by_name(), std::string(name));
  if (kind == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
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

zetasql_base::StatusOr<std::unique_ptr<ScalarFunctionCallExpr>>
BuiltinScalarFunction::CreateCall(
    FunctionKind kind, const LanguageOptions& language_options,
    const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> arguments,
    ResolvedFunctionCallBase::ErrorMode error_mode) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<BuiltinScalarFunction> function,
      CreateValidated(kind, language_options, output_type, arguments));
  return ScalarFunctionCallExpr::Create(std::move(function),
                                        std::move(arguments), error_mode);
}

std::unique_ptr<BuiltinScalarFunction> BuiltinScalarFunction::CreateUnvalidated(
    FunctionKind kind, const Type* output_type) {
  auto result = CreateValidated(kind, LanguageOptions::MaximumFeatures(),
                                output_type, {});
  ZETASQL_CHECK_OK(result.status());
  return std::move(result.value());
}

zetasql_base::StatusOr<BuiltinScalarFunction*>
BuiltinScalarFunction::CreateValidatedRaw(
    FunctionKind kind, const LanguageOptions& language_options,
    const Type* output_type,
    const std::vector<std::unique_ptr<ValueExpr>>& arguments) {
  std::vector<const Type*> input_types;
  for (const auto& expr : arguments) {
    input_types.push_back(expr->output_type());
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
      ZETASQL_RETURN_IF_ERROR(ValidateInputTypesSupportEqualityComparison(
          kind, input_types));
      return new ComparisonFunction(kind, output_type);
    case FunctionKind::kLeast:
      ZETASQL_RETURN_IF_ERROR(ValidateInputTypesSupportOrderComparison(
          kind, input_types));
      return new LeastFunction(output_type);
    case FunctionKind::kGreatest:
      ZETASQL_RETURN_IF_ERROR(ValidateInputTypesSupportOrderComparison(
          kind, input_types));
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
      ZETASQL_RETURN_IF_ERROR(ValidateInputTypesSupportEqualityComparison(
          kind, input_types));
      ZETASQL_ASSIGN_OR_RETURN(auto fct,
                       CreateLikeFunction(kind, output_type, arguments));
      return fct.release();
    }
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
      return new MathFunction(kind, output_type);
    case FunctionKind::kConcat:
      return new ConcatFunction(kind, output_type);
    case FunctionKind::kLower:
    case FunctionKind::kUpper:
      return new CaseConverterFunction(kind, output_type);
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
      return new StringFunction(kind, output_type);
    case FunctionKind::kToCodePoints:
      return new ToCodePointsFunction;
    case FunctionKind::kCodePointsToString:
    case FunctionKind::kCodePointsToBytes:
      return new CodePointsToFunction(kind, output_type);
    case FunctionKind::kRegexpContains:
    case FunctionKind::kRegexpMatch:
    case FunctionKind::kRegexpExtract:
    case FunctionKind::kRegexpExtractAll:
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
    case FunctionKind::kJsonExtract:
    case FunctionKind::kJsonExtractScalar:
    case FunctionKind::kJsonExtractArray:
    case FunctionKind::kJsonQuery:
    case FunctionKind::kJsonValue:
      return BuiltinFunctionRegistry::GetScalarFunction(kind, output_type);
    case FunctionKind::kArrayConcat:
      return new ArrayConcatFunction(kind, output_type);
    case FunctionKind::kArrayLength:
      return new ArrayLengthFunction(kind, output_type);
    case FunctionKind::kArrayToString:
      return new ArrayToStringFunction(kind, output_type);
    case FunctionKind::kArrayReverse:
      return new ArrayReverseFunction(output_type);
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
      return new CivilTimeTruncFunction(kind, output_type);
    case FunctionKind::kDateTrunc:
      return new DateTruncFunction(kind, output_type);
    case FunctionKind::kTimestampTrunc:
      return new TimestampTruncFunction(kind, output_type);
    case FunctionKind::kExtractFrom:
      return new ExtractFromFunction(kind, output_type);
    case FunctionKind::kExtractDateFrom:
      return new ExtractDateFromFunction(kind, output_type);
    case FunctionKind::kExtractTimeFrom:
      return new ExtractTimeFromFunction(kind, output_type);
    case FunctionKind::kExtractDatetimeFrom:
      return new ExtractDatetimeFromFunction(kind, output_type);
    case FunctionKind::kFormatDate:
      return new FormatDateFunction(kind, output_type);
    case FunctionKind::kFormatDatetime:
      return new FormatDatetimeFunction(kind, output_type);
    case FunctionKind::kFormatTime:
      return new FormatTimeFunction(kind, output_type);
    case FunctionKind::kFormatTimestamp:
      return new FormatTimestampFunction(kind, output_type);
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
    case FunctionKind::kStringFromTimestamp:
      return new StringFromTimestampFunction(kind, output_type);
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
    default:
      ZETASQL_RET_CHECK_FAIL() << BuiltinFunctionCatalog::GetDebugNameByKind(kind)
                       << " is not a scalar function";
      break;
  }
}

zetasql_base::StatusOr<std::unique_ptr<BuiltinScalarFunction>>
BuiltinScalarFunction::CreateValidated(
    FunctionKind kind, const LanguageOptions& language_options,
    const Type* output_type,
    const std::vector<std::unique_ptr<ValueExpr>>& arguments) {
  ZETASQL_ASSIGN_OR_RETURN(
      BuiltinScalarFunction * func,
      CreateValidatedRaw(kind, language_options, output_type, arguments));
  return std::unique_ptr<BuiltinScalarFunction>(func);
}

// Create a Regexp evaluation function based on the function kind
// and whether or not it is a constant expression.
static RegexpFunction::EvalFunction CreateEvalFunction(
    const FunctionKind& kind, TypeKind input_kind, const ConstExpr* pattern,
    functions::RegExp* regexp, absl::Status* status) {
  switch (FCT(kind, input_kind)) {
    case FCT(FunctionKind::kRegexpContains, TYPE_STRING): {
      return WrapOrInitRegexpFunction<TYPE_STRING>(&Contains<TYPE_STRING>,
                                                   pattern, regexp, status);
    }
    case FCT(FunctionKind::kRegexpContains, TYPE_BYTES): {
      return WrapOrInitRegexpFunction<TYPE_BYTES>(&Contains<TYPE_BYTES>,
                                                  pattern, regexp, status);
    }
    case FCT(FunctionKind::kRegexpMatch, TYPE_STRING): {
      return WrapOrInitRegexpFunction<TYPE_STRING>(&Match<TYPE_STRING>, pattern,
                                                   regexp, status);
    }
    case FCT(FunctionKind::kRegexpMatch, TYPE_BYTES): {
      return WrapOrInitRegexpFunction<TYPE_BYTES>(&Match<TYPE_BYTES>, pattern,
                                                  regexp, status);
    }
    case FCT(FunctionKind::kRegexpExtract, TYPE_STRING): {
      return WrapOrInitRegexpFunction<TYPE_STRING>(&Extract<TYPE_STRING>,
                                                   pattern, regexp, status);
    }
    case FCT(FunctionKind::kRegexpExtract, TYPE_BYTES): {
      return WrapOrInitRegexpFunction<TYPE_BYTES>(&Extract<TYPE_BYTES>, pattern,
                                                  regexp, status);
    }
    case FCT(FunctionKind::kRegexpExtractAll, TYPE_STRING): {
      return WrapOrInitRegexpFunction<TYPE_STRING>(&ExtractAll<TYPE_STRING>,
                                                   pattern, regexp, status);
    }
    case FCT(FunctionKind::kRegexpExtractAll, TYPE_BYTES): {
      return WrapOrInitRegexpFunction<TYPE_BYTES>(&ExtractAll<TYPE_BYTES>,
                                                  pattern, regexp, status);
    }
    case FCT(FunctionKind::kRegexpReplace, TYPE_STRING): {
      return WrapOrInitRegexpFunction<TYPE_STRING>(&Replace<TYPE_STRING>,
                                                   pattern, regexp, status);
    }
    case FCT(FunctionKind::kRegexpReplace, TYPE_BYTES): {
      return WrapOrInitRegexpFunction<TYPE_BYTES>(&Replace<TYPE_BYTES>, pattern,
                                                  regexp, status);
    }
  }
  *status = ::zetasql_base::UnimplementedErrorBuilder()
            << "Unsupported regexp function: "
            << static_cast<std::underlying_type<FunctionKind>::type>(kind);
  return nullptr;
}

zetasql_base::StatusOr<std::unique_ptr<BuiltinScalarFunction>>
BuiltinScalarFunction::CreateLikeFunction(
    FunctionKind kind, const Type* output_type,
    const std::vector<std::unique_ptr<ValueExpr>>& arguments) {
  if (arguments[1]->IsConstant()) {
    const ConstExpr* pattern_expr =
        static_cast<const ConstExpr*>(arguments[1].get());
    if (!pattern_expr->value().is_null()) {
      // Build and precompile the regexp.
      const std::string& pattern =
          pattern_expr->value().type_kind() == TYPE_STRING
              ? pattern_expr->value().string_value()
              : pattern_expr->value().bytes_value();
      std::unique_ptr<RE2> regexp;
      ZETASQL_RETURN_IF_ERROR(functions::CreateLikeRegexp(
          pattern, arguments[1]->output_type()->kind(),
          &regexp));
      return std::unique_ptr<BuiltinScalarFunction>(
          new LikeFunction(kind, output_type, std::move(regexp)));
    }
  }

  // The pattern is not a constant expression or it is null; build and
  // compile the regexp at evaluation time.
  return std::unique_ptr<BuiltinScalarFunction>(
      new LikeFunction(kind, output_type, nullptr /* regexp */));
}

zetasql_base::StatusOr<std::unique_ptr<BuiltinScalarFunction>>
BuiltinScalarFunction::CreateRegexpFunction(
    FunctionKind kind, const Type* output_type,
    const std::vector<std::unique_ptr<ValueExpr>>& arguments) {
  std::vector<const Type*> input_types;
  for (const auto& expr : arguments) {
    input_types.push_back(expr->output_type());
  }
  auto regexp = absl::make_unique<functions::RegExp>();
  TypeKind input_kind = input_types[0]->kind();
  // Pattern is either nullptr or constant expression.
  const ConstExpr* pattern =
      arguments[1]->IsConstant()
          ? static_cast<const ConstExpr*>(arguments[1].get())
          : nullptr;

  absl::Status status;
  RegexpFunction::EvalFunction eval_func =
      CreateEvalFunction(kind, input_kind, pattern, regexp.get(), &status);
  ZETASQL_RETURN_IF_ERROR(status);
  return std::unique_ptr<BuiltinScalarFunction>(
      new RegexpFunction(std::move(regexp), eval_func, kind, output_type));
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

bool LeastFunction::Eval(absl::Span<const Value> args,
                         EvaluationContext* context, Value* result,
                         absl::Status* status) const {
  DCHECK_GT(args.size(), 0);
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

bool GreatestFunction::Eval(absl::Span<const Value> args,
                            EvaluationContext* context, Value* result,
                            absl::Status* status) const {
  DCHECK_GT(args.size(), 0);
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

zetasql_base::StatusOr<Value> ToCodePointsFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  DCHECK_EQ(args.size(), 1);
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

zetasql_base::StatusOr<Value> CodePointsToFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  DCHECK_EQ(args.size(), 1);
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

zetasql_base::StatusOr<Value> GenerateArrayFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  DCHECK_GE(args.size(), 2);
  DCHECK_LE(args.size(), 4);
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
        context->options().max_value_byte_size, ZETASQL_LOC);
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

zetasql_base::StatusOr<Value> RangeBucketFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  DCHECK_EQ(args.size(), 2);
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

bool ArithmeticFunction::Eval(absl::Span<const Value> args,
                              EvaluationContext* context, Value* result,
                              absl::Status* status) const {
  if (kind() == FunctionKind::kUnaryMinus ||
      kind() == FunctionKind::kSafeNegate) {
    DCHECK_EQ(1, args.size());
  } else {
    DCHECK_EQ(2, args.size());
  }
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }

  switch (FCT(kind(), args[0].type_kind())) {
    case FCT(FunctionKind::kAdd, TYPE_INT64):
      return InvokeBinary<int64_t>(&functions::Add<int64_t>, args, result, status);
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

    case FCT(FunctionKind::kMultiply, TYPE_INT64):
      return InvokeBinary<int64_t>(&functions::Multiply<int64_t>, args, result,
                                 status);
    case FCT(FunctionKind::kMultiply, TYPE_UINT64):
      return InvokeBinary<uint64_t>(&functions::Multiply<uint64_t>, args, result,
                                  status);
    case FCT(FunctionKind::kMultiply, TYPE_DOUBLE):
      return InvokeBinary<double>(&functions::Multiply<double>, args, result,
                                  status);
    case FCT(FunctionKind::kMultiply, TYPE_NUMERIC):
      return InvokeBinary<NumericValue>(&functions::Multiply<NumericValue>,
                                        args, result, status);
    case FCT(FunctionKind::kMultiply, TYPE_BIGNUMERIC):
      return InvokeBinary<BigNumericValue>(
          &functions::Multiply<BigNumericValue>, args, result, status);

    case FCT(FunctionKind::kDivide, TYPE_DOUBLE):
      return InvokeBinary<double>(&functions::Divide<double>, args, result,
                                  status);
    case FCT(FunctionKind::kDivide, TYPE_NUMERIC):
      return InvokeBinary<NumericValue>(&functions::Divide<NumericValue>, args,
                                        result, status);
    case FCT(FunctionKind::kDivide, TYPE_BIGNUMERIC):
      return InvokeBinary<BigNumericValue>(&functions::Divide<BigNumericValue>,
                                           args, result, status);

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
      return SafeInvokeBinary<int64_t>(&functions::Subtract<int64_t>, args, result,
                                     status);
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
      return SafeInvokeBinary<int64_t>(&functions::Multiply<int64_t>, args, result,
                                     status);
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
      return InvokeUnary<int64_t>(&functions::UnaryMinus<int64_t, int64_t>, args,
                                result, status);
    case FCT(FunctionKind::kUnaryMinus, TYPE_INT32):
      return InvokeUnary<int32_t>(&functions::UnaryMinus<int32_t, int32_t>, args,
                                result, status);
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

    case FCT(FunctionKind::kSafeNegate, TYPE_INT64):
      return SafeInvokeUnary<int64_t>(&functions::UnaryMinus<int64_t, int64_t>, args,
                                    result, status);
    case FCT(FunctionKind::kSafeNegate, TYPE_INT32):
      return SafeInvokeUnary<int32_t>(&functions::UnaryMinus<int32_t, int32_t>, args,
                                    result, status);
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

bool ComparisonFunction::Eval(absl::Span<const Value> args,
                              EvaluationContext* context, Value* result,
                              absl::Status* status) const {
  DCHECK_EQ(2, args.size());

  const Value& x = args[0];
  const Value& y = args[1];

  if (kind() == FunctionKind::kEqual) {
    *result = x.SqlEquals(y);
    if (!result->is_valid()) {
      *status = ::zetasql_base::UnimplementedErrorBuilder()
                << "Unsupported comparison function: " << debug_name()
                << " with inputs " << TypeKind_Name(x.type_kind()) << " and "
                << TypeKind_Name(y.type_kind());
      return false;
    }
    return true;
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
    case FCT2(FunctionKind::kLessOrEqual, TYPE_ENUM, TYPE_ENUM):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_NUMERIC, TYPE_NUMERIC):
    case FCT2(FunctionKind::kLessOrEqual, TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
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
        if (!compare_less.Eval({x.element(i), y.element(i)}, context, result,
                               status)) {
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
        if (!compare_less.Eval({y.element(i), x.element(i)}, context, result,
                               status)) {
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

bool ExistsFunction::Eval(absl::Span<const Value> args,
                          EvaluationContext* context, Value* result,
                          absl::Status* status) const {
  DCHECK_EQ(1, args.size());
  *result = Value::Bool(!args[0].empty());
  return true;
}

bool ArrayConcatFunction::Eval(absl::Span<const Value> args,
                               EvaluationContext* context, Value* result,
                               absl::Status* status) const {
  DCHECK_LE(1, args.size());
  if (HasNulls(args)) {
    Value tracked_value = Value::Null(output_type());
    if (tracked_value.physical_byte_size() >
        context->options().max_value_byte_size) {
      *status = MakeMaxArrayValueByteSizeExceededError(
          context->options().max_value_byte_size, ZETASQL_LOC);
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
          context->options().max_value_byte_size, ZETASQL_LOC);
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

bool ArrayLengthFunction::Eval(absl::Span<const Value> args,
                               EvaluationContext* context, Value* result,
                               absl::Status* status) const {
  DCHECK_EQ(1, args.size());
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  *result = Value::Int64(args[0].num_elements());
  return true;
}

bool ArrayElementFunction::Eval(absl::Span<const Value> args,
                                EvaluationContext* context, Value* result,
                                absl::Status* status) const {
  DCHECK_EQ(2, args.size());
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

zetasql_base::StatusOr<Value> ArrayToStringFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  DCHECK_GE(args.size(), 2);
  DCHECK_LE(args.size(), 3);
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

zetasql_base::StatusOr<Value> ArrayReverseFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  DCHECK_EQ(args.size(), 1);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }

  MaybeSetNonDeterministicArrayOutput(args[0], context);

  std::vector<Value> elements = args[0].elements();
  std::reverse(elements.begin(), elements.end());
  return Value::Array(output_type()->AsArray(), elements);
}

bool IsFunction::Eval(absl::Span<const Value> args, EvaluationContext* context,
                      Value* result, absl::Status* status) const {
  DCHECK_EQ(1, args.size());
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

zetasql_base::StatusOr<Value> CastFunction::Eval(absl::Span<const Value> args,
                                         EvaluationContext* context) const {
  DCHECK_GE(args.size(), 1);
  const Value& v = args[0];
  const bool return_null_on_error =
      args.size() == 2 ? args[1].bool_value() : false;

  zetasql_base::StatusOr<Value> status_or =
      CastValue(v, context->GetDefaultTimeZone(), context->GetLanguageOptions(),
                output_type());
  if (!status_or.ok() && return_null_on_error) {
    return zetasql_base::StatusOr<Value>(Value::Null(output_type()));
  }
  if (HasFloatingPoint(v.type()) &&
      !HasFloatingPoint(output_type())) {
    context->SetNonDeterministicOutput();
  }
  return status_or;
}

bool BitCastFunction::Eval(absl::Span<const Value> args,
                           EvaluationContext* context, Value* result,
                           absl::Status* status) const {
  DCHECK_EQ(args.size(), 1);
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  const Value& val = args[0];
  switch (FCT(kind(), val.type_kind())) {
    case FCT(FunctionKind::kBitCastToInt32, TYPE_UINT32):
      return InvokeUnary<int32_t, uint32_t>(&functions::BitCast<uint32_t, int32_t>,
                                        args, result, status);
    case FCT(FunctionKind::kBitCastToInt64, TYPE_UINT64):
      return InvokeUnary<int64_t, uint64_t>(&functions::BitCast<uint64_t, int64_t>,
                                        args, result, status);
    case FCT(FunctionKind::kBitCastToUint32, TYPE_INT32):
      return InvokeUnary<uint32_t, int32_t>(&functions::BitCast<int32_t, uint32_t>,
                                        args, result, status);
    case FCT(FunctionKind::kBitCastToUint64, TYPE_INT64):
      return InvokeUnary<uint64_t, int64_t>(&functions::BitCast<int64_t, uint64_t>,
                                        args, result, status);
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

bool LogicalFunction::Eval(absl::Span<const Value> args,
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
      DCHECK(!(known_true && known_false));
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
      DCHECK(!(known_true && known_false));
      *result = known_true
                    ? Value::Bool(true)
                    : (known_false ? Value::Bool(false) : Value::NullBool());
      return true;
    }
    case FCT(FunctionKind::kNot, TYPE_BOOL): {
      DCHECK_EQ(1, args.size());
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
  static ::zetasql_base::StatusOr<std::unique_ptr<BuiltinAggregateAccumulator>> Create(
      const BuiltinAggregateFunction* function, const Type* input_type,
      absl::Span<const Value> args, EvaluationContext* context) {
    auto accumulator = absl::WrapUnique(
        new BuiltinAggregateAccumulator(function, input_type, args, context));
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

  ::zetasql_base::StatusOr<Value> GetFinalResult(bool inputs_in_defined_order) override;

 private:

  BuiltinAggregateAccumulator(const BuiltinAggregateFunction* function,
                              const Type* input_type,
                              absl::Span<const Value> args,
                              EvaluationContext* context)
      : function_(function),
        input_type_(input_type),
        args_(args.begin(), args.end()),
        context_(context) {}

  ::zetasql_base::StatusOr<Value> GetFinalResultInternal(bool inputs_in_defined_order);

  MemoryAccountant* accountant() { return context_->memory_accountant(); }

  const BuiltinAggregateFunction* function_;
  const Type* input_type_;
  const std::vector<Value> args_;
  EvaluationContext* context_;

  // The number of bytes currently requested from 'accountant()'.
  int64_t requested_bytes_ = 0;

  // AnyValue
  Value any_value_;
  // Count of non-null values.
  int64_t count_ = 0;
  int64_t countif_ = 0;
  double out_double_ = 0;              // Max, Min, Avg
  zetasql_base::ExactFloat out_exact_float_ = 0;     // Sum
  double avg_ = 0;                     // VarPop, VarSamp, StddevPop, StddevSamp
  double variance_ = 0;                // VarPop, VarSamp, StddevPop, StddevSamp
  int64_t out_int64_ = 0;                // Max, Min
  uint64_t out_uint64_ = 0;              // Max, Min
  DatetimeValue out_datetime_;         // Max, Min
  __int128 out_int128_ = 0;            // Sum
  unsigned __int128 out_uint128_ = 0;  // Sum
  NumericValue out_numeric_;           // Min, Max
  BigNumericValue out_bignumeric_;     // Min, Max
  NumericValue::SumAggregator numeric_aggregator_;  // Avg, Sum
  BigNumericValue::SumAggregator bignumeric_aggregator_;  // Avg, Sum
  NumericValue::VarianceAggregator numeric_variance_aggregator_;  // Var, Stddev
  std::string out_string_ = "";                  // Max, Min, StringAgg
  std::string delimiter_ = ",";                  // StringAgg
  // OrAgg, AndAgg, LogicalOr, LogicalAnd.
  bool has_null_ = false;
  bool has_true_ = false;
  bool has_false_ = false;
  // Bitwise aggregates.
  int32_t bit_int32_ = 0;
  int64_t bit_int64_ = 0;
  uint32_t bit_uint32_ = 0;
  uint64_t bit_uint64_ = 0;
  std::vector<Value> array_agg_;  // ArrayAgg and ArrayConcatAgg.
  // An output array for Min, Max.
  Value min_max_out_array_;
};

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
      out_double_ = -std::numeric_limits<double>::infinity();
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
    case FCT(FunctionKind::kMax, TYPE_STRING):
    case FCT(FunctionKind::kMax, TYPE_BYTES):
      out_string_.clear();
      break;
    case FCT(FunctionKind::kMax, TYPE_ARRAY):
      min_max_out_array_ = Value::Invalid();
      break;

      // Min
    case FCT(FunctionKind::kMin, TYPE_FLOAT):
    case FCT(FunctionKind::kMin, TYPE_DOUBLE):
      out_double_ = std::numeric_limits<double>::infinity();
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
    case FCT(FunctionKind::kMin, TYPE_STRING):
    case FCT(FunctionKind::kMin, TYPE_BYTES):
      out_string_.clear();
      break;
    case FCT(FunctionKind::kMin, TYPE_ARRAY):
      min_max_out_array_ = Value::Invalid();
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

    case FCT(FunctionKind::kVarPop, TYPE_DOUBLE):
    case FCT(FunctionKind::kVarSamp, TYPE_DOUBLE):
      avg_ = 0;
      variance_ = 0;
      break;
    case FCT(FunctionKind::kVarPop, TYPE_NUMERIC):
    case FCT(FunctionKind::kVarSamp, TYPE_NUMERIC):
      numeric_variance_aggregator_ = NumericValue::VarianceAggregator();
      break;
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
      double delta;
      if (!functions::Subtract(value.ToDouble(), out_double_, &delta, status) ||
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
    case FCT(FunctionKind::kVarPop, TYPE_DOUBLE):
    case FCT(FunctionKind::kVarSamp, TYPE_DOUBLE): {
      *status =
          UpdateMeanAndVariance(value.ToDouble(), count_, &avg_, &variance_);
      if (!status->ok()) return false;
      break;
    }
    // Variance and Stddev for NumericValue
    case FCT(FunctionKind::kVarPop, TYPE_NUMERIC):
    case FCT(FunctionKind::kVarSamp, TYPE_NUMERIC): {
      numeric_variance_aggregator_.Add(value.numeric_value());
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
        out_double_ = std::numeric_limits<double>::quiet_NaN();
      } else {
        out_double_ = std::max(out_double_, value.ToDouble());
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
    case FCT(FunctionKind::kMax, TYPE_UINT64): {
      out_uint64_ = std::max(out_uint64_, value.uint64_value());
      break;
    }
    case FCT(FunctionKind::kMax, TYPE_STRING): {
      bytes_to_return = out_string_.size();
      out_string_ = std::max(out_string_, value.string_value());
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

      // Min
    case FCT(FunctionKind::kMin, TYPE_FLOAT):
    case FCT(FunctionKind::kMin, TYPE_DOUBLE): {
      if (std::isnan(value.ToDouble()) || std::isnan(out_double_)) {
        out_double_ = std::numeric_limits<double>::quiet_NaN();
      } else {
        out_double_ = std::min(out_double_, value.ToDouble());
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
    case FCT(FunctionKind::kMin, TYPE_STRING): {
      bytes_to_return = out_string_.size();
      out_string_ = (count_ > 1) ? std::min(out_string_, value.string_value())
                                 : value.string_value();
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
  }

  accountant()->ReturnBytes(bytes_to_return);
  requested_bytes_ -= bytes_to_return;

  if (!accountant()->RequestBytes(additional_bytes_to_request, status)) {
    return false;
  }
  requested_bytes_ += additional_bytes_to_request;

  return true;
}

::zetasql_base::StatusOr<Value> BuiltinAggregateAccumulator::GetFinalResult(
    bool inputs_in_defined_order) {
  ZETASQL_ASSIGN_OR_RETURN(const Value result,
                   GetFinalResultInternal(inputs_in_defined_order));
  if (result.physical_byte_size() > context_->options().max_value_byte_size) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "Aggregate values are limited to "
           << context_->options().max_value_byte_size << " bytes";
  }
  return result;
}

::zetasql_base::StatusOr<Value> BuiltinAggregateAccumulator::GetFinalResultInternal(
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
    case FCT(FunctionKind::kVarPop, TYPE_DOUBLE):
      return count_ > 0 ? Value::Double(variance_) : Value::NullDouble();
    case FCT(FunctionKind::kVarSamp, TYPE_DOUBLE): {
      if (count_ <= 1) return Value::NullDouble();
      // var_samp = variance * count / (count - 1)
      double tmp;
      if (!functions::Divide(static_cast<double>(count_),
                             static_cast<double>(count_ - 1), &tmp, &error) ||
          !functions::Multiply(variance_, tmp, &variance_, &error)) {
        return error;
      }
      return Value::Double(variance_);
    }
    // Variance and Stddev for NumericValue
    case FCT(FunctionKind::kVarPop, TYPE_NUMERIC): {
      return CreateValueFromOptional(
          numeric_variance_aggregator_.GetPopulationVariance(count_));
    }
    case FCT(FunctionKind::kVarSamp, TYPE_NUMERIC): {
      return CreateValueFromOptional(
          numeric_variance_aggregator_.GetSamplingVariance(count_));
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
    case FCT(FunctionKind::kMax, TYPE_ENUM):
    case FCT(FunctionKind::kMin, TYPE_ENUM):
      return count_ > 0 ? Value::Enum(output_type->AsEnum(), out_int64_)
                        : Value::Null(output_type);

    case FCT(FunctionKind::kMax, TYPE_ARRAY):
    case FCT(FunctionKind::kMin, TYPE_ARRAY): {
      if (count_ > 0) {
        return min_max_out_array_;
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
  }
  return ::zetasql_base::UnimplementedErrorBuilder()
         << "Unsupported aggregate function: " << function_->debug_name() << "("
         << input_type_->DebugString() << ")";
}

}  // namespace

::zetasql_base::StatusOr<std::unique_ptr<AggregateAccumulator>>
BuiltinAggregateFunction::CreateAccumulator(absl::Span<const Value> args,
                                            EvaluationContext* context) const {
  return BuiltinAggregateAccumulator::Create(this, input_type(), args, context);
}

namespace {

// Accumulator implementation for BinaryStatFunction.
class BinaryStatAccumulator : public AggregateAccumulator {
 public:
  static zetasql_base::StatusOr<std::unique_ptr<BinaryStatAccumulator>> Create(
      const BinaryStatFunction* function, const Type* input_type,
      EvaluationContext* context) {
    auto accumulator = absl::WrapUnique(
        new BinaryStatAccumulator(function, input_type, context));
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

  ::zetasql_base::StatusOr<Value> GetFinalResult(bool inputs_in_defined_order) override;

 private:
  BinaryStatAccumulator(const BinaryStatFunction* function,
                        const Type* input_type, EvaluationContext* context)
      : function_(function),
        input_type_(input_type),
        min_required_pair_count_(
            function_->kind() == FunctionKind::kCovarPop ? 1 : 2),
        context_(context) {}

  const BinaryStatFunction* function_;
  const Type* input_type_;
  const int64_t min_required_pair_count_;
  EvaluationContext* context_;

  int64_t requested_bytes_ = 0;

  int64_t pair_count_ = 0;
  double mean_x_ = 0;
  double variance_x_ = 0;
  double mean_y_ = 0;
  double variance_y_ = 0;
  double covar_ = 0;
  bool input_has_nan_or_inf_ = false;
  NumericValue::CovarianceAggregator numeric_covariance_aggregator_;  // Covar
  NumericValue::CorrelationAggregator numeric_correlation_aggregator_;  // Corr
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
      break;
    case FCT2(FunctionKind::kCovarPop, TYPE_DOUBLE, TYPE_DOUBLE):
    case FCT2(FunctionKind::kCovarSamp, TYPE_DOUBLE, TYPE_DOUBLE):
      pair_count_ = 0;
      mean_x_ = 0;
      variance_x_ = 0;
      mean_y_ = 0;
      variance_y_ = 0;
      covar_ = 0;
      input_has_nan_or_inf_ = false;
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
    case FCT2(FunctionKind::kCovarPop, TYPE_DOUBLE, TYPE_DOUBLE):
    case FCT2(FunctionKind::kCovarSamp, TYPE_DOUBLE, TYPE_DOUBLE):
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
  }

  return status->ok();
}

::zetasql_base::StatusOr<Value> BinaryStatAccumulator::GetFinalResult(
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
    case FCT2(FunctionKind::kCovarPop, TYPE_DOUBLE, TYPE_DOUBLE):
      out_double = covar_;
      break;
    case FCT2(FunctionKind::kCovarSamp, TYPE_DOUBLE, TYPE_DOUBLE):
      // out_double = covar * pair_count / (pair_count - 1)
      if (!functions::Multiply(covar_, static_cast<double>(pair_count_),
                               &out_double, &error) ||
          !functions::Divide(out_double, static_cast<double>(pair_count_ - 1),
                             &out_double, &error)) {
        return error;
      }
      break;
    default: {
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported function: " << function_->debug_name() << "("
             << input_type_->DebugString() << ")";
    }
  }

  return Value::Double(out_double);
}

}  // namespace

::zetasql_base::StatusOr<std::unique_ptr<AggregateAccumulator>>
BinaryStatFunction::CreateAccumulator(absl::Span<const Value> args,
                                      EvaluationContext* context) const {
  return BinaryStatAccumulator::Create(this, input_type(), context);
}

zetasql_base::StatusOr<Value> LikeFunction::Eval(absl::Span<const Value> args,
                                         EvaluationContext* context) const {
  CHECK_EQ(2, args.size());
  if (HasNulls(args)) return Value::Null(output_type());
  const std::string& text = args[0].type_kind() == TYPE_STRING
                                ? args[0].string_value()
                                : args[0].bytes_value();

  if (regexp_ != nullptr) {
    // Regexp is precompiled
    return Value::Bool(RE2::FullMatch(text, *regexp_));
  } else {
    // Regexp is not precompiled, compile it on the fly
    const std::string& pattern = args[1].type_kind() == TYPE_STRING
                                     ? args[1].string_value()
                                     : args[1].bytes_value();
    std::unique_ptr<RE2> regexp;
    ZETASQL_RETURN_IF_ERROR(functions::CreateLikeRegexp(
        pattern, args[0].type_kind(),
        &regexp));
    return Value::Bool(RE2::FullMatch(text, *regexp));
  }
}

bool BitwiseFunction::Eval(absl::Span<const Value> args,
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
      return InvokeUnary<uint32_t>(&functions::BitwiseNot<uint32_t>, args, result,
                                 status);
    case FCT(FunctionKind::kBitwiseNot, TYPE_UINT64):
      return InvokeUnary<uint64_t>(&functions::BitwiseNot<uint64_t>, args, result,
                                 status);
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
      return InvokeBinary<uint32_t>(&functions::BitwiseOr<uint32_t>, args, result,
                                  status);
    case FCT(FunctionKind::kBitwiseOr, TYPE_UINT64):
      return InvokeBinary<uint64_t>(&functions::BitwiseOr<uint64_t>, args, result,
                                  status);
    case FCT(FunctionKind::kBitwiseOr, TYPE_BYTES):
      return InvokeBytes<std::string>(
          &functions::BitwiseBinaryOpBytes<std::bit_or>, result, status,
          args[0].bytes_value(), args[1].bytes_value());

    case FCT(FunctionKind::kBitwiseXor, TYPE_INT32):
      return InvokeBinary<int32_t>(&functions::BitwiseXor<int32_t>, args, result,
                                 status);
    case FCT(FunctionKind::kBitwiseXor, TYPE_INT64):
      return InvokeBinary<int64_t>(&functions::BitwiseXor<int64_t>, args, result,
                                 status);
    case FCT(FunctionKind::kBitwiseXor, TYPE_UINT32):
      return InvokeBinary<uint32_t>(&functions::BitwiseXor<uint32_t>, args, result,
                                  status);
    case FCT(FunctionKind::kBitwiseXor, TYPE_UINT64):
      return InvokeBinary<uint64_t>(&functions::BitwiseXor<uint64_t>, args, result,
                                  status);
    case FCT(FunctionKind::kBitwiseXor, TYPE_BYTES):
      return InvokeBytes<std::string>(
          &functions::BitwiseBinaryOpBytes<std::bit_xor>, result, status,
          args[0].bytes_value(), args[1].bytes_value());

    case FCT(FunctionKind::kBitwiseAnd, TYPE_INT32):
      return InvokeBinary<int32_t>(&functions::BitwiseAnd<int32_t>, args, result,
                                 status);
    case FCT(FunctionKind::kBitwiseAnd, TYPE_INT64):
      return InvokeBinary<int64_t>(&functions::BitwiseAnd<int64_t>, args, result,
                                 status);
    case FCT(FunctionKind::kBitwiseAnd, TYPE_UINT32):
      return InvokeBinary<uint32_t>(&functions::BitwiseAnd<uint32_t>, args, result,
                                  status);
    case FCT(FunctionKind::kBitwiseAnd, TYPE_UINT64):
      return InvokeBinary<uint64_t>(&functions::BitwiseAnd<uint64_t>, args, result,
                                  status);
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

bool BitCountFunction::Eval(absl::Span<const Value> args,
                            EvaluationContext* context, Value* result,
                            absl::Status* status) const {
  CHECK_EQ(1, args.size());
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

bool MathFunction::Eval(absl::Span<const Value> args,
                        EvaluationContext* context, Value* result,
                        absl::Status* status) const {
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  switch (FCT(kind(), output_type()->kind())) {
    case FCT(FunctionKind::kAbs, TYPE_INT32):
      return InvokeUnary<int32_t>(&functions::Abs<int32_t>, args, result, status);
    case FCT(FunctionKind::kAbs, TYPE_INT64):
      return InvokeUnary<int64_t>(&functions::Abs<int64_t>, args, result, status);
    case FCT(FunctionKind::kAbs, TYPE_UINT32):
      return InvokeUnary<uint32_t>(&functions::Abs<uint32_t>, args, result, status);
    case FCT(FunctionKind::kAbs, TYPE_UINT64):
      return InvokeUnary<uint64_t>(&functions::Abs<uint64_t>, args, result, status);
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
      return InvokeUnary<int32_t>(&functions::Sign<int32_t>, args, result, status);
    case FCT(FunctionKind::kSign, TYPE_INT64):
      return InvokeUnary<int64_t>(&functions::Sign<int64_t>, args, result, status);
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
    case FCT(FunctionKind::kPow, TYPE_DOUBLE):
      return InvokeBinary<double>(&functions::Pow<double>, args, result,
                                  status);
    case FCT(FunctionKind::kPow, TYPE_NUMERIC):
      return InvokeBinary<NumericValue>(&functions::Pow<NumericValue>, args,
                                        result, status);
    case FCT(FunctionKind::kExp, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::Exp<double>, args, result, status);
    case FCT(FunctionKind::kNaturalLogarithm, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::NaturalLogarithm<double>, args,
                                 result, status);
    case FCT(FunctionKind::kDecimalLogarithm, TYPE_DOUBLE):
      return InvokeUnary<double>(&functions::DecimalLogarithm<double>, args,
                                 result, status);
    case FCT(FunctionKind::kLogarithm, TYPE_DOUBLE):
      if (args.size() == 1) {
        return InvokeUnary<double>(&functions::NaturalLogarithm<double>, args,
                                   result, status);
      } else {
        return InvokeBinary<double>(&functions::Logarithm<double>, args, result,
                                    status);
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

    case FCT(FunctionKind::kRound, TYPE_DOUBLE):
      if (args.size() == 1) {
        return InvokeUnary<double>(&functions::Round<double>, args, result,
                                   status);
      } else {
        return InvokeBinary<double, double, int64_t>(
            &functions::RoundDecimal<double>, args, result, status);
      }
    case FCT(FunctionKind::kRound, TYPE_FLOAT):
      if (args.size() == 1) {
        return InvokeUnary<float>(&functions::Round<float>, args, result,
                                  status);
      } else {
        return InvokeBinary<float, float, int64_t>(
            &functions::RoundDecimal<float>, args, result, status);
      }
    case FCT(FunctionKind::kRound, TYPE_NUMERIC):
      if (args.size() == 1) {
        return InvokeUnary<NumericValue>(&functions::Round<NumericValue>, args,
                                         result, status);
      } else {
        return InvokeBinary<NumericValue, NumericValue, int64_t>(
            &functions::RoundDecimal<NumericValue>, args, result, status);
      }
    case FCT(FunctionKind::kRound, TYPE_BIGNUMERIC):
      if (args.size() == 1) {
        return InvokeUnary<BigNumericValue>(&functions::Round<BigNumericValue>,
                                            args, result, status);
      } else {
        return InvokeBinary<BigNumericValue, BigNumericValue, int64_t>(
            &functions::RoundDecimal<BigNumericValue>, args, result, status);
      }
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

bool StringFunction::Eval(absl::Span<const Value> args,
                          EvaluationContext* context, Value* result,
                          absl::Status* status) const {
  if (HasNulls(args)) {
    *result = Value::Null(output_type());
    return true;
  }
  switch (FCT_TYPE_ARITY(kind(), args[0].type_kind(), args.size())) {
    case FCT_TYPE_ARITY(FunctionKind::kStrpos, TYPE_STRING, 2):
      return Invoke<int64_t>(&functions::StrposUtf8, result, status,
                           args[0].string_value(), args[1].string_value());
    case FCT_TYPE_ARITY(FunctionKind::kStrpos, TYPE_BYTES, 2):
      return Invoke<int64_t>(&functions::StrposBytes, result, status,
                           args[0].bytes_value(), args[1].bytes_value());
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
  }
  *status = ::zetasql_base::UnimplementedErrorBuilder()
            << "Unsupported string function: " << debug_name();
  return false;
}

zetasql_base::StatusOr<Value> RegexpFunction::Eval(absl::Span<const Value> args,
                                           EvaluationContext* context) const {
  if (HasNulls(args)) return Value::Null(output_type());
  return func_(args, regexp_.get());
}

zetasql_base::StatusOr<Value> SplitFunction::Eval(absl::Span<const Value> args,
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

zetasql_base::StatusOr<Value> ConcatFunction::Eval(absl::Span<const Value> args,
                                           EvaluationContext* context) const {
  if (HasNulls(args)) return Value::Null(output_type());

  int64_t output_size = 0;
  if (output_type()->kind() == TYPE_STRING) {
    std::string result;
    for (const Value& in : args) {
      output_size += in.string_value().size();
      if (output_size > context->options().max_value_byte_size) {
        return ConcatError(context->options().max_value_byte_size, ZETASQL_LOC);
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
        return ConcatError(context->options().max_value_byte_size, ZETASQL_LOC);
      }
    }
    for (const Value& in : args) {
      result.append(in.bytes_value());
    }
    return Value::Bytes(result);
  }
}

zetasql_base::StatusOr<Value> CaseConverterFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  if (HasNulls(args)) return Value::Null(output_type());
  functions::Utf8CaseFunction case_function;
  std::string result;
  absl::Status error;
  switch (FCT(kind(), output_type()->kind())) {
    case FCT(FunctionKind::kUpper, TYPE_STRING):
      if (!case_function.Upper(args[0].string_value(), &result, &error)) {
        return error;
      } else {
        return Value::String(result);
      }
    case FCT(FunctionKind::kLower, TYPE_STRING):
      if (!case_function.Lower(args[0].string_value(), &result, &error)) {
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

zetasql_base::StatusOr<Value> MakeProtoFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  CHECK_EQ(args.size(), fields_.size());
  absl::Cord proto_cord;
  std::string bytes_str;
  {
    google::protobuf::io::StringOutputStream cord_output(&bytes_str);

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
  proto_cord = absl::Cord(bytes_str);
  return Value::Proto(output_type()->AsProto(), proto_cord);
}

// Sets the proto field denoted by <path> to <new_field_value>. The first proto
// field in <path> is looked up with regards to <parent_proto>.
static zetasql_base::StatusOr<Value> ReplaceProtoFields(
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
    }
    ZETASQL_RETURN_IF_ERROR(MergeValueToProtoField(
        new_field_value, path.back(), /*use_wire_format_annotations=*/false,
        &factory, message_to_modify));
  }

  return Value::Proto(parent_proto.type()->AsProto(),
                      absl::Cord(mutable_root_message->SerializeAsString()));
}

// Sets the field denoted by <path> to <new_field_value>. <path_index>
// indicates which Struct field in <path> should be extracted from
// <parent_struct>.
static zetasql_base::StatusOr<Value> ReplaceStructFields(
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

zetasql_base::StatusOr<Value> ReplaceFieldsFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
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

zetasql_base::StatusOr<Value> NullaryFunction::Eval(absl::Span<const Value> args,
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
    default:
      break;
  }
  return ::zetasql_base::UnimplementedErrorBuilder()
         << "Unsupported nullary function: " << debug_name();
}

zetasql_base::StatusOr<Value> DateTimeUnaryFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
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
                                      :  Value::Int32(args[0].date_value());
  }
  return ::zetasql_base::UnimplementedErrorBuilder()
         << "Unsupported function: " << debug_name();
}

zetasql_base::StatusOr<Value> FormatDateFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  DCHECK_EQ(args.size(), 2);
  if (HasNulls(args)) return Value::Null(output_type());
  std::string result_string;
  ZETASQL_RETURN_IF_ERROR(functions::FormatDateToString(
      args[0].string_value(), args[1].date_value(),
      &result_string));
  return Value::String(result_string);
}

zetasql_base::StatusOr<Value> FormatDatetimeFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  DCHECK_EQ(args.size(), 2);
  if (HasNulls(args)) return Value::Null(output_type());
  std::string result_string;
  ZETASQL_RETURN_IF_ERROR(functions::FormatDatetimeToString(
      args[0].string_value(), args[1].datetime_value(),
      &result_string));
  return Value::String(result_string);
}

zetasql_base::StatusOr<Value> FormatTimeFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  DCHECK_EQ(args.size(), 2);
  if (HasNulls(args)) return Value::Null(output_type());
  std::string result_string;
  ZETASQL_RETURN_IF_ERROR(functions::FormatTimeToString(
      args[0].string_value(), args[1].time_value(),
      &result_string));
  return Value::String(result_string);
}

zetasql_base::StatusOr<Value> FormatTimestampFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  if (args[0].is_null() || args[1].is_null() ||
      (args.size() == 3 && args[2].is_null())) {
    return Value::Null(output_type());
  }
  std::string result_string;
  if (args.size() == 2) {
    ZETASQL_RETURN_IF_ERROR(functions::FormatTimestampToString(
        args[0].string_value(), args[1].ToUnixMicros(),
        context->GetDefaultTimeZone(), &result_string));
  } else {
    ZETASQL_RETURN_IF_ERROR(functions::FormatTimestampToString(
        args[0].string_value(), args[1].ToUnixMicros(), args[2].string_value(),
        &result_string));
  }
  return Value::String(result_string);
}

zetasql_base::StatusOr<Value> TimestampConversionFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  if (HasNulls(args)) return Value::Null(output_type());
  if (!args.empty() && args[0].type()->IsDatetime()) {
    absl::Time timestamp;
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
    int64_t timestamp_micros;
    if (args.size() == 2 && args[1].type()->IsString()) {
      ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTimestamp(
          args[0].string_value(), args[1].string_value(),
          functions::kMicroseconds, false, &timestamp_micros));
    } else if (args.size() == 1) {
      ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTimestamp(
          args[0].string_value(), context->GetDefaultTimeZone(),
          functions::kMicroseconds, true, &timestamp_micros));
    } else {
      return MakeEvalError() << "Unsupported function: " << debug_name();
    }
    return Value::TimestampFromUnixMicros(timestamp_micros);
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

zetasql_base::StatusOr<Value> CivilTimeConstructionAndConversionFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  if (HasNulls(args)) return Value::Null(output_type());
  switch (kind()) {
    case FunctionKind::kDate: {
      int32_t date;
      if (args.size() == 3 && Int64Only(args, context)) {
        ZETASQL_RETURN_IF_ERROR(functions::ConstructDate(args[0].int64_value(),
                                                 args[1].int64_value(),
                                                 args[2].int64_value(),
                                                 &date));
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
      } else {
        return ::zetasql_base::UnimplementedErrorBuilder()
               << "Unsupported function: " << debug_name();
      }
      return Value::Date(date);
    } break;
    case FunctionKind::kTime:
      {
        TimeValue time;
        if (args.size() == 3 && Int64Only(args, context)) {
          ZETASQL_RETURN_IF_ERROR(functions::ConstructTime(args[0].int64_value(),
                                                   args[1].int64_value(),
                                                   args[2].int64_value(),
                                                   &time));
          return Value::Time(time);
        } else if (args.size() == 1 && args[0].type()->IsDatetime()) {
          ZETASQL_RETURN_IF_ERROR(functions::ExtractTimeFromDatetime(
              args[0].datetime_value(), &time));
        } else if (!args.empty() && args[0].type()->IsTimestamp()) {
          if (args.size() == 2 && args[1].type()->IsString()) {
            ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToTime(
                args[0].ToTime(), args[1].string_value(), &time));
          } else if (args.size() == 1) {
            ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToTime(
                args[0].ToTime(), context->GetDefaultTimeZone(), &time));
          } else {
            return MakeEvalError() << "Unsupported function: " << debug_name();
          }
        } else {
          ZETASQL_RET_CHECK_FAIL() << "Unexpected function call for " << debug_name();
        }
        return Value::Time(time);
      }
      break;
    case FunctionKind::kDatetime:
      {
        DatetimeValue datetime;
        if (args.size() == 6 && Int64Only(args, context)) {
          ZETASQL_RETURN_IF_ERROR(functions::ConstructDatetime(
              args[0].int64_value(),
              args[1].int64_value(),
              args[2].int64_value(),
              args[3].int64_value(),
              args[4].int64_value(),
              args[5].int64_value(),
              &datetime));
        } else if (args.size() == 2 && args[0].type()->IsDate() &&
                   args[1].type()->IsTime()) {
          ZETASQL_RETURN_IF_ERROR(functions::ConstructDatetime(
              args[0].date_value(), args[1].time_value(), &datetime));
        } else if (args.size() == 1 && args[0].type()->IsDate()) {
          ZETASQL_RETURN_IF_ERROR(functions::ConstructDatetime(args[0].date_value(),
                                                       TimeValue(), &datetime));
        } else if (!args.empty() && args[0].type()->IsTimestamp()) {
          if (args.size() == 2 && args[1].type()->IsString()) {
            ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToDatetime(
                args[0].ToTime(), args[1].string_value(), &datetime));
          } else if (args.size() == 1) {
            ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToDatetime(
                args[0].ToTime(), context->GetDefaultTimeZone(), &datetime));
          } else {
            return MakeEvalError() << "Unsupported function: " << debug_name();
          }
        } else {
          ZETASQL_RET_CHECK_FAIL() << "Unexpected function call for " << debug_name();
        }
        return Value::Datetime(datetime);
      }
      break;
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unexpected function kind";
  }
}

// This function converts INT64 to TIMESTAMP, and also support identity
// "conversion" from TIMESTAMP to TIMESTAMP.
zetasql_base::StatusOr<Value> TimestampFromIntFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
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

zetasql_base::StatusOr<Value> IntFromTimestampFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
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

zetasql_base::StatusOr<Value> StringFromTimestampFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  if (args[0].is_null() || (args.size() == 2 && args[1].is_null())) {
    return Value::Null(output_type());
  }
  int64_t timestamp;
  functions::TimestampScale scale;
  switch (args[0].type_kind()) {
    case TYPE_TIMESTAMP:
      timestamp = args[0].ToUnixMicros();
      scale = functions::kMicroseconds;
      break;
    default:
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "String function only supports TIMESTAMP types but type "
             << args[0].type()->DebugString() << " provided.";
  }

  std::string result_string;
  absl::TimeZone timezone;
  if (args.size() == 1) {
    timezone = context->GetDefaultTimeZone();
  } else {
    ZETASQL_RETURN_IF_ERROR(functions::MakeTimeZone(args[1].string_value(), &timezone));
  }
  if (args[0].type_kind() == TYPE_TIMESTAMP) {
    ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToStringWithTruncation(
          timestamp,
          scale,
          timezone,
          &result_string));
  } else {
    ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToStringWithoutTruncation(
          timestamp,
          scale,
          timezone,
          &result_string));
  }
  return Value::String(result_string);
}

zetasql_base::StatusOr<Value> ParseDateFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  DCHECK_EQ(args.size(), 2);
  if (HasNulls(args)) return Value::Null(output_type());
  int32_t date;
  ZETASQL_RETURN_IF_ERROR(functions::ParseStringToDate(
      args[0].string_value(), args[1].string_value(), &date));
  return Value::Date(date);
}

static functions::TimestampScale GetTimestampScale(
    const LanguageOptions& options) {
  if (options.LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
    return functions::TimestampScale::kNanoseconds;
  } else {
    return functions::TimestampScale::kMicroseconds;
  }
}

zetasql_base::StatusOr<Value> ParseDatetimeFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  DCHECK_EQ(args.size(), 2);
  if (HasNulls(args)) return Value::Null(output_type());
  DatetimeValue datetime;
  ZETASQL_RETURN_IF_ERROR(functions::ParseStringToDatetime(
      args[0].string_value(), args[1].string_value(),
      GetTimestampScale(context->GetLanguageOptions()), &datetime));
  return Value::Datetime(datetime);
}

zetasql_base::StatusOr<Value> ParseTimeFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  DCHECK_EQ(args.size(), 2);
  if (HasNulls(args)) return Value::Null(output_type());
  TimeValue time;
  ZETASQL_RETURN_IF_ERROR(functions::ParseStringToTime(
      args[0].string_value(), args[1].string_value(),
      GetTimestampScale(context->GetLanguageOptions()), &time));
  return Value::Time(time);
}

zetasql_base::StatusOr<Value> ParseTimestampFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  ZETASQL_RET_CHECK(args.size() == 2 || args.size() == 3);
  if (HasNulls(args)) return Value::Null(output_type());
  if (context->GetLanguageOptions().LanguageFeatureEnabled(
          FEATURE_TIMESTAMP_NANOS)) {
    absl::Time timestamp;
    if (args.size() == 2) {
      ZETASQL_RETURN_IF_ERROR(functions::ParseStringToTimestamp(
          args[0].string_value(), args[1].string_value(),
          context->GetDefaultTimeZone(), &timestamp));
    } else {
      ZETASQL_RETURN_IF_ERROR(functions::ParseStringToTimestamp(
          args[0].string_value(), args[1].string_value(),
          args[2].string_value(), &timestamp));
    }
    return Value::Timestamp(timestamp);
  } else {
    int64_t timestamp;
    if (args.size() == 2) {
      ZETASQL_RETURN_IF_ERROR(functions::ParseStringToTimestamp(
          args[0].string_value(), args[1].string_value(),
          context->GetDefaultTimeZone(), &timestamp));
    } else {
      ZETASQL_RETURN_IF_ERROR(functions::ParseStringToTimestamp(
          args[0].string_value(), args[1].string_value(),
          args[2].string_value(), &timestamp));
    }
    return Value::TimestampFromUnixMicros(timestamp);
  }
}

zetasql_base::StatusOr<Value> DateTruncFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  // The signature arguments are (<date>, <datepart>).
  ZETASQL_RET_CHECK_EQ(args.size(), 2);
  if (args[0].is_null() || args[1].is_null()) {
    return Value::Null(output_type());
  }
  functions::DateTimestampPart part =
      static_cast<functions::DateTimestampPart>(args[1].enum_value());
  int32_t date;
  ZETASQL_RETURN_IF_ERROR(functions::TruncateDate(args[0].date_value(), part, &date));
  return values::Date(date);
}

zetasql_base::StatusOr<Value> TimestampTruncFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  // The signature arguments are (<timestamp>, <datepart> [, <timezone>]).
  ZETASQL_RET_CHECK(args.size() == 2 || args.size() == 3);
  if (args[0].is_null() || args[1].is_null() ||
      (args.size() == 3 && args[2].is_null())) {
    return Value::Null(output_type());
  }
  functions::DateTimestampPart part =
      static_cast<functions::DateTimestampPart>(args[1].enum_value());
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

zetasql_base::StatusOr<Value> FromProtoFunction::Eval(
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
      int64_t timestamp;
      google::protobuf::Timestamp proto_timestamp;
      functions::TimestampScale scale =
          GetTimestampScale(context->GetLanguageOptions());
      proto_timestamp.CopyFrom(*message);
      ZETASQL_RETURN_IF_ERROR(functions::ConvertProto3TimestampToTimestamp(
          proto_timestamp, scale, &timestamp));
      return (scale == functions::TimestampScale::kNanoseconds)
                 ? Value::Timestamp(absl::FromUnixNanos(timestamp))
                 : Value::TimestampFromUnixMicros(timestamp);
      break;
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

zetasql_base::StatusOr<Value> ToProtoFunction::Eval(const absl::Span<const Value> args,
                                            EvaluationContext* context) const {
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

zetasql_base::StatusOr<Value> EnumValueDescriptorProtoFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
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

zetasql_base::StatusOr<Value> CivilTimeTruncFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  // The signature arguments are ([<datetime>|<time>], <datepart>)
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return Value::Null(output_type());
  }
  functions::DateTimestampPart part =
      static_cast<functions::DateTimestampPart>(args[1].enum_value());
  switch (kind()) {
    case FunctionKind::kDatetimeTrunc: {
      DatetimeValue datetime;
      ZETASQL_RETURN_IF_ERROR(functions::TruncateDatetime(args[0].datetime_value(),
                                                  part, &datetime));
      return Value::Datetime(datetime);
    }
    case FunctionKind::kTimeTrunc: {
      TimeValue time;
      ZETASQL_RETURN_IF_ERROR(
          functions::TruncateTime(args[0].time_value(), part, &time));
      return Value::Time(time);
    }
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported function: " << debug_name();
  }
}

zetasql_base::StatusOr<Value> DateTimeDiffFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
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
    case FCT(FunctionKind::kDatetimeAdd, TYPE_DATETIME): {
      DatetimeValue datetime;
      ZETASQL_RETURN_IF_ERROR(functions::AddDatetime(args[0].datetime_value(), part,
                                         args[1].int64_value(), &datetime));
      return Value::Datetime(datetime);
    }
    case FCT(FunctionKind::kDatetimeSub, TYPE_DATETIME): {
      DatetimeValue datetime;
      ZETASQL_RETURN_IF_ERROR(functions::SubDatetime(args[0].datetime_value(), part,
                                         args[1].int64_value(), &datetime));
      return Value::Datetime(datetime);
    }
    case FCT(FunctionKind::kDatetimeDiff, TYPE_DATETIME):
      ZETASQL_RETURN_IF_ERROR(functions::DiffDatetimes(
          args[0].datetime_value(), args[1].datetime_value(), part, &value64));
      return Value::Int64(value64);
    case FCT(FunctionKind::kDateDiff, TYPE_DATE):
      ZETASQL_RETURN_IF_ERROR(functions::DiffDates(
          args[0].date_value(), args[1].date_value(), part, &value32));
      return output_type()->IsInt64() ? Value::Int64(value32)
                                      : Value::Int32(value32);
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
    case FCT(FunctionKind::kTimestampAdd, TYPE_TIMESTAMP): {
      // We can hardcode the time zone to the default because it is only
      // used for error messaging.
      ZETASQL_RETURN_IF_ERROR(functions::AddTimestamp(
          args[0].ToUnixMicros(), functions::kMicroseconds,
          context->GetDefaultTimeZone(), part, args[1].int64_value(),
          &value64));
      return Value::TimestampFromUnixMicros(value64);
    }
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

zetasql_base::StatusOr<Value> ExtractFromFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
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
      ZETASQL_RETURN_IF_ERROR(functions::ExtractFromTime(
          part, args[0].time_value(), &value32));
      return output_type()->IsInt64() ? Value::Int64(value32)
                                      : Value::Int32(value32);
    default:
      {}
  }
  return ::zetasql_base::UnimplementedErrorBuilder()
         << "Unsupported function: " << debug_name();
}

zetasql_base::StatusOr<Value> ExtractDateFromFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
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

zetasql_base::StatusOr<Value> ExtractTimeFromFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
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

zetasql_base::StatusOr<Value> ExtractDatetimeFromFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
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

zetasql_base::StatusOr<Value> RandFunction::Eval(absl::Span<const Value> args,
                                         EvaluationContext* context) const {
  ZETASQL_RET_CHECK(args.empty());
  return Value::Double(
      absl::Uniform<double>(*context->GetRandomNumberGenerator(), 0, 1));
}

zetasql_base::StatusOr<Value> ErrorFunction::Eval(absl::Span<const Value> args,
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

bool UserDefinedScalarFunction::Eval(absl::Span<const Value> args,
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
    if (comparator(tuples[current_tuple_id], tuples[tuple_id]))  break;
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
  const int num_buckets = std::min<int64_t>(tuples.size(), bucket_count_argument);
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
  for (int bucket_id = 1;
       bucket_id <= num_buckets_with_one_more_tuple;
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
       bucket_id <= num_buckets;
       ++bucket_id) {
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
  const auto has_different_value =
      [&tuples, &values, current_tuple_id, excluded_tuple_id, ignore_nulls](
          int peer_tuple_id) {
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
      while (offset >= 0 &&
          values[window.start_tuple_id + offset].is_null()) {
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
        if (!values[window.start_tuple_id + offset].is_null()
            && ++num_non_nulls > n_value) {
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
  DCHECK(!arg_values.empty());
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
       &comparator, &current_output, &partition](int alternative_tuple_id) {
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

  // The optional arguments <offset> and <default expression> must have
  // been populated with default expressions by the algebrizer if they are not
  // specified.
  ZETASQL_RET_CHECK_EQ(2, args.size());
  const std::vector<Value>& values_arg = args[0];
  // First argument <value expression> must have been evaluated on each
  // input tuple.
  ZETASQL_RET_CHECK_EQ(tuples.size(), args[0].size());
  // Second argument <offset> must be a constant and of type double.
  ZETASQL_RET_CHECK_EQ(1, args[1].size());
  ZETASQL_RET_CHECK(args[1][0].type()->IsDouble());

  if (args[1][0].is_null()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "The second argument to the function PERCENTILE_CONT must not be"
              " null";
  }

  const double percentile = args[1][0].double_value();
  if (!(percentile >= 0 && percentile <= 1)) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "The second argument to the function PERCENTILE_CONT must be in"
              " [0, 1]; got "
           << percentile;
  }

  ZETASQL_ASSIGN_OR_RETURN(PercentileEvaluator percentile_evalutor,
                   PercentileEvaluator::Create(percentile));

  std::vector<double> normal_values;
  normal_values.reserve(values_arg.size());
  size_t num_nulls = 0;
  for (const Value& value_arg : values_arg) {
    ZETASQL_RET_CHECK(value_arg.type()->IsDouble());
    if (value_arg.is_null()) {
      ++num_nulls;
    } else {
      normal_values.push_back(value_arg.double_value());
    }
  }

  double result_value;
  bool result_is_not_null = percentile_evalutor.ComputePercentileCont<false>(
      normal_values.begin(), normal_values.end(),
      (ignore_nulls_ ? 0 : num_nulls), &result_value);
  result->resize(
      values_arg.size(),
      result_is_not_null ? Value::Double(result_value) : Value::NullDouble());
  return absl::OkStatus();
}

template <typename T, typename V = T, typename ValueCreationFn = Value(*)(T)>
Value ComputePercentileDisc(
    const PercentileEvaluator& percentile_evalutor,
    const std::vector<Value>& values_arg,
    const Type* type,
    V (Value::*extract_value_fn)() const /* e.g., &Value::double_value */,
    const ValueCreationFn& value_creation_fn /* e.g., &Value::Double */,
    bool ignore_nulls) {
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

  auto itr = percentile_evalutor.ComputePercentileDisc<T, false>(
      normal_values.begin(), normal_values.end(),
      (ignore_nulls ? 0 : num_nulls));
  return itr == normal_values.end() ?
      Value::Null(type) : value_creation_fn(*itr);
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

  // The optional arguments <offset> and <default expression> must have
  // been populated with default expressions by the algebrizer if they are not
  // specified.
  ZETASQL_RET_CHECK_EQ(2, args.size());
  const std::vector<Value>& values_arg = args[0];
  // First argument <value expression> must have been evaluated on each
  // input tuple.
  ZETASQL_RET_CHECK_EQ(tuples.size(), args[0].size());
  // Second argument <offset> must be a constant and of type double.
  ZETASQL_RET_CHECK_EQ(1, args[1].size());
  ZETASQL_RET_CHECK(args[1][0].type()->IsDouble());

  if (args[1][0].is_null()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "The second argument to the function PERCENTILE_CONT must not be"
              " null";
  }

  const double percentile = args[1][0].double_value();
  if (!(percentile >= 0 && percentile <= 1)) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "The second argument to the function PERCENTILE_CONT must be in"
              " [0, 1]; got "
           << percentile;
  }

  ZETASQL_ASSIGN_OR_RETURN(PercentileEvaluator percentile_evalutor,
                   PercentileEvaluator::Create(percentile));

  Value output_value;
  const Type* type = output_type();
  switch (output_type()->kind()) {
    case TYPE_INT64:
      output_value = ComputePercentileDisc<int64_t>(
          percentile_evalutor, values_arg, type, &Value::int64_value,
          &Value::Int64, ignore_nulls_);
      break;
    case TYPE_INT32:
      output_value = ComputePercentileDisc<int32_t>(
          percentile_evalutor, values_arg, type, &Value::int32_value,
          &Value::Int32, ignore_nulls_);
      break;
    case TYPE_UINT64:
      output_value = ComputePercentileDisc<uint64_t>(
          percentile_evalutor, values_arg, type, &Value::uint64_value,
          &Value::Uint64, ignore_nulls_);
      break;
    case TYPE_UINT32:
      output_value = ComputePercentileDisc<uint32_t>(
          percentile_evalutor, values_arg, type, &Value::uint32_value,
          &Value::Uint32, ignore_nulls_);
      break;
    case TYPE_DOUBLE:
      output_value = ComputePercentileDisc<double>(
          percentile_evalutor, values_arg, type, &Value::double_value,
          &Value::Double, ignore_nulls_);
      break;
    case TYPE_FLOAT:
      output_value = ComputePercentileDisc<float>(
          percentile_evalutor, values_arg, type, &Value::float_value,
          &Value::Float, ignore_nulls_);
      break;
    case TYPE_NUMERIC:
      output_value = ComputePercentileDisc<NumericValue>(
          percentile_evalutor, values_arg, type, &Value::numeric_value,
          &Value::Numeric, ignore_nulls_);
      break;
    case TYPE_BIGNUMERIC:
      output_value = ComputePercentileDisc<BigNumericValue>(
          percentile_evalutor, values_arg, type, &Value::bignumeric_value,
          &Value::BigNumeric, ignore_nulls_);
      break;
    case TYPE_BYTES:
      output_value =
          ComputePercentileDisc<absl::string_view, const std::string&>(
              percentile_evalutor, values_arg, type, &Value::bytes_value,
              &Value::Bytes, ignore_nulls_);
      break;
    case TYPE_STRING:
      output_value =
          ComputePercentileDisc<absl::string_view, const std::string&>(
              percentile_evalutor, values_arg, type, &Value::string_value,
              &Value::String, ignore_nulls_);
      break;
    case TYPE_BOOL:
      output_value = ComputePercentileDisc<bool>(
          percentile_evalutor, values_arg, type, &Value::bool_value,
          &Value::Bool, ignore_nulls_);
      break;
    case TYPE_DATE:
      output_value = ComputePercentileDisc<int32_t>(
          percentile_evalutor, values_arg, type, &Value::date_value,
          &Value::Date, ignore_nulls_);
      break;
    case TYPE_DATETIME:
      output_value = ComputePercentileDisc<int64_t>(
          percentile_evalutor, values_arg, type,
          &Value::ToPacked64DatetimeMicros, &Value::DatetimeFromPacked64Micros,
          ignore_nulls_);
      break;
    case TYPE_TIME:
      output_value = ComputePercentileDisc<int64_t>(
          percentile_evalutor, values_arg, type, &Value::ToPacked64TimeMicros,
          &Value::TimeFromPacked64Micros, ignore_nulls_);
      break;
    case TYPE_TIMESTAMP:
      output_value = ComputePercentileDisc<int64_t>(
          percentile_evalutor, values_arg, type, &Value::ToUnixMicros,
          &Value::TimestampFromUnixMicros, ignore_nulls_);
      break;
    case TYPE_ENUM:
      output_value =
          ComputePercentileDisc<int32_t, int32_t, std::function<Value(int32_t)>>(
              percentile_evalutor, values_arg, type,
              &Value::enum_value,
              [type](int32_t value) -> Value {
                return Value::Enum(type->AsEnum(), value);
              },
              ignore_nulls_);
      break;
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported argument type for percentile_disc.";
  }
  result->resize(values_arg.size(), output_value);
  return absl::OkStatus();
}

}  // namespace zetasql
