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

// Builtin functions and methods used in the reference implementation.

#ifndef ZETASQL_REFERENCE_IMPL_FUNCTION_H_
#define ZETASQL_REFERENCE_IMPL_FUNCTION_H_

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/proto/type_annotation.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/reference_impl/tuple_comparator.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include <cstdint>
#include "absl/random/random.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "re2/re2.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

enum class FunctionKind {
  // Arithmetic functions
  kAdd,
  kSubtract,
  kMultiply,
  kDivide,
  kDiv,
  kSafeAdd,
  kSafeSubtract,
  kSafeMultiply,
  kSafeDivide,
  kMod,
  kUnaryMinus,
  kSafeNegate,
  // Comparison functions
  kEqual,
  kLess,
  kLessOrEqual,
  // Logical functions
  kAnd,
  kNot,
  kOr,
  // Aggregate functions
  kAndAgg,  // private function that ANDs all input values incl. NULLs
  kAnyValue,
  kApproxCountDistinct,
  kApproxTopSum,
  kArrayAgg,
  kArrayConcatAgg,
  kAvg,
  kCount,
  kCountIf,
  kCorr,
  kCovarPop,
  kCovarSamp,
  kLogicalAnd,
  kLogicalOr,
  kMax,
  kMin,
  kOrAgg,       // private function that ORs all input values incl. NULLs
  kStringAgg,
  kSum,
  kVarPop,
  kVarSamp,
  // Exists function
  kExists,
  // IsNull function
  kIsNull,
  kIsTrue,
  kIsFalse,
  // Cast function
  kCast,
  // Least and greatest functions
  kLeast,
  kGreatest,
  // Array functions
  // Note: All array functions *must* set the EvaluationContext to have
  // non-deterministic output if the output depends on the order of an input
  // array that is not order-preserving. See MaybeSetNonDeterministicArrayOutput
  // in the .cc file, and b/32308061 for an example of how this can cause test
  // failures.
  kArrayConcat,
  kArrayLength,
  kArrayToString,
  kArrayReverse,
  kArrayAtOrdinal,
  kArrayAtOffset,
  kSafeArrayAtOrdinal,
  kSafeArrayAtOffset,
  kMakeProto,
  // Date/Time functions
  kDateAdd,
  kDateSub,
  kDateDiff,
  kDateTrunc,
  kDatetimeAdd,
  kDatetimeSub,
  kDatetimeDiff,
  kDatetimeTrunc,
  kTimeAdd,
  kTimeSub,
  kTimeDiff,
  kTimeTrunc,
  kTimestampAdd,
  kTimestampSub,
  kTimestampDiff,
  kTimestampTrunc,
  kCurrentDate,
  kCurrentDatetime,
  kCurrentTime,
  kCurrentTimestamp,
  kDateFromUnixDate,
  kUnixDate,
  kExtractFrom,
  kExtractDateFrom,
  kExtractTimeFrom,
  kExtractDatetimeFrom,
  kFormatDate,
  kFormatDatetime,
  kFormatTime,
  kFormatTimestamp,
  kDate,
  kTimestamp,
  kTime,
  kDatetime,
  // Conversion functions
  kTimestampSeconds,
  kTimestampMillis,
  kTimestampMicros,
  kTimestampFromUnixSeconds,
  kTimestampFromUnixMillis,
  kTimestampFromUnixMicros,
  kSecondsFromTimestamp,
  kMillisFromTimestamp,
  kMicrosFromTimestamp,
  kStringFromTimestamp,
  // Numbering functions
  kDenseRank,
  kRank,
  kRowNumber,
  kPercentRank,
  kCumeDist,
  kNtile,
  // Navigation functions
  kFirstValue,
  kLastValue,
  kNthValue,
  kLead,
  kLag,
  kPercentileCont,
  kPercentileDisc,

  // Random functions
  kRand,

  // Error function
  kError,
};

// Provides two utility methods to look up a built-in function name or function
// kind.
class BuiltinFunctionCatalog {
 public:
  BuiltinFunctionCatalog(const BuiltinFunctionCatalog&) = delete;
  BuiltinFunctionCatalog& operator=(const BuiltinFunctionCatalog&) = delete;

  static zetasql_base::StatusOr<FunctionKind> GetKindByName(
      const absl::string_view& name);

  static std::string GetDebugNameByKind(FunctionKind kind);

 private:
  BuiltinFunctionCatalog() {}
};

// Abstract built-in scalar function.
class BuiltinScalarFunction : public ScalarFunctionBody {
 public:
  BuiltinScalarFunction(const BuiltinScalarFunction&) = delete;
  BuiltinScalarFunction& operator=(const BuiltinScalarFunction&) = delete;

  BuiltinScalarFunction(FunctionKind kind, const Type* output_type)
      : ScalarFunctionBody(output_type),
        kind_(kind) {}

  ~BuiltinScalarFunction() override {}

  FunctionKind kind() const { return kind_; }

  std::string debug_name() const override;

  // Returns true if any of the input values is null.
  static bool HasNulls(absl::Span<const Value> args);

  // Validates the input types according to the language options, and returns a
  // ScalarFunctionCallExpr upon success.
  static zetasql_base::StatusOr<std::unique_ptr<ScalarFunctionCallExpr>> CreateCall(
      FunctionKind kind, const LanguageOptions& language_options,
      const Type* output_type,
      const std::vector<std::unique_ptr<ValueExpr>> arguments,
      ResolvedFunctionCallBase::ErrorMode error_mode =
          ResolvedFunctionCallBase::DEFAULT_ERROR_MODE);

  // If 'arguments' is not empty, validates the types of the inputs. Currently
  // it checks whether the inputs support equality comparison where
  // applicable, and whether civil time types are enabled in the language option
  // if there is any in the input types.
  static zetasql_base::StatusOr<std::unique_ptr<BuiltinScalarFunction>> CreateValidated(
      FunctionKind kind, const LanguageOptions& language_options,
      const Type* output_type,
      const std::vector<std::unique_ptr<ValueExpr>>& arguments);

 private:
  // Like CreateValidated(), but returns a raw pointer with ownership.
  static zetasql_base::StatusOr<BuiltinScalarFunction*> CreateValidatedRaw(
      FunctionKind kind, const LanguageOptions& language_options,
      const Type* output_type,
      const std::vector<std::unique_ptr<ValueExpr>>& arguments);

  // Makes it easier to write test cases known to have valid input parameters.
  static std::unique_ptr<BuiltinScalarFunction> CreateUnvalidated(
      FunctionKind kind, const Type* output_type);

  // Creates a like function.
  static zetasql_base::StatusOr<std::unique_ptr<BuiltinScalarFunction>>
  CreateLikeFunction(FunctionKind kind, const Type* output_type,
                     const std::vector<std::unique_ptr<ValueExpr>>& arguments);

  // Creates a regexp function.
  static zetasql_base::StatusOr<std::unique_ptr<BuiltinScalarFunction>>
  CreateRegexpFunction(
      FunctionKind kind, const Type* output_type,
      const std::vector<std::unique_ptr<ValueExpr>>& arguments);

  FunctionKind kind_;
};

// Alternate form of BuiltinScalarFunction that is easier to implement for
// functions that are slow enough that return ::zetasql_base::StatusOr<Value> from
// Eval() doesn't really matter.
class SimpleBuiltinScalarFunction : public BuiltinScalarFunction {
 public:
  using BuiltinScalarFunction::BuiltinScalarFunction;

  virtual ::zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                                       EvaluationContext* context) const = 0;

  bool Eval(absl::Span<const Value> args, EvaluationContext* context,
            Value* result, ::zetasql_base::Status* status) const override {
    auto status_or_value = Eval(args, context);
    if (!status_or_value.ok()) {
      *status = status_or_value.status();
      return false;
    }
    *result = std::move(status_or_value.ValueOrDie());
    return true;
  }
};

// Abstract built-in aggregate function.
class BuiltinAggregateFunction : public AggregateFunctionBody {
 public:
  BuiltinAggregateFunction(FunctionKind kind, const Type* output_type,
                           int num_input_fields, const Type* input_type,
                           bool ignores_null = true)
      : AggregateFunctionBody(output_type, num_input_fields, input_type,
                              ignores_null),
        kind_(kind) {}

  BuiltinAggregateFunction(const BuiltinAggregateFunction&) = delete;
  BuiltinAggregateFunction& operator=(const BuiltinAggregateFunction&) = delete;

  FunctionKind kind() const { return kind_; }

  std::string debug_name() const override;

  ::zetasql_base::StatusOr<std::unique_ptr<AggregateAccumulator>> CreateAccumulator(
      absl::Span<const Value> args, EvaluationContext* context) const override;

 private:
  const FunctionKind kind_;
};

class BinaryStatFunction : public BuiltinAggregateFunction {
 public:
  BinaryStatFunction(FunctionKind kind, const Type* output_type,
                     const Type* input_type)
      : BuiltinAggregateFunction(kind, output_type, /*num_input_fields=*/2,
                                 input_type, /*ignores_null=*/true) {}

  BinaryStatFunction(const BinaryStatFunction&) = delete;
  BinaryStatFunction& operator=(const BinaryStatFunction&) = delete;

  ::zetasql_base::StatusOr<std::unique_ptr<AggregateAccumulator>> CreateAccumulator(
      absl::Span<const Value> args, EvaluationContext* context) const override;
};

class UserDefinedScalarFunction : public ScalarFunctionBody {
 public:
  UserDefinedScalarFunction(const FunctionEvaluator& evaluator,
                            const Type* output_type,
                            const std::string& function_name)
      : ScalarFunctionBody(output_type),
        evaluator_(evaluator),
        function_name_(function_name) {}
  std::string debug_name() const override;
  bool Eval(absl::Span<const Value> args, EvaluationContext* context,
            Value* result, ::zetasql_base::Status* status) const override;

 private:
  FunctionEvaluator evaluator_;
  const std::string function_name_;
};

// Abstract built-in (non-aggregate) analytic function.
class BuiltinAnalyticFunction : public AnalyticFunctionBody {
 public:
  BuiltinAnalyticFunction(FunctionKind kind, const Type* output_type)
      : AnalyticFunctionBody(output_type),
        kind_(kind) {}

  BuiltinAnalyticFunction(const BuiltinAnalyticFunction&) = delete;
  BuiltinAnalyticFunction& operator=(const BuiltinAnalyticFunction&) = delete;

  FunctionKind kind() const { return kind_; }

  std::string debug_name() const override;

 private:
  FunctionKind kind_;
};

class ArithmeticFunction : public BuiltinScalarFunction {
 public:
  using BuiltinScalarFunction::BuiltinScalarFunction;
  bool Eval(absl::Span<const Value> args, EvaluationContext* context,
            Value* result, ::zetasql_base::Status* status) const override;
};

class ComparisonFunction : public BuiltinScalarFunction {
 public:
  using BuiltinScalarFunction::BuiltinScalarFunction;
  bool Eval(absl::Span<const Value> args, EvaluationContext* context,
            Value* result, ::zetasql_base::Status* status) const override;
};

class LogicalFunction : public BuiltinScalarFunction {
 public:
  using BuiltinScalarFunction::BuiltinScalarFunction;
  bool Eval(absl::Span<const Value> args, EvaluationContext* context,
            Value* result, ::zetasql_base::Status* status) const override;
};

class ExistsFunction : public BuiltinScalarFunction {
 public:
  using BuiltinScalarFunction::BuiltinScalarFunction;
  bool Eval(absl::Span<const Value> args, EvaluationContext* context,
            Value* result, ::zetasql_base::Status* status) const override;
};

class ArrayLengthFunction : public BuiltinScalarFunction {
 public:
  using BuiltinScalarFunction::BuiltinScalarFunction;
  bool Eval(absl::Span<const Value> args, EvaluationContext* context,
            Value* result, ::zetasql_base::Status* status) const override;
};

class ArrayConcatFunction : public BuiltinScalarFunction {
 public:
  using BuiltinScalarFunction::BuiltinScalarFunction;
  bool Eval(absl::Span<const Value> args, EvaluationContext* context,
            Value* result, ::zetasql_base::Status* status) const override;
};

class ArrayToStringFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  ::zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                               EvaluationContext* context) const override;
};

class ArrayReverseFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit ArrayReverseFunction(const Type* output_type)
      : SimpleBuiltinScalarFunction(FunctionKind::kArrayReverse, output_type) {}
  ::zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                               EvaluationContext* context) const override;
};

class IsFunction : public BuiltinScalarFunction {
 public:
  using BuiltinScalarFunction::BuiltinScalarFunction;
  bool Eval(absl::Span<const Value> args, EvaluationContext* context,
            Value* result, ::zetasql_base::Status* status) const override;
};

class CastFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  ::zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                               EvaluationContext* context) const override;
};

class ArrayElementFunction : public BuiltinScalarFunction {
 public:
  ArrayElementFunction(int base, bool safe, const Type* output_type)
      : BuiltinScalarFunction(
            safe ? (base == 0 ? FunctionKind::kSafeArrayAtOffset
                              : FunctionKind::kSafeArrayAtOrdinal)
                 : (base == 0 ? FunctionKind::kArrayAtOffset
                              : FunctionKind::kArrayAtOrdinal),
            output_type),
        base_(base),
        safe_(safe) {
    CHECK(base_ == 0 || base_ == 1) << base_;
  }
  bool Eval(absl::Span<const Value> args, EvaluationContext* context,
            Value* result, ::zetasql_base::Status* status) const override;

 protected:
  // This function supports both 0 based offset and 1 based ordinals, the value
  // of base_ can be either 0 or 1.
  int base_;
  // Safe accesses will return NULL rather than raising an error on an out-of-
  // bounds position.
  const bool safe_;
};

class LeastFunction : public BuiltinScalarFunction {
 public:
  explicit LeastFunction(const Type* output_type)
      : BuiltinScalarFunction(FunctionKind::kLeast, output_type) {}
  bool Eval(absl::Span<const Value> args, EvaluationContext* context,
            Value* result, ::zetasql_base::Status* status) const override;
};

class GreatestFunction : public BuiltinScalarFunction {
 public:
  explicit GreatestFunction(const Type* output_type)
      : BuiltinScalarFunction(FunctionKind::kGreatest,
                              output_type) {}
  bool Eval(absl::Span<const Value> args, EvaluationContext* context,
            Value* result, ::zetasql_base::Status* status) const override;
};

class ConcatFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

// The field descriptors passed to the constructor must outlive the function
// object. The output ProtoType has a pointer to the google::protobuf::Descriptor that
// owns the field descriptors, i.e., their life span is tied to that of
// 'output_type'.
class MakeProtoFunction : public SimpleBuiltinScalarFunction {
 public:
  typedef std::pair<const google::protobuf::FieldDescriptor*, FieldFormat::Format>
      FieldAndFormat;

  MakeProtoFunction(const ProtoType* output_type,
                    const std::vector<FieldAndFormat>& fields)
      : SimpleBuiltinScalarFunction(FunctionKind::kMakeProto, output_type),
        fields_(fields) {}
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;

 private:
  std::vector<FieldAndFormat> fields_;  // Not owned.
};

class NullaryFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class DateTimeUnaryFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class FormatDateFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class FormatDatetimeFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class FormatTimeFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class FormatTimestampFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class TimestampFromIntFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class IntFromTimestampFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class StringFromTimestampFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class DateTimeDiffFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class CivilTimeTruncFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class DateTruncFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class TimestampTruncFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class ExtractFromFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class TimestampConversionFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class CivilTimeConstructionAndConversionFunction
    : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class ExtractDateFromFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class ExtractTimeFromFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class ExtractDatetimeFromFunction : public SimpleBuiltinScalarFunction {
 public:
  using SimpleBuiltinScalarFunction::SimpleBuiltinScalarFunction;
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class RandFunction : public SimpleBuiltinScalarFunction {
 public:
  RandFunction()
      : SimpleBuiltinScalarFunction(FunctionKind::kRand, types::DoubleType()) {}
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;

 protected:
  mutable absl::BitGen rand_;
};

class ErrorFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit ErrorFunction(const Type* output_type)
      : SimpleBuiltinScalarFunction(FunctionKind::kError, output_type) {}
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

// Returns the ordinal (1-based) rank of each row. All rows in the same ordering
// group are peers and receive the same rank value, and the subsequent rank
// value is incremented by 1.
class DenseRankFunction : public BuiltinAnalyticFunction {
 public:
  DenseRankFunction()
      : BuiltinAnalyticFunction(FunctionKind::kDenseRank, types::Int64Type()) {
  }

  bool RequireTupleComparator() const override {
    return true;
  }

  zetasql_base::Status Eval(const TupleSchema& schema,
                    const absl::Span<const TupleData* const>& tuples,
                    const absl::Span<const std::vector<Value>>& args,
                    const absl::Span<const AnalyticWindow>& windows,
                    const TupleComparator* comparator,
                    ResolvedFunctionCallBase::ErrorMode error_mode,
                    EvaluationContext* context,
                    std::vector<Value>* result) const override;
};

// Returns the ordinal (1-based) rank of each row. All rows in the same ordering
// group are peers and receive the same rank value, and the subsequent rank
// value is offset by the number of peers.
class RankFunction : public BuiltinAnalyticFunction {
 public:
  RankFunction()
      : BuiltinAnalyticFunction(FunctionKind::kRank, types::Int64Type()) {
  }

  bool RequireTupleComparator() const override {
    return true;
  }

  zetasql_base::Status Eval(const TupleSchema& schema,
                    const absl::Span<const TupleData* const>& tuples,
                    const absl::Span<const std::vector<Value>>& args,
                    const absl::Span<const AnalyticWindow>& windows,
                    const TupleComparator* comparator,
                    ResolvedFunctionCallBase::ErrorMode error_mode,
                    EvaluationContext* context,
                    std::vector<Value>* result) const override;
};

// Returns the sequential row ordinal (1-based) of each row.
class RowNumberFunction : public BuiltinAnalyticFunction {
 public:
  RowNumberFunction()
      : BuiltinAnalyticFunction(FunctionKind::kRowNumber, types::Int64Type()) {
  }

  bool RequireTupleComparator() const override {
    return false;
  }

  zetasql_base::Status Eval(const TupleSchema& schema,
                    const absl::Span<const TupleData* const>& tuples,
                    const absl::Span<const std::vector<Value>>& args,
                    const absl::Span<const AnalyticWindow>& windows,
                    const TupleComparator* comparator,
                    ResolvedFunctionCallBase::ErrorMode error_mode,
                    EvaluationContext* context,
                    std::vector<Value>* result) const override;
};

// Return the percentile rank of a row defined as (RK-1)/(NR-1), where RK is
// the RANK of the row and NR is the number of rows in the partition.
// If NR=1, returns 0.
class PercentRankFunction : public BuiltinAnalyticFunction {
 public:
  PercentRankFunction()
      : BuiltinAnalyticFunction(FunctionKind::kPercentRank,
                                types::DoubleType()) {
  }

  bool RequireTupleComparator() const override {
    return true;
  }

  zetasql_base::Status Eval(const TupleSchema& schema,
                    const absl::Span<const TupleData* const>& tuples,
                    const absl::Span<const std::vector<Value>>& args,
                    const absl::Span<const AnalyticWindow>& windows,
                    const TupleComparator* comparator,
                    ResolvedFunctionCallBase::ErrorMode error_mode,
                    EvaluationContext* context,
                    std::vector<Value>* result) const override;
};

// Returns the relative rank of a row defined as NP/NR, where NP is defined to
// be the number of rows preceding or peer with the current row in the window
// ordering of the partition and NR is the number of rows in the partition.
class CumeDistFunction : public BuiltinAnalyticFunction {
 public:
  CumeDistFunction()
      : BuiltinAnalyticFunction(FunctionKind::kCumeDist, types::DoubleType()) {
  }

  bool RequireTupleComparator() const override {
    return true;
  }

  zetasql_base::Status Eval(const TupleSchema& schema,
                    const absl::Span<const TupleData* const>& tuples,
                    const absl::Span<const std::vector<Value>>& args,
                    const absl::Span<const AnalyticWindow>& windows,
                    const TupleComparator* comparator,
                    ResolvedFunctionCallBase::ErrorMode error_mode,
                    EvaluationContext* context,
                    std::vector<Value>* result) const override;
};

// NTILE(<constant integer expression>)
// Divides the rows into  <constant integer expression> buckets based on row
// ordering and returns the 1-based bucket number that is assigned to each row.
// The number of rows in the buckets can differ by at most 1. The remainder
// values (the remainder of number of rows divided by buckets) are distributed
// one for each bucket, starting with bucket 1.
class NtileFunction : public BuiltinAnalyticFunction {
 public:
  NtileFunction()
      : BuiltinAnalyticFunction(FunctionKind::kNtile, types::Int64Type()) {
  }

  bool RequireTupleComparator() const override { return true; }

  zetasql_base::Status Eval(const TupleSchema& schema,
                    const absl::Span<const TupleData* const>& tuples,
                    const absl::Span<const std::vector<Value>>& args,
                    const absl::Span<const AnalyticWindow>& windows,
                    const TupleComparator* comparator,
                    ResolvedFunctionCallBase::ErrorMode error_mode,
                    EvaluationContext* context,
                    std::vector<Value>* result) const override;

 private:
  // Returns true if tuples in 'tuples' that are peers with the tuple at
  // 'key_tuple_id' via 'comparator' are not equal.
  //
  // Requires that 'tuples' are ordered by 'comparator'. 'key_tuple_id' is
  // an index for 'tuples'.
  static bool OrderingPeersAreNotEqual(
      const TupleSchema& schema, int key_tuple_id,
      absl::Span<const TupleData* const> tuples,
      const TupleComparator& comparator);
};

// FIRST_VALUE(<value expression>)
// Returns the value of the <value expression> for the first row in the
// window frame.
class FirstValueFunction : public BuiltinAnalyticFunction {
 public:
  explicit FirstValueFunction(
      const Type* output_type,
      ResolvedAnalyticFunctionCall::NullHandlingModifier null_handling_modifier)
      : BuiltinAnalyticFunction(FunctionKind::kFirstValue, output_type),
        ignore_nulls_(null_handling_modifier ==
            ResolvedAnalyticFunctionCall::IGNORE_NULLS) {}

  bool RequireTupleComparator() const override { return true; }

  zetasql_base::Status Eval(const TupleSchema& schema,
                    const absl::Span<const TupleData* const>& tuples,
                    const absl::Span<const std::vector<Value>>& args,
                    const absl::Span<const AnalyticWindow>& windows,
                    const TupleComparator* comparator,
                    ResolvedFunctionCallBase::ErrorMode error_mode,
                    EvaluationContext* context,
                    std::vector<Value>* result) const override;

 private:
  const bool ignore_nulls_;
};

// LAST_VALUE(<value expression>)
// Returns the value of the <value expression> for the last row in the
// window frame.
class LastValueFunction : public BuiltinAnalyticFunction {
 public:
  explicit LastValueFunction(
      const Type* output_type,
      ResolvedAnalyticFunctionCall::NullHandlingModifier null_handling_modifier)
      : BuiltinAnalyticFunction(FunctionKind::kLastValue, output_type),
        ignore_nulls_(null_handling_modifier ==
            ResolvedAnalyticFunctionCall::IGNORE_NULLS) {}

  bool RequireTupleComparator() const override { return true; }

  zetasql_base::Status Eval(const TupleSchema& schema,
                    const absl::Span<const TupleData* const>& tuples,
                    const absl::Span<const std::vector<Value>>& args,
                    const absl::Span<const AnalyticWindow>& windows,
                    const TupleComparator* comparator,
                    ResolvedFunctionCallBase::ErrorMode error_mode,
                    EvaluationContext* context,
                    std::vector<Value>* result) const override;

 private:
  const bool ignore_nulls_;
};

// NTH_VALUE(<value expression>, <constant integer expression>)
// Returns the value of <value expression> at the Nth row of the window frame,
// where Nth is defined by the <constant integer expression>.
class NthValueFunction : public BuiltinAnalyticFunction {
 public:
  explicit NthValueFunction(
      const Type* output_type,
      ResolvedAnalyticFunctionCall::NullHandlingModifier null_handling_modifier)
      : BuiltinAnalyticFunction(FunctionKind::kNthValue, output_type),
        ignore_nulls_(null_handling_modifier ==
            ResolvedAnalyticFunctionCall::IGNORE_NULLS) {}

  bool RequireTupleComparator() const override { return true; }

  zetasql_base::Status Eval(const TupleSchema& schema,
                    const absl::Span<const TupleData* const>& tuples,
                    const absl::Span<const std::vector<Value>>& args,
                    const absl::Span<const AnalyticWindow>& windows,
                    const TupleComparator* comparator,
                    ResolvedFunctionCallBase::ErrorMode error_mode,
                    EvaluationContext* context,
                    std::vector<Value>* result) const override;

 private:
  const bool ignore_nulls_;
};

// LEAD(<value expression>, <offset>, <default expression>)
// Returns the value of the <value expression> on a row that is <offset> number
// of rows after the current row r. The value of <default expression> is
// returned as the result if there is no row corresponding to the <offset>
// number of rows after the current row.  At <offset> 0, <value expression> is
// computed on the current row. If <offset> is null or negative, an error is
// produced.
class LeadFunction : public BuiltinAnalyticFunction {
 public:
  explicit LeadFunction(const Type* output_type)
      : BuiltinAnalyticFunction(FunctionKind::kLead, output_type) {}

  bool RequireTupleComparator() const override { return true; }

  zetasql_base::Status Eval(const TupleSchema& schema,
                    const absl::Span<const TupleData* const>& tuples,
                    const absl::Span<const std::vector<Value>>& args,
                    const absl::Span<const AnalyticWindow>& windows,
                    const TupleComparator* comparator,
                    ResolvedFunctionCallBase::ErrorMode error_mode,
                    EvaluationContext* context,
                    std::vector<Value>* result) const override;
};

// LAG(<value expression>, <offset>, <default expression>)
// Returns the value of the <value expression> on a row that is <offset> number
// of rows before the current row r. The value of <default expression> is
// returned as the result if there is no row corresponding to the <offset>
// number of rows before the current row.  At <offset> 0, <value expression> is
// computed on the current row. If <offset> is null or negative, an error is
// produced.
class LagFunction : public BuiltinAnalyticFunction {
 public:
  explicit LagFunction(const Type* output_type)
      : BuiltinAnalyticFunction(FunctionKind::kLead, output_type) {}

  bool RequireTupleComparator() const override { return true; }

  zetasql_base::Status Eval(const TupleSchema& schema,
                    const absl::Span<const TupleData* const>& tuples,
                    const absl::Span<const std::vector<Value>>& args,
                    const absl::Span<const AnalyticWindow>& windows,
                    const TupleComparator* comparator,
                    ResolvedFunctionCallBase::ErrorMode error_mode,
                    EvaluationContext* context,
                    std::vector<Value>* result) const override;
};

// PERCENTILE_CONT(<value expression>, <percentile>)
// Returns the percentile from <value expression> at given <percentile>, with
// possible linear interpolation. When <percentile> is 0, returns the min value;
// when <percentile> is 1, returns the max value; when <percentile> is 0.5,
// returns the median.
class PercentileContFunction : public BuiltinAnalyticFunction {
 public:
  PercentileContFunction(
      const Type* output_type,
      ResolvedAnalyticFunctionCall::NullHandlingModifier null_handling_modifier)
      : BuiltinAnalyticFunction(FunctionKind::kPercentileCont, output_type),
        ignore_nulls_(null_handling_modifier !=
          ResolvedAnalyticFunctionCall::RESPECT_NULLS) {}

  bool RequireTupleComparator() const override { return false; }

  zetasql_base::Status Eval(const TupleSchema& schema,
                    const absl::Span<const TupleData* const>& tuples,
                    const absl::Span<const std::vector<Value>>& args,
                    const absl::Span<const AnalyticWindow>& windows,
                    const TupleComparator* comparator,
                    ResolvedFunctionCallBase::ErrorMode error_mode,
                    EvaluationContext* context,
                    std::vector<Value>* result) const override;

 private:
  bool ignore_nulls_;
};

// PERCENTILE_DISC(<value expression>, <percentile>)
// Returns the discrete percentile from <value expression>.
class PercentileDiscFunction : public BuiltinAnalyticFunction {
 public:
  PercentileDiscFunction(
      const Type* output_type,
      ResolvedAnalyticFunctionCall::NullHandlingModifier null_handling_modifier)
      : BuiltinAnalyticFunction(FunctionKind::kPercentileDisc, output_type),
        ignore_nulls_(null_handling_modifier !=
          ResolvedAnalyticFunctionCall::RESPECT_NULLS) {}

  bool RequireTupleComparator() const override { return false; }

  zetasql_base::Status Eval(const TupleSchema& schema,
                    const absl::Span<const TupleData* const>& tuples,
                    const absl::Span<const std::vector<Value>>& args,
                    const absl::Span<const AnalyticWindow>& windows,
                    const TupleComparator* comparator,
                    ResolvedFunctionCallBase::ErrorMode error_mode,
                    EvaluationContext* context,
                    std::vector<Value>* result) const override;

 private:
  bool ignore_nulls_;
};


}  // namespace zetasql

#endif  // ZETASQL_REFERENCE_IMPL_FUNCTION_H_
