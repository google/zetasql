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

#include "zetasql/reference_impl/functions/range.h"

#include <optional>
#include <utility>
#include <vector>

#include "zetasql/public/functions/range.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/function.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

// RANGE's start and end being null are treated as -inf and +inf, respectively
bool DoTwoRangesOverlap(const Value& range1, const Value& range2) {
  const bool is_range1_start_before_range2_end =
      range1.start().is_null() || range2.end().is_null() ||
      range1.start().LessThan(range2.end());
  const bool is_range2_start_before_range1_end =
      range2.start().is_null() || range1.end().is_null() ||
      range2.start().LessThan(range1.end());
  return is_range1_start_before_range2_end && is_range2_start_before_range1_end;
}

// start being null/unbounded represents -inf
bool IsStartLessThan(const Value& s1, const Value& s2) {
  return s1.is_null() ? true : s2.is_null() ? false : s1.LessThan(s2);
}

// end being null/unbounded represents +inf
bool IsEndLessThan(const Value& e1, const Value& e2) {
  return e1.is_null() ? false : e2.is_null() ? true : e1.LessThan(e2);
}

class RangeFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit RangeFunction(const Type* output_type)
      : SimpleBuiltinScalarFunction(FunctionKind::kRangeCtor, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

absl::StatusOr<Value> RangeFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);
  ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
  ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[1], context));
  ZETASQL_ASSIGN_OR_RETURN(Value range_value, Value::MakeRange(args[0], args[1]));
  return range_value;
}

class RangeIsStartUnboundedFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit RangeIsStartUnboundedFunction(const Type* output_type)
      : SimpleBuiltinScalarFunction(FunctionKind::kRangeIsStartUnbounded,
                                    output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

absl::StatusOr<Value> RangeIsStartUnboundedFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
  return args[0].is_null() ? Value::NullBool()
                           : Value::Bool(args[0].start().is_null());
}

class RangeIsEndUnboundedFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit RangeIsEndUnboundedFunction(const Type* output_type)
      : SimpleBuiltinScalarFunction(FunctionKind::kRangeIsEndUnbounded,
                                    output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

absl::StatusOr<Value> RangeIsEndUnboundedFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
  return args[0].is_null() ? Value::NullBool()
                           : Value::Bool(args[0].end().is_null());
}

class RangeStartFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit RangeStartFunction(const Type* output_type)
      : SimpleBuiltinScalarFunction(FunctionKind::kRangeStart, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

absl::StatusOr<Value> RangeStartFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
  return args[0].is_null()
             ? Value::Null(args[0].type()->AsRange()->element_type())
             : args[0].start();
}

class RangeEndFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit RangeEndFunction(const Type* output_type)
      : SimpleBuiltinScalarFunction(FunctionKind::kRangeEnd, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

absl::StatusOr<Value> RangeEndFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
  return args[0].is_null()
             ? Value::Null(args[0].type()->AsRange()->element_type())
             : args[0].end();
}

class RangeOverlapsFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit RangeOverlapsFunction(const Type* output_type)
      : SimpleBuiltinScalarFunction(FunctionKind::kRangeOverlaps, output_type) {
  }
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

absl::StatusOr<Value> RangeOverlapsFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);

  if (HasNulls(args)) {
    return Value::NullBool();
  }
  ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
  ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[1], context));
  return Value::Bool(DoTwoRangesOverlap(args[0], args[1]));
}

class RangeIntersectFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit RangeIntersectFunction(const Type* output_type)
      : SimpleBuiltinScalarFunction(FunctionKind::kRangeIntersect,
                                    output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

absl::StatusOr<Value> RangeIntersectFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);

  if (HasNulls(args)) {
    return Value::Null(types::RangeTypeFromSimpleTypeKind(
        args[0].type()->AsRange()->element_type()->kind()));
  }
  ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
  ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[1], context));
  if (!DoTwoRangesOverlap(args[0], args[1])) {
    return MakeEvalError()
           << "Provided RANGE inputs: " << args[0] << " and " << args[1]
           << " do not overlap. "
           << "Please check RANGE_OVERLAPS before calling RANGE_INTERSECT";
  }

  // range intersection: [max(s1, s2), min(e1, e2))
  const Value intersect_start =
      IsStartLessThan(args[0].start(), args[1].start()) ? args[1].start()
                                                        : args[0].start();
  const Value intersect_end = IsEndLessThan(args[0].end(), args[1].end())
                                  ? args[0].end()
                                  : args[1].end();
  return Value::MakeRange(intersect_start, intersect_end);
}

class GenerateRangeArrayFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit GenerateRangeArrayFunction(const Type* output_type)
      : SimpleBuiltinScalarFunction(FunctionKind::kGenerateRangeArray,
                                    output_type) {}

  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;

  absl::StatusOr<std::vector<Value>> EvalForTimestampElement(
      const Value& range, const IntervalValue& step, bool last_partial_range,
      EvaluationContext* context) const;
};

absl::StatusOr<Value> GenerateRangeArrayFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_DCHECK_GE(args.size(), 2);
  ZETASQL_DCHECK_LE(args.size(), 3);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }

  ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));

  ZETASQL_DCHECK(args[0].type()->IsRangeType());
  if (args[0].type()->AsRange()->element_type()->kind() !=
      TypeKind::TYPE_TIMESTAMP) {
    return ::zetasql_base::UnimplementedErrorBuilder()
           << "Unsupported argument type for generate_range_array.";
  }
  ZETASQL_ASSIGN_OR_RETURN(std::vector<Value> result,
                   EvalForTimestampElement(args[0], args[1].interval_value(),
                                           args[2].bool_value(), context));
  return Value::MakeArray(output_type()->AsArray(), std::move(result));
}

absl::StatusOr<std::vector<Value>>
GenerateRangeArrayFunction::EvalForTimestampElement(
    const Value& range, const IntervalValue& step, bool last_partial_range,
    EvaluationContext* context) const {
  ZETASQL_ASSIGN_OR_RETURN(functions::TimestampRangeArrayGenerator generator,
                   functions::TimestampRangeArrayGenerator::Create(
                       step, last_partial_range,
                       GetTimestampScale(context->GetLanguageOptions())));
  std::optional<absl::Time> range_start =
      range.start().is_null() ? std::nullopt
                              : std::make_optional(range.start().ToTime());
  std::optional<absl::Time> range_end =
      range.end().is_null() ? std::nullopt
                            : std::make_optional(range.end().ToTime());
  std::vector<Value> result;
  int64_t bytes_so_far = 0;
  ZETASQL_RETURN_IF_ERROR(generator.Generate(
      range_start, range_end,
      [&result, &bytes_so_far, context](absl::Time start,
                                        absl::Time end) -> absl::Status {
        ZETASQL_ASSIGN_OR_RETURN(Value range, Value::MakeRange(Value::Timestamp(start),
                                                       Value::Timestamp(end)));
        bytes_so_far += range.physical_byte_size();
        if (bytes_so_far > context->options().max_value_byte_size) {
          return MakeMaxArrayValueByteSizeExceededError(
              context->options().max_value_byte_size, ZETASQL_LOC);
        }
        result.push_back(range);
        return absl::OkStatus();
      }));
  return result;
}

}  // namespace

void RegisterBuiltinRangeFunctions() {
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kRangeCtor},
      [](FunctionKind kind, const Type* output_type) {
        return new RangeFunction(output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kRangeIsStartUnbounded},
      [](FunctionKind kind, const Type* output_type) {
        return new RangeIsStartUnboundedFunction(output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kRangeIsEndUnbounded},
      [](FunctionKind kind, const Type* output_type) {
        return new RangeIsEndUnboundedFunction(output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kRangeStart},
      [](FunctionKind kind, const Type* output_type) {
        return new RangeStartFunction(output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kRangeEnd},
      [](FunctionKind kind, const Type* output_type) {
        return new RangeEndFunction(output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kRangeOverlaps},
      [](FunctionKind kind, const Type* output_type) {
        return new RangeOverlapsFunction(output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kRangeIntersect},
      [](FunctionKind kind, const Type* output_type) {
        return new RangeIntersectFunction(output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kGenerateRangeArray},
      [](FunctionKind kind, const Type* output_type) {
        return new GenerateRangeArrayFunction(output_type);
      });
}

}  // namespace zetasql
