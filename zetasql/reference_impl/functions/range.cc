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

#include "zetasql/reference_impl/function.h"
#include "absl/types/span.h"

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
}

}  // namespace zetasql
