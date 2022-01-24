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

// This file contains the code for evaluating aggregate functions.

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/common.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/function.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/reference_impl/tuple_comparator.h"
#include "zetasql/reference_impl/variable_id.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include <cstdint>
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

using zetasql::types::EmptyStructType;
using zetasql::values::Bool;

namespace zetasql {

// -------------------------------------------------------
// AggregateArg
// -------------------------------------------------------

absl::StatusOr<std::unique_ptr<AggregateArg>> AggregateArg::Create(
    const VariableId& variable,
    std::unique_ptr<const AggregateFunctionBody> function,
    std::vector<std::unique_ptr<ValueExpr>> arguments, Distinctness distinct,
    std::unique_ptr<ValueExpr> having_expr,
    const HavingModifierKind having_modifier_kind,
    std::vector<std::unique_ptr<KeyArg>> order_by_keys,
    std::unique_ptr<ValueExpr> limit,
    std::unique_ptr<RelationalOp> group_rows_subquery,
    ResolvedFunctionCallBase::ErrorMode error_mode,
    std::unique_ptr<ValueExpr> filter,
    const std::vector<ResolvedCollation>& collation_list) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<AggregateFunctionCallExpr> aggregate_expr,
                   AggregateFunctionCallExpr::Create(std::move(function),
                                                     std::move(arguments)));
  return absl::WrapUnique(new AggregateArg(
      variable, std::move(aggregate_expr), distinct, std::move(having_expr),
      having_modifier_kind, std::move(order_by_keys), std::move(limit),
      std::move(group_rows_subquery), error_mode, std::move(filter),
      collation_list));
}

absl::Status AggregateArg::SetSchemasForEvaluation(
    const TupleSchema& group_schema,
    absl::Span<const TupleSchema* const> params_schemas) {
  std::vector<const TupleSchema*> params_and_group_schemas =
      ConcatSpans(params_schemas, {&group_schema});
  std::unique_ptr<TupleSchema> group_rows_schema;
  if (group_rows_subquery_ != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        group_rows_subquery_->SetSchemasForEvaluation(params_schemas));
    group_rows_schema = group_rows_subquery_->CreateOutputSchema();
    params_and_group_schemas =
        ConcatSpans(params_schemas, {group_rows_schema.get()});
  }

  if (having_modifier_kind() != AggregateArg::kHavingNone &&
      having_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(mutable_having_expr()->SetSchemasForEvaluation(
        params_and_group_schemas));
  }

  if (filter() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        mutable_filter()->SetSchemasForEvaluation(params_and_group_schemas));
  }

  for (int i = 0; i < input_field_list_size(); ++i) {
    ZETASQL_RETURN_IF_ERROR(mutable_input_field(i)->SetSchemasForEvaluation(
        params_and_group_schemas));
  }

  if (limit() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(mutable_limit()->SetSchemasForEvaluation(params_schemas));
  }

  for (int i = 0; i < parameter_list_size(); ++i) {
    ZETASQL_RETURN_IF_ERROR(
        mutable_parameter(i)->SetSchemasForEvaluation(params_schemas));
  }
  group_schema_ =
      absl::make_unique<const TupleSchema>(group_schema.variables());
  return absl::OkStatus();
}

namespace {

// Variant of the aggregator accumulator interfaces that is able to look at both
// an input row and its corresponding Value. The Value corresponding to an input
// row contains everything that the aggregate function aggregates. If that
// involves more than one thing, it is a struct with the appropriate number of
// fields. (The AggregateFunctionBody knows what type to expect because it knows
// the number of fields involved in the aggregation.) The usage is essentially
// the same as AggregateAccumulator,
class IntermediateAggregateAccumulator {
 public:
  virtual ~IntermediateAggregateAccumulator() {}

  virtual absl::Status Reset() = 0;

  virtual bool Accumulate(const TupleData& input_row, const Value& input_value,
                          bool* stop_accumulation, absl::Status* status) = 0;

  virtual absl::StatusOr<Value> GetFinalResult(
      bool inputs_in_defined_order) = 0;
};

// Adapts AggregateAccumulator to IntermediateAggregateAccumulator.
class AggregateAccumulatorAdaptor : public IntermediateAggregateAccumulator {
 public:
  AggregateAccumulatorAdaptor(const Type* output_type,
                              ResolvedFunctionCallBase::ErrorMode error_mode,
                              std::unique_ptr<AggregateAccumulator> accumulator)
      : output_type_(output_type),
        error_mode_(error_mode),
        accumulator_(std::move(accumulator)) {}

  AggregateAccumulatorAdaptor(const AggregateAccumulatorAdaptor&) = delete;
  AggregateAccumulatorAdaptor& operator=(const AggregateAccumulatorAdaptor&) =
      delete;

  absl::Status Reset() override { return accumulator_->Reset(); }

  bool Accumulate(const TupleData& input_row, const Value& input_value,
                  bool* stop_accumulation, absl::Status* status) override {
    absl::Status error;
    if (!accumulator_->Accumulate(input_value, stop_accumulation, &error)) {
      if (ShouldSuppressError(error, error_mode_)) {
        safe_result_ = Value::Null(output_type_);
        *stop_accumulation = true;
        return true;
      }
      *status = error;
      return false;
    }
    return true;
  }

  absl::StatusOr<Value> GetFinalResult(bool inputs_in_defined_order) override {
    if (safe_result_.is_valid()) return safe_result_;

    const absl::StatusOr<Value> status_or_value =
        accumulator_->GetFinalResult(inputs_in_defined_order);
    if (!status_or_value.ok()) {
      const absl::Status& error = status_or_value.status();
      if (ShouldSuppressError(error, error_mode_)) {
        return Value::Null(output_type_);
      }
      return error;
    }
    return status_or_value.value();
  }

 private:
  const Type* output_type_;
  const ResolvedFunctionCallBase::ErrorMode error_mode_;
  std::unique_ptr<AggregateAccumulator> accumulator_;
  Value safe_result_;
};

// Accumulator that enforces LIMITs on the list of input rows to aggregate.
class LimitAccumulator : public IntermediateAggregateAccumulator {
 public:
  LimitAccumulator(
      int64_t limit,
      std::unique_ptr<IntermediateAggregateAccumulator> accumulator)
      : limit_(limit), accumulator_(std::move(accumulator)) {}

  absl::Status Reset() override {
    num_rows_accumulated_ = 0;
    return accumulator_->Reset();
  }

  LimitAccumulator(const LimitAccumulator&) = delete;
  LimitAccumulator& operator=(const LimitAccumulator&) = delete;

  bool Accumulate(const TupleData& input_row, const Value& value,
                  bool* stop_accumulation, absl::Status* status) override {
    // Handles the LIMIT 0 case.
    if (num_rows_accumulated_ >= limit_) {
      *stop_accumulation = true;
      return true;
    }

    if (!accumulator_->Accumulate(input_row, value, stop_accumulation,
                                  status)) {
      return false;
    }
    ++num_rows_accumulated_;

    if (num_rows_accumulated_ >= limit_) {
      *stop_accumulation = true;
      return true;
    }
    return true;
  }

  absl::StatusOr<Value> GetFinalResult(bool inputs_in_defined_order) override {
    return accumulator_->GetFinalResult(inputs_in_defined_order);
  }

 private:
  const int64_t limit_;
  std::unique_ptr<IntermediateAggregateAccumulator> accumulator_;

  int64_t num_rows_accumulated_ = 0;
};

// Accumulator that orders the list of input rows to aggregate.
class OrderByAccumulator : public IntermediateAggregateAccumulator {
 public:
  OrderByAccumulator(
      absl::Span<const KeyArg* const> keys,
      absl::Span<const int> slots_for_keys,
      absl::Span<const int> slots_for_values,
      absl::Span<const TupleData* const> params,
      std::unique_ptr<IntermediateAggregateAccumulator> accumulator,
      EvaluationContext* context)
      : keys_(keys.begin(), keys.end()),
        slots_for_keys_(slots_for_keys.begin(), slots_for_keys.end()),
        slots_for_values_(slots_for_values.begin(), slots_for_values.end()),
        params_(params.begin(), params.end()),
        accumulator_(std::move(accumulator)),
        context_(context),
        inputs_(context_->memory_accountant()) {}

  OrderByAccumulator(const OrderByAccumulator&) = delete;
  OrderByAccumulator& operator=(const OrderByAccumulator&) = delete;

  absl::Status Reset() override {
    inputs_.Clear();
    return absl::OkStatus();
  }

  bool Accumulate(const TupleData& input_row, const Value& value,
                  bool* stop_accumulation, absl::Status* status) override {
    *stop_accumulation = false;

    auto input = absl::make_unique<TupleData>(input_row);
    input->AddSlots(1);
    input->mutable_slot(input->num_slots() - 1)->SetValue(value);
    return inputs_.PushBack(std::move(input), status);
  }

  absl::StatusOr<Value> GetFinalResult(
      bool /* inputs_in_defined_order */) override {
    ZETASQL_ASSIGN_OR_RETURN(
        auto tuple_comparator,
        TupleComparator::Create(keys_, slots_for_keys_, params_, context_));
    inputs_.Sort(*tuple_comparator, context_->options().always_use_stable_sort);

    const bool inputs_in_defined_order = tuple_comparator->IsUniquelyOrdered(
        inputs_.GetTuplePtrs(), slots_for_values_);

    ZETASQL_RETURN_IF_ERROR(accumulator_->Reset());

    bool stop_accumulation;
    absl::Status status;
    while (!inputs_.IsEmpty()) {
      std::unique_ptr<TupleData> input_row = inputs_.PopFront();
      ZETASQL_RET_CHECK(!input_row->slots().empty());
      const Value value = input_row->slots().back().value();
      input_row->RemoveSlots(1);

      if (!accumulator_->Accumulate(*input_row, value, &stop_accumulation,
                                    &status)) {
        return status;
      }
      if (stop_accumulation) break;
    }
    return accumulator_->GetFinalResult(inputs_in_defined_order);
  }

 private:
  const std::vector<const KeyArg*> keys_;
  const std::vector<int> slots_for_keys_;
  const std::vector<int> slots_for_values_;
  const std::vector<const TupleData*> params_;
  std::unique_ptr<IntermediateAggregateAccumulator> accumulator_;
  EvaluationContext* context_;
  // The last slot of each TupleData here is the Value passed to the
  // corresponding call to Accumulate().
  TupleDataDeque inputs_;
};

// Accumulator that keeps the top N values and accumulates them in
// order. Functionally equivalent to LimitAccumulator(OrderByAccumulator) but
// uses less memory.
class TopNAccumulator : public IntermediateAggregateAccumulator {
 public:
  TopNAccumulator(const int64_t n,
                  std::unique_ptr<TupleComparator> tuple_comparator,
                  std::unique_ptr<IntermediateAggregateAccumulator> accumulator,
                  EvaluationContext* context)
      : n_(n),
        tuple_comparator_(std::move(tuple_comparator)),
        top_n_(*tuple_comparator_, context->memory_accountant()),
        accumulator_(std::move(accumulator)) {}

  absl::Status Reset() override {
    top_n_.Clear();
    return absl::OkStatus();
  }

  TopNAccumulator(const TopNAccumulator&) = delete;
  TopNAccumulator& operator=(const TopNAccumulator&) = delete;

  bool Accumulate(const TupleData& input_row, const Value& value,
                  bool* stop_accumulation, absl::Status* status) override {
    *stop_accumulation = false;

    auto input = absl::make_unique<TupleData>(input_row);
    input->AddSlots(1);
    input->mutable_slot(input->num_slots() - 1)->SetValue(value);

    if (!top_n_.Insert(std::move(input), status)) return false;

    // 'if' should be good enough; 'while' is for paranoia.
    while (top_n_.GetSize() > n_) {
      top_n_.PopBack();
    }

    return true;
  }

  absl::StatusOr<Value> GetFinalResult(
      bool /* inputs_in_defined_order */) override {
    bool stop_accumulation;
    absl::Status status;
    while (!top_n_.IsEmpty()) {
      std::unique_ptr<TupleData> input_row = top_n_.PopFront();
      ZETASQL_RET_CHECK(!input_row->slots().empty());
      const Value value = input_row->slots().back().value();
      input_row->RemoveSlots(1);

      if (!accumulator_->Accumulate(*input_row, value, &stop_accumulation,
                                    &status)) {
        return status;
      }
      if (stop_accumulation) break;
    }

    // The value of 'inputs_in_defined_order' does not matter because this class
    // is never used for compliance or random query testing, as it is an
    // optimization. If that changes, passing true here will cause us to be too
    // aggressive in declaring test failures (which is better than silently
    // dropping results).
    return accumulator_->GetFinalResult(/*inputs_in_defined_order=*/true);
  }

 private:
  const int64_t n_;
  const std::unique_ptr<TupleComparator> tuple_comparator_;
  // The last slot of each TupleData in this queue is the Value passed to the
  // corresponding call to Accumulate().
  TupleDataOrderedQueue top_n_;
  std::unique_ptr<IntermediateAggregateAccumulator> accumulator_;
};

namespace {

// TODO: Extend to support Array and Struct later.
// Returns Bytes value which represents the sort key for input <value> with
// given <collator>. If the input value is null, NullBytes() is returned.
absl::StatusOr<Value> GetValueSortKey(const Value& value,
                                      const ZetaSqlCollator& collator) {
  ZETASQL_RET_CHECK(value.type()->IsString())
      << "Cannot get sort key for value in non-String type: "
      << value.type()->DebugString();

  if (value.is_null()) {
    return values::NullBytes();
  }
  absl::Cord sort_key;
  ZETASQL_RETURN_IF_ERROR(collator.GetSortKeyUtf8(value.string_value(), &sort_key));
  return values::Bytes(sort_key);
}

}  // namespace

// Accumulator that only passes through distinct values.
class DistinctAccumulator : public IntermediateAggregateAccumulator {
 public:
  DistinctAccumulator(
      std::unique_ptr<IntermediateAggregateAccumulator> accumulator,
      EvaluationContext* context,
      std::unique_ptr<const ZetaSqlCollator> collator)
      : distinct_values_(context->memory_accountant()),
        accumulator_(std::move(accumulator)),
        collator_(std::move(collator)) {}

  absl::Status Reset() override {
    distinct_values_.Clear();
    return accumulator_->Reset();
  }

  DistinctAccumulator(const DistinctAccumulator&) = delete;
  DistinctAccumulator& operator=(const DistinctAccumulator&) = delete;

  bool Accumulate(const TupleData& input_row, const Value& value,
                  bool* stop_accumulation, absl::Status* status) override {
    *stop_accumulation = false;

    bool distinct;

    Value value_to_insert;
    if (collator_ == nullptr) {
      value_to_insert = value;
    } else {
      absl::StatusOr<Value> collated_distinct_key =
          GetValueSortKey(value, *(collator_));
      if (!collated_distinct_key.ok()) {
        *status = collated_distinct_key.status();
        return false;
      }
      value_to_insert = collated_distinct_key.value();
    }

    if (!distinct_values_.Insert(value_to_insert, &distinct, status)) {
      return false;
    }

    if (distinct) {
      if (!accumulator_->Accumulate(input_row, value, stop_accumulation,
                                    status)) {
        return false;
      }
    }
    return true;
  }

  absl::StatusOr<Value> GetFinalResult(bool inputs_in_defined_order) override {
    return accumulator_->GetFinalResult(inputs_in_defined_order);
  }

 private:
  ValueHashSet distinct_values_;
  std::unique_ptr<IntermediateAggregateAccumulator> accumulator_;
  const std::unique_ptr<const ZetaSqlCollator> collator_;
};

// Accumulator that discards NULL values.
class IgnoresNullAccumulator : public IntermediateAggregateAccumulator {
 public:
  // If 'use_compound_values' is true, then the values passed to Accumulate()
  // are non-NULL structs and should be discarded if any of their fields are
  // NULL.
  IgnoresNullAccumulator(
      bool use_compound_values,
      std::unique_ptr<IntermediateAggregateAccumulator> accumulator)
      : use_compound_values_(use_compound_values),
        accumulator_(std::move(accumulator)) {}

  IgnoresNullAccumulator(const IgnoresNullAccumulator&) = delete;
  IgnoresNullAccumulator& operator=(const IgnoresNullAccumulator&) = delete;

  absl::Status Reset() override { return accumulator_->Reset(); }

  bool Accumulate(const TupleData& input_row, const Value& value,
                  bool* stop_accumulation, absl::Status* status) override {
    *stop_accumulation = false;

    bool ignore = false;
    if (use_compound_values_) {
      for (const Value& field_value : value.fields()) {
        if (field_value.is_null()) {
          ignore = true;
          break;
        }
      }
    } else {
      ignore = value.is_null();
    }

    if (ignore) return true;
    return accumulator_->Accumulate(input_row, value, stop_accumulation,
                                    status);
  }

  absl::StatusOr<Value> GetFinalResult(bool inputs_in_defined_order) override {
    return accumulator_->GetFinalResult(inputs_in_defined_order);
  }

 private:
  const bool use_compound_values_;
  std::unique_ptr<IntermediateAggregateAccumulator> accumulator_;
};

// Accumulator that filters out rows that don't have the extremal value of some
// expression. It does this in one pass over the input by keeping track of the
// current extremal value and the aggregation for that value, and resetting the
// aggregation whenever it finds a new extremal value.
//
// As an example, consider COUNT(* HAVING MAX foo) for these input rows:
//   foo: 10 -> saw new max, the count is now 1, and max is 10
//   foo: 10 -> saw current max, the count is now 2 and max is still 10
//   foo: 5  -> less than the current max, the count is still 2, and max is 10
//   foo: 20 -> saw new max, reset count to 1 and max to 20
//   foo: 20 -> saw current max, the count is now 2 and max is still 20
class HavingExtremalValueAccumulator : public IntermediateAggregateAccumulator {
 public:
  HavingExtremalValueAccumulator(
      absl::Span<const TupleData* const> params, const ValueExpr* having_expr,
      bool use_max,
      std::unique_ptr<IntermediateAggregateAccumulator> accumulator,
      EvaluationContext* context)
      : params_(params.begin(), params.end()),
        having_expr_(having_expr),
        use_max_(use_max),
        accumulator_(std::move(accumulator)),
        context_(context) {}

  HavingExtremalValueAccumulator(const HavingExtremalValueAccumulator&) =
      delete;
  HavingExtremalValueAccumulator& operator=(
      const HavingExtremalValueAccumulator&) = delete;

  absl::Status Reset() override {
    extremal_having_value_ = Value::Invalid();
    return accumulator_->Reset();
  }

  bool Accumulate(const TupleData& input_row, const Value& value,
                  bool* stop_accumulation, absl::Status* status) override {
    *stop_accumulation = false;

    TupleSlot slot;
    if (!having_expr_->EvalSimple(
            ConcatSpans(absl::Span<const TupleData* const>(params_),
                        {&input_row}),
            context_, &slot, status)) {
      return false;
    }
    const Value& having_value = slot.value();

    // Compute the new extremal having value.
    Value new_extremal_having_value;
    if (!extremal_having_value_.is_valid()) {
      new_extremal_having_value = having_value;
    } else {
      // We use a BuiltinAggregateFunction here to stay consistent with all of
      // the SQL semantics regarding MAX/MIN in the presence of NULLs and NaNs.
      const BuiltinAggregateFunction max_function(
          use_max_ ? FunctionKind::kMax : FunctionKind::kMin,
          having_value.type(), /*num_input_fields=*/1, having_value.type());
      auto status_or_accumulator = max_function.CreateAccumulator(
          /*args=*/{}, /*collator_list=*/{}, context_);
      if (!status_or_accumulator.ok()) {
        *status = status_or_accumulator.status();
        return false;
      }
      std::unique_ptr<AggregateAccumulator>& accumulator =
          status_or_accumulator.value();
      *status = accumulator->Reset();
      if (!status->ok()) return false;
      bool dummy_stop_accumulation;
      if (!accumulator->Accumulate(extremal_having_value_,
                                   &dummy_stop_accumulation, status)) {
        return false;
      }
      if (!accumulator->Accumulate(having_value, &dummy_stop_accumulation,
                                   status)) {
        return false;
      }
      auto status_or_value = accumulator->GetFinalResult(
          /*inputs_in_defined_order=*/false);
      if (!status_or_value.ok()) {
        *status = status_or_value.status();
        return false;
      }
      new_extremal_having_value = std::move(status_or_value).value();
    }

    // We update 'extremal_having_value_' and reset the accumulation if
    // 'new_extremal_having_value' is not the same as
    // 'extremal_having_value_'. We use ComparisonFunction because it handles
    // NaNs correctly.
    bool reset;
    if (!extremal_having_value_.is_valid()) {
      reset = true;
    } else {
      const ComparisonFunction equals_function(FunctionKind::kEqual,
                                               types::BoolType());
      Value equals_result;
      if (!equals_function.Eval(
              params_, {extremal_having_value_, new_extremal_having_value},
              context_, &equals_result, status)) {
        return false;
      }
      reset = !(equals_result == Bool(true));
    }
    if (reset) {
      extremal_having_value_ = new_extremal_having_value;
      *status = accumulator_->Reset();
      if (!status->ok()) return false;
    }

    // Accumulate if 'having_value' equals 'extremal_having_value_'.  NaN is not
    // equal to itself, so this will not accumulate for NaN even if it triggered
    // a reset. For this reason, it's important to use the ComparisonFunction
    // here and not Value::Equals().
    const ComparisonFunction equals_function(FunctionKind::kEqual,
                                             types::BoolType());
    Value equals_result;
    if (!equals_function.Eval(params_, {extremal_having_value_, having_value},
                              context_, &equals_result, status)) {
      return false;
    }
    const bool accumulate = (equals_result == Bool(true));
    if (!accumulate) return true;

    // We can never stop accumulating because we might see another MAX/MIN
    // later.
    bool dummy_stop_accumulation;
    return accumulator_->Accumulate(input_row, value, &dummy_stop_accumulation,
                                    status);
  }

  absl::StatusOr<Value> GetFinalResult(bool inputs_in_defined_order) override {
    return accumulator_->GetFinalResult(inputs_in_defined_order);
  }

 private:
  const std::vector<const TupleData*> params_;
  const ValueExpr* having_expr_;
  const bool use_max_;
  Value extremal_having_value_;
  std::unique_ptr<IntermediateAggregateAccumulator> accumulator_;
  EvaluationContext* context_;
};

// Accumulator which runs an inner accumulator on only the subset of rows for
// which a filter expression evaluates to true.
class FilteredArgAccumulator : public IntermediateAggregateAccumulator {
 public:
  FilteredArgAccumulator(
      absl::Span<const TupleData* const> params,
      std::unique_ptr<IntermediateAggregateAccumulator> accumulator,
      const ValueExpr* filter, EvaluationContext* context)
      : params_(params.begin(), params.end()),
        accumulator_(std::move(accumulator)),
        filter_(filter),
        context_(context) {}

  absl::Status Reset() override { return accumulator_->Reset(); }

  bool Accumulate(const TupleData& input_row, const Value& input_value,
                  bool* stop_accumulation, absl::Status* status) override {
    *stop_accumulation = false;

    TupleSlot slot;
    if (!filter_->EvalSimple(
            ConcatSpans(absl::Span<const TupleData* const>(params_),
                        {&input_row}),
            context_, &slot, status)) {
      return false;
    }
    const Value& filter_value = slot.value();

    if (filter_value.is_null() || !filter_value.bool_value()) {
      // Row is skipped
      return true;
    }

    return accumulator_->Accumulate(input_row, input_value, stop_accumulation,
                                    status);
  }

  absl::StatusOr<Value> GetFinalResult(bool inputs_in_defined_order) override {
    return accumulator_->GetFinalResult(inputs_in_defined_order);
  }

 private:
  const std::vector<const TupleData*> params_;

  // Underlying accumulator that runs on input rows that satisfy the filter
  std::unique_ptr<IntermediateAggregateAccumulator> accumulator_;

  // Filter expression to be applied to each row. Must return BOOL type.
  // A value of TRUE indicates that the row should be processed by
  // <accumulator_>. A value of FALSE or NULL indicates that the row should be
  // skipped.
  const ValueExpr* filter_;

  // EvaluationContext for evaluating the filter.
  EvaluationContext* context_;
};

class WithGroupRowsAccumulator : public IntermediateAggregateAccumulator {
 public:
  WithGroupRowsAccumulator(
      absl::Span<const TupleData* const> params,
      const RelationalOp* group_rows_subquery,
      std::vector<const ValueExpr*> agg_fn_input_fields,
      const Type* agg_fn_input_type,
      std::unique_ptr<IntermediateAggregateAccumulator> accumulator,
      EvaluationContext* context)
      : params_(params),
        group_rows_subquery_(group_rows_subquery),
        agg_fn_input_fields_(agg_fn_input_fields),
        agg_fn_input_type_(agg_fn_input_type),
        inputs_(context->memory_accountant()),
        accumulator_(std::move(accumulator)),
        context_(context) {
  }

  absl::Status Reset() override {
    inputs_.Clear();
    return accumulator_->Reset();
  }

  WithGroupRowsAccumulator(const WithGroupRowsAccumulator&) = delete;
  WithGroupRowsAccumulator& operator=(const WithGroupRowsAccumulator&) = delete;

  bool Accumulate(const TupleData& input_row, const Value& value,
                  bool* stop_accumulation, absl::Status* status) override {
    *stop_accumulation = false;

    auto input = absl::make_unique<TupleData>(input_row);
    return inputs_.PushBack(std::move(input), status);
  }

  absl::StatusOr<Value> GetFinalResult(
      bool inputs_in_defined_order) override {
    // Set inputs_ on context to make it available for GROUP_ROWS scan;
    // Fetch all rows from the subquery, running accumulator_ through them;
    // compute the argument expression values for the aggregate function
    // (similar to what IntermediateAggregateAccumulatorAdaptor does).

    context_->set_active_group_rows(&inputs_);
    auto cleanup = absl::MakeCleanup([this]() {
      context_->set_active_group_rows(nullptr);
    });

    // TODO: don't we need to pass different params_ here? params_
    // seem to be for the aggregate function itself, not for the subquery, on
    // the other hand the same params are passed to all aggregates. Reference:
    // https://github.com/google/zetasql/blob/master/zetasql/reference_impl/aggregate_op.cc?l=1089&rcl=327634640
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<TupleIterator> input_iter,
                     group_rows_subquery_->CreateIterator(
                         params_, /*num_extra_slots=*/0, context_));
    absl::Status status;
    while (true) {
      const TupleData* next_input = input_iter->Next();
      if (next_input == nullptr) {
        ZETASQL_RETURN_IF_ERROR(input_iter->Status());
        break;
      }
      std::vector<Value> values(agg_fn_input_fields_.size());
      for (int i = 0; i < agg_fn_input_fields_.size(); ++i) {
        const ValueExpr* value_expr = agg_fn_input_fields_[i];

        std::shared_ptr<TupleSlot::SharedProtoState> shared_state;
        VirtualTupleSlot slot(&values[i], &shared_state);

        if (!value_expr->Eval(
                ConcatSpans(absl::Span<const TupleData* const>(params_),
                            {next_input}),
                context_, &slot, &status)) {
          return status;
        }
      }

      Value value;
      if (values.size() == 1) {
        value = std::move(values[0]);
      } else {
        value = Value::UnsafeStruct(agg_fn_input_type_->AsStruct(),
                                    std::move(values));
      }
      bool stop_accumulation;
      if (!accumulator_->Accumulate(*next_input, value, &stop_accumulation,
                                      &status)) {
        return status;
      }
      if (stop_accumulation) {
        break;
      }
    }

    return accumulator_->GetFinalResult(inputs_in_defined_order);
  }

 private:
  absl::Span<const TupleData* const> params_;
  const RelationalOp* group_rows_subquery_;
  std::vector<const ValueExpr*> agg_fn_input_fields_;
  const Type* agg_fn_input_type_;
  TupleDataDeque inputs_;
  std::unique_ptr<IntermediateAggregateAccumulator> accumulator_;
  EvaluationContext* context_;
};

// Adapts IntermediateAggregateAccumulator to AggregateArgAccumulator.
class IntermediateAggregateAccumulatorAdaptor : public AggregateArgAccumulator {
 public:
  IntermediateAggregateAccumulatorAdaptor(
      absl::Span<const TupleData* const> params,
      absl::Span<const ValueExpr* const> value_exprs, const Type* input_type,
      std::unique_ptr<IntermediateAggregateAccumulator> accumulator,
      EvaluationContext* context)
      : params_(params.begin(), params.end()),
        value_exprs_(value_exprs.begin(), value_exprs.end()),
        input_type_(input_type),
        accumulator_(std::move(accumulator)),
        context_(context) {}

  IntermediateAggregateAccumulatorAdaptor(
      const IntermediateAggregateAccumulatorAdaptor&) = delete;
  IntermediateAggregateAccumulatorAdaptor& operator=(
      const IntermediateAggregateAccumulatorAdaptor&) = delete;

  bool Accumulate(const TupleData& input_row, bool* stop_accumulation,
                  absl::Status* status) override {
    std::vector<Value> values(value_exprs_.size());
    for (int i = 0; i < value_exprs_.size(); ++i) {
      const ValueExpr* value_expr = value_exprs_[i];

      std::shared_ptr<TupleSlot::SharedProtoState> shared_state;
      VirtualTupleSlot slot(&values[i], &shared_state);

      if (!value_expr->Eval(
              ConcatSpans(absl::Span<const TupleData* const>(params_),
                          {&input_row}),
              context_, &slot, status)) {
        return false;
      }
    }

    Value value;
    if (values.size() == 1) {
      value = std::move(values[0]);
    } else {
      value = Value::UnsafeStruct(input_type_->AsStruct(), std::move(values));
    }

    return accumulator_->Accumulate(input_row, value, stop_accumulation,
                                    status);
  }

  absl::StatusOr<Value> GetFinalResult(bool inputs_in_defined_order) override {
    return accumulator_->GetFinalResult(inputs_in_defined_order);
  }

 private:
  const std::vector<const TupleData*> params_;
  const std::vector<const ValueExpr*> value_exprs_;
  const Type* input_type_;
  std::unique_ptr<IntermediateAggregateAccumulator> accumulator_;
  EvaluationContext* context_;
};

}  // namespace

static absl::Status PopulateSlotsForKeysAndValues(
    const TupleSchema& schema, absl::Span<const KeyArg* const> order_by_keys,
    std::vector<int>* slots_for_keys, std::vector<int>* slots_for_values) {
  // First populate 'slots_for_keys'.
  slots_for_keys->reserve(order_by_keys.size());
  for (const KeyArg* order_by_key : order_by_keys) {
    const absl::optional<int> slot_idx =
        schema.FindIndexForVariable(order_by_key->variable());
    ZETASQL_RET_CHECK(slot_idx.has_value()).EmitStackTrace()
        << order_by_key->DebugString()
        << " order_by_key->variable()=" << order_by_key->variable();
    slots_for_keys->push_back(slot_idx.value());
  }
  absl::flat_hash_set<int> slots_for_keys_set(slots_for_keys->begin(),
                                              slots_for_keys->end());

  // The other slots contain values.
  slots_for_values->reserve(schema.num_variables() - slots_for_keys_set.size());
  for (int i = 0; i < schema.num_variables(); ++i) {
    if (!slots_for_keys_set.contains(i)) {
      slots_for_values->push_back(i);
    }
  }

  return absl::OkStatus();
}

namespace {

absl::StatusOr<CollatorList> MakeCollatorList(
    const std::vector<ResolvedCollation>& collation_list) {
  CollatorList collator_list;

  if (collation_list.empty()) {
    return collator_list;
  }

  for (const ResolvedCollation& resolved_collation : collation_list) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ZetaSqlCollator> collator,
                     GetCollatorFromResolvedCollation(resolved_collation));
    collator_list.push_back(std::move(collator));
  }

  return std::move(collator_list);
}

}  // namespace

absl::StatusOr<std::unique_ptr<AggregateArgAccumulator>>
AggregateArg::CreateAccumulator(absl::Span<const TupleData* const> params,
                                EvaluationContext* context) const {
  // Build the underlying AggregateAccumulator.
  std::vector<Value> args(parameter_list_size());
  for (int i = 0; i < parameter_list_size(); ++i) {
    std::shared_ptr<TupleSlot::SharedProtoState> shared_state;
    VirtualTupleSlot slot(&args[i], &shared_state);
    absl::Status status;
    if (!parameter(i)->Eval(params, context, &slot, &status)) return status;
  }
  ZETASQL_ASSIGN_OR_RETURN(CollatorList collator_list,
                   MakeCollatorList(collation_list()));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<AggregateAccumulator> underlying_accumulator,
                   aggregate_function()->function()->CreateAccumulator(
                       args, std::move(collator_list), context));

  // Adapt the underlying AggregateAccumulator to the
  // IntermediateAggregateAccumulator interface so that we can stack other
  // intermediate accumulators on top of it.
  std::unique_ptr<IntermediateAggregateAccumulator> accumulator =
      absl::make_unique<AggregateAccumulatorAdaptor>(
          aggregate_function()->output_type(), error_mode_,
          std::move(underlying_accumulator));

  const TupleSchema* agg_fn_input_schema = group_schema_.get();
  std::unique_ptr<const TupleSchema> group_rows_schema;
  if (group_rows_subquery_ != nullptr) {
    group_rows_schema = group_rows_subquery_->CreateOutputSchema();
    agg_fn_input_schema = group_rows_schema.get();
  }
  // LIMIT support.
  bool consumed_order_by = false;
  if (limit() != nullptr) {
    TupleSlot limit_slot;
    absl::Status status;
    if (!limit()->EvalSimple(params, context, &limit_slot, &status))
      return status;
    const Value& limit_val = limit_slot.value();
    if (limit_val.int64_value() < 0) {
      return ::zetasql_base::OutOfRangeErrorBuilder()
             << "Limit requires non-negative count";
    }
    if (context->options().use_top_n_accumulator_when_possible &&
        !order_by_keys().empty()) {
      // Optimization: to save memory, use TopNAccumulator instead of
      // LimitAccumulator(OrderByAccumulator).
      std::vector<int> slots_for_keys;
      std::vector<int> slots_for_values;
      ZETASQL_RETURN_IF_ERROR(
          PopulateSlotsForKeysAndValues(*agg_fn_input_schema, order_by_keys(),
                                        &slots_for_keys, &slots_for_values));
      ZETASQL_ASSIGN_OR_RETURN(auto tuple_comparator,
                       TupleComparator::Create(order_by_keys(), slots_for_keys,
                                               params, context));
      accumulator = absl::make_unique<TopNAccumulator>(
          limit_val.int64_value(), std::move(tuple_comparator),
          std::move(accumulator), context);
      consumed_order_by = true;
      context->set_used_top_n_accumulator(true);
    } else {
      accumulator = absl::make_unique<LimitAccumulator>(limit_val.int64_value(),
                                                        std::move(accumulator));
    }
  }

  // ORDER BY support.
  if (!consumed_order_by && !order_by_keys().empty()) {
    std::vector<int> slots_for_keys;
    std::vector<int> slots_for_values;
    ZETASQL_RETURN_IF_ERROR(
        PopulateSlotsForKeysAndValues(*agg_fn_input_schema, order_by_keys(),
                                      &slots_for_keys, &slots_for_values));

    accumulator = absl::make_unique<OrderByAccumulator>(
        order_by_keys(), slots_for_keys, slots_for_values, params,
        std::move(accumulator), context);
  }

  // DISTINCT support.
  if (distinct()) {
    ZETASQL_ASSIGN_OR_RETURN(CollatorList collator_list,
                     MakeCollatorList(collation_list()));
    ZETASQL_RET_CHECK_LE(collator_list.size(), 1);
    accumulator = absl::make_unique<DistinctAccumulator>(
        std::move(accumulator), context,
        collator_list.empty() ? nullptr : std::move(collator_list[0]));
  }

  // Support for aggregation functions that ignore NULLs.
  if (ignores_null()) {
    const bool use_compound_values = (num_input_fields() > 1);
    accumulator = absl::make_unique<IgnoresNullAccumulator>(
        use_compound_values, std::move(accumulator));
  }

  // HAVING MAX/MIN support.
  if (having_modifier_kind() != AggregateArg::kHavingNone) {
    ZETASQL_RET_CHECK(having_expr() != nullptr);
    const bool use_max = (having_modifier_kind() == AggregateArg::kHavingMax);
    accumulator = absl::make_unique<HavingExtremalValueAccumulator>(
        params, having_expr(), use_max, std::move(accumulator), context);
  }

  // Filter support
  if (filter() != nullptr) {
    accumulator = absl::make_unique<FilteredArgAccumulator>(
        params, std::move(accumulator), filter(), context);
  }

  // Adapt 'accumulator' to the AggregateArgAccumulator interface.
  const Type* type = input_type();
  std::vector<const ValueExpr*> input_fields;
  input_fields.reserve(input_field_list_size());
  for (int i = 0; i < input_field_list_size(); ++i) {
    input_fields.push_back(input_field(i));
  }
  if (group_rows_subquery_ != nullptr) {
    // Create accumulator that knows the subquery and how to accumulate the
    // aggregate function in the end. At iteration if would interact with
    // GroupRowsOp.
    accumulator = absl::make_unique<WithGroupRowsAccumulator>(
        params, group_rows_subquery_.get(), std::move(input_fields), type,
        std::move(accumulator), context);
    type = EmptyStructType();
    input_fields.clear();
  }

  // Adapt 'accumulator' to the AggregateArgAccumulator interface.
  return absl::make_unique<IntermediateAggregateAccumulatorAdaptor>(
      params, input_fields, type, std::move(accumulator), context);
}

absl::StatusOr<Value> AggregateArg::EvalAgg(
    absl::Span<const TupleData* const> group,
    absl::Span<const TupleData* const> params,
    EvaluationContext* context) const {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<AggregateArgAccumulator> accumulator,
                   CreateAccumulator(params, context));

  bool stop_aggregation;
  absl::Status status;
  for (const TupleData* row : group) {
    if (!accumulator->Accumulate(*row, &stop_aggregation, &status)) {
      return status;
    }
    if (stop_aggregation) break;
  }

  const absl::StatusOr<Value> status_or_result = accumulator->GetFinalResult(
      /*inputs_in_defined_order=*/false);
  if (!status_or_result.ok()) {
    if (ShouldSuppressError(status_or_result.status(), error_mode_)) {
      return Value::Null(type());
    }
    return status_or_result.status();
  }
  return status_or_result.value();
}

std::string AggregateArg::DebugInternal(const std::string& indent,
                                        bool verbose) const {
  std::string result;
  absl::StrAppend(&result, "$", variable().ToString());
  if (verbose) {
    absl::StrAppend(&result, "[", type()->DebugString(), "]");
  }
  absl::StrAppend(&result, " := ");

  std::vector<std::string> order_keys_strs;
  for (const KeyArg* order_key : order_by_keys()) {
    order_keys_strs.push_back(
        order_key->DebugInternal("" /* indent */, verbose));
  }

  absl::StrAppend(
      &result, distinct() == kDistinct ? "DISTINCT " : "",
      aggregate_function()->DebugInternal(indent, verbose),
      having_expr_ != nullptr
          ? absl::StrCat(
                " HAVING ",
                (having_modifier_kind_ == kHavingMax ? "MAX " : "MIN "),
                having_expr_->DebugInternal("" /* indent */, verbose))
          : "",
      !order_by_keys().empty()
          ? absl::StrCat(" ORDER BY ", absl::StrJoin(order_keys_strs, ","))
          : "",
      limit_ != nullptr ? absl::StrCat(" LIMIT ", limit_->DebugInternal(
                                                      "" /* indent */, verbose))
                        : "",
      !ignores_null() ? " [ignores_null = false]" : "",
      filter_ != nullptr ? absl::StrCat(" FILTER ", filter_->DebugInternal(
                                                        /*indent=*/"", verbose))
                         : "",
      !collation_list_.empty()
          ? absl::StrCat(" ", ResolvedCollation::ToString(collation_list_))
          : "");
  if (group_rows_subquery_ != nullptr) {
    std::string indent_child = indent + AggregateOp::kIndentSpace;
    absl::StrAppend(&result, indent_child, AggregateOp::kIndentFork,
                    "with_group_rows_subquery: ",
                    group_rows_subquery_->DebugInternal(
                        indent_child + AggregateOp::kIndentSpace, verbose));
  }
  return result;
}

// Returns 'order_keys' as a vector<const KeyArg*>. The caller owns the returned
// pointers.
static std::vector<const KeyArg*> ReleaseAllOrderKeys(
    std::vector<std::unique_ptr<KeyArg>> order_keys) {
  std::vector<const KeyArg*> ret;
  ret.reserve(order_keys.size());
  for (std::unique_ptr<KeyArg>& key : order_keys) {
    ret.push_back(key.release());
  }
  return ret;
}

AggregateArg::AggregateArg(
    const VariableId& variable,
    std::unique_ptr<AggregateFunctionCallExpr> function, Distinctness distinct,
    std::unique_ptr<ValueExpr> having_expr,
    const HavingModifierKind having_modifier_kind,
    std::vector<std::unique_ptr<KeyArg>> order_by_keys,
    std::unique_ptr<ValueExpr> limit,
    std::unique_ptr<RelationalOp> group_rows_subquery,
    ResolvedFunctionCallBase::ErrorMode error_mode,
    std::unique_ptr<ValueExpr> filter,
    const std::vector<ResolvedCollation>& collation_list)
    : ExprArg(variable, std::move(function)),
      distinct_(distinct),
      having_expr_(std::move(having_expr)),
      having_modifier_kind_(having_modifier_kind),
      order_by_keys_(ReleaseAllOrderKeys(std::move(order_by_keys))),
      order_by_keys_deleter_(&order_by_keys_),
      limit_(std::move(limit)),
      group_rows_subquery_(std::move(group_rows_subquery)),
      error_mode_(error_mode),
      filter_(std::move(filter)),
      collation_list_(collation_list) {}

const AggregateFunctionCallExpr* AggregateArg::aggregate_function() const {
  return static_cast<const AggregateFunctionCallExpr*>(value_expr());
}

AggregateFunctionCallExpr* AggregateArg::mutable_aggregate_function() {
  return static_cast<AggregateFunctionCallExpr*>(mutable_value_expr());
}

int AggregateArg::num_input_fields() const {
  return aggregate_function()->function()->num_input_fields();
}

const Type* AggregateArg::input_type() const {
  return aggregate_function()->function()->input_type();
}

bool AggregateArg::ignores_null() const {
  return aggregate_function()->function()->ignores_null();
}

const ValueExpr* AggregateArg::input_field(int i) const {
  return aggregate_function()->GetArgs()[i]->node()->AsValueExpr();
}

ValueExpr* AggregateArg::mutable_input_field(int i) {
  return mutable_aggregate_function()
      ->GetMutableArgs()[i]
      ->mutable_node()
      ->AsMutableValueExpr();
}

int AggregateArg::parameter_list_size() const {
  return aggregate_function()->GetArgs().size() - num_input_fields();
}

const ValueExpr* AggregateArg::parameter(int i) const {
  return aggregate_function()
      ->GetArgs()[i + num_input_fields()]
      ->node()
      ->AsValueExpr();
}

ValueExpr* AggregateArg::mutable_parameter(int i) {
  return mutable_aggregate_function()
      ->GetMutableArgs()[i + num_input_fields()]
      ->mutable_node()
      ->AsMutableValueExpr();
}

const ValueExpr* AggregateArg::filter() const { return filter_.get(); }
ValueExpr* AggregateArg::mutable_filter() { return filter_.get(); }

// -------------------------------------------------------
// AggregateOp
// -------------------------------------------------------

std::string AggregateOp::GetIteratorDebugString(
    absl::string_view input_iter_debug_string) {
  return absl::StrCat("AggregationTupleIterator(", input_iter_debug_string,
                      ")");
}

absl::StatusOr<std::unique_ptr<AggregateOp>> AggregateOp::Create(
    std::vector<std::unique_ptr<KeyArg>> keys,
    std::vector<std::unique_ptr<AggregateArg>> aggregators,
    std::unique_ptr<RelationalOp> input) {
  for (auto& arg : keys) {
    ZETASQL_RETURN_IF_ERROR(ValidateTypeSupportsEqualityComparison(arg->type()));
  }
  return absl::WrapUnique(new AggregateOp(
      std::move(keys), std::move(aggregators), std::move(input)));
}

absl::Status AggregateOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_RETURN_IF_ERROR(mutable_input()->SetSchemasForEvaluation(params_schemas));

  const std::unique_ptr<const TupleSchema> input_schema =
      mutable_input()->CreateOutputSchema();

  for (KeyArg* key : mutable_keys()) {
    ZETASQL_RETURN_IF_ERROR(key->mutable_value_expr()->SetSchemasForEvaluation(
        ConcatSpans(params_schemas, {input_schema.get()})));
  }

  for (AggregateArg* arg : mutable_aggregators()) {
    ZETASQL_RETURN_IF_ERROR(
        arg->SetSchemasForEvaluation(*input_schema, params_schemas));
  }

  return absl::OkStatus();
}

namespace {

// Outputs a pre-computed list of tuples.
class AggregateTupleIterator : public TupleIterator {
 public:
  AggregateTupleIterator(
      absl::Span<const TupleData* const> params,
      std::unique_ptr<TupleDataDeque> tuples,
      std::unique_ptr<TupleIterator> input_iter_for_debug_string,
      std::unique_ptr<TupleSchema> output_schema, EvaluationContext* context)
      : params_(params.begin(), params.end()),
        output_schema_(std::move(output_schema)),
        tuples_(std::move(tuples)),
        input_iter_for_debug_string_(std::move(input_iter_for_debug_string)),
        context_(context) {}

  AggregateTupleIterator(const AggregateTupleIterator&) = delete;
  AggregateTupleIterator& operator=(const AggregateTupleIterator&) = delete;

  const TupleSchema& Schema() const override { return *output_schema_; }

  TupleData* Next() override {
    if (tuples_->IsEmpty()) return nullptr;
    if (num_next_calls_ %
            absl::GetFlag(
                FLAGS_zetasql_call_verify_not_aborted_rows_period) ==
        0) {
      absl::Status status = context_->VerifyNotAborted();
      if (!status.ok()) {
        status_ = status;
        return nullptr;
      }
    }
    ++num_next_calls_;

    current_ = tuples_->PopFront();
    return current_.get();
  }

  absl::Status Status() const override { return status_; }

  std::string DebugString() const override {
    return AggregateOp::GetIteratorDebugString(
        input_iter_for_debug_string_->DebugString());
  }

 private:
  const std::vector<const TupleData*> params_;
  const std::unique_ptr<TupleSchema> output_schema_;
  const std::vector<const AggregateArg*> aggregators_;
  const std::unique_ptr<TupleDataDeque> tuples_;
  // We store a TupleIterator instead of the debug string to avoid computing the
  // debug string unnecessarily.
  const std::unique_ptr<TupleIterator> input_iter_for_debug_string_;
  std::unique_ptr<TupleData> current_;
  EvaluationContext* context_;
  absl::Status status_;
  int64_t num_next_calls_ = 0;
};

// The bool is true if we should stop accumulation for the corresponding
// accumulator.
using AccumulatorList =
    std::vector<std::pair<std::unique_ptr<AggregateArgAccumulator>, bool>>;

// The data associated with a grouping key during aggregation.
class GroupValue {
 public:
  // Reserves bytes for 'key' with 'accountant' and returns a new GroupValue.
  static absl::StatusOr<std::unique_ptr<GroupValue>> Create(
      std::unique_ptr<TupleData> key, MemoryAccountant* accountant) {
    const int64_t bytes_size = key->GetPhysicalByteSize();
    absl::Status status;
    if (!accountant->RequestBytes(bytes_size, &status)) {
      return status;
    }
    return absl::WrapUnique(
        new GroupValue(std::move(key), bytes_size, accountant));
  }

  GroupValue(const GroupValue&) = delete;
  GroupValue& operator=(const GroupValue&) = delete;

  ~GroupValue() { ConsumeKey(); }

  // Unregisters the key with the 'accountant' and returns it.
  std::unique_ptr<TupleData> ConsumeKey() {
    if (key_ != nullptr) {
      accountant_->ReturnBytes(key_physical_byte_size_);
    }
    return std::move(key_);
  }

  AccumulatorList* mutable_accumulator_list() { return &accumulator_list_; }

 private:
  GroupValue(std::unique_ptr<TupleData> key, int64_t key_physical_byte_size,
             MemoryAccountant* accountant)
      : key_(std::move(key)),
        key_physical_byte_size_(key_physical_byte_size),
        accountant_(accountant) {}

  std::unique_ptr<TupleData> key_;
  int64_t key_physical_byte_size_ = 0;
  MemoryAccountant* accountant_ = nullptr;
  AccumulatorList accumulator_list_;
};

}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>> AggregateOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<TupleIterator> input_iter,
      input()->CreateIterator(params, /*num_extra_slots=*/0, context));

  // The key is owned by the <group_map_keys_memory> defined below.
  absl::flat_hash_map<TupleDataPtr, std::unique_ptr<GroupValue>> group_map;
  std::vector<std::unique_ptr<TupleData>> group_map_keys_memory;

  CollatorList collators;

  // Prepare collators for each KeyArg.
  for (const KeyArg* key : keys()) {
    if (key->collation() == nullptr) {
      collators.push_back(nullptr);
      continue;
    }
    TupleSlot collation_slot;
    absl::Status status;
    if (!key->collation()->EvalSimple(params, context, &collation_slot,
                                      &status)) {
      return status;
    }

    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ZetaSqlCollator> collator,
        GetCollatorFromResolvedCollationValue(collation_slot.value()));
    collators.push_back(std::move(collator));
  }

  absl::Status status;
  while (true) {
    const TupleData* next_input = input_iter->Next();
    if (next_input == nullptr) {
      ZETASQL_RETURN_IF_ERROR(input_iter->Status());
      break;
    }

    // Determine the key to 'group_to_accumulator_map'.
    const std::vector<const TupleData*> params_and_input_tuple =
        ConcatSpans(params, {next_input});
    auto key_data = absl::make_unique<TupleData>(keys().size());
    // If collator is present for <key_data[i]>, <collated_key_data[i]> is
    // collation_key for value of <key_data[i]>. Otherwise,
    // <collated_key_data[i]> is the same as <key_data[i]>.
    auto collated_key_data = absl::make_unique<TupleData>(keys().size());

    for (int i = 0; i < keys().size(); ++i) {
      TupleSlot* slot = key_data->mutable_slot(i);
      const KeyArg* key = keys()[i];
      absl::Status status;
      if (!key->value_expr()->EvalSimple(params_and_input_tuple, context, slot,
                                         &status)) {
        return status;
      }

      Value* collated_slot_value =
          collated_key_data->mutable_slot(i)->mutable_value();
      if (collators[i] == nullptr) {
        *collated_slot_value = slot->value();
      } else {
        ZETASQL_ASSIGN_OR_RETURN(*collated_slot_value,
                         GetValueSortKey(slot->value(), *(collators[i])));
      }
    }

    // Look up the value in 'group_to_accumulator_map', initializing a new one
    // if necessary.
    AccumulatorList* accumulators = nullptr;
    std::unique_ptr<GroupValue>* found_group_value =
        zetasql_base::FindOrNull(group_map, TupleDataPtr(collated_key_data.get()));
    if (found_group_value == nullptr) {
      // Create the new GroupValue.
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<GroupValue> inserted_group_value,
                       GroupValue::Create(std::move(key_data),
                                          context->memory_accountant()));

      // Initialize the accumulators.
      accumulators = inserted_group_value->mutable_accumulator_list();
      accumulators->reserve(aggregators().size());
      for (const AggregateArg* aggregator : aggregators()) {
        std::pair<std::unique_ptr<AggregateArgAccumulator>, bool>
            accumulator_and_stop_bit;
        ZETASQL_ASSIGN_OR_RETURN(accumulator_and_stop_bit.first,
                         aggregator->CreateAccumulator(params, context));
        accumulators->push_back(std::move(accumulator_and_stop_bit));
      }

      // Insert the new GroupValue.
      ZETASQL_RET_CHECK(group_map
                    .emplace(TupleDataPtr(collated_key_data.get()),
                             std::move(inserted_group_value))
                    .second);
      group_map_keys_memory.push_back(std::move(collated_key_data));
    } else {
      accumulators = (*found_group_value)->mutable_accumulator_list();
      key_data.reset();
      collated_key_data.reset();
    }

    // Accumulate.
    ZETASQL_RET_CHECK_EQ(accumulators->size(), aggregators().size());
    bool all_accumulators_stopped = true;
    for (auto& accumulator_and_stop_bit : *accumulators) {
      bool& stop_bit = accumulator_and_stop_bit.second;
      if (stop_bit) continue;
      if (!accumulator_and_stop_bit.first->Accumulate(*next_input, &stop_bit,
                                                      &status)) {
        return status;
      }
      if (!stop_bit) all_accumulators_stopped = false;
    }

    if (all_accumulators_stopped && keys().empty()) {
      // We are doing full aggregation and all the accumulators have stopped, we
      // can stop reading the input.
      break;
    }
  }

  // Build the tuples that the iterator should return.
  auto tuples = absl::make_unique<TupleDataDeque>(context->memory_accountant());
  for (auto& entry : group_map) {
    // Destruction of the 'group_value' will clear all memory used by its
    // members.
    std::unique_ptr<GroupValue> group_value = std::move(entry.second);
    AccumulatorList& accumulators = *group_value->mutable_accumulator_list();

    std::unique_ptr<TupleData> tuple = group_value->ConsumeKey();
    tuple->AddSlots(accumulators.size() + num_extra_slots);

    for (int i = 0; i < accumulators.size(); ++i) {
      AggregateArgAccumulator& accumulator = *accumulators[i].first;
      ZETASQL_ASSIGN_OR_RETURN(Value value, accumulator.GetFinalResult(
                                        /*inputs_in_defined_order=*/false));
      tuple->mutable_slot(keys().size() + i)->SetValue(value);
    }
    // This can free up considerable memory. E.g., for STRING_AGG.
    accumulators.clear();

    if (!tuples->PushBack(std::move(tuple), &status)) {
      return status;
    }
  }

  // Clears <group_map_keys_memory> and <group_map> to reclaim the memory since
  // they are not used anymore.
  group_map_keys_memory.clear();
  group_map.clear();

  if (tuples->IsEmpty()) {
    if (keys().empty()) {
      // We are doing full aggregation over empty input, so we must compute
      // trivial values for the aggregators.
      auto tuple =
          absl::make_unique<TupleData>(aggregators().size() + num_extra_slots);
      for (int i = 0; i < aggregators().size(); ++i) {
        const AggregateArg* aggregator = aggregators()[i];
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<AggregateArgAccumulator> accumulator,
                         aggregator->CreateAccumulator(params, context));
        ZETASQL_ASSIGN_OR_RETURN(Value value, accumulator->GetFinalResult(
                                          /*inputs_in_defined_order=*/true));
        tuple->mutable_slot(i)->SetValue(value);
      }
      if (!tuples->PushBack(std::move(tuple), &status)) {
        return status;
      }
    }
  } else {
    for (const KeyArg* key : keys()) {
      if (key->type()->IsFloatingPoint()) {
        context->SetNonDeterministicOutput();
      }
    }
  }

  // Sort the tuples by key as described above.
  //
  // TODO: Consider eliminating this sort. The downside is that
  // AggregationTupleIterator will then give a non-deterministic ordering of
  // groups, which can break the reference implementation compliance tests
  // (which are based on purely textual matching). It can also break some user
  // tests.
  std::vector<int> slots_for_keys;
  slots_for_keys.reserve(keys().size());
  for (int i = 0; i < keys().size(); ++i) {
    slots_for_keys.push_back(i);
  }
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<TupleComparator> tuple_comparator,
      TupleComparator::Create(keys(), slots_for_keys, params, context));
  tuples->Sort(*tuple_comparator, context->options().always_use_stable_sort);

  auto input_schema =
      absl::make_unique<TupleSchema>(input_iter->Schema().variables());
  std::unique_ptr<TupleIterator> iter =
      absl::make_unique<AggregateTupleIterator>(params, std::move(tuples),
                                                std::move(input_iter),
                                                CreateOutputSchema(), context);
  return MaybeReorder(std::move(iter), context);
}

std::unique_ptr<TupleSchema> AggregateOp::CreateOutputSchema() const {
  std::vector<VariableId> vars;
  vars.reserve(keys().size() + aggregators().size());
  for (const KeyArg* key : keys()) {
    vars.push_back(key->variable());
  }
  for (const AggregateArg* aggregator : aggregators()) {
    vars.push_back(aggregator->variable());
  }

  return absl::make_unique<TupleSchema>(vars);
}

std::string AggregateOp::IteratorDebugString() const {
  return GetIteratorDebugString(input()->IteratorDebugString());
}

std::string AggregateOp::DebugInternal(const std::string& indent,
                                       bool verbose) const {
  return absl::StrCat("AggregateOp(",
                      ArgDebugString({"keys", "aggregators", "input"},
                                     {kN, kN, k1}, indent, verbose),
                      ")");
}

AggregateOp::AggregateOp(std::vector<std::unique_ptr<KeyArg>> keys,
                         std::vector<std::unique_ptr<AggregateArg>> aggregators,
                         std::unique_ptr<RelationalOp> input) {
  SetArgs<KeyArg>(kKey, std::move(keys));
  SetArgs<AggregateArg>(kAggregator, std::move(aggregators));
  SetArg(kInput, absl::make_unique<RelationalArg>(std::move(input)));
}

absl::Span<const KeyArg* const> AggregateOp::keys() const {
  return GetArgs<KeyArg>(kKey);
}

absl::Span<KeyArg* const> AggregateOp::mutable_keys() {
  return GetMutableArgs<KeyArg>(kKey);
}

absl::Span<const AggregateArg* const> AggregateOp::aggregators() const {
  return GetArgs<AggregateArg>(kAggregator);
}

absl::Span<AggregateArg* const> AggregateOp::mutable_aggregators() {
  return GetMutableArgs<AggregateArg>(kAggregator);
}

const RelationalOp* AggregateOp::input() const {
  return GetArg(kInput)->node()->AsRelationalOp();
}

RelationalOp* AggregateOp::mutable_input() {
  return GetMutableArg(kInput)->mutable_node()->AsMutableRelationalOp();
}

//  static
absl::StatusOr<std::unique_ptr<GroupRowsOp>> GroupRowsOp::Create(
    std::vector<std::unique_ptr<ExprArg>> columns) {
  return absl::WrapUnique(new GroupRowsOp(std::move(columns)));
}

GroupRowsOp::GroupRowsOp(std::vector<std::unique_ptr<ExprArg>> columns) {
  SetArgs<ExprArg>(kColumn, std::move(columns));
}

absl::Span<const ExprArg* const> GroupRowsOp::columns() const {
  return GetArgs<ExprArg>(kColumn);
}

std::unique_ptr<TupleSchema> GroupRowsOp::CreateOutputSchema() const {
  std::vector<VariableId> vars;
  vars.reserve(columns().size());
  for (const ExprArg* column : columns()) {
    vars.push_back(column->variable());
  }
  return absl::make_unique<TupleSchema>(vars);
}

absl::Status GroupRowsOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  return absl::OkStatus();
}

std::string GroupRowsOp::DebugInternal(const std::string& indent,
                                       bool verbose) const {
  return absl::StrCat("GroupRowsOp(",
                      ArgDebugString({"columns"}, {kN}, indent, verbose), ")");
}

//  static
std::string GroupRowsOp::GetIteratorDebugString(
    absl::string_view input_iter_debug_string) {
  return absl::StrCat("GroupRowsTupleIterator(", input_iter_debug_string, ")");
}

std::string GroupRowsOp::IteratorDebugString() const {
  return GetIteratorDebugString("<outer_from_clause>");
}

namespace {

// Iterates over TupleDataDeque
// Outputs a pre-computed list of tuples.
class TupleDataDequeIterator : public TupleIterator {
 public:
  TupleDataDequeIterator(
      const TupleDataDeque& tuples,
      int num_extra_slots,
      std::unique_ptr<TupleSchema> output_schema, EvaluationContext* context):
        tuples_(tuples.GetTuplePtrs()),
        output_schema_(std::move(output_schema)),
        num_extra_slots_(num_extra_slots),
        context_(context) {}

  TupleDataDequeIterator(const TupleDataDequeIterator&) = delete;
  TupleDataDequeIterator& operator=(const TupleDataDequeIterator&) = delete;

  const TupleSchema& Schema() const override { return *output_schema_; }

  TupleData* Next() override {
    if (current_input_tuple_ >= tuples_.size()) return nullptr;
    if (current_input_tuple_ %
            absl::GetFlag(
                FLAGS_zetasql_call_verify_not_aborted_rows_period) ==
        0) {
      absl::Status status = context_->VerifyNotAborted();
      if (!status.ok()) {
        status_ = status;
        return nullptr;
      }
    }
    const TupleData* input_tuple = tuples_[current_input_tuple_];
    current_ = std::make_unique<TupleData>(input_tuple->slots());
    current_->AddSlots(num_extra_slots_);
    current_input_tuple_++;
    return current_.get();
  }

  absl::Status Status() const override { return status_; }

  std::string DebugString() const override {
    return "GroupRowsIterator()";
  }

 private:
  const std::vector<const TupleData*> tuples_;
  int64_t current_input_tuple_ = 0;
  const std::unique_ptr<TupleSchema> output_schema_;
  int num_extra_slots_;
  std::unique_ptr<TupleData> current_;
  EvaluationContext* context_;
  absl::Status status_;
};

}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>> GroupRowsOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  if (context->active_group_rows() == nullptr) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "GROUP_ROWS() cannot read group rows data it the current context";
  }

  // The key is owned by the GroupValue.
  absl::flat_hash_map<TupleDataPtr, std::unique_ptr<GroupValue>> group_map;

  std::unique_ptr<TupleIterator> iter =
      absl::make_unique<TupleDataDequeIterator>(*context->active_group_rows(),
                                                num_extra_slots,
                                                CreateOutputSchema(), context);
  return MaybeReorder(std::move(iter), context);
}

}  // namespace zetasql
