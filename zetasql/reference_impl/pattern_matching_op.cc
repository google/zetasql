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

// This file contains the implementation code for evaluating analytic functions.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/functions/match_recognize/compiled_pattern.h"
#include "zetasql/public/functions/match_recognize/match_partition.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/reference_impl/tuple_comparator.h"
#include "zetasql/reference_impl/variable_id.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

using functions::match_recognize::CompiledPattern;
using functions::match_recognize::Match;
using functions::match_recognize::MatchOptions;
using functions::match_recognize::MatchPartition;
using functions::match_recognize::MatchResult;

// -------------------------------------------------------
// PatternMatchingOp
// -------------------------------------------------------

std::string PatternMatchingOp::GetIteratorDebugString(
    absl::string_view input_iter_debug_string) {
  return absl::StrCat("PatternMatchingTupleIterator(", input_iter_debug_string,
                      ")");
}

absl::StatusOr<std::unique_ptr<PatternMatchingOp>> PatternMatchingOp::Create(
    std::vector<std::unique_ptr<KeyArg>> partition_keys,
    std::vector<VariableId> match_result_variables,
    std::vector<std::string> pattern_variable_names,
    std::vector<std::unique_ptr<ValueExpr>> predicates,
    std::unique_ptr<const CompiledPattern> pattern,
    std::unique_ptr<RelationalOp> input) {
  ZETASQL_RET_CHECK_EQ(pattern_variable_names.size(), predicates.size());
  // We expect 4 match result variables, see the definition of
  // PatternMatchingOp for details.
  //   1. match_id,
  //   2. row_number
  //   3. assigned_label
  //   4. is_sentinel.
  ZETASQL_RET_CHECK_EQ(match_result_variables.size(), 4);
  return absl::WrapUnique(new PatternMatchingOp(
      std::move(partition_keys), std::move(match_result_variables),
      std::move(pattern_variable_names), std::move(predicates),
      std::move(pattern), std::move(input)));
}

absl::Status PatternMatchingOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_RETURN_IF_ERROR(mutable_input()->SetSchemasForEvaluation(params_schemas));
  const std::unique_ptr<const TupleSchema> input_schema =
      input()->CreateOutputSchema();

  for (KeyArg* key : mutable_partition_keys()) {
    ZETASQL_RETURN_IF_ERROR(key->mutable_value_expr()->SetSchemasForEvaluation(
        ConcatSpans(params_schemas, {input_schema.get()})));
  }

  for (ExprArg* predicate : mutable_predicates()) {
    ZETASQL_RETURN_IF_ERROR(predicate->mutable_value_expr()->SetSchemasForEvaluation(
        ConcatSpans(params_schemas, {input_schema.get()})));
  }

  return absl::OkStatus();
}

namespace {
// Partitions the tuples from 'input_iter' (which must have
// 'analytic_args.size()' extra slots by 'partition_keys'. Evaluates all of the
// 'analytic_args' on each partition and adds corresponding values to the
// tuples.
class PatternMatchingTupleIterator : public TupleIterator {
 public:
  PatternMatchingTupleIterator(
      absl::Span<const TupleData* const> params,
      absl::Span<const KeyArg* const> partition_keys,
      absl::Span<const int> slots_for_partition_keys,
      std::vector<VariableId> match_result_variables,
      absl::Span<const std::string> pattern_variable_names,
      absl::Span<const ExprArg* const> predicates,
      const CompiledPattern* pattern, std::unique_ptr<TupleIterator> input_iter,
      std::unique_ptr<TupleComparator> partition_comparator,
      std::unique_ptr<TupleSchema> output_schema, EvaluationContext* context)
      : params_(params.begin(), params.end()),
        partition_keys_(partition_keys.begin(), partition_keys.end()),
        slots_for_partition_keys_(slots_for_partition_keys.begin(),
                                  slots_for_partition_keys.end()),
        match_result_variables_(std::move(match_result_variables)),
        pattern_variable_names_(pattern_variable_names.begin(),
                                pattern_variable_names.end()),
        predicates_(predicates.begin(), predicates.end()),
        pattern_(pattern),
        input_iter_(std::move(input_iter)),
        partition_comparator_(std::move(partition_comparator)),
        output_schema_(std::move(output_schema)),
        current_input_partition_(context->memory_accountant()),
        predicate_vals_(predicates_.size(), false),
        unconsumed_match_results_(context->memory_accountant()),
        context_(context) {}

  PatternMatchingTupleIterator(const PatternMatchingTupleIterator&) = delete;
  PatternMatchingTupleIterator& operator=(const PatternMatchingTupleIterator&) =
      delete;

  const TupleSchema& Schema() const override { return *output_schema_; }

  // Evaluates the pattern predicates on 'row' and stores the results in
  // 'predicate_vals_'.
  absl::Status EvaluatePredicatesOnRow(const TupleData& row) {
    absl::Status status;
    TupleSlot result;
    for (int i = 0; i < predicates_.size(); ++i) {
      if (predicates_[i]->value_expr()->EvalSimple(
              ConcatSpans(absl::Span<const TupleData* const>(params_), {&row}),
              context_, &result, &status)) {
        ZETASQL_RET_CHECK(result.value().is_valid());
        ZETASQL_RET_CHECK(result.value().type_kind() == TypeKind::TYPE_BOOL);
        predicate_vals_[i] =
            !result.value().is_null() && result.value().bool_value();
      } else {
        return status;
      }
    }
    return absl::OkStatus();
  }

  // Reset state for matches in the next partition.
  absl::Status ResetMatchPartition() {
    ZETASQL_ASSIGN_OR_RETURN(current_match_partition_,
                     pattern_->CreateMatchPartition(MatchOptions{}));
    current_input_partition_.Clear();
    current_input_partition_ptrs_.clear();
    next_input_tuple_idx_ = 0;
    return absl::OkStatus();
  }

  // Returns true if a new partition was loaded.
  // False if no new rows found (i.e., end of input).
  // Returns an error if something goes wrong (e.g. out of memory or the input
  // iterator returns an error).
  absl::StatusOr<bool> LoadNextPartition() {
    ZETASQL_RETURN_IF_ERROR(ResetMatchPartition());

    // Load the new partition.
    std::unique_ptr<TupleData> first_tuple_in_current_partition;
    if (first_tuple_in_next_partition_ == nullptr) {
      // We are loading the first tuple of the first partition.
      const TupleData* input_data = input_iter_->Next();
      if (input_data == nullptr) {
        ZETASQL_RETURN_IF_ERROR(input_iter_->Status());
        if (status_.ok()) {
          Finalize();
        }
        // Reached end of input.
        return false;
      }
      first_tuple_in_current_partition =
          std::make_unique<TupleData>(*input_data);
    } else {
      first_tuple_in_current_partition =
          std::move(first_tuple_in_next_partition_);
    }
    TupleData* first_tuple_in_current_partition_ptr =
        first_tuple_in_current_partition.get();
    absl::Status status;
    if (!current_input_partition_.PushBack(
            std::move(first_tuple_in_current_partition), &status)) {
      ZETASQL_RET_CHECK(!status.ok());
      return status;
    }

    // We have determined the first tuple of the next partition. Now load
    // the rest.
    while (true) {
      const TupleData* input_data = input_iter_->Next();
      if (input_data == nullptr) {
        ZETASQL_RETURN_IF_ERROR(input_iter_->Status());
        finished_last_partition_ = true;
        break;
      }

      const bool comparator_equals =
          !(*partition_comparator_)(*first_tuple_in_current_partition_ptr,
                                    *input_data) &&
          !(*partition_comparator_)(*input_data,
                                    *first_tuple_in_current_partition_ptr);
      if (!comparator_equals) {
        // We are done loading the current partition. 'input_data' belongs
        // in the next partition.
        first_tuple_in_next_partition_ =
            std::make_unique<TupleData>(*input_data);
        break;
      }
      // 'input_data' belongs in the current partition (which we are still
      // loading). Fail if we're unable to fit it.
      absl::Status status;
      if (!current_input_partition_.PushBack(
              std::make_unique<TupleData>(*input_data), &status)) {
        ZETASQL_RET_CHECK(!status.ok());
        return status;
      }
    }

    // Cache the tuple pointers.
    current_input_partition_ptrs_ = current_input_partition_.GetTuplePtrs();
    return true;
  }

  // Should be called after returning all rows.
  void Finalize() {
    if (!output_empty_) {
      // Partitioning by a floating point type is a non-deterministic
      // operation unless the output is empty.
      for (const KeyArg* key : partition_keys_) {
        if (key->type()->IsFloatingPoint()) {
          context_->SetNonDeterministicOutput();
        }
      }
    }
  }

  std::unique_ptr<TupleData> CopyTupleAndAddMatchResultValues(
      const TupleData* source_tuple, int match_id, int row_number,
      absl::string_view assigned_label) {
    int num_input_variables = input_iter_->Schema().num_variables();

    auto output_tuple = std::make_unique<TupleData>(
        num_input_variables + match_result_variables_.size());

    // Copy input values.
    for (int i = 0; i < num_input_variables; ++i) {
      output_tuple->mutable_slot(i)->SetValue(source_tuple->slot(i).value());
    }

    // Add match result values.
    output_tuple->mutable_slot(num_input_variables)
        ->SetValue(Value::Int64(match_id));
    output_tuple->mutable_slot(num_input_variables + 1)
        ->SetValue(Value::Int64(row_number));
    output_tuple->mutable_slot(num_input_variables + 2)
        ->SetValue(Value::String(assigned_label));
    // This is not a sentinel row.
    output_tuple->mutable_slot(num_input_variables + 3)
        ->SetValue(Value::Bool(false));

    return output_tuple;
  }

  // Every match ends in a sentinel row. This is used to detect empty matches
  // in the following op.
  // TODO: we should only need the partition keys. However, AggregateOp
  // currently precomputes values before applying the FILTER, so we cannot have
  // invalid slots until we improve that framework to avoid precomputating
  // values before applying the filter.
  std::unique_ptr<TupleData> MakeSentinelRow(int match_id) {
    int num_input_variables = input_iter_->Schema().num_variables();
    auto sentinel_tuple = std::make_unique<TupleData>(
        input_iter_->Schema().num_variables() + match_result_variables_.size());

    // Copy input values. Any tuple from the current partition will do.
    for (int i = 0; i < num_input_variables; ++i) {
      sentinel_tuple->mutable_slot(i)->SetValue(
          current_input_partition_ptrs_.front()->slot(i).value());
    }

    // Copy the partition keys from the first row in the partition
    // This is repeating the work above but leaving it here because when AggOp
    // is no longer precomputing values before FILTER, we can remove the above
    // code and supply partitioning keys from here.
    for (int p : slots_for_partition_keys_) {
      sentinel_tuple->mutable_slot(p)->SetValue(
          current_input_partition_ptrs_.front()->slot(p).value());
    }
    // Set the match id to seal the current match.
    sentinel_tuple->mutable_slot(num_input_variables)
        ->SetValue(Value::Int64(match_id));
    // Set the row number to invalid for the sentinel row.
    sentinel_tuple->mutable_slot(num_input_variables + 1)
        ->SetValue(Value::Invalid());
    // Label is invalid for the sentinel row, but the FILTER looking for the
    // assigned label (for scoped aggregates like max(A.x) which ranges over
    // assigned to `A`, will check this slot.
    sentinel_tuple->mutable_slot(num_input_variables + 2)
        ->SetValue(Value::NullString());
    // Most important: set `is_sentinel` to true.
    sentinel_tuple->mutable_slot(num_input_variables + 3)
        ->SetValue(Value::Bool(true));

    return sentinel_tuple;
  }

  // The iterator's main entry point.
  TupleData* Next() override {
    absl::Status status =
        PeriodicallyVerifyNotAborted(context_, num_next_calls_);
    if (!status.ok()) {
      status_ = status;
      return nullptr;
    }
    ++num_next_calls_;

    while (unconsumed_match_results_.IsEmpty()) {
      if (next_input_tuple_idx_ == current_input_partition_ptrs_.size()) {
        absl::StatusOr<bool> success = LoadNextPartition();
        if (!success.ok()) {
          status_ = success.status();
          return nullptr;
        }
        if (!*success) {
          // No more partitions to fetch. End of input.
          return nullptr;
        }
      }

      // Exhausted all matches found so far in the current partition, or we have
      // just started. The partition still has unconsumed input tuples. Keep
      // feeding them until we find matches, or reach the end of the partition.
      while (unconsumed_match_results_.IsEmpty() &&
             next_input_tuple_idx_ < current_input_partition_ptrs_.size()) {
        // We have loaded a partition and are consuming it.
        const TupleData* current_input_tuple =
            current_input_partition_ptrs_[next_input_tuple_idx_++];

        absl::Status status = EvaluatePredicatesOnRow(*current_input_tuple);
        if (!status.ok()) {
          status_ = status;
          return nullptr;
        }

        absl::StatusOr<MatchResult> match_result =
            current_match_partition_->AddRow(predicate_vals_);
        if (!match_result.ok()) {
          status_ = match_result.status();
          return nullptr;
        }

        if (next_input_tuple_idx_ == current_input_partition_ptrs_.size()) {
          // Exhausted the current partition. See if there are any other
          // unreturned matches.
          absl::StatusOr<MatchResult> finalized_match_result =
              current_match_partition_->Finalize();
          if (!finalized_match_result.ok()) {
            status_ = finalized_match_result.status();
            return nullptr;
          }
          match_result->new_matches.insert(
              match_result->new_matches.end(),
              finalized_match_result->new_matches.begin(),
              finalized_match_result->new_matches.end());
        }

        for (const Match& match : match_result->new_matches) {
          int idx = match.start_row_index;
          for (int assigned_label : match.pattern_vars_by_row) {
            if (!unconsumed_match_results_.PushBack(
                    CopyTupleAndAddMatchResultValues(
                        current_input_partition_ptrs_[idx], match.match_id, idx,
                        pattern_variable_names_[assigned_label]),
                    &status_)) {
              return nullptr;
            }
            ++idx;
          }

          // Add a sentinel row for this match (so that we can also detect empty
          // matches in the following op).
          if (!unconsumed_match_results_.PushBack(
                  MakeSentinelRow(match.match_id), &status_)) {
            return nullptr;
          }
        }
      }
    }

    ABSL_DCHECK(!unconsumed_match_results_.IsEmpty());
    output_empty_ = false;
    current_ = unconsumed_match_results_.PopFront();
    return current_.get();
  }

  absl::Status Status() const override { return status_; }

  std::string DebugString() const override {
    return PatternMatchingOp::GetIteratorDebugString(
        input_iter_->DebugString());
  }

 private:
  const std::vector<const TupleData*> params_;
  const std::vector<const KeyArg*> partition_keys_;
  const std::vector<int> slots_for_partition_keys_;
  const std::vector<VariableId> match_result_variables_;
  const std::vector<absl::string_view> pattern_variable_names_;
  const std::vector<const ExprArg*> predicates_;
  const CompiledPattern* pattern_;
  std::unique_ptr<TupleIterator> input_iter_;
  std::unique_ptr<TupleComparator> partition_comparator_;
  std::unique_ptr<TupleSchema> output_schema_;

  // The last tuple returned. NULL if Next() has never been called.
  std::unique_ptr<TupleData> current_;

  // The partition we are currently consuming, augmented by the values
  // of the analytic arguments. Empty if Next() has never been called.
  TupleDataDeque current_input_partition_;
  // The tuples in the current input partition. Needed because the same row may
  // appear in multiple matches, and we may need to fetch all the way back.
  // This always matches the tuples held in 'current_input_partition_'.
  std::vector<const TupleData*> current_input_partition_ptrs_;

  // Index of the next tuple to read from current_input_partition_.
  int next_input_tuple_idx_ = 0;

  // True if 'current_partition_' is the last one.
  bool finished_last_partition_ = false;
  bool output_empty_ = true;

  // Represents the current match partition.
  std::unique_ptr<MatchPartition> current_match_partition_;
  // Holds the value of each predicate on the current row to be passed to the
  // current MatchPartition.
  std::vector<bool> predicate_vals_;

  // Sometimes we discover several matches after inspecting 1 input row.
  // If we haven't returned them all, we need to keep reading from this until
  // it's empty.
  TupleDataDeque unconsumed_match_results_;

  // NULL if we haven't loaded any partitions yet or 'is_last_partition_' is
  // true.
  std::unique_ptr<TupleData> first_tuple_in_next_partition_;
  EvaluationContext* context_;
  absl::Status status_;
  int64_t num_next_calls_ = 0;
};
}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>>
PatternMatchingOp::CreateIterator(absl::Span<const TupleData* const> params,
                                  int num_extra_slots,
                                  EvaluationContext* context) const {
  // Add 4 slots for:
  // * Match number
  // * Row number
  // * Assigned label
  // * is_sentinel
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<TupleIterator> iter,
      input()->CreateIterator({params}, 4 + num_extra_slots, context));

  std::vector<int> slots_for_partition_keys;
  slots_for_partition_keys.reserve(partition_keys().size());
  for (const KeyArg* partition_key : partition_keys()) {
    std::optional<int> slot =
        iter->Schema().FindIndexForVariable(partition_key->variable());
    ZETASQL_RET_CHECK(slot.has_value())
        << "Could not find variable " << partition_key->variable()
        << " in schema " << iter->Schema().DebugString();
    slots_for_partition_keys.push_back(slot.value());
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<TupleComparator> partition_comparator,
      TupleComparator::Create(partition_keys(), slots_for_partition_keys,
                              params, context));

  iter = std::make_unique<PatternMatchingTupleIterator>(
      params, partition_keys(), slots_for_partition_keys,
      match_result_variables_, pattern_variable_names_, predicates(),
      pattern_.get(), std::move(iter), std::move(partition_comparator),
      CreateOutputSchema(), context);

  return MaybeReorder(std::move(iter), context);
}

std::unique_ptr<TupleSchema> PatternMatchingOp::CreateOutputSchema() const {
  std::unique_ptr<TupleSchema> input_schema = input()->CreateOutputSchema();

  // Partition keys are already in the input schema. Now we need to add 3
  // variables for the match results:
  // * Match number
  // * Row number
  // * Assigned label
  // * Is sentinel row
  std::vector<VariableId> variables = input_schema->variables();
  variables.reserve(variables.size() + 4);
  for (const VariableId& match_result_variable : match_result_variables_) {
    variables.push_back(match_result_variable);
  }
  return std::make_unique<TupleSchema>(variables);
}

std::string PatternMatchingOp::IteratorDebugString() const {
  return GetIteratorDebugString(input()->IteratorDebugString());
}

std::string PatternMatchingOp::DebugInternal(const std::string& indent,
                                             bool verbose) const {
  return absl::StrCat("PatternMatchingOp(",
                      ArgDebugString({"input", "partition_keys", "predicates"},
                                     {k1, kN, kN}, indent, verbose),
                      ",\n", indent, "pattern=(", pattern_->DebugString(), ")",
                      "\n)");
}

PatternMatchingOp::PatternMatchingOp(
    std::vector<std::unique_ptr<KeyArg>> partition_keys,
    std::vector<VariableId> match_result_variables,
    std::vector<std::string> pattern_variable_names,
    std::vector<std::unique_ptr<ValueExpr>> predicates,
    std::unique_ptr<const CompiledPattern> pattern,
    std::unique_ptr<RelationalOp> input)
    : match_result_variables_(std::move(match_result_variables)),
      pattern_variable_names_(std::move(pattern_variable_names)),
      pattern_(std::move(pattern)) {
  SetArg(kInput, std::make_unique<RelationalArg>(std::move(input)));
  SetArgs<KeyArg>(kPartitionKey, std::move(partition_keys));
  SetArgs<ExprArg>(kPredicate, MakeExprArgList(std::move(predicates)));
}

absl::Span<const KeyArg* const> PatternMatchingOp::partition_keys() const {
  return GetArgs<KeyArg>(kPartitionKey);
}

absl::Span<KeyArg* const> PatternMatchingOp::mutable_partition_keys() {
  return GetMutableArgs<KeyArg>(kPartitionKey);
}

absl::Span<const ExprArg* const> PatternMatchingOp::predicates() const {
  return GetArgs<ExprArg>(kPredicate);
}

absl::Span<ExprArg* const> PatternMatchingOp::mutable_predicates() {
  return GetMutableArgs<ExprArg>(kPredicate);
}

const RelationalOp* PatternMatchingOp::input() const {
  return GetArg(kInput)->node()->AsRelationalOp();
}

RelationalOp* PatternMatchingOp::mutable_input() {
  return GetMutableArg(kInput)->mutable_node()->AsMutableRelationalOp();
}

}  // namespace zetasql
