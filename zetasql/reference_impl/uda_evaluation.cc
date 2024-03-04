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

#include "zetasql/reference_impl/uda_evaluation.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/functions/differential_privacy.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/reference_impl/type_helpers.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/serialization.pb.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

class UserDefinedAggregateFunctionEvaluator
    : public AggregateFunctionEvaluator {
 public:
  UserDefinedAggregateFunctionEvaluator(
      std::unique_ptr<RelationalOp> algebrized_tree,
      std::vector<std::string> argument_names,
      std::vector<bool> argument_is_aggregate)
      : algebrized_tree_(std::move(algebrized_tree)),
        argument_names_(std::move(argument_names)),
        argument_is_aggregate_(std::move(argument_is_aggregate)) {}
  ~UserDefinedAggregateFunctionEvaluator() override = default;

  void SetEvaluationContext(EvaluationContext* context) override {
    eval_context_ = context;
  }

  absl::Status Reset() override {
    memory_accountant_ = std::make_unique<MemoryAccountant>(
        EvaluationOptions().max_intermediate_byte_size);
    inputs_ = std::make_unique<TupleDataDeque>(memory_accountant_.get());
    return absl::OkStatus();
  }

  absl::Status Accumulate(absl::Span<const Value*> args,
                          bool* stop_accumulation) override {
    // Accumulate each input row into `inputs` for use in GetFinalResult().
    std::vector<TupleSlot> tuple_slots;
    for (const Value* arg : args) {
      TupleSlot tuple_slot;
      tuple_slot.SetValue(*arg);
      tuple_slots.push_back(std::move(tuple_slot));
    }
    absl::Status status;
    bool ok = inputs_->PushBack(
        std::make_unique<TupleData>(std::move(tuple_slots)), &status);
    ZETASQL_RET_CHECK(ok == status.ok());
    return status;
  }

  absl::StatusOr<Value> GetFinalResult() override {
    ZETASQL_RET_CHECK(eval_context_ != nullptr)
        << "UserDefinedAggregateFunctionEvaluator must have EvaluationContext "
        << "set before calling GetFinalResult().";
    // Create a local context to evaluate the UDA function body on the
    // accumulated rows.
    std::unique_ptr<EvaluationContext> local_context =
        eval_context_->MakeChildContext();
    local_context->set_active_group_rows(inputs_.get());

    if (!inputs_->IsEmpty()) {
      auto first_row = inputs_->GetTuplePtrs()[0];
      for (int i = 0; i < argument_names_.size(); ++i) {
        // NOT_AGGREGATE arguments should be mapped by value since they are
        // represented by a FunctionArgumentRefExpr. These arguments have a
        // constant value for each grouped rows, so we can just add them once.
        if (!local_context->HasFunctionArgumentRef(argument_names_[i]) &&
            !argument_is_aggregate_[i]) {
          ZETASQL_RETURN_IF_ERROR(local_context->AddFunctionArgumentRef(
              argument_names_[i], first_row->slot(i).value()));
        }
      }
    }

    ZETASQL_RETURN_IF_ERROR(
        algebrized_tree_->SetSchemasForEvaluation(/*params_schemas=*/{}));
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<TupleIterator> iter,
        algebrized_tree_->Eval(/*params=*/{},
                               /*num_extra_slots=*/0, local_context.get()));
    Value result;
    while (true) {
      const TupleData* next_input = iter->Next();
      if (next_input == nullptr) {
        ZETASQL_RETURN_IF_ERROR(iter->Status());
        break;
      }
      ZETASQL_RET_CHECK_GE(next_input->num_slots(), 1);
      // ComputeOp stores the result of the function expression evaluation
      // in the last slot.
      result = next_input->slot(next_input->num_slots() - 1).value();
    }
    return result;
  }

 private:
  std::unique_ptr<RelationalOp> algebrized_tree_;
  std::vector<std::string> argument_names_;
  std::vector<bool> argument_is_aggregate_;
  std::unique_ptr<MemoryAccountant> memory_accountant_;
  std::unique_ptr<TupleDataDeque> inputs_;
  EvaluationContext* eval_context_;
};

std::unique_ptr<AggregateFunctionEvaluator>
MakeUserDefinedAggregateFunctionEvaluator(
    std::unique_ptr<RelationalOp> algebrized_tree,
    std::vector<std::string> argument_names,
    std::vector<bool> argument_is_aggregate) {
  return std::make_unique<UserDefinedAggregateFunctionEvaluator>(
      std::move(algebrized_tree), std::move(argument_names),
      std::move(argument_is_aggregate));
}

}  // namespace zetasql
