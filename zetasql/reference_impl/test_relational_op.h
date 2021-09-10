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

// A RelationalOp for use in unit tests.
#ifndef ZETASQL_REFERENCE_IMPL_TEST_RELATIONAL_OP_H_
#define ZETASQL_REFERENCE_IMPL_TEST_RELATIONAL_OP_H_

#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/tuple_test_util.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace zetasql {

class TestRelationalOp : public RelationalOp {
 public:
  TestRelationalOp(const std::vector<VariableId>& variables,
                   const std::vector<TupleData>& values, bool preserves_order)
      : variables_(variables),
        values_(values),
        preserves_order_(preserves_order) {}

  TestRelationalOp(const TestRelationalOp&) = delete;
  TestRelationalOp& operator=(const TestRelationalOp&) = delete;

  absl::Status SetSchemasForEvaluation(
      absl::Span<const TupleSchema* const> params_schemas) override {
    // Eval() ignores the parameters.
    return absl::OkStatus();
  }

  absl::StatusOr<std::unique_ptr<TupleIterator>> CreateIterator(
      absl::Span<const TupleData* const> /*params*/, int num_extra_slots,
      EvaluationContext* context) const override {
    std::vector<TupleData> iter_values = values_;
    for (TupleData& data : iter_values) {
      ZETASQL_RET_CHECK_EQ(data.num_slots(), variables_.size());
      data.AddSlots(num_extra_slots);
    }

    std::unique_ptr<TupleIterator> iter = absl::make_unique<TestTupleIterator>(
        variables_, iter_values, preserves_order_,
        /*end_status=*/absl::OkStatus());
    return iter;
  }

  std::unique_ptr<TupleSchema> CreateOutputSchema() const override {
    return absl::make_unique<TupleSchema>(variables_);
  }

  std::string IteratorDebugString() const override {
    return TestTupleIterator::GetDebugString();
  }

  std::string DebugInternal(const std::string& indent,
                            bool verbose) const override {
    return absl::StrCat("TestRelationalOp");
  }

 private:
  const std::vector<VariableId> variables_;
  const std::vector<TupleData> values_;
  const bool preserves_order_;
};

}  // namespace zetasql

#endif  // ZETASQL_REFERENCE_IMPL_TEST_RELATIONAL_OP_H_
