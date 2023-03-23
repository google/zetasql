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

#ifndef ZETASQL_ANALYZER_ANALYZER_OUTPUT_MUTATOR_H_
#define ZETASQL_ANALYZER_ANALYZER_OUTPUT_MUTATOR_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/common/timer_util.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/proto/logging.pb.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"

namespace zetasql {
// Helper to allow mutating AnalyzerOutput.
class AnalyzerOutputMutator {
 public:
  // 'column_factory' and 'output' must outlive AnalyzerOutputMutator.
  explicit AnalyzerOutputMutator(AnalyzerOutput* output) : output_(*output) {}
  explicit AnalyzerOutputMutator(const std::unique_ptr<AnalyzerOutput>& output)
      : output_(*output) {}

  // Updates the output with the new ResolvedNode (and new max column id).
  absl::Status Update(std::unique_ptr<const ResolvedNode> node,
                      zetasql_base::SequenceNumber& column_id_seq_num) {
    ZETASQL_RET_CHECK(node != nullptr);
    output_.max_column_id_ = static_cast<int>(column_id_seq_num.GetNext() - 1);
    if (node->IsStatement()) {
      output_.statement_.reset(node.release()->GetAs<ResolvedStatement>());
      return absl::OkStatus();
    }
    if (node->IsExpression()) {
      output_.expr_.reset(node.release()->GetAs<ResolvedExpr>());
      return absl::OkStatus();
    }
    return absl::Status(
        absl::StatusCode::kInternal,
        absl::StrCat("Unexpected new node type: ", node->node_kind_string()));
  }

  AnalyzerOutputProperties& mutable_output_properties() {
    return output_.analyzer_output_properties_;
  }

  std::unique_ptr<const ResolvedNode> release_output_node() {
    if (output_.statement_ != nullptr) {
      return std::move(output_.statement_);
    } else {
      return std::move(output_.expr_);
    }
  }

  AnalyzerRuntimeInfo& mutable_runtime_info() { return output_.runtime_info_; }

  internal::TimedValue& overall_timed_value() {
    return mutable_runtime_info().impl_->overall_timed_value;
  }
  internal::TimedValue& resolver_timed_value() {
    return mutable_runtime_info().impl_->resolver_timed_value;
  }
  internal::TimedValue& validator_timed_value() {
    return mutable_runtime_info().impl_->validator_timed_value;
  }
  internal::TimedValue& rewriters_timed_value() {
    return mutable_runtime_info().impl_->rewriters_timed_value;
  }

  static absl::StatusOr<std::unique_ptr<const AnalyzerOutput>>
  FinalizeAnalyzerOutput(std::unique_ptr<AnalyzerOutput> output);

 private:
  AnalyzerOutput& output_;
};
}  // namespace zetasql
#endif  // ZETASQL_ANALYZER_ANALYZER_OUTPUT_MUTATOR_H_
