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

#ifndef ZETASQL_COMMON_EVALUATOR_TEST_TABLE_H_
#define ZETASQL_COMMON_EVALUATOR_TEST_TABLE_H_

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/simple_evaluator_table_iterator.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "zetasql/base/status.h"
#include "zetasql/base/clock.h"

namespace zetasql {

// A SimpleTable that returns an EvaluatorTestTableIterator.
class EvaluatorTestTable : public SimpleTable {
 public:
  // 'values[i][j]' is the value of 'columns[j]' in the i-th row.
  EvaluatorTestTable(
      const std::string& name,
      const std::vector<std::pair<std::string, const Type*>>& columns,
      const std::vector<std::vector<Value>>& values,
      const absl::Status& end_status,
      const absl::flat_hash_set<int>& column_filter_idxs = {},
      const std::function<void()>& cancel_cb = [] {},
      const std::function<void(absl::Time)>& set_deadline_cb =
          [](absl::Time deadline) {},
      zetasql_base::Clock* clock = zetasql_base::Clock::RealClock())
      : SimpleTable(name, columns),
        end_status_(end_status),
        column_filter_idxs_(column_filter_idxs),
        cancel_cb_(cancel_cb),
        set_deadline_cb_(set_deadline_cb),
        clock_(clock) {
    SetContents(values);
  }

  EvaluatorTestTable(const EvaluatorTestTable&) = delete;
  EvaluatorTestTable& operator=(const EvaluatorTestTable&) = delete;

  absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
  CreateEvaluatorTableIterator(
      absl::Span<const int> column_idxs) const override {
    std::vector<const Column*> columns;
    std::vector<std::shared_ptr<const std::vector<Value>>> column_values;
    column_values.reserve(column_idxs.size());
    absl::flat_hash_set<int> scan_column_filter_idxs;
    for (int i = 0; i < column_idxs.size(); ++i) {
      const int column_idx = column_idxs[i];
      columns.push_back(GetColumn(column_idx));
      column_values.push_back(column_major_contents()[column_idx]);

      if (column_filter_idxs_.contains(column_idx)) {
        ZETASQL_RET_CHECK(scan_column_filter_idxs.insert(i).second);
      }
    }

    return absl::make_unique<SimpleEvaluatorTableIterator>(
        columns, column_values, num_rows(), end_status_,
        scan_column_filter_idxs, cancel_cb_, set_deadline_cb_, clock_);
  }

 private:
  const absl::Status end_status_;
  const absl::flat_hash_set<int> column_filter_idxs_;
  const std::function<void()> cancel_cb_;
  const std::function<void(absl::Time)> set_deadline_cb_;
  zetasql_base::Clock* clock_;
};

}  // namespace zetasql

#endif  // ZETASQL_COMMON_EVALUATOR_TEST_TABLE_H_
