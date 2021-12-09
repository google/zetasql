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

#include "zetasql/analyzer/column_cycle_detector.h"

#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/id_string.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

ColumnCycleDetector::~ColumnCycleDetector() { ZETASQL_DCHECK(visiting_stack_.empty()); }

absl::Status ColumnCycleDetector::VisitNewColumn(const IdString& column) {
  ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(&visiting_, column));
  ZETASQL_RET_CHECK(!current_column().has_value() ||
            edges_[current_column().value()].contains(column))
      << "Current column " << current_column().value()
      << " is not present in edges";
  visiting_stack_.push_back(column);
  return absl::OkStatus();
}

absl::Status ColumnCycleDetector::FinishCurrentColumn() {
  // Remove from 'visiting_' and 'visiting_stack_'.
  ZETASQL_RET_CHECK(current_column().has_value());
  auto it = visiting_.find(current_column().value());
  ZETASQL_RET_CHECK(it != visiting_.end())
      << "Column not found: " << current_column().value();
  visiting_.erase(it);

  ZETASQL_RET_CHECK(!visiting_stack_.empty());
  visiting_stack_.pop_back();
  return absl::OkStatus();
}

absl::Status ColumnCycleDetector::AddDependencyOn(const IdString& column) {
  ZETASQL_RET_CHECK(current_column().has_value());

  absl::flat_hash_set<IdString, IdStringHash>& adj_edges =
      edges_[current_column().value()];
  if (!zetasql_base::InsertIfNotPresent(&adj_edges, column)) {
    // Edge was already accounted for. See comments on 'edges_'.
    return absl::OkStatus();
  }

  // Are we introducing a cycle?
  if (visiting_.contains(column)) {
    std::string message;
    if (visiting_.size() == 1) {
      // This is a self-recursive object, so return a custom error message.
      absl::StrAppend(&message, "The column ", current_column()->ToString(),
                      " is recursive");
    } else {
      // The first element on visiting_stack_ is an empty IdString, so we skip
      // it.
      absl::StrAppend(
          &message, "Recursive dependencies detected when resolving column ",
          visiting_stack_[0].ToString(), ", which include objects (",
          absl::StrJoin(visiting_stack_.begin(), visiting_stack_.end(), ", ",
                        [](std::string* out, const IdString& id) {
                          absl::StrAppend(out, id.ToString());
                        }),
          ")");
    }
    return MakeSqlErrorAt(ast_node_) << message;
  }
  return absl::OkStatus();
}

absl::optional<IdString> ColumnCycleDetector::current_column() const {
  if (visiting_stack_.empty()) {
    return absl::optional<IdString>();
  } else {
    return visiting_stack_.back();
  }
}

}  // namespace zetasql
