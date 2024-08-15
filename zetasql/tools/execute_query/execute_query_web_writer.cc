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

#include "zetasql/tools/execute_query/execute_query_web_writer.h"

#include <memory>
#include <utility>
#include <vector>

#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/tools/execute_query/output_query_result.h"
#include "absl/status/status.h"
#include "external/mstch/mstch/include/mstch/mstch.hpp"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

// Prints the result of executing a query as an HTML table.
absl::Status RenderResultsAsTable(std::unique_ptr<EvaluatorTableIterator> iter,
                                  mstch::map &table_params) {
  std::vector<mstch::node> columnNames;
  columnNames.reserve(iter->NumColumns());
  for (int i = 0; i < iter->NumColumns(); ++i) {
    columnNames.push_back(iter->GetColumnName(i));
  }

  std::vector<mstch::node> rows;
  while (iter->NextRow()) {
    std::vector<mstch::node> row_values;
    row_values.reserve(iter->NumColumns());
    for (int i = 0; i < iter->NumColumns(); ++i) {
      row_values.push_back(zetasql::ValueToOutputString(
          iter->GetValue(i), /*escape_strings=*/false));
    }
    rows.push_back(std::move(row_values));
  }
  table_params = mstch::map({
      {"columnNames", mstch::node(std::move(columnNames))},
      {"rows", mstch::node(std::move(rows))},
  });
  return iter->Status();
}

}  // namespace

absl::Status ExecuteQueryWebWriter::executed(
    const ResolvedNode &ast, std::unique_ptr<EvaluatorTableIterator> iter) {
  current_statement_params_["result_executed"] = true;

  mstch::map table_params;
  ZETASQL_RETURN_IF_ERROR(RenderResultsAsTable(std::move(iter), table_params));
  current_statement_params_["result_executed_table"] = std::move(table_params);
  got_results_ = true;
  return absl::OkStatus();
}

absl::Status ExecuteQueryWebWriter::ExecutedExpression(const ResolvedNode &ast,
                                                       const Value &value) {
  current_statement_params_["result_executed"] = true;
  current_statement_params_["result_executed_text"] =
      OutputPrettyStyleExpressionResult(value, /*include_box=*/false);
  got_results_ = true;
  return absl::OkStatus();
}

}  // namespace zetasql
