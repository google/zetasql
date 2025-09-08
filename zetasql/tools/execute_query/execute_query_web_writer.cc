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
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/tools/execute_query/output_query_result.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "external/mstch/mstch/include/mstch/mstch.hpp"
#include "re2/re2.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

// Prints the result of executing a query as an HTML table.
absl::Status RenderResultsAsTable(std::unique_ptr<EvaluatorTableIterator> iter,
                                  mstch::map &table_params) {
  std::vector<mstch::node> columnNames;
  columnNames.reserve(iter->NumColumns() + 1);
  columnNames.push_back(std::string("#"));
  for (int i = 0; i < iter->NumColumns(); ++i) {
    columnNames.push_back(iter->GetColumnName(i));
  }

  std::vector<mstch::node> rows;
  int row_num = 1;
  while (iter->NextRow()) {
    std::vector<mstch::node> row_values;
    row_values.reserve(iter->NumColumns() + 1);
    row_values.push_back(absl::StrCat(row_num++));
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

std::string DecorateASTDebugStringWithHTMLTags(std::string ast_debug_string) {
  // First HTML-escape the string, since the AST tree is rendered without
  // auto-escaping in the template processor.
  absl::StrReplaceAll({{"&", "&amp;"}, {"<", "&lt;"}, {">", "&gt;"}},
                      &ast_debug_string);

  // Make all the node names bold.
  // The regex matches node names that appear on their own line and are prefixed
  // by some tree characters. The (?m) enables multi-line mode so that the ^ in
  // the capturing group matches the start of each line separately.
  static LazyRE2 kNodeName = {R"re((?m)(^[ |+-]*)([A-Z][A-Za-z]*))re"};
  RE2::GlobalReplace(&ast_debug_string, *kNodeName,
                     R"html(\1<span class="ast-node">\2</span>)html");

  // Wrap all column IDs in a span, so that they can be highlighted on hover.
  // If the Column ID is `col1#3`, the two capturing groups are `col1` and `3`
  // and the span will have the class `col1_3`
  static LazyRE2 kColumnId = {R"re((\$?[A-Za-z0-9_]+)#(\d+))re"};
  RE2::GlobalReplace(&ast_debug_string, *kColumnId,
                     R"html(<span class="ast-col \1_\2">\0</span>)html");

  // For generated columns that start with a dollar-sign, the above replacement
  // would have resulted in an invalid class name, since $ is not allowed in
  // class names. So we fix that separately.
  absl::StrReplaceAll({{R"html(<span class="ast-col $)html",
                        R"html(<span class="ast-col dollar_)html"}},
                      &ast_debug_string);

  return ast_debug_string;
}

}  // namespace

absl::Status ExecuteQueryWebWriter::resolved(const ResolvedNode& ast) {
  // The result_analyzed string contains HTML, so the template contains
  // `result_analyzed` in a triple mustache to disable HTML escaping.
  // We make sure that the string is HTML-escaped before inserting it into the
  // template.
  current_statement_params_["result_analyzed"] =
      DecorateASTDebugStringWithHTMLTags(ast.DebugString());
  got_results_ = true;
  return absl::OkStatus();
}

absl::Status ExecuteQueryWebWriter::executed(
    const ResolvedNode& ast, std::unique_ptr<EvaluatorTableIterator> iter) {
  current_statement_params_["result_executed"] = true;

  mstch::array tables;
  mstch::map result_params;
  mstch::map table_params;
  ZETASQL_RETURN_IF_ERROR(RenderResultsAsTable(std::move(iter), table_params));
  result_params["table"] = std::move(table_params);
  tables.push_back(std::move(result_params));
  current_statement_params_["result_executed_tables"] = std::move(tables);
  got_results_ = true;
  return absl::OkStatus();
}

absl::Status ExecuteQueryWebWriter::executed_multi(
    const ResolvedNode& ast,
    std::vector<absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>>
        results) {
  current_statement_params_["result_executed"] = true;

  mstch::array tables;
  for (auto& result : results) {
    mstch::map result_params;
    if (result.ok()) {
      mstch::map table_params;
      ZETASQL_RETURN_IF_ERROR(RenderResultsAsTable(std::move(*result), table_params));
      result_params["table"] = std::move(table_params);
    } else {
      result_params["error"] = std::string(result.status().message());
    }
    tables.push_back(std::move(result_params));
  }
  current_statement_params_["result_executed_tables"] = std::move(tables);
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
