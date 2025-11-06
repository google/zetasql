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
#include "mstch/mstch.hpp"
#include "re2/re2.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

// Prints the result of executing a query as an HTML table.
absl::Status RenderResultsAsTable(std::unique_ptr<EvaluatorTableIterator> iter,
                                  mstch::map& table_params) {
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
  // by some tree characters. It captures both nodes that are printed as a
  // multi-line debug string, and nodes that are printed on a single line and
  // are followed by an open parenthesis.
  //
  // The (?m) enables multi-line mode so that the ^ in the capturing group
  // matches the start of each line separately.
  // The first capturing group is (^[ |+-]*) which captures the ASCII tree
  // characters that precede the node name.
  // The second capturing group is ([A-Z][A-Za-z]*) which captures the node
  // name.
  // The third capturing group is (\(|$) which matches the end of the
  // string or an open parenthesis, depending on whether the node name is
  // printed on a single line or multiple lines. The interior of the capturing
  // group is an alternation | between an escaped open parenthesis \( and an
  // end-of-line indicator $.

  // Matches:
  // +-LimitOffsetScan
  // | +-OrderByScan
  // +-FunctionCall(ZetaSQL:$and(BOOL,  BOOL) -> BOOL)
  //
  // Does not match:
  // +-Supplier.S_ACCTBAL#15 -> Table name
  //
  // The string is replaced with the captured ASCII tree characters and the node
  // name wrapped in a span with the class `ast-node`.
  static LazyRE2 kNodeName = {R"re((?m)(^[ |+-]*)([A-Z][A-Za-z]*)(\(|$))re"};
  RE2::GlobalReplace(&ast_debug_string, *kNodeName,
                     R"html(\1<span class="ast-node">\2</span>\3)html");

  // The following adds span tags to allow column IDs to be highlighted on
  // hover. There are two levels of spans added:
  //
  // 1) Created columns that are marked with a `{c}` are wrapped in a span with
  // class `ast-col-src`. The `{c}` marker is enabled by the
  // DebugStringConfig.print_created_columns option.
  //
  // 2) All column IDs are wrapped in a span with class `ast-col`. These two
  // together allow us to define CSS rules to highlight these columns.

  // Created columns come in two formats. One is `<columnID>{c} :=`. The
  // columnID is wrapped in an `ast-col-src` span here.
  //
  // Original:
  //   +-A#7{c} := ColumnRef(type=ARRAY<INT64>, column=$aggregate.$agg1#6)
  // Modified:
  //   +-<span class="ast-col-src">A#7</span> := ColumnRef(...)
  static LazyRE2 kColumnIdSrc_1 = {R"re(((\$?[A-Za-z0-9_]+)#(\d+)){c} :=)re"};
  RE2::GlobalReplace(&ast_debug_string, *kColumnIdSrc_1,
                     R"html(<span class="ast-col-src">\1</span> :=)html");

  // The other format is `{c}=...` where the trailing substring can contain
  // multiple created columns. The entire line after = is wrapped in an
  // `ast-col-src` span.
  //
  // clang-format off
  // Original:
  //   +-TableScan(column_list{c}=[KeyValue.Key#1], table=KeyValue, column_index_list=[0])
  // Modified:
  //   +-TableScan(column_list=<span class="ast-col-src">[KeyValue.Key#1], table=KeyValue, column_index_list=[0])</span>
  // clang-format on
  //
  static LazyRE2 kColumnIdSrc_2 = {R"re({c}=(.*))re"};
  RE2::GlobalReplace(&ast_debug_string, *kColumnIdSrc_2,
                     R"html(=<span class="ast-col-src">\1</span>)html");

  // Then we identify columnIDs everywhere and wrap them each in an `ast-col`
  // span.
  //
  // Original:
  //   +-column_list=[KeyValue.Key#1]
  // Modified:
  //  +-column_list=[KeyValue.<span class="ast-col Key_1">Key#1</span>]
  static LazyRE2 kColumnId = {R"re((\$?[A-Za-z0-9_]+)#(\d+))re"};
  RE2::GlobalReplace(&ast_debug_string, *kColumnId,
                     R"html(<span class="ast-col \1_\2">\0</span>)html");

  // For generated columns that start with a dollar-sign, the above replacement
  // would have resulted in an invalid class name, since $ is not allowed in
  // class names. So we fix that separately.
  absl::StrReplaceAll({{R"html(ast-col $)html", R"html(ast-col dollar_)html"}},
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
      DecorateASTDebugStringWithHTMLTags(ast.DebugString(
          ResolvedNode::DebugStringConfig{.print_created_columns = true}));
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

absl::Status ExecuteQueryWebWriter::ExecutedExpression(const ResolvedNode& ast,
                                                       const Value& value) {
  current_statement_params_["result_executed"] = true;
  current_statement_params_["result_executed_text"] =
      OutputPrettyStyleExpressionResult(value, /*include_box=*/false);
  got_results_ = true;
  return absl::OkStatus();
}

}  // namespace zetasql
