//
// Copyright 2019 ZetaSQL Authors
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

#include "zetasql/experimental/output_query_result.h"

#include "zetasql/compliance/type_helpers.h"
#include "zetasql/public/strings.h"

namespace zetasql {

namespace {

::zetasql_base::StatusOr<const Table*> GetTableForDMLStatement(
    const ResolvedStatement* resolved_stmt) {
  const ResolvedTableScan* scan = nullptr;
  switch (resolved_stmt->node_kind()) {
    case RESOLVED_DELETE_STMT:
      scan = resolved_stmt->GetAs<ResolvedDeleteStmt>()->table_scan();
      break;
    case RESOLVED_UPDATE_STMT:
      scan = resolved_stmt->GetAs<ResolvedUpdateStmt>()->table_scan();
      break;
    case RESOLVED_INSERT_STMT:
      scan = resolved_stmt->GetAs<ResolvedInsertStmt>()->table_scan();
      break;
    case RESOLVED_MERGE_STMT:
      scan = resolved_stmt->GetAs<ResolvedMergeStmt>()->table_scan();
      break;
    default:
      ZETASQL_RET_CHECK_FAIL()
          << "GetTableForDMLStatement() does not support node kind "
          << ResolvedNodeKind_Name(resolved_stmt->node_kind());
  }

  return scan->table();
}

// Populates the following, assuming 'resolved_stmt' is a statement,
// 'algebrized_stmt' is the corresponding algebrized ValueExpr, and 'value' is
// the corresponding result:
// - 'num_rows_modified' with the number of rows modified
//   (only set for DML statements).
// - 'is_value_table' with true if 'resolved_stmt' operates on a value table.
// - 'result_table' with the array representing the output of a query, or the
//   full contents of the modified table after a DML statement.
// - 'column_names' with the names of the columns of the rows in 'result_table'.
::zetasql_base::Status GetOutputColumnInfo(const ResolvedStatement* resolved_stmt,
                                   const ValueExpr* algebrized_stmt,
                                   const Value& result,
                                   absl::optional<int64_t>* num_rows_modified,
                                   bool* is_value_table, Value* result_table,
                                   std::vector<std::string>* column_names) {
  switch (resolved_stmt->node_kind()) {
    case RESOLVED_QUERY_STMT: {
      const ResolvedQueryStmt* resolved_query =
          resolved_stmt->GetAs<ResolvedQueryStmt>();
      *is_value_table = resolved_query->is_value_table();
      *result_table = result;
      for (const auto& output_column : resolved_query->output_column_list()) {
        column_names->push_back(output_column->name());
      }
      break;
    }
    case RESOLVED_DELETE_STMT:
    case RESOLVED_UPDATE_STMT:
    case RESOLVED_INSERT_STMT:
    case RESOLVED_MERGE_STMT: {
      ZETASQL_RET_CHECK(result.type()->IsStruct());
      const StructType* result_type = result.type()->AsStruct();

      ZETASQL_RET_CHECK_EQ(2, result_type->num_fields());
      ZETASQL_RET_CHECK_EQ(kDMLOutputNumRowsModifiedColumnName,
                   result_type->field(0).name);
      ZETASQL_RET_CHECK_EQ(kDMLOutputAllRowsColumnName, result_type->field(1).name);

      *num_rows_modified = result.field(0).int64_value();
      *result_table = result.field(1);
      ZETASQL_RET_CHECK(result_table->type()->IsArray());

      ZETASQL_ASSIGN_OR_RETURN(const Table* table,
                       GetTableForDMLStatement(resolved_stmt));
      *is_value_table = table->IsValueTable();

      const Type* element_type =
          result_table->type()->AsArray()->element_type();
      if (*is_value_table) {
        column_names->push_back("value");
      } else {
        ZETASQL_RET_CHECK(element_type->IsStruct());
        for (const StructField& field : element_type->AsStruct()->fields()) {
          column_names->push_back(field.name);
        }
      }
      break;
    }
    default:
      ZETASQL_RET_CHECK_FAIL()
          << "GetOutputColumnInfo() does not support resolved node kind "
          << ResolvedNodeKind_Name(resolved_stmt->node_kind());
  }
  return zetasql_base::OkStatus();
}

// Helper function to produce a std::string representation of a 'row',
// given the input column std::string values and column buffer lengths.
std::string GenerateRowStringFromColumns(
    const std::vector<std::string>& column_strings,
    const std::vector<size_t>& column_buffer_lengths) {
  CHECK_EQ(column_strings.size(), column_buffer_lengths.size());
  std::string row_string = "|";
  for (int col_idx = 0; col_idx < column_strings.size(); ++col_idx) {
    // One space is added before and after the value.  The value is
    // left-justified, and padded to the related column_buffer_lengths value.
    absl::StrAppend(&row_string, " ");
    absl::StrAppend(&row_string, column_strings[col_idx]);
    int pad_size = static_cast<int>(column_buffer_lengths[col_idx] -
                                    column_strings[col_idx].length());
    // The column_buffer_lengths are the max lengths of all the column
    // values for each column, so this column length should not be bigger
    // than the related max length.
    CHECK_GE(pad_size, 0);
    if (pad_size > 0) {
      absl::StrAppend(&row_string, std::string(pad_size, ' '));
    }
    absl::StrAppend(&row_string, " |");
  }
  absl::StrAppend(&row_string, "\n");
  return row_string;
}

// Converts 'value' to an output std::string and returns it.
std::string ValueToOutputString(const Value& value) {
  if (value.is_null()) return "NULL";
  if (value.type()->IsStruct()) {
    return absl::StrCat(
        "{",
        absl::StrJoin(value.fields(), ", ",
                      [](std::string* out, const zetasql::Value& value) {
                        absl::StrAppend(out, ValueToOutputString(value));
                      }),
        "}");
  } else if (value.type()->IsArray()) {
    return absl::StrCat(
        "[",
        absl::StrJoin(value.elements(), ", ",
                      [](std::string* out, const zetasql::Value& value) {
                        absl::StrAppend(out, ValueToOutputString(value));
                      }),
        "]");
  } else if (value.type()->IsBytes()) {
    return EscapeBytes(value.bytes_value());
  } else if (value.type()->IsString()) {
    return value.string_value();
  } else {
    return value.DebugString();
  }
}

// Returns a std::string to separate rows in an output table.  A "+" indicates
// a column boundary, and "-" appears above or below a column.
//
// Example:
// +------+----+-----+------------+------------------------+---+---+
std::string GetRowSeparator(const std::vector<size_t>& max_column_lengths) {
  std::string separator = "+";
  for (int col_idx = 0; col_idx < max_column_lengths.size(); ++col_idx) {
    absl::StrAppend(&separator, std::string(max_column_lengths[col_idx] + 2, '-'),
                    "+");
  }
  absl::StrAppend(&separator, "\n");
  return separator;
}

}  // namespace

std::string ToPrettyOutputStyle(const zetasql::Value& result, bool is_value_table,
                           const std::vector<std::string>& column_names) {
  // The 'result' Value is expected to be a non-NULL array of struct values,
  // if it is not as expected then return an error std::string.
  if (result.is_null()) return "<null result>";
  const ArrayType* array_type = result.type()->AsArray();
  if (array_type == nullptr) return "<non-array result>";
  const int32_t num_result_rows = result.num_elements();

  int num_result_columns;
  if (is_value_table) {
    num_result_columns = 1;
  } else {
    const StructType* struct_row_type = array_type->element_type()->AsStruct();
    if (struct_row_type == nullptr) return "<non-array-of-struct result>";
    num_result_columns = struct_row_type->num_fields();
  }

  if (num_result_columns != column_names.size()) {
    return absl::StrCat("<mismatched column count: got ", num_result_columns,
                        ", expected ", column_names.size(), ">");
  }

  std::vector<size_t> max_column_lengths(num_result_columns, 0);
  for (int idx = 0; idx < num_result_columns; ++idx) {
    max_column_lengths[idx] =
        std::max(max_column_lengths[idx], column_names[idx].length());
  }

  std::vector<std::vector<std::string>> row_values;
  for (int row_idx = 0; row_idx < num_result_rows; ++row_idx) {
    std::vector<std::string> column_values(num_result_columns);
    for (int col_idx = 0; col_idx < num_result_columns; ++col_idx) {
      const Value& row_value = result.element(row_idx);
      const Value& column_value =
          is_value_table ? row_value : row_value.field(col_idx);

      // TODO Consider using value.GetSQLLiteral here.
      column_values[col_idx] = ValueToOutputString(column_value);
      max_column_lengths[col_idx] = std::max(max_column_lengths[col_idx],
                                             column_values[col_idx].length());
    }
    row_values.push_back(column_values);
  }

  // Construct the row separator.
  std::string separator = GetRowSeparator(max_column_lengths);

  std::string output = separator;
  // Add the column names. (Value tables do not have column names.)
  if (!is_value_table) {
    absl::StrAppend(
        &output, GenerateRowStringFromColumns(column_names, max_column_lengths),
        separator);
  }
  for (int row_idx = 0; row_idx < num_result_rows; ++row_idx) {
    absl::StrAppend(&output, GenerateRowStringFromColumns(row_values[row_idx],
                                                          max_column_lengths));
  }
  absl::StrAppend(&output, separator);

  return output;
}

std::string OutputPrettyStyleQueryResult(
    const zetasql::Value& result, const ResolvedStatement* resolved_stmt,
    const zetasql::ValueExpr* algebrized_tree) {
  absl::optional<int64_t> num_rows_modified;
  bool is_value_table;
  std::vector<std::string> column_names;
  Value result_table;
  ZETASQL_CHECK_OK(GetOutputColumnInfo(resolved_stmt, algebrized_tree, result,
                               &num_rows_modified, &is_value_table,
                               &result_table, &column_names));

  const std::string result_table_string =
      ToPrettyOutputStyle(result_table, is_value_table, column_names);
  std::string output_text;
  if (result_table_string.empty()) {
    absl::StrAppend(&output_text, "** Could not format output **\n");
    absl::StrAppend(&output_text, result_table.Format(), "\n");
  } else {
    absl::StrAppend(&output_text, result_table_string);  // Includes newline
  }
  if (num_rows_modified.has_value()) {
    absl::StrAppend(&output_text,
                    "Number of rows modified: ", num_rows_modified.value(),
                    "\n");
  }
  return output_text;
}

std::string OutputPrettyStyleExpressionResult(const zetasql::Value& result) {
  std::string value_str = ValueToOutputString(result);
  std::string separator = GetRowSeparator({value_str.length()});

  std::string output = separator;
  absl::StrAppend(
      &output, GenerateRowStringFromColumns({value_str}, {value_str.length()}));
  absl::StrAppend(&output, separator);
  return output;
}

}  // namespace zetasql
