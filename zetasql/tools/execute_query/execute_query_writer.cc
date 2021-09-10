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

#include "zetasql/tools/execute_query/execute_query_writer.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/tools/execute_query/output_query_result.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// Prints the result of executing a query. Currently requires loading all the
// results into memory to format pretty output.
absl::Status PrintResults(std::unique_ptr<EvaluatorTableIterator> iter,
                          std::ostream& out) {
  TypeFactory type_factory;

  std::vector<StructField> struct_fields;
  struct_fields.reserve(iter->NumColumns());
  for (int i = 0; i < iter->NumColumns(); ++i) {
    struct_fields.emplace_back(iter->GetColumnName(i), iter->GetColumnType(i));
  }

  const StructType* struct_type;
  ZETASQL_RETURN_IF_ERROR(type_factory.MakeStructType(struct_fields, &struct_type));

  std::vector<Value> rows;
  while (true) {
    if (!iter->NextRow()) {
      ZETASQL_RETURN_IF_ERROR(iter->Status());
      break;
    }

    std::vector<Value> fields;
    fields.reserve(iter->NumColumns());
    for (int i = 0; i < iter->NumColumns(); ++i) {
      fields.push_back(iter->GetValue(i));
    }

    rows.push_back(Value::Struct(struct_type, fields));
  }

  const ArrayType* array_type;
  ZETASQL_RETURN_IF_ERROR(type_factory.MakeArrayType(struct_type, &array_type));

  const Value result = Value::Array(array_type, rows);

  std::vector<std::string> column_names;
  column_names.reserve(iter->NumColumns());
  for (int i = 0; i < iter->NumColumns(); ++i) {
    column_names.push_back(iter->GetColumnName(i));
  }

  out << ToPrettyOutputStyle(result,
                             /*is_value_table=*/false, column_names)
      << std::endl;

  return absl::OkStatus();
}

ExecuteQueryStreamWriter::ExecuteQueryStreamWriter(std::ostream& out)
    : stream_{out} {}

absl::Status ExecuteQueryStreamWriter::parsed(
    absl::string_view parsed_debug_string) {
  stream_ << parsed_debug_string << std::endl;
  return absl::OkStatus();
}

absl::Status ExecuteQueryStreamWriter::resolved(const ResolvedNode& ast) {
  stream_ << ast.DebugString() << std::endl;
  return absl::OkStatus();
}

absl::Status ExecuteQueryStreamWriter::explained(const ResolvedNode& ast,
                                                 absl::string_view explain) {
  stream_ << explain << std::endl;
  return absl::OkStatus();
}

absl::Status ExecuteQueryStreamWriter::executed(
    const ResolvedNode& ast, std::unique_ptr<EvaluatorTableIterator> iter) {
  return PrintResults(std::move(iter), stream_);
}

absl::Status ExecuteQueryStreamWriter::ExecutedExpression(
    const ResolvedNode& ast, const Value& value) {
  stream_ << OutputPrettyStyleExpressionResult(value, /*include_box=*/false);
  return absl::OkStatus();
}

}  // namespace zetasql
