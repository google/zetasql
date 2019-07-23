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

#include "zetasql/compliance/type_helpers.h"

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/strings.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

static std::string GetColumnAlias(const std::string& alias) {
  // We use the empty std::string as anonymous column name.
  return IsInternalAlias(alias) ? "" : alias;
}

static zetasql_base::StatusOr<const StructType*> CreateStructTypeForTableRow(
    const ResolvedColumnList& table_columns, TypeFactory* type_factory) {
  std::vector<StructType::StructField> fields;
  fields.reserve(table_columns.size());
  for (int i = 0; i < table_columns.size(); ++i) {
    fields.push_back(
        {GetColumnAlias(table_columns[i].name()), table_columns[i].type()});
  }
  const StructType* table_struct;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeStructType(fields, &table_struct));
  return table_struct;
}

const char* kDMLOutputNumRowsModifiedColumnName = "num_rows_modified";
const char* kDMLOutputAllRowsColumnName = "all_rows";

zetasql_base::StatusOr<const ArrayType*> CreateTableArrayType(
    const ResolvedColumnList& table_columns, bool is_value_table,
    TypeFactory* type_factory) {
  const Type* element_type = nullptr;
  if (is_value_table) {
    ZETASQL_RET_CHECK_EQ(1, table_columns.size());
    element_type = table_columns[0].type();
  } else {
    ZETASQL_ASSIGN_OR_RETURN(const StructType* table_struct,
                     CreateStructTypeForTableRow(table_columns, type_factory));
    element_type = table_struct;
  }
  const ArrayType* table_array;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeArrayType(element_type, &table_array));
  return table_array;
}

zetasql_base::StatusOr<const StructType*> CreateDMLOutputType(
    const ArrayType* table_array_type, TypeFactory* type_factory) {
  std::vector<StructType::StructField> fields;
  fields.emplace_back(kDMLOutputNumRowsModifiedColumnName, types::Int64Type());
  fields.emplace_back(kDMLOutputAllRowsColumnName, table_array_type);

  const StructType* dml_output_type;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeStructType(fields, &dml_output_type));
  return dml_output_type;
}

}  // namespace zetasql
