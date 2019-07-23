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

// Helper methods for producing the types required by TestDriver
// implementations.

#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/base/statusor.h"

#ifndef ZETASQL_COMPLIANCE_TYPE_HELPERS_H_
#define ZETASQL_COMPLIANCE_TYPE_HELPERS_H_

namespace zetasql {

// Creates the Relational Algebra representation of a relation from the
// resolved AST, which is modeled as an array in the compliance tests.
zetasql_base::StatusOr<const ArrayType*> CreateTableArrayType(
    const ResolvedColumnList& table_columns, bool is_value_table,
    TypeFactory* type_factory);

// Names of DML output columns used by CreateDMLOutputType().
extern const char* kDMLOutputNumRowsModifiedColumnName;
extern const char* kDMLOutputAllRowsColumnName;

// Creates the DML output struct type corresponding to a DML statement on a
// table whose corresponding array type is 'table_array_type'.
//
// The returned type is a struct with two fields: an int64_t representing the
// number of rows modified by the statement, and an array of structs, where each
// element of the array represents a row of the modified table.
zetasql_base::StatusOr<const StructType*> CreateDMLOutputType(
    const ArrayType* table_array_type, TypeFactory* type_factory);

}  // namespace zetasql

#endif  // ZETASQL_COMPLIANCE_TYPE_HELPERS_H_
