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

#ifndef ZETASQL_ANALYZER_SET_OPERATION_RESOLVER_BASE_H_
#define ZETASQL_ANALYZER_SET_OPERATION_RESOLVER_BASE_H_

#include <functional>
#include <string>
#include <vector>

#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/column_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"

namespace zetasql {

// This stateless class contains basic functionalities needed to resolve set
// operation.
class SetOperationResolverBase {
 public:
  SetOperationResolverBase(const LanguageOptions& language_options,
                           Coercer& coercer, ColumnFactory& column_factory);

  // Returns the SQL string for the given set operation type enum.
  static std::string GetSQLForOperation(
      ResolvedSetOperationScan::SetOperationType op_type);

  // Returns the set operation type enum for the given `metadata`.
  static absl::StatusOr<ResolvedSetOperationScan::SetOperationType>
  GetSetOperationType(const ASTSetOperationMetadata* metadata);

  // Returns the input argument type for the given `column` in the context of
  // the `resolved_scan`.
  InputArgumentType GetColumnInputArgumentType(
      const ResolvedColumn& column, const ResolvedScan* resolved_scan) const;

  // Coerces the types given by `column_type_lists` into a list of common
  // super types. Returns an error if
  // - column type does not meet set operation constraint of `op_type`; or
  // - the type coercion for any columns is impossible.
  //
  // `column_identifier_in_error_string` is a function that takes in a
  // column_idx and returns its column identifier used in error strings.
  absl::StatusOr<std::vector<const Type*>> GetSuperTypesOfSetOperation(
      absl::Span<const std::vector<InputArgumentType>> column_type_lists,
      const ASTNode* error_location,
      ResolvedSetOperationScan::SetOperationType op_type,
      std::function<std::string(int)> column_identifier_in_error_string) const;

  // Returns a column list where column names are from `final_column_names`
  // and column types are from `final_column_types`.
  //
  // REQUIRES: `final_column_names` and `final_column_types` must have the same
  // length.
  absl::StatusOr<ResolvedColumnList> BuildFinalColumnList(
      absl::Span<const IdString> final_column_names,
      const std::vector<const Type*>& final_column_types, IdString table_name,
      std::function<void(const ResolvedColumn&)> record_column_access);

 private:
  const LanguageOptions& language_options_;
  Coercer& coercer_;
  ColumnFactory& column_factory_;
};

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_SET_OPERATION_RESOLVER_BASE_H_
