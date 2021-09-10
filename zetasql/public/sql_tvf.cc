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

#include "zetasql/public/sql_tvf.h"

#include "zetasql/common/errors.h"
#include "zetasql/base/ret_check.h"

namespace zetasql {

// TODO: Change this to 'SQL_table_function', rather than
// 'Lazy_resolution_tvf'.  This is currently 'Lazy_resolution_tvf'
// in order to minimize test diffs during a major refactoring.  Once the
// refactoring is submitted, change this to 'SQL_table_function' and
// update all the tests.
//
// static
const char SQLTableValuedFunction::kSQLTableValuedFunctionGroup[] =
    "Lazy_resolution_tvf";

// static
absl::Status SQLTableValuedFunction::Create(
    const ::zetasql::ResolvedCreateTableFunctionStmt* create_tvf_statement,
    std::unique_ptr<SQLTableValuedFunction>* simple_sql_tvf) {
  return Create(
      create_tvf_statement, /*tvf_options=*/ {}, simple_sql_tvf);
}

// static
absl::Status SQLTableValuedFunction::Create(
    const ::zetasql::ResolvedCreateTableFunctionStmt* create_tvf_statement,
    TableValuedFunctionOptions tvf_options,
    std::unique_ptr<SQLTableValuedFunction>* simple_sql_tvf) {
  ZETASQL_RET_CHECK_NE(create_tvf_statement, nullptr);
  // Only SQL TVFs are supported.
  ZETASQL_RET_CHECK_NE(create_tvf_statement->query(), nullptr);
  // Only non-templated SQL TVFs are supported.
  ZETASQL_RET_CHECK(!create_tvf_statement->signature().IsTemplated());

  ZETASQL_RETURN_IF_ERROR(
      create_tvf_statement->signature().IsValidForTableValuedFunction());

  simple_sql_tvf->reset(
      new SQLTableValuedFunction(create_tvf_statement, tvf_options));
  return absl::OkStatus();
}

absl::Status SQLTableValuedFunction::Resolve(
    const AnalyzerOptions* analyzer_options,
    const std::vector<TVFInputArgumentType>& actual_arguments,
    const FunctionSignature& concrete_signature, Catalog* catalog,
    TypeFactory* type_factory,
    std::shared_ptr<TVFSignature>* tvf_signature) const {
  // Note that the concrete signature might have deprecation warnings attached.
  // If so, then we need to propagate those deprecation warnings to the
  // returned signature.
  TVFSignatureOptions tvf_signature_options;
  tvf_signature_options.additional_deprecation_warnings =
      concrete_signature.AdditionalDeprecationWarnings();
  tvf_signature->reset(
      new TVFSignature(actual_arguments, tvf_schema_, tvf_signature_options));
  return absl::OkStatus();
}

// static
TVFRelation SQLTableValuedFunction::GetQueryOutputSchema(
    const ResolvedCreateTableFunctionStmt& create_tvf_statement) {
  if (create_tvf_statement.is_value_table()) {
    return TVFRelation::ValueTable(
        create_tvf_statement.query()->column_list(0).type());
  }
  return create_tvf_statement.signature().result_type().options()
      .relation_input_schema();
}

// static
FunctionSignature SQLTableValuedFunction::GetEmptyFunctionSignature() {
  // The result type is meaningless.
  return FunctionSignature(/*result_type=*/FunctionArgumentType::AnyRelation(),
                           /*arguments=*/{}, /*context_ptr=*/nullptr);
}

}  // namespace zetasql
