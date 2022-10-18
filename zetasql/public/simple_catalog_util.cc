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

#include "zetasql/public/simple_catalog_util.h"

#include <memory>
#include <utility>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/resolved_ast/resolved_ast.h"

namespace zetasql {

absl::Status AddFunctionFromCreateFunction(
    absl::string_view create_sql_stmt, const AnalyzerOptions& analyzer_options,
    std::unique_ptr<const AnalyzerOutput>& analyzer_output,
    SimpleCatalog& catalog, bool allow_persistent_function) {
  ZETASQL_RET_CHECK(analyzer_options.language().SupportsStatementKind(
      RESOLVED_CREATE_FUNCTION_STMT));
  ZETASQL_RETURN_IF_ERROR(AnalyzeStatement(create_sql_stmt, analyzer_options, &catalog,
                                   catalog.type_factory(), &analyzer_output))
      << create_sql_stmt;
  const ResolvedStatement* resolved = analyzer_output->resolved_statement();
  ZETASQL_RET_CHECK(resolved->Is<ResolvedCreateFunctionStmt>());
  const ResolvedCreateFunctionStmt* resolved_create =
      resolved->GetAs<ResolvedCreateFunctionStmt>();
  if (!allow_persistent_function) {
    ZETASQL_RET_CHECK_EQ(resolved_create->create_scope(),
                 ResolvedCreateStatementEnums::CREATE_TEMP);
  }
  std::unique_ptr<Function> function;
  if (resolved_create->function_expression() != nullptr) {
    std::unique_ptr<SQLFunction> sql_function;
    ZETASQL_RETURN_IF_ERROR(SQLFunction::Create(
        absl::StrJoin(resolved_create->name_path(), "."), FunctionEnums::SCALAR,
        {resolved_create->signature()},
        /*function_options=*/{}, resolved_create->function_expression(),
        resolved_create->argument_name_list(),
        /*aggregate_expression_list=*/{},
        /*parse_resume_location=*/{}, &sql_function));
    function = std::move(sql_function);
  } else {
    function = std::make_unique<TemplatedSQLFunction>(
        resolved_create->name_path(), resolved_create->signature(),
        resolved_create->argument_name_list(),
        ParseResumeLocation::FromStringView(resolved_create->code()));
  }

  function->set_sql_security(resolved_create->sql_security());

  ZETASQL_RET_CHECK(catalog.AddOwnedFunctionIfNotPresent(&function));
  return absl::OkStatus();
}

}  // namespace zetasql
