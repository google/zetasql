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
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/sql_tvf.h"
#include "zetasql/public/sql_view.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/templated_sql_tvf.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

static absl::StatusOr<std::unique_ptr<Function>>
MakeFunctionFromCreateFunctionImpl(
    const ResolvedCreateFunctionStmt& create_function_stmt,
    std::optional<FunctionOptions> function_options,
    bool legacy_joined_name_path = false) {
  FunctionOptions options;
  if (function_options.has_value()) {
    options = *function_options;
  } else {
    // User-defined functions often use CamelCase. Upper casing that makes it
    // unreadable.
    options.set_uses_upper_case_sql_name(false);
  }
  FunctionEnums::Mode function_mode = create_function_stmt.is_aggregate()
                                          ? FunctionEnums::AGGREGATE
                                          : FunctionEnums::SCALAR;
  std::unique_ptr<Function> function;
  std::vector<std::string> name_path = create_function_stmt.name_path();
  if (legacy_joined_name_path) {
    name_path = {absl::StrJoin(create_function_stmt.name_path(), ".")};
  }
  if (create_function_stmt.function_expression() != nullptr) {
    std::unique_ptr<SQLFunction> sql_function;
    ZETASQL_ASSIGN_OR_RETURN(function,
                     SQLFunction::Create(
                         std::move(name_path), function_mode,
                         create_function_stmt.signature(), std::move(options),
                         create_function_stmt.function_expression(),
                         create_function_stmt.argument_name_list(),
                         &create_function_stmt.aggregate_expression_list(),
                         /*parse_resume_location=*/std::nullopt));
  } else if (create_function_stmt.language() == "SQL") {
    function = std::make_unique<TemplatedSQLFunction>(
        create_function_stmt.name_path(), create_function_stmt.signature(),
        create_function_stmt.argument_name_list(),
        ParseResumeLocation::FromStringView(create_function_stmt.code()),
        function_mode, options);
  } else {
    // The group arg is just used for distinguishing classes of functions
    // in some error messages.
    std::vector<FunctionSignature> signatures = {
        create_function_stmt.signature()};
    function = std::make_unique<Function>(create_function_stmt.name_path(),
                                          /*group=*/"External_function",
                                          function_mode, signatures, options);
  }

  function->set_sql_security(create_function_stmt.sql_security());
  return function;
}

absl::Status AddFunctionFromCreateFunction(
    absl::string_view create_sql_stmt, const AnalyzerOptions& analyzer_options,
    bool allow_persistent_function,
    std::optional<FunctionOptions> function_options,
    std::unique_ptr<const AnalyzerOutput>& analyzer_output,
    Catalog& resolving_catalog, SimpleCatalog& catalog) {
  ZETASQL_RET_CHECK(analyzer_options.language().SupportsStatementKind(
      RESOLVED_CREATE_FUNCTION_STMT));
  ZETASQL_RETURN_IF_ERROR(AnalyzeStatement(create_sql_stmt, analyzer_options,
                                   &resolving_catalog, catalog.type_factory(),
                                   &analyzer_output))
      << create_sql_stmt;
  const ResolvedStatement* resolved = analyzer_output->resolved_statement();
  ZETASQL_RET_CHECK(resolved->Is<ResolvedCreateFunctionStmt>());
  const ResolvedCreateFunctionStmt* resolved_create =
      resolved->GetAs<ResolvedCreateFunctionStmt>();
  if (!allow_persistent_function) {
    ZETASQL_RET_CHECK_EQ(resolved_create->create_scope(),
                 ResolvedCreateStatementEnums::CREATE_TEMP);
  }
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<Function> function,
      MakeFunctionFromCreateFunctionImpl(*resolved_create, function_options,
                                         /*legacy_joined_name_path=*/true));

  ZETASQL_RET_CHECK(catalog.AddOwnedFunctionIfNotPresent(&function))
      << absl::StrJoin(resolved_create->name_path(), ".");
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<Function>> MakeFunctionFromCreateFunction(
    const ResolvedCreateFunctionStmt& create_function_stmt,
    std::optional<FunctionOptions> function_options) {
  return MakeFunctionFromCreateFunctionImpl(create_function_stmt,
                                            std::move(function_options));
}

absl::Status AddTVFFromCreateTableFunction(
    absl::string_view create_tvf_stmt, const AnalyzerOptions& analyzer_options,
    bool allow_persistent,
    std::unique_ptr<const AnalyzerOutput>& analyzer_output,
    SimpleCatalog& catalog) {
  ZETASQL_RET_CHECK(analyzer_options.language().SupportsStatementKind(
      RESOLVED_CREATE_TABLE_FUNCTION_STMT));
  ZETASQL_RETURN_IF_ERROR(AnalyzeStatement(create_tvf_stmt, analyzer_options, &catalog,
                                   catalog.type_factory(), &analyzer_output))
      << create_tvf_stmt;
  const ResolvedStatement* resolved = analyzer_output->resolved_statement();
  ZETASQL_RET_CHECK(resolved->Is<ResolvedCreateTableFunctionStmt>());
  const ResolvedCreateTableFunctionStmt* stmt =
      resolved->GetAs<ResolvedCreateTableFunctionStmt>();
  if (!allow_persistent) {
    ZETASQL_RET_CHECK_EQ(stmt->create_scope(),
                 ResolvedCreateStatementEnums::CREATE_TEMP);
  }

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<TableValuedFunction> tvf,
                   MakeTVFFromCreateTableFunction(*stmt));

  ZETASQL_RET_CHECK(catalog.AddOwnedTableValuedFunctionIfNotPresent(&tvf));

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<TableValuedFunction>>
MakeTVFFromCreateTableFunction(const ResolvedCreateTableFunctionStmt& stmt) {
  std::unique_ptr<TableValuedFunction> tvf;
  if (stmt.query() != nullptr) {
    ZETASQL_RET_CHECK(!stmt.signature().IsTemplated());
    ZETASQL_RET_CHECK_EQ(stmt.language(), "SQL");
    std::unique_ptr<SQLTableValuedFunction> sql_tvf;
    ZETASQL_RETURN_IF_ERROR(SQLTableValuedFunction::Create(&stmt, &sql_tvf));
    tvf = std::move(sql_tvf);
  } else if (stmt.language() == "SQL" && !stmt.code().empty()) {
    ZETASQL_RET_CHECK(stmt.signature().IsTemplated());
    tvf = std::make_unique<TemplatedSQLTVF>(
        stmt.name_path(), stmt.signature(), stmt.argument_name_list(),
        ParseResumeLocation::FromStringView(stmt.code()));
  } else {
    const FunctionArgumentType& result_type = stmt.signature().result_type();
    ZETASQL_RET_CHECK(result_type.IsRelation());
    ZETASQL_RET_CHECK(result_type.options().has_relation_input_schema())
        << "Only TVFs with fixed output table schemas are supported";

    tvf = std::make_unique<FixedOutputSchemaTVF>(
        stmt.name_path(), stmt.signature(),
        result_type.options().relation_input_schema());
  }

  // Give an error if any options were set that weren't handled above.
  // Some fields are okay to ignore here so we mark them accessed.
  for (const auto& col : stmt.output_column_list()) {
    col->MarkFieldsAccessed();
  }
  if (stmt.query() != nullptr) {
    stmt.query()->MarkFieldsAccessed();
  }
  ZETASQL_RETURN_IF_ERROR(stmt.CheckFieldsAccessed());

  return tvf;
}

absl::Status AddViewFromCreateView(
    absl::string_view create_view_stmt, const AnalyzerOptions& analyzer_options,
    bool allow_non_temp, std::unique_ptr<const AnalyzerOutput>& analyzer_output,
    SimpleCatalog& catalog) {
  ZETASQL_RET_CHECK(analyzer_options.language().SupportsStatementKind(
      RESOLVED_CREATE_VIEW_STMT));
  ZETASQL_RETURN_IF_ERROR(AnalyzeStatement(create_view_stmt, analyzer_options, &catalog,
                                   catalog.type_factory(), &analyzer_output))
      << create_view_stmt;
  const ResolvedStatement* resolved = analyzer_output->resolved_statement();
  ZETASQL_RET_CHECK(resolved->Is<ResolvedCreateViewStmt>());
  const ResolvedCreateViewStmt* stmt =
      resolved->GetAs<ResolvedCreateViewStmt>();
  if (!allow_non_temp) {
    ZETASQL_RET_CHECK_EQ(stmt->create_scope(),
                 ResolvedCreateStatementEnums::CREATE_TEMP);
  }
  std::vector<SimpleSQLView::NameAndType> columns;
  for (int i = 0; i < stmt->output_column_list_size(); ++i) {
    const ResolvedOutputColumn* col = stmt->output_column_list(i);
    columns.push_back({.name = col->name(), .type = col->column().type()});
  }
  SimpleSQLView::SqlSecurity security = stmt->sql_security();
  // ZetaSQL defines the default SQL security to be "DEFINER"
  if (security == SQLView::kSecurityUnspecified) {
    security = SQLView::kSecurityDefiner;
  }
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<SimpleSQLView> sql_view,
      SimpleSQLView::Create(absl::StrJoin(stmt->name_path(), "."), columns,
                            security, stmt->is_value_table(), stmt->query()));
  std::string view_name = sql_view->Name();
  ZETASQL_RET_CHECK(catalog.AddOwnedTableIfNotPresent(view_name, std::move(sql_view)));
  return absl::OkStatus();
}

absl::Status AddTableFromCreateTable(
    absl::string_view create_table_stmt,
    const AnalyzerOptions& analyzer_options, bool allow_non_temp,
    std::unique_ptr<const AnalyzerOutput>& analyzer_output, SimpleTable*& table,
    SimpleCatalog& catalog) {
  table = nullptr;

  ZETASQL_RET_CHECK(analyzer_options.language().SupportsStatementKind(
      RESOLVED_CREATE_TABLE_STMT));
  ZETASQL_RETURN_IF_ERROR(AnalyzeStatement(create_table_stmt, analyzer_options,
                                   &catalog, catalog.type_factory(),
                                   &analyzer_output))
      << create_table_stmt;
  const ResolvedStatement* resolved = analyzer_output->resolved_statement();
  ZETASQL_RET_CHECK(resolved->Is<ResolvedCreateTableStmt>());
  const ResolvedCreateTableStmt* stmt =
      resolved->GetAs<ResolvedCreateTableStmt>();
  if (!allow_non_temp) {
    ZETASQL_RET_CHECK_EQ(stmt->create_scope(),
                 ResolvedCreateStatementEnums::CREATE_TEMP);
  }

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<SimpleTable> created_table,
                   MakeTableFromCreateTable(*stmt));

  SimpleTable* table_ptr = created_table.get();
  const std::string table_name = created_table->Name();
  ZETASQL_RET_CHECK(
      catalog.AddOwnedTableIfNotPresent(table_name, std::move(created_table)));

  // Give an error if any options were set that weren't handled above.
  ZETASQL_RETURN_IF_ERROR(stmt->CheckFieldsAccessed());

  table = table_ptr;
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<SimpleTable>> MakeTableFromCreateTable(
    const ResolvedCreateTableStmtBase& stmt) {
  ZETASQL_RET_CHECK_EQ(stmt.name_path().size(), 1)
      << "Table names with sub-catalogs are not supported";

  std::vector<SimpleTable::NameAndType> columns;
  for (const auto& column_def : stmt.column_definition_list()) {
    columns.push_back({column_def->name(), column_def->type()});
  }
  auto created_table = std::make_unique<SimpleTable>(
      absl::StrJoin(stmt.name_path(), "."), columns);

  // Give an error if any options were set that weren't handled above.
  ZETASQL_RETURN_IF_ERROR(stmt.CheckFieldsAccessed());

  return created_table;
}

}  // namespace zetasql
