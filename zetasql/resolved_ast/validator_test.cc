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

#include "zetasql/resolved_ast/validator.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/make_node_vector.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_builder.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/test_utils.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {
namespace testing {

using ::testing::_;
using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

// Similar to MakeSelect1Stmt(), except the output column list in the returned
// tree has a different column id from that produced by the ProjectScan.
std::unique_ptr<ResolvedQueryStmt> MakeSelect1StmtWithWrongColumnId(
    IdStringPool& pool) {
  std::vector<ResolvedColumn> empty_column_list;
  std::unique_ptr<ResolvedSingleRowScan> input_scan =
      MakeResolvedSingleRowScan(empty_column_list);

  ResolvedColumn column_x =
      ResolvedColumn(1, pool.Make("tbl"), pool.Make("x"), types::Int64Type());

  ResolvedColumn column_x_wrong_id =
      ResolvedColumn(2, pool.Make("tbl"), pool.Make("x"), types::Int64Type());

  std::vector<ResolvedColumn> column_list;
  column_list.push_back(column_x);

  auto project_scan = MakeResolvedProjectScan(
      column_list,
      MakeNodeVector(MakeResolvedComputedColumn(
          column_list.back(), MakeResolvedLiteral(Value::Int64(1)))),
      std::move(input_scan));

  std::vector<std::unique_ptr<const ResolvedOutputColumn>> output_column_list;
  output_column_list.push_back(
      MakeResolvedOutputColumn("x", column_x_wrong_id));

  std::unique_ptr<ResolvedQueryStmt> query_stmt =
      MakeResolvedQueryStmt(std::move(output_column_list),
                            /*is_value_table=*/false, std::move(project_scan));
  EXPECT_EQ(R"(
QueryStmt
+-output_column_list=
| +-tbl.x#2 AS x [INT64]
+-query=
  +-ProjectScan
    +-column_list=[tbl.x#1]
    +-expr_list=
    | +-x#1 := Literal(type=INT64, value=1)
    +-input_scan=
      +-SingleRowScan
)",
            absl::StrCat("\n", query_stmt->DebugString()));
  return query_stmt;
}

// Returns a hand-constructed resolved tree representing "SELECT 1 AS x, 2 AS
// y". If <unique_column_ids> is true, uses separate column ids to represent x
// and y, which will result in a valid tree.
//
// If <unique_column_ids> is false, the same column ids will be used for x and
// y, which will result in an invalid tree.
std::unique_ptr<ResolvedQueryStmt> MakeSelectStmtWithMultipleColumns(
    IdStringPool& pool, bool unique_column_ids) {
  std::vector<ResolvedColumn> empty_column_list;
  std::unique_ptr<ResolvedSingleRowScan> input_scan =
      MakeResolvedSingleRowScan(empty_column_list);

  ResolvedColumn column_x =
      ResolvedColumn(1, pool.Make("tbl"), pool.Make("x"), types::Int64Type());
  ResolvedColumn column_y =
      ResolvedColumn(unique_column_ids ? 2 : 1, pool.Make("tbl"),
                     pool.Make("y"), types::Int64Type());

  std::unique_ptr<ResolvedProjectScan> project_scan = MakeResolvedProjectScan(
      {column_x, column_y},
      MakeNodeVector(MakeResolvedComputedColumn(
                         column_x, MakeResolvedLiteral(Value::Int64(1))),
                     MakeResolvedComputedColumn(
                         column_y, MakeResolvedLiteral(Value::Int64(2)))),
      std::move(input_scan));

  std::vector<std::unique_ptr<ResolvedOutputColumn>> output_column_list =
      MakeNodeVector(MakeResolvedOutputColumn("x", column_x),
                     MakeResolvedOutputColumn("y", column_y));

  std::unique_ptr<ResolvedQueryStmt> query_stmt =
      MakeResolvedQueryStmt(std::move(output_column_list),
                            /*is_value_table=*/false, std::move(project_scan));

  if (unique_column_ids) {
    EXPECT_EQ(R"(
QueryStmt
+-output_column_list=
| +-tbl.x#1 AS x [INT64]
| +-tbl.y#2 AS y [INT64]
+-query=
  +-ProjectScan
    +-column_list=tbl.[x#1, y#2]
    +-expr_list=
    | +-x#1 := Literal(type=INT64, value=1)
    | +-y#2 := Literal(type=INT64, value=2)
    +-input_scan=
      +-SingleRowScan
)",
              absl::StrCat("\n", query_stmt->DebugString()));
  } else {
    EXPECT_EQ(R"(
QueryStmt
+-output_column_list=
| +-tbl.x#1 AS x [INT64]
| +-tbl.y#1 AS y [INT64]
+-query=
  +-ProjectScan
    +-column_list=tbl.[x#1, y#1]
    +-expr_list=
    | +-x#1 := Literal(type=INT64, value=1)
    | +-y#1 := Literal(type=INT64, value=2)
    +-input_scan=
      +-SingleRowScan
)",
              absl::StrCat("\n", query_stmt->DebugString()));
  }
  return query_stmt;
}

enum class WithQueryShape {
  kValid,
  kNotScanAllColumns,
  kFailToRenameColumn,
  kColumnTypeMismatch,
};
std::unique_ptr<const ResolvedStatement> MakeWithQuery(IdStringPool& pool,
                                                       WithQueryShape shape) {
  std::string with_query_name = "with_query_name";
  ResolvedColumn column_x =
      ResolvedColumn(1, pool.Make("tbl"), pool.Make("x"), types::Int64Type());
  ResolvedColumn column_y =
      ResolvedColumn(2, pool.Make("tbl"), pool.Make("y"), types::StringType());
  ResolvedColumn column_x2 =
      ResolvedColumn(3, pool.Make("tbl"), pool.Make("x2"), types::Int64Type());
  ResolvedColumn column_y2 =
      shape == WithQueryShape::kColumnTypeMismatch
          ? ResolvedColumn(4, pool.Make("tbl"), pool.Make("y2"),
                           types::DoubleType())
          : ResolvedColumn(4, pool.Make("tbl"), pool.Make("y2"),
                           types::StringType());

  ResolvedWithRefScanBuilder with_scan =
      ResolvedWithRefScanBuilder()
          .add_column_list(column_x2)
          .set_with_query_name(with_query_name);
  switch (shape) {
    case WithQueryShape::kValid:
    case WithQueryShape::kColumnTypeMismatch:
      with_scan.add_column_list(column_y2);
      break;
    case WithQueryShape::kFailToRenameColumn:
      with_scan.add_column_list(column_y);
      break;
    case WithQueryShape::kNotScanAllColumns:
      break;
  }

  return ResolvedQueryStmtBuilder()
      .add_output_column_list(
          ResolvedOutputColumnBuilder().set_column(column_x2).set_name(
              column_x2.name()))
      .set_query(
          ResolvedWithScanBuilder()
              .set_recursive(false)
              .set_query(std::move(with_scan))
              .add_column_list(column_x2)
              .add_with_entry_list(
                  ResolvedWithEntryBuilder()
                      .set_with_query_name(with_query_name)
                      .set_with_subquery(
                          ResolvedProjectScanBuilder()
                              .add_column_list(column_x)
                              .add_expr_list(
                                  ResolvedComputedColumnBuilder()
                                      .set_column(column_x)
                                      .set_expr(
                                          ResolvedLiteralBuilder()
                                              .set_value(Value::Int64(1))
                                              .set_type(types::Int64Type())))
                              .add_column_list(column_y)
                              .add_expr_list(
                                  ResolvedComputedColumnBuilder()
                                      .set_column(column_y)
                                      .set_expr(
                                          ResolvedLiteralBuilder()
                                              .set_value(Value::String("a"))
                                              .set_type(types::StringType())))
                              .set_input_scan(MakeResolvedSingleRowScan()))))
      .Build()
      .value();
}

TEST(ValidatorTest, ValidQueryStatement) {
  IdStringPool pool;
  std::unique_ptr<ResolvedQueryStmt> query_stmt = MakeSelect1Stmt(pool);
  Validator validator;
  ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(query_stmt.get()));

  // Make sure the statement can be validated multiple times on the same
  // Validator object.
  ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(query_stmt.get()));
}

TEST(ValidatorTest, ValidExpression) {
  std::unique_ptr<ResolvedExpr> expr = MakeResolvedLiteral(Value::Int64(1));
  Validator validator;
  ZETASQL_ASSERT_OK(validator.ValidateStandaloneResolvedExpr(expr.get()));

  // Make sure the expression can be validated multiple times on the same
  // Validator object.
  ZETASQL_ASSERT_OK(validator.ValidateStandaloneResolvedExpr(expr.get()));
}

TEST(ValidatorTest, ValidWithScan) {
  IdStringPool pool;
  std::unique_ptr<const ResolvedStatement> valid_stmt =
      MakeWithQuery(pool, WithQueryShape::kValid);
  Validator validator;
  ZETASQL_EXPECT_OK(validator.ValidateResolvedStatement(valid_stmt.get()));

  // Make sure statement can be validated multiple times.
  ZETASQL_EXPECT_OK(validator.ValidateResolvedStatement(valid_stmt.get()));
}

TEST(ValidatorTest, InvalidWithScans) {
  IdStringPool pool;
  Validator validator;

  std::unique_ptr<const ResolvedStatement> missing_column =
      MakeWithQuery(pool, WithQueryShape::kNotScanAllColumns);
  EXPECT_THAT(validator.ValidateResolvedStatement(missing_column.get()),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("ResolvedWithRefScan must scan exactly the "
                                 "columns projected from the with query")));

  std::unique_ptr<const ResolvedStatement> not_renamed_column =
      MakeWithQuery(pool, WithQueryShape::kFailToRenameColumn);
  EXPECT_THAT(validator.ValidateResolvedStatement(not_renamed_column.get()),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Duplicate column id 2 in column tbl.y#2")));

  std::unique_ptr<const ResolvedStatement> type_missmatch =
      MakeWithQuery(pool, WithQueryShape::kColumnTypeMismatch);
  EXPECT_THAT(
      validator.ValidateResolvedStatement(type_missmatch.get()),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr(
              "Type mismatch between ResolvedWithRefScan and with query")));
}

TEST(ValidatorTest, InvalidExpression) {
  TypeFactory type_factory;
  ResolvedColumn column(1, zetasql::IdString::MakeGlobal("tbl"),
                        zetasql::IdString::MakeGlobal("col1"),
                        types::Int64Type());
  std::unique_ptr<ResolvedExpr> expr = WrapInFunctionCall(
      &type_factory, MakeResolvedLiteral(Value::Int64(1)),
      MakeResolvedColumnRef(types::Int64Type(), column, false),
      MakeResolvedLiteral(Value::Int64(2)));
  Validator validator;

  // Repeat twice to ensure that the validator behaves the same way when reused.
  for (int i = 0; i < 2; ++i) {
    absl::Status status = validator.ValidateStandaloneResolvedExpr(expr.get());

    // Make sure error message is as expected.
    ASSERT_THAT(
        status,
        StatusIs(absl::StatusCode::kInternal,
                 HasSubstr("Incorrect reference to column tbl.col1#1")));

    // Make sure the tree dump has emphasis on the expected node.
    ASSERT_THAT(status.message(),
                HasSubstr("ColumnRef(type=INT64, column=tbl.col1#1) "
                          "(validation failed here)"));
  }
}

TEST(ValidatorTest, InvalidQueryStatement) {
  IdStringPool pool;
  std::unique_ptr<ResolvedQueryStmt> query_stmt =
      MakeSelect1StmtWithWrongColumnId(pool);
  Validator validator;

  // Repeat twice to ensure that the validator behaves the same way when reused.
  for (int i = 0; i < 2; ++i) {
    // Verify error message
    absl::Status status = validator.ValidateResolvedStatement(query_stmt.get());
    ASSERT_THAT(status,
                StatusIs(absl::StatusCode::kInternal,
                         HasSubstr("Incorrect reference to column tbl.x#2")));

    // Verify node emphasized in tree dump
    ASSERT_THAT(
        status,
        StatusIs(
            _, HasSubstr("| +-tbl.x#2 AS x [INT64] (validation failed here)")));
  }
}

TEST(ValidatorTest, ValidStatementAfterInvalidStatement) {
  // Make sure that after validating an invalid statement, the validator is left
  // in a state where it can still validate another valid statement later.
  Validator validator;
  IdStringPool pool;

  std::unique_ptr<ResolvedQueryStmt> valid_query_stmt = MakeSelect1Stmt(pool);
  std::unique_ptr<ResolvedQueryStmt> invalid_query_stmt =
      MakeSelect1StmtWithWrongColumnId(pool);

  ASSERT_THAT(validator.ValidateResolvedStatement(invalid_query_stmt.get()),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Incorrect reference to column tbl.x#2")));
  ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(valid_query_stmt.get()));
}

TEST(ValidatorTest, ValidQueryStatementMultipleColumns) {
  IdStringPool pool;
  std::unique_ptr<ResolvedQueryStmt> query_stmt =
      MakeSelectStmtWithMultipleColumns(pool, /*unique_column_ids=*/true);
  Validator validator;
  ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(query_stmt.get()));
}

TEST(ValidatorTest, InvalidStatementDueToDuplicateColumnIds) {
  IdStringPool pool;
  std::unique_ptr<ResolvedQueryStmt> query_stmt =
      MakeSelectStmtWithMultipleColumns(pool, /*unique_column_ids=*/false);
  Validator validator;
  ASSERT_THAT(validator.ValidateResolvedStatement(query_stmt.get()),
              StatusIs(absl::StatusCode::kInternal));
}

TEST(ValidateTest, QueryStmtWithNullExpr) {
  IdStringPool pool;
  std::unique_ptr<ResolvedQueryStmt> query_stmt = MakeSelect1Stmt(pool);
  const_cast<ResolvedComputedColumn*>(
      query_stmt->query()->GetAs<ResolvedProjectScan>()->expr_list(0))
      ->release_expr();

  Validator validator;
  ASSERT_THAT(
      validator.ValidateResolvedStatement(query_stmt.get()),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr("| +-x#1 := <nullptr AST node> (validation failed here)")));
}

TEST(ValidateTest, CreateFunctionStmtWithRemoteAndInvalidLanguage) {
  std::unique_ptr<ResolvedCreateFunctionStmt> create_function_stmt =
      MakeResolvedCreateFunctionStmt(
          /*name_path=*/{"foo"},
          /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
          /*has_explicit_return_type=*/true,
          types::Int32Type(),
          /*argument_name_list=*/{},
          /*signature=*/{{types::Int32Type()}, {}, nullptr},
          /*is_aggregate=*/false,
          /*language=*/"SQL",
          /*code=*/"",
          /*aggregate_expression_list=*/{},
          /*function_expression=*/nullptr,
          /*option_list=*/{},
          /*sql_security=*/ResolvedCreateStatement::SQL_SECURITY_UNSPECIFIED,
          /*determinism_level=*/
          ResolvedCreateStatement::DETERMINISM_UNSPECIFIED,
          /*is_remote=*/true,
          /*connection=*/nullptr);

  Validator validator;
  ASSERT_THAT(
      validator.ValidateResolvedStatement(create_function_stmt.get()),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("is_remote is true iff language is \"REMOTE\"")));
}

TEST(ValidateTest, CreateFunctionStmtWithRemoteAndRemoteLanguage) {
  std::unique_ptr<ResolvedCreateFunctionStmt> create_function_stmt =
      MakeResolvedCreateFunctionStmt(
          /*name_path=*/{"foo"},
          /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
          /*has_explicit_return_type=*/true, types::Int32Type(),
          /*argument_name_list=*/{},
          /*signature=*/{{types::Int32Type()}, {}, nullptr},
          /*is_aggregate=*/false,
          /*language=*/"remote",
          /*code=*/"",
          /*aggregate_expression_list=*/{},
          /*function_expression=*/nullptr,
          /*option_list=*/{},
          /*sql_security=*/ResolvedCreateStatement::SQL_SECURITY_UNSPECIFIED,
          /*determinism_level=*/
          ResolvedCreateStatement::DETERMINISM_UNSPECIFIED,
          /*is_remote=*/true,
          /*connection=*/nullptr);

  Validator validator;
  ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(create_function_stmt.get()));
}

TEST(ValidateTest,
     CreateFunctionStmtWithRemoteAndCodeWithRemoteFunctionFeatureEnabled) {
  std::unique_ptr<ResolvedCreateFunctionStmt> create_function_stmt =
      MakeResolvedCreateFunctionStmt(
          /*name_path=*/{"foo"},
          /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
          /*has_explicit_return_type=*/true, types::Int32Type(),
          /*argument_name_list=*/{},
          /*signature=*/{{types::Int32Type()}, {}, nullptr},
          /*is_aggregate=*/false,
          /*language=*/"REMOTE",
          /*code=*/"return 1;",
          /*aggregate_expression_list=*/{},
          /*function_expression=*/nullptr,
          /*option_list=*/{},
          /*sql_security=*/ResolvedCreateStatement::SQL_SECURITY_UNSPECIFIED,
          /*determinism_level=*/
          ResolvedCreateStatement::DETERMINISM_UNSPECIFIED,
          /*is_remote=*/true,
          /*connection=*/nullptr);
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_3_REMOTE_FUNCTION);
  Validator validator(language_options);
  ASSERT_THAT(
      validator.ValidateResolvedStatement(create_function_stmt.get()),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("stmt->code().empty()")));
}

TEST(ValidateTest,
     CreateFunctionStmtWithRemoteAndCodeWithRemoteFunctionFeatureNotEnabled) {
  std::unique_ptr<ResolvedCreateFunctionStmt> create_function_stmt =
      MakeResolvedCreateFunctionStmt(
          /*name_path=*/{"foo"},
          /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
          /*has_explicit_return_type=*/true, types::Int32Type(),
          /*argument_name_list=*/{},
          /*signature=*/{{types::Int32Type()}, {}, nullptr},
          /*is_aggregate=*/false,
          /*language=*/"REMOTE",
          /*code=*/"return 1;",
          /*aggregate_expression_list=*/{},
          /*function_expression=*/nullptr,
          /*option_list=*/{},
          /*sql_security=*/ResolvedCreateStatement::SQL_SECURITY_UNSPECIFIED,
          /*determinism_level=*/
          ResolvedCreateStatement::DETERMINISM_UNSPECIFIED,
          /*is_remote=*/true,
          /*connection=*/nullptr);

  Validator validator;
  ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(create_function_stmt.get()));
}

TEST(ValidateTest, CreateFunctionStmtWithConnectionButNotRemote) {
  SimpleConnection connection("connection_id");
  std::unique_ptr<ResolvedCreateFunctionStmt> create_function_stmt =
      MakeResolvedCreateFunctionStmt(
          /*name_path=*/{"foo"},
          /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
          /*has_explicit_return_type=*/true,
          types::Int32Type(),
          /*argument_name_list=*/{},
          /*signature=*/{{types::Int32Type()}, {}, nullptr},
          /*is_aggregate=*/false,
          /*language=*/"SQL",
          /*code=*/"",
          /*aggregate_expression_list=*/{},
          /*function_expression=*/nullptr,
          /*option_list=*/{},
          /*sql_security=*/ResolvedCreateStatement::SQL_SECURITY_UNSPECIFIED,
          /*determinism_level=*/
          ResolvedCreateStatement::DETERMINISM_UNSPECIFIED,
          /*is_remote=*/false,
          MakeResolvedConnection(&connection));

  Validator validator;
  ASSERT_THAT(
      validator.ValidateResolvedStatement(create_function_stmt.get()),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("stmt->is_remote()")));
}

TEST(ValidateTest, CreateFunctionStmtWithRemoteLanguageButNotRemote) {
  std::unique_ptr<ResolvedCreateFunctionStmt> create_function_stmt =
      MakeResolvedCreateFunctionStmt(
          /*name_path=*/{"foo"},
          /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
          /*has_explicit_return_type=*/true,
          types::Int32Type(),
          /*argument_name_list=*/{},
          /*signature=*/{{types::Int32Type()}, {}, nullptr},
          /*is_aggregate=*/false,
          /*language=*/"REMOTE",
          /*code=*/"",
          /*aggregate_expression_list=*/{},
          /*function_expression=*/nullptr,
          /*option_list=*/{},
          /*sql_security=*/ResolvedCreateStatement::SQL_SECURITY_UNSPECIFIED,
          /*determinism_level=*/
          ResolvedCreateStatement::DETERMINISM_UNSPECIFIED,
          /*is_remote=*/false,
          /*connection=*/nullptr);

  Validator validator;
  ASSERT_THAT(
      validator.ValidateResolvedStatement(create_function_stmt.get()),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("is_remote is true iff language is \"REMOTE\"")));
}

TEST(ValidateTest, CreateProcedureStmtNonSQLFeatureNotEnabled) {
  std::unique_ptr<ResolvedCreateProcedureStmt> create_procedure_stmt =
      MakeResolvedCreateProcedureStmt(
          /*name_path=*/{"foo"},
          /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
          /*argument_name_list=*/{},
          /*signature=*/{{types::Int32Type()}, {}, nullptr},
          /*option_list=*/{},
          /*procedure_body=*/"",
          /*connection=*/nullptr,
          /*language=*/"PYTHON",
          /*code=*/"");

  Validator validator;
  ASSERT_THAT(
      validator.ValidateResolvedStatement(create_procedure_stmt.get()),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("stmt->language()")));
}

TEST(ValidateTest, CreateProcedureStmtNonSQLConnectionFeatureNotEnabled) {
  SimpleConnection connection("connection_id");
  std::unique_ptr<ResolvedCreateProcedureStmt> create_procedure_stmt =
      MakeResolvedCreateProcedureStmt(
          /*name_path=*/{"foo"},
          /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
          /*argument_name_list=*/{},
          /*signature=*/{{types::Int32Type()}, {}, nullptr},
          /*option_list=*/{},
          /*procedure_body=*/"",
          /*connection=*/MakeResolvedConnection(&connection),
          /*language=*/"PYTHON",
          /*code=*/"");

  Validator validator;
  ASSERT_THAT(
      validator.ValidateResolvedStatement(create_procedure_stmt.get()),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("stmt->connection()")));
}

TEST(ValidateTest, CreateProcedureStmtNonSQLFeatureEnabledMissingLanguage) {
  std::unique_ptr<ResolvedCreateProcedureStmt> create_procedure_stmt =
      MakeResolvedCreateProcedureStmt(
          /*name_path=*/{"foo"},
          /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
          /*argument_name_list=*/{},
          /*signature=*/{{types::Int32Type()}, {}, nullptr},
          /*option_list=*/{},
          /*procedure_body=*/"sql",
          /*connection=*/nullptr,
          /*language=*/"",
          /*code=*/"code");

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_NON_SQL_PROCEDURE);
  Validator validator(language_options);
  ASSERT_THAT(validator.ValidateResolvedStatement(create_procedure_stmt.get()),
              StatusIs(absl::StatusCode::kInternal, HasSubstr("stmt->code()")));
}

TEST(ValidateTest, CreateProcedureStmtNonSQLFeatureEnabledHasBodyAndLanguage) {
  std::unique_ptr<ResolvedCreateProcedureStmt> create_procedure_stmt =
      MakeResolvedCreateProcedureStmt(
          /*name_path=*/{"foo"},
          /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
          /*argument_name_list=*/{},
          /*signature=*/{{types::Int32Type()}, {}, nullptr},
          /*option_list=*/{},
          /*procedure_body=*/"body",
          /*connection=*/nullptr,
          /*language=*/"python",
          /*code=*/"");

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_NON_SQL_PROCEDURE);
  Validator validator(language_options);
  ASSERT_THAT(validator.ValidateResolvedStatement(create_procedure_stmt.get()),
              StatusIs(absl::StatusCode::kInternal));
}

TEST(ValidateTest, CreateProcedureStmtNonSQLFeatureEnabledHasLanguage) {
  std::unique_ptr<ResolvedCreateProcedureStmt> create_procedure_stmt =
      MakeResolvedCreateProcedureStmt(
          /*name_path=*/{"foo"},
          /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
          /*argument_name_list=*/{},
          /*signature=*/{{types::Int32Type()}, {}, nullptr},
          /*option_list=*/{},
          /*procedure_body=*/"",
          /*connection=*/nullptr,
          /*language=*/"PYTHON",
          /*code=*/"");

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_NON_SQL_PROCEDURE);
  Validator validator(language_options);
  ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(create_procedure_stmt.get()));
}

TEST(ValidateTest, CreateProcedureStmtNonSQLFeatureEnabledHasLanguageAndCode) {
  std::unique_ptr<ResolvedCreateProcedureStmt> create_procedure_stmt =
      MakeResolvedCreateProcedureStmt(
          /*name_path=*/{"foo"},
          /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
          /*argument_name_list=*/{},
          /*signature=*/{{types::Int32Type()}, {}, nullptr},
          /*option_list=*/{},
          /*procedure_body=*/"",
          /*connection=*/nullptr,
          /*language=*/"PYTHON",
          /*code=*/"code");

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_NON_SQL_PROCEDURE);
  Validator validator(language_options);
  ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(create_procedure_stmt.get()));
}

TEST(ValidateTest, CreateProcedureStmtNonSQLFeatureEnabled) {
  SimpleConnection connection("connection_id");
  std::unique_ptr<ResolvedCreateProcedureStmt> create_procedure_stmt =
      MakeResolvedCreateProcedureStmt(
          /*name_path=*/{"foo"},
          /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
          /*argument_name_list=*/{},
          /*signature=*/{{types::Int32Type()}, {}, nullptr},
          /*option_list=*/{},
          /*procedure_body=*/"",
          /*connection=*/MakeResolvedConnection(&connection),
          /*language=*/"PYTHON",
          /*code=*/"code");

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_NON_SQL_PROCEDURE);
  Validator validator(language_options);
  ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(create_procedure_stmt.get()));
}

TEST(ValidateTest, AnonymizedAggregateScan) {
  IdStringPool pool;
  auto anon_function = std::make_unique<Function>("anon_count", "test_group",
                                                  Function::AGGREGATE);
  FunctionSignature sig(FunctionArgumentType(types::Int64Type(), 1), {},
                        static_cast<int64_t>(1234));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto anon_count_call,
                       ResolvedAggregateFunctionCallBuilder()
                           .set_type(types::Int64Type())
                           .set_function(anon_function.get())
                           .set_signature(std::move(sig))
                           .Build());

  ResolvedColumn column_anon_count =
      ResolvedColumn(1, pool.Make("agg"), pool.Make("c"), types::Int64Type());
  auto custom_option = MakeResolvedOption(
      "", "custom_option",
      MakeResolvedLiteral(types::Int64Type(), Value::Int64(1)));

  auto query_stmt_builder =
      ResolvedQueryStmtBuilder()
          .add_output_column_list(
              MakeResolvedOutputColumn("c", column_anon_count))
          .set_query(ResolvedProjectScanBuilder()
                         .add_column_list(column_anon_count)
                         .set_input_scan(
                             ResolvedAnonymizedAggregateScanBuilder()
                                 .add_column_list(column_anon_count)
                                 .set_input_scan(MakeResolvedSingleRowScan())
                                 .add_aggregate_list(
                                     ResolvedComputedColumnBuilder()
                                         .set_column(column_anon_count)
                                         .set_expr(std::move(anon_count_call)))
                                 .set_k_threshold_expr(
                                     ResolvedColumnRefBuilder()
                                         .set_type(types::Int64Type())
                                         .set_column(column_anon_count)
                                         .set_is_correlated(false))
                                 .add_anonymization_option_list(
                                     std::move(custom_option))));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto query_stmt, std::move(query_stmt_builder).Build());
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_ANONYMIZATION);
  AllowedHintsAndOptions allowed_hints_and_options;
  allowed_hints_and_options.AddAnonymizationOption("custom_option",
                                                   types::Int64Type());
  ValidatorOptions validator_options{.allowed_hints_and_options =
                                         allowed_hints_and_options};
  Validator validator(language_options, validator_options);
  ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(query_stmt.get()));
}

TEST(ValidatorTest, ValidCreateModelStatement_Local) {
  IdStringPool pool;
  ResolvedColumn x(1, pool.Make("tbl"), pool.Make("x"), types::Int64Type());
  auto statement = MakeResolvedCreateModelStmt(
      /*name_path=*/{"m"},
      /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
      /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
      /*option_list=*/{},
      /*output_column_list=*/
      MakeNodeVector(MakeResolvedOutputColumn(/*name=*/"x", /*column=*/x)),
      /*query=*/
      MakeResolvedProjectScan(
          /*column_list=*/{x},
          /*expr_list=*/
          MakeNodeVector(MakeResolvedComputedColumn(
              x, MakeResolvedLiteral(Value::Int64(1)))),
          /*input_scan=*/MakeResolvedSingleRowScan()),
      /*transform_input_column_list=*/{},
      /*transform_list=*/{},
      /*transform_output_column_list=*/{},
      /*transform_analytic_function_group_list=*/{},
      /*input_column_definition_list=*/{},
      /*output_column_definition_list=*/{},
      /*is_remote=*/false,
      /*connection=*/{});

  Validator validator;
  ZETASQL_EXPECT_OK(validator.ValidateResolvedStatement(statement.get()));
}

TEST(ValidatorTest, ValidCreateModelStatement_Imported) {
  IdStringPool pool;
  ResolvedColumn i1(1, pool.Make("tbl"), pool.Make("i1'"), types::Int64Type());
  ResolvedColumn i2(2, pool.Make("tbl"), pool.Make("i2"), types::DoubleType());
  ResolvedColumn o1(3, pool.Make("tbl"), pool.Make("o1"), types::BoolType());

  auto statement = MakeResolvedCreateModelStmt(
      /*name_path=*/{"m"},
      /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
      /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
      /*option_list=*/{},
      /*output_column_list=*/{},
      /*query=*/{},
      /*transform_input_column_list=*/{},
      /*transform_list=*/{},
      /*transform_output_column_list=*/{},
      /*transform_analytic_function_group_list=*/{},
      /*input_column_definition_list=*/
      MakeNodeVector(MakeResolvedColumnDefinition(/*name=*/"i1",
                                                  /*type=*/types::Int64Type(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/i1,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{}),
                     MakeResolvedColumnDefinition(/*name=*/"i2",
                                                  /*type=*/types::DoubleType(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/i2,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{})),
      /*output_column_definition_list=*/
      MakeNodeVector(MakeResolvedColumnDefinition(/*name=*/"o1",
                                                  /*type=*/types::BoolType(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/o1,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{})),
      /*is_remote=*/false,
      /*connection=*/{});

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_4_REMOTE_MODEL);
  Validator validator(language_options);
  ZETASQL_EXPECT_OK(validator.ValidateResolvedStatement(statement.get()));
}

TEST(ValidatorTest, ValidCreateModelStatement_ImportedV13_Invalid) {
  IdStringPool pool;
  ResolvedColumn i1(1, pool.Make("tbl"), pool.Make("i1'"), types::Int64Type());
  ResolvedColumn i2(2, pool.Make("tbl"), pool.Make("i2"), types::DoubleType());
  ResolvedColumn o1(3, pool.Make("tbl"), pool.Make("o1"), types::BoolType());

  auto statement = MakeResolvedCreateModelStmt(
      /*name_path=*/{"m"},
      /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
      /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
      /*option_list=*/{},
      /*output_column_list=*/{},
      /*query=*/{},
      /*transform_input_column_list=*/{},
      /*transform_list=*/{},
      /*transform_output_column_list=*/{},
      /*transform_analytic_function_group_list=*/{},
      /*input_column_definition_list=*/
      MakeNodeVector(MakeResolvedColumnDefinition(/*name=*/"i1",
                                                  /*type=*/types::Int64Type(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/i1,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{}),
                     MakeResolvedColumnDefinition(/*name=*/"i2",
                                                  /*type=*/types::DoubleType(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/i2,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{})),
      /*output_column_definition_list=*/
      MakeNodeVector(MakeResolvedColumnDefinition(/*name=*/"o1",
                                                  /*type=*/types::BoolType(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/o1,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{})),
      /*is_remote=*/false,
      /*connection=*/{});

  Validator validator;
  EXPECT_THAT(validator.ValidateResolvedStatement(statement.get()),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("stmt->query() != nullptr")));
}

TEST(ValidatorTest, ValidCreateModelStatement_Remote) {
  IdStringPool pool;
  ResolvedColumn i1(1, pool.Make("tbl"), pool.Make("i1'"), types::Int64Type());
  ResolvedColumn i2(2, pool.Make("tbl"), pool.Make("i2"), types::DoubleType());
  ResolvedColumn o1(3, pool.Make("tbl"), pool.Make("o1"), types::BoolType());
  SimpleConnection connection("c");

  auto statement = MakeResolvedCreateModelStmt(
      /*name_path=*/{"m"},
      /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
      /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
      /*option_list=*/
      MakeNodeVector(MakeResolvedOption(
          "", "abc",
          MakeResolvedLiteral(types::StringType(), Value::String("def")))),
      /*output_column_list=*/{},
      /*query=*/{},
      /*transform_input_column_list=*/{},
      /*transform_list=*/{},
      /*transform_output_column_list=*/{},
      /*transform_analytic_function_group_list=*/{},
      /*input_column_definition_list=*/
      MakeNodeVector(MakeResolvedColumnDefinition(/*name=*/"i1",
                                                  /*type=*/types::Int64Type(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/i1,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{}),
                     MakeResolvedColumnDefinition(/*name=*/"i2",
                                                  /*type=*/types::DoubleType(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/i2,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{})),
      /*output_column_definition_list=*/
      MakeNodeVector(MakeResolvedColumnDefinition(/*name=*/"o1",
                                                  /*type=*/types::BoolType(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/o1,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{})),
      /*is_remote=*/true,
      /*connection=*/MakeResolvedConnection(&connection));

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_4_REMOTE_MODEL);
  Validator validator(language_options);
  ZETASQL_EXPECT_OK(validator.ValidateResolvedStatement(statement.get()));
}

TEST(ValidatorTest, ValidCreateModelStatement_RemoteV13_Invalid) {
  IdStringPool pool;
  ResolvedColumn i1(1, pool.Make("tbl"), pool.Make("i1'"), types::Int64Type());
  ResolvedColumn i2(2, pool.Make("tbl"), pool.Make("i2"), types::DoubleType());
  ResolvedColumn o1(3, pool.Make("tbl"), pool.Make("o1"), types::BoolType());
  SimpleConnection connection("c");

  auto statement = MakeResolvedCreateModelStmt(
      /*name_path=*/{"m"},
      /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
      /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
      /*option_list=*/
      MakeNodeVector(MakeResolvedOption(
          "", "abc",
          MakeResolvedLiteral(types::StringType(), Value::String("def")))),
      /*output_column_list=*/{},
      /*query=*/{},
      /*transform_input_column_list=*/{},
      /*transform_list=*/{},
      /*transform_output_column_list=*/{},
      /*transform_analytic_function_group_list=*/{},
      /*input_column_definition_list=*/
      MakeNodeVector(MakeResolvedColumnDefinition(/*name=*/"i1",
                                                  /*type=*/types::Int64Type(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/i1,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{}),
                     MakeResolvedColumnDefinition(/*name=*/"i2",
                                                  /*type=*/types::DoubleType(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/i2,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{})),
      /*output_column_definition_list=*/
      MakeNodeVector(MakeResolvedColumnDefinition(/*name=*/"o1",
                                                  /*type=*/types::BoolType(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/o1,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{})),
      /*is_remote=*/true,
      /*connection=*/MakeResolvedConnection(&connection));

  Validator validator;
  EXPECT_THAT(validator.ValidateResolvedStatement(statement.get()),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("FEATURE_V_1_4_REMOTE_MODEL")));
}

TEST(ValidatorTest, ValidCreateModelStatement_SchemaAndQuery_Invalid) {
  IdStringPool pool;
  ResolvedColumn i1(1, pool.Make("tbl"), pool.Make("i1'"), types::Int64Type());
  ResolvedColumn i2(2, pool.Make("tbl"), pool.Make("i2"), types::DoubleType());
  ResolvedColumn o1(3, pool.Make("tbl"), pool.Make("o1"), types::BoolType());
  ResolvedColumn x(4, pool.Make("tbl"), pool.Make("x"), types::Int64Type());

  auto statement = MakeResolvedCreateModelStmt(
      /*name_path=*/{"m"},
      /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
      /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
      /*option_list=*/{},
      /*output_column_list=*/
      MakeNodeVector(MakeResolvedOutputColumn(/*name=*/"x", /*column=*/x)),
      /*query=*/
      MakeResolvedProjectScan(
          /*column_list=*/{x},
          /*expr_list=*/
          MakeNodeVector(MakeResolvedComputedColumn(
              x, MakeResolvedLiteral(Value::Int64(1)))),
          /*input_scan=*/MakeResolvedSingleRowScan()),
      /*transform_input_column_list=*/{},
      /*transform_list=*/{},
      /*transform_output_column_list=*/{},
      /*transform_analytic_function_group_list=*/{},
      /*input_column_definition_list=*/
      MakeNodeVector(MakeResolvedColumnDefinition(/*name=*/"i1",
                                                  /*type=*/types::Int64Type(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/i1,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{}),
                     MakeResolvedColumnDefinition(/*name=*/"i2",
                                                  /*type=*/types::DoubleType(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/i2,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{})),
      /*output_column_definition_list=*/
      MakeNodeVector(MakeResolvedColumnDefinition(/*name=*/"o1",
                                                  /*type=*/types::BoolType(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/o1,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{})),
      /*is_remote=*/false,
      /*connection=*/{});

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_4_REMOTE_MODEL);
  Validator validator(language_options);
  EXPECT_THAT(
      validator.ValidateResolvedStatement(statement.get()),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("stmt->input_column_definition_list().empty()")));
}

TEST(ValidatorTest, ValidCreateModelStatement_ConnectionNoRemote_Invalid) {
  IdStringPool pool;
  ResolvedColumn i1(1, pool.Make("tbl"), pool.Make("i1'"), types::Int64Type());
  ResolvedColumn i2(2, pool.Make("tbl"), pool.Make("i2"), types::DoubleType());
  ResolvedColumn o1(3, pool.Make("tbl"), pool.Make("o1"), types::BoolType());
  SimpleConnection connection("c");

  auto statement = MakeResolvedCreateModelStmt(
      /*name_path=*/{"m"},
      /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
      /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
      /*option_list=*/{},
      /*output_column_list=*/{},
      /*query=*/{},
      /*transform_input_column_list=*/{},
      /*transform_list=*/{},
      /*transform_output_column_list=*/{},
      /*transform_analytic_function_group_list=*/{},
      /*input_column_definition_list=*/
      MakeNodeVector(MakeResolvedColumnDefinition(/*name=*/"i1",
                                                  /*type=*/types::Int64Type(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/i1,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{}),
                     MakeResolvedColumnDefinition(/*name=*/"i2",
                                                  /*type=*/types::DoubleType(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/i2,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{})),
      /*output_column_definition_list=*/
      MakeNodeVector(MakeResolvedColumnDefinition(/*name=*/"o1",
                                                  /*type=*/types::BoolType(),
                                                  /*annotations=*/{},
                                                  /*is_hidden=*/false,
                                                  /*column=*/o1,
                                                  /*generated_column_info=*/{},
                                                  /*default_value=*/{})),
      /*is_remote=*/false,
      /*connection=*/MakeResolvedConnection(&connection));

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_4_REMOTE_MODEL);
  Validator validator(language_options);
  EXPECT_THAT(
      validator.ValidateResolvedStatement(statement.get()),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("stmt->connection()")));
}

TEST(ValidatorTest, ValidCreateModelStatement_EmptyV13_Invalid) {
  auto statement = MakeResolvedCreateModelStmt(
      /*name_path=*/{"m"},
      /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
      /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
      /*option_list=*/{},
      /*output_column_list=*/{},
      /*query=*/{},
      /*transform_input_column_list=*/{},
      /*transform_list=*/{},
      /*transform_output_column_list=*/{},
      /*transform_analytic_function_group_list=*/{},
      /*input_column_definition_list=*/{},
      /*output_column_definition_list=*/{},
      /*is_remote=*/false,
      /*connection=*/{});

  Validator validator;
  EXPECT_THAT(validator.ValidateResolvedStatement(statement.get()),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("stmt->query() != nullptr")));
}

TEST(ValidatorTest, ValidCreateModelStatement_EmptyV14_Invalid) {
  auto statement = MakeResolvedCreateModelStmt(
      /*name_path=*/{"m"},
      /*create_scope=*/ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
      /*create_mode=*/ResolvedCreateStatement::CREATE_DEFAULT,
      /*option_list=*/{},
      /*output_column_list=*/{},
      /*query=*/{},
      /*transform_input_column_list=*/{},
      /*transform_list=*/{},
      /*transform_output_column_list=*/{},
      /*transform_analytic_function_group_list=*/{},
      /*input_column_definition_list=*/{},
      /*output_column_definition_list=*/{},
      /*is_remote=*/false,
      /*connection=*/{});

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_4_REMOTE_MODEL);
  Validator validator(language_options);
  EXPECT_THAT(
      validator.ValidateResolvedStatement(statement.get()),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("!stmt->input_column_definition_list().empty()")));
}

}  // namespace testing
}  // namespace zetasql
