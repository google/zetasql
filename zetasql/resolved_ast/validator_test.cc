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

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/make_node_vector.h"
#include "zetasql/resolved_ast/resolved_ast.h"
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

}  // namespace testing
}  // namespace zetasql
