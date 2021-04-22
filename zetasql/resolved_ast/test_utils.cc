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

#include "zetasql/resolved_ast/test_utils.h"

#include "zetasql/base/logging.h"
#include "zetasql/resolved_ast/make_node_vector.h"

namespace zetasql {
namespace testing {
// Returns a hand-constructed resolved tree representing "SELECT 1 AS x".
std::unique_ptr<ResolvedQueryStmt> MakeSelect1Stmt(IdStringPool& pool) {
  std::vector<ResolvedColumn> empty_column_list;
  std::unique_ptr<ResolvedSingleRowScan> input_scan =
      MakeResolvedSingleRowScan(empty_column_list);

  ResolvedColumn column_x =
      ResolvedColumn(1, pool.Make("tbl"), pool.Make("x"), types::Int64Type());

  std::vector<ResolvedColumn> column_list;
  column_list.push_back(column_x);

  std::unique_ptr<ResolvedProjectScan> project_scan = MakeResolvedProjectScan(
      column_list,
      MakeNodeVector(MakeResolvedComputedColumn(
          column_list.back(), MakeResolvedLiteral(Value::Int64(1)))),
      std::move(input_scan));

  std::vector<std::unique_ptr<ResolvedOutputColumn>> output_column_list =
      MakeNodeVector(MakeResolvedOutputColumn("x", column_x));

  std::unique_ptr<ResolvedQueryStmt> query_stmt =
      MakeResolvedQueryStmt(std::move(output_column_list),
                            /*is_value_table=*/false, std::move(project_scan));
  ZETASQL_CHECK_EQ(R"(
QueryStmt
+-output_column_list=
| +-tbl.x#1 AS x [INT64]
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

std::unique_ptr<ResolvedFunctionCall> WrapInFunctionCall(
    TypeFactory* type_factory, std::unique_ptr<ResolvedExpr> arg1,
    std::unique_ptr<ResolvedExpr> arg2, std::unique_ptr<ResolvedExpr> arg3) {
  const Type* int64_type = type_factory->get_int64();
  static Function* function =
      new Function("test", "test_group", Function::SCALAR);
  FunctionArgumentTypeList function_arg_types;

  std::vector<std::unique_ptr<ResolvedExpr>> args;
  if (arg1 != nullptr) {
    function_arg_types.push_back(FunctionArgumentType(arg1->type()));
    args.push_back(std::move(arg1));
  }
  if (arg2 != nullptr) {
    function_arg_types.push_back(FunctionArgumentType(arg2->type()));
    args.push_back(std::move(arg2));
  }
  if (arg3 != nullptr) {
    function_arg_types.push_back(FunctionArgumentType(arg3->type()));
    args.push_back(std::move(arg3));
  }

  FunctionSignature sig(FunctionArgumentType(int64_type), function_arg_types,
                        static_cast<int64_t>(1234));
  return MakeResolvedFunctionCall(type_factory->get_int64(), function, sig,
                                  std::move(args),
                                  ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

}  // namespace testing
}  // namespace zetasql
