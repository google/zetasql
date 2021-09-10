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

#include "zetasql/analyzer/input_argument_type_resolver_helper.h"

#include <memory>
#include <vector>

#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "gtest/gtest.h"

namespace zetasql {

TEST(ExprResolverHelperTest, LambdaInputArgumentType) {
  ASTLambda astLambda;
  std::vector<const ASTNode*> arg_ast_nodes;
  arg_ast_nodes.push_back(&astLambda);
  std::vector<std::unique_ptr<const ResolvedExpr>> arguments;
  arguments.push_back(nullptr);
  std::vector<InputArgumentType> input_arguments;
  ASTIntLiteral int_literal;
  arg_ast_nodes.push_back(&int_literal);
  arguments.push_back(MakeResolvedLiteral(Value::Int64(1)));

  GetInputArgumentTypesForGenericArgumentList(arg_ast_nodes, arguments,
                                              &input_arguments);
  ASSERT_EQ(input_arguments.size(), 2);
  ASSERT_TRUE(input_arguments[0].is_lambda());
  ASSERT_EQ(input_arguments[0].type(), nullptr);

  ASSERT_FALSE(input_arguments[1].is_lambda());
}

}  // namespace zetasql
