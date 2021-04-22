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

#ifndef ZETASQL_RESOLVED_AST_TEST_UTILS_H_
#define ZETASQL_RESOLVED_AST_TEST_UTILS_H_

#include "zetasql/resolved_ast/resolved_ast.h"

namespace zetasql {
namespace testing {

// Returns a hand-constructed resolved tree representing "SELECT 1 AS x".
std::unique_ptr<ResolvedQueryStmt> MakeSelect1Stmt(IdStringPool& pool);

// Wraps up to 3 expressions as arguments in a call to a test function.
std::unique_ptr<ResolvedFunctionCall> WrapInFunctionCall(
    TypeFactory* type_factory, std::unique_ptr<ResolvedExpr> arg1 = nullptr,
    std::unique_ptr<ResolvedExpr> arg2 = nullptr,
    std::unique_ptr<ResolvedExpr> arg3 = nullptr);
}  // namespace testing
}  // namespace zetasql

#endif  // ZETASQL_RESOLVED_AST_TEST_UTILS_H_
