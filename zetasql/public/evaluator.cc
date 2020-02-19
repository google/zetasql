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

#include "zetasql/public/evaluator.h"

#include "zetasql/reference_impl/functions/register_all.h"

namespace zetasql {

PreparedExpression::PreparedExpression(const std::string& sql,
                                       TypeFactory* type_factory)
    : PreparedExpressionBase(sql, type_factory) {
  RegisterAllOptionalBuiltinFunctions();
}

PreparedExpression::PreparedExpression(const std::string& sql,
                                       const EvaluatorOptions& options)
    : PreparedExpressionBase(sql, options) {
  RegisterAllOptionalBuiltinFunctions();
}

PreparedExpression::PreparedExpression(const ResolvedExpr* expression,
                                       const EvaluatorOptions& options)
    : PreparedExpressionBase(expression, options) {
  RegisterAllOptionalBuiltinFunctions();
}

PreparedQuery::PreparedQuery(const std::string& sql,
                             const EvaluatorOptions& options)
    : PreparedQueryBase(sql, options) {
  RegisterAllOptionalBuiltinFunctions();
}
PreparedQuery::PreparedQuery(const ResolvedQueryStmt* stmt,
                             const EvaluatorOptions& options)
    : PreparedQueryBase(stmt, options) {
  RegisterAllOptionalBuiltinFunctions();
}

}  // namespace zetasql
