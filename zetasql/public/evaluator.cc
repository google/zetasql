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

#include "zetasql/public/evaluator.h"

#include "zetasql/common/evaluator_registration_utils.h"

namespace zetasql {

PreparedExpression::PreparedExpression(const std::string& sql,
                                       TypeFactory* type_factory)
    : PreparedExpressionBase(sql, type_factory) {
  internal::EnableFullEvaluatorFeatures();
}

PreparedExpression::PreparedExpression(const std::string& sql,
                                       const EvaluatorOptions& options)
    : PreparedExpressionBase(sql, options) {
  internal::EnableFullEvaluatorFeatures();
}

PreparedExpression::PreparedExpression(const ResolvedExpr* expression,
                                       const EvaluatorOptions& options)
    : PreparedExpressionBase(expression, options) {
  internal::EnableFullEvaluatorFeatures();
}

PreparedQuery::PreparedQuery(const std::string& sql,
                             const EvaluatorOptions& options)
    : PreparedQueryBase(sql, options) {
  internal::EnableFullEvaluatorFeatures();
}

PreparedQuery::PreparedQuery(const ResolvedQueryStmt* stmt,
                             const EvaluatorOptions& options)
    : PreparedQueryBase(stmt, options) {
  internal::EnableFullEvaluatorFeatures();
}

PreparedModify::PreparedModify(const std::string& sql,
                               const EvaluatorOptions& options)
    : PreparedModifyBase(sql, options) {
  internal::EnableFullEvaluatorFeatures();
}

PreparedModify::PreparedModify(const ResolvedStatement* stmt,
                               const EvaluatorOptions& options)
    : PreparedModifyBase(stmt, options) {
  internal::EnableFullEvaluatorFeatures();
}

}  // namespace zetasql
