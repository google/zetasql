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

#ifndef ZETASQL_PUBLIC_PREPARED_EXPRESSION_CONSTANT_EVALUATOR_H_
#define ZETASQL_PUBLIC_PREPARED_EXPRESSION_CONSTANT_EVALUATOR_H_

#include <utility>

#include "zetasql/public/constant_evaluator.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/statusor.h"

namespace zetasql {

// A ConstantEvaluator that uses PreparedExpression to do in-memory expression
// evaluation.
class PreparedExpressionConstantEvaluator final : public ConstantEvaluator {
 public:
  explicit PreparedExpressionConstantEvaluator(EvaluatorOptions options)
      : eval_options_(std::move(options)) {}

  PreparedExpressionConstantEvaluator(EvaluatorOptions options,
                                      const LanguageOptions& language_options)
      : eval_options_(std::move(options)),
        language_options_(language_options) {}

  absl::StatusOr<Value> Evaluate(
      const ResolvedExpr& constant_expression) override;

 private:
  const EvaluatorOptions eval_options_;
  const LanguageOptions language_options_;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_PREPARED_EXPRESSION_CONSTANT_EVALUATOR_H_
