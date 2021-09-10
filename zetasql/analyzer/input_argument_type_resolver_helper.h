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

#ifndef ZETASQL_ANALYZER_INPUT_ARGUMENT_TYPE_RESOLVER_HELPER_H_
#define ZETASQL_ANALYZER_INPUT_ARGUMENT_TYPE_RESOLVER_HELPER_H_

#include <memory>
#include <vector>

#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/function.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/types/optional.h"

namespace zetasql {

// Get an InputArgumentType for a ResolvedExpr, identifying whether or not it
// is a parameter and pointing at the literal value inside <expr> if
// appropriate.  <expr> must outlive the returned object.
InputArgumentType GetInputArgumentTypeForExpr(const ResolvedExpr* expr);

// Get a list of <InputArgumentType> from a list of <ASTNode> and
// <ResolvedExpr>, invoking GetInputArgumentTypeForExpr() on each of the
// <argument_ast_nodes> and <arguments>.
// This method is called before signature matching. Lambdas are not resolved
// yet. <argument_ast_nodes> are used to determine InputArgumentType for lambda
// arguments.
void GetInputArgumentTypesForGenericArgumentList(
    const std::vector<const ASTNode*>& argument_ast_nodes,
    const std::vector<std::unique_ptr<const ResolvedExpr>>& arguments,
    std::vector<InputArgumentType>* input_arguments);

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_INPUT_ARGUMENT_TYPE_RESOLVER_HELPER_H_
