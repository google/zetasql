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

#include "zetasql/analyzer/lambda_util.h"

#include <vector>

#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/id_string.h"
#include "absl/status/status.h"

namespace zetasql {

// Extracts lambda argument name from the `arg_expr` and appends it to `names`.
// Return error if the `arg_expr` is not a path expression with a single
// identifier.
static absl::Status ExtractArgumentNameFromExpr(const ASTExpression* arg_expr,
                                                std::vector<IdString>* names) {
  const ASTPathExpression* path_expr =
      arg_expr->GetAsOrNull<ASTPathExpression>();
  if (path_expr == nullptr) {
    return MakeSqlErrorAt(arg_expr)
           << "Lambda argument name must be a single identifier";
  }
  if (path_expr->num_names() != 1) {
    return MakeSqlErrorAt(arg_expr)
           << "Lambda argument name must be a single identifier";
  }
  if (names != nullptr) {
    names->push_back(path_expr->name(0)->GetAsIdString());
  }
  return absl::OkStatus();
}

// Extracts lambda argument names from `ast_lambda` and append them to `names`.
// Returns error if the argument list is not in the right shape. `names` could
// nullptr when only check is needed.
static absl::Status ExtractLambdaArgumentNames(const ASTLambda* ast_lambda,
                                               std::vector<IdString>* names) {
  const ASTExpression* args_expr = ast_lambda->argument_list();
  if (args_expr->node_kind() == AST_STRUCT_CONSTRUCTOR_WITH_PARENS) {
    const ASTStructConstructorWithParens* struct_cons =
        args_expr->GetAsOrDie<ASTStructConstructorWithParens>();
    const absl::Span<const ASTExpression* const>& fields =
        struct_cons->field_expressions();
    if (names != nullptr) {
      names->reserve(fields.size());
    }
    for (const ASTExpression* field : fields) {
      ZETASQL_RETURN_IF_ERROR(ExtractArgumentNameFromExpr(field, names));
    }
  } else if (args_expr->node_kind() == AST_PATH_EXPRESSION) {
    ZETASQL_RETURN_IF_ERROR(ExtractArgumentNameFromExpr(args_expr, names));
  } else {
    return MakeSqlErrorAt(args_expr) << "Expecting lambda argument list";
  }
  return absl::OkStatus();
}

zetasql_base::StatusOr<std::vector<IdString>> ExtractLambdaArgumentNames(
    const ASTLambda* ast_lambda) {
  std::vector<IdString> names;
  ZETASQL_RETURN_IF_ERROR(ExtractLambdaArgumentNames(ast_lambda, &names));
  return names;
}

absl::Status ValidateLambdaArgumentListIsIdentifierList(
    const ASTLambda* ast_lambda) {
  return ExtractLambdaArgumentNames(ast_lambda, /*names=*/nullptr);
}

}  // namespace zetasql
