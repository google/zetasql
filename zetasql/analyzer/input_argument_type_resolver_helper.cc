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
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/function.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"

namespace zetasql {

InputArgumentType GetInputArgumentTypeForExpr(const ResolvedExpr* expr) {
  ZETASQL_DCHECK(expr != nullptr);
  if (expr->type()->IsStruct() && expr->node_kind() == RESOLVED_MAKE_STRUCT) {
    const ResolvedMakeStruct* struct_expr = expr->GetAs<ResolvedMakeStruct>();
    std::vector<InputArgumentType> field_types;
    field_types.reserve(struct_expr->field_list_size());
    for (const std::unique_ptr<const ResolvedExpr>& argument :
         struct_expr->field_list()) {
      field_types.push_back(GetInputArgumentTypeForExpr(argument.get()));
    }
    // We construct a custom InputArgumentType for structs that may have
    // some literal and some non-literal fields.
    return InputArgumentType(expr->type()->AsStruct(), field_types);
  }

  // Literals that were explicitly casted (i.e., the original expression was
  // 'CAST(<literal> AS <type>)') are treated like non-literals with
  // respect to subsequent coercion.
  if (expr->node_kind() == RESOLVED_LITERAL &&
      !expr->GetAs<ResolvedLiteral>()->has_explicit_type()) {
    if (expr->GetAs<ResolvedLiteral>()->value().is_null()) {
      // This is a literal NULL that does not have an explicit type, so
      // it can coerce to anything.
      return InputArgumentType::UntypedNull();
    }
    // This is a literal empty array that does not have an explicit type,
    // so it can coerce to any array type.
    if (expr->GetAs<ResolvedLiteral>()->value().is_empty_array()) {
      return InputArgumentType::UntypedEmptyArray();
    }
    return InputArgumentType(expr->GetAs<ResolvedLiteral>()->value());
  }

  if (expr->node_kind() == RESOLVED_PARAMETER &&
      expr->GetAs<ResolvedParameter>()->is_untyped()) {
    // Undeclared parameters can be coerced to any type.
    return InputArgumentType::UntypedQueryParameter();
  }

  if (expr->node_kind() == RESOLVED_FUNCTION_CALL &&
      expr->GetAs<ResolvedFunctionCall>()->function()->FullName(
          true /* include_group */) == "ZetaSQL:error") {
    // This is an ERROR(message) function call.  We special case this to
    // make the output argument coercible to anything so expressions like
    //   IF(<condition>, <value>, ERROR("message"))
    // work for any value type.
    //
    // Note that this case does not apply if ERROR() is wrapped in a CAST, since
    // that expression has an explicit type. For example,
    // COALESCE('abc', CAST(ERROR('def') AS BYTES)) fails because BYTES does not
    // implicitly coerce to STRING.
    return InputArgumentType::UntypedNull();
  }

  return InputArgumentType(expr->type(),
                           expr->node_kind() == RESOLVED_PARAMETER);
}

static InputArgumentType GetInputArgumentTypeForGenericArgument(
    const ASTNode* argument_ast_node, const ResolvedExpr* expr) {
  ZETASQL_DCHECK(argument_ast_node != nullptr);
  // Only lambdas uses nullptr as placeholder.
  if (argument_ast_node->Is<ASTLambda>() || expr == nullptr) {
    ZETASQL_DCHECK(expr == nullptr) << "Lambda must have a nullptr placeholder";
    ZETASQL_DCHECK(argument_ast_node->Is<ASTLambda>())
        << "A nullptr placeholder can only be used for a lambda argument";
    return InputArgumentType::LambdaInputArgumentType();
  }
  ZETASQL_DCHECK(expr != nullptr);
  return GetInputArgumentTypeForExpr(expr);
}

void GetInputArgumentTypesForGenericArgumentList(
    const std::vector<const ASTNode*>& argument_ast_nodes,
    const std::vector<std::unique_ptr<const ResolvedExpr>>& arguments,
    std::vector<InputArgumentType>* input_arguments) {
  ZETASQL_DCHECK_EQ(argument_ast_nodes.size(), arguments.size());
  input_arguments->clear();
  input_arguments->reserve(arguments.size());
  for (int i = 0; i < argument_ast_nodes.size(); i++) {
    input_arguments->push_back(GetInputArgumentTypeForGenericArgument(
        argument_ast_nodes[i], arguments[i].get()));
  }
}

}  // namespace zetasql
