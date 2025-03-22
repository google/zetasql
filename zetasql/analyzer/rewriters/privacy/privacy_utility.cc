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

#include "zetasql/analyzer/rewriters/privacy/privacy_utility.h"

#include <memory>
#include <utility>
#include <vector>

#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/analyzer/named_argument_info.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/common/errors.h"
#include "zetasql/parser/ast_node.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {
zetasql_base::StatusBuilder MakeSqlErrorAtNode(const ResolvedNode& node) {
  zetasql_base::StatusBuilder builder = MakeSqlError();
  const auto* parse_location = node.GetParseLocationRangeOrNULL();
  if (parse_location != nullptr) {
    builder.AttachPayload(parse_location->start().ToInternalErrorLocation());
  }
  return builder;
}

absl::StatusOr<std::unique_ptr<ResolvedExpr>> ResolveFunctionCall(
    absl::string_view function_name,
    std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
    std::vector<NamedArgumentInfo> named_arguments, Resolver* resolver) {
  // In order for the resolver to provide error locations, it needs ASTNode
  // locations from the original SQL. However, the functions in these
  // transforms do not necessarily appear in the SQL so they have no locations.
  // Any errors produced here are internal errors, so error locations are not
  // meaningful and we use location stubs instead.
  ASTFunctionCall dummy_ast_function;
  FakeASTNode dummy_ast_location;
  std::vector<const ASTNode*> dummy_arg_locations(arguments.size(),
                                                  &dummy_ast_location);

  // Stub out query/expr resolution info structs. This is ok because we aren't
  // doing any actual resolution here (so we don't need NameScopes, etc.). We
  // are just transforming a function call, and creating a new
  // ResolvedFunctionCall with already-resolved arguments.
  NameScope empty_name_scope;
  QueryResolutionInfo query_resolution_info(resolver);
  ExprResolutionInfo expr_resolution_info(&query_resolution_info,
                                          &empty_name_scope,
                                          {
                                              .allows_aggregation = true,
                                              .allows_analytic = false,
                                          });

  std::unique_ptr<const ResolvedExpr> result;
  absl::Status status = resolver->ResolveFunctionCallWithResolvedArguments(
      &dummy_ast_function, dummy_arg_locations,
      /*match_internal_signatures=*/true, function_name, std::move(arguments),
      std::move(named_arguments), &expr_resolution_info, &result);

  // We expect that the caller passes valid/coercible arguments. An error only
  // occurs if that contract is violated, so this is an internal error.
  ZETASQL_RET_CHECK(status.ok()) << status;

  // The resolver inserts the actual function call for aggregate functions
  // into query_resolution_info, so we need to extract it if applicable.
  if (query_resolution_info.aggregate_columns_to_compute().size() == 1) {
    std::unique_ptr<ResolvedComputedColumnBase> col =
        absl::WrapUnique(const_cast<ResolvedComputedColumnBase*>(
            query_resolution_info.release_aggregate_columns_to_compute()
                .front()
                .release()));
    ZETASQL_RET_CHECK(col->Is<ResolvedComputedColumn>());
    result = col->GetAs<ResolvedComputedColumn>()->release_expr();
  }
  return absl::WrapUnique(const_cast<ResolvedExpr*>(result.release()));
}

}  // namespace zetasql
