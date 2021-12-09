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

#ifndef ZETASQL_ANALYZER_SUBSTITUTE_H_
#define ZETASQL_ANALYZER_SUBSTITUTE_H_

#include <memory>
#include <string>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

// Parses 'expression', which may reference names in 'variables' and 'lambdas'.
// Returns a ResolvedExpr that incorporates 'expressions', 'variables' and
// 'lambdas' into a single tree. The SQL is analyzed with 'options', 'catalog',
// and 'type_factory'. In the resulting resolved AST, we project the variable
// subsitutions so that they only appear once in the resulting AST, rather than
// appearing everywhere where they are referenced.
//
// For this node to reasonably be incorporated into a valid parent AST,
// 'options' must contain a zetasql_base::SequenceNumber that is higher than the highest
// column id currently present in the parent AST or any of the 'variable'
// expressions. 'options' must also use the same arenas as the parent AST did
// during its analysis.
//
// 'expression' must not directly reference any positional query parameters (?)
// and it must only reference named parameters that are keys in 'lambdas'.
//
// 'variables' is a list of name->ResolvedExpr pairs. Each name is available
// when we analyze 'expression' as a column, with the same value as the
// ResolvedExpr. Note that each ResolvedExpr is only computed once. There must
// be no collision between the names in 'variables' and 'lambdas'.
//
// 'lambdas' is a map of lambda names to ResolvedInlineLambdas. Lambdas can be
// invoked with INVOKE function calls in the SQL (see example below). The first
// argument is a named parameter with the name of a lambda in 'lambdas'. The
// remaining arguments are a list of columns defined in the SQL, which are to
// replace lambda argument references in the lambda body. NOTE: INVOKE function
// is a special function only available inside AnalyzeSubstitute for handling
// lambdas. For example a INVOKE call:
//     INVOKE(@mylambda, element, offset)
// with mylambda as '(e, i)->e+i', will be equivalent to: 'element + offset'.
//
// This is primarily designed for use in resolved AST rewrite rules. For
// example, suppose you wanted to make a rewrite rule for
//
// <map_arg>[KEY(<key_arg>)]
//
// You could use the following code to make such a substitution:
//
// AnalyzeSubstitute(options, catalog, type_factory, "R(
//     CASE
//       -- If the map is null the result is null.
//       WHEN m IS NULL THEN NULL
//       -- If the key isn't found, then it's an error.
//       ELSE IFNULL( ( SELECT value FROM UNNEST(m) WHERE key = k ),
//                    ERROR(FORMAT("key %t is not in map", k)) )
//     END)",
//     {{"m", <resolved_expr_for_map_arg>},
//      {"k", <resolved_expr_for_key_arg>}});
//
// The return value of this call will be the AST equivalent to:
//
// ( SELECT CASE WHEN m ... END
//   FROM ( SELECT map_arg AS m, key_arg as k ) )
//
// Note that actually producing this AST by hand would take possibly more than
// a hundred lines of code.
absl::StatusOr<std::unique_ptr<ResolvedExpr>> AnalyzeSubstitute(
    AnalyzerOptions options, Catalog& catalog, TypeFactory& type_factory,
    absl::string_view expression,
    const absl::flat_hash_map<std::string, const ResolvedExpr*>& variables,
    const absl::flat_hash_map<std::string, const ResolvedInlineLambda*>&
        lambdas = {});

// Helper function which translates errors returned by AnalyzeSubstitute() into
// internal errors. This function is intended for rewriters who expect their
// calls to AnalyzeSubstitute() to always succeed.
//
// Typical usage:
//   ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> expr,
//      AnalyzeSubstitute(...), _.With(ExpectAnalyzeSubstituteSuccess));
absl::Status ExpectAnalyzeSubstituteSuccess(zetasql_base::StatusBuilder status_builder);

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_SUBSTITUTE_H_
