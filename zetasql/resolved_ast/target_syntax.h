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

#ifndef ZETASQL_RESOLVED_AST_TARGET_SYNTAX_H_
#define ZETASQL_RESOLVED_AST_TARGET_SYNTAX_H_

#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/container/flat_hash_map.h"

namespace zetasql {

// Describes the target syntax attached to a resolved AST.
// This is a side channel used by SQLBuilder to decide which SQL syntax to
// produce when multiple choices are available.
enum class SQLBuildTargetSyntax {
  kGroupByAll,  // Represents GROUP BY ALL produced a
                // ResolvedAggregateScan.
  kAnonymousGraphElement,  // Represents a ResolvedGraphElementScan that
                           // was not given a user supplied alias.
  kGqlWith,                // Represents a ResolvedProjectScan produced by
                           // graph WITH operator.
  // The below 3 GQL subquery target syntaxes are mutually exclusive with each
  // other.
  kGqlSubquery,  // Represents a ResolvedSubqueryExpr that should be rendered
                 // as a regular GQL subquery.
  kGqlExistsSubqueryGraphPattern,  // Represents an EXISTS GQL subquery
                                   // in partial form 1 (graph pattern case).
  kGqlExistsSubqueryLinearOps,     // Represents an EXISTS GQL subquery
                                   // in partial form 2 (linear ops case).
};

using TargetSyntaxMap =
    absl::flat_hash_map<ResolvedNode*, SQLBuildTargetSyntax>;

}  // namespace zetasql

#endif  // ZETASQL_RESOLVED_AST_TARGET_SYNTAX_H_
