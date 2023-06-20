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

#ifndef ZETASQL_RESOLVED_AST_RESOLVED_AST_HELPER_H_
#define ZETASQL_RESOLVED_AST_RESOLVED_AST_HELPER_H_

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"

namespace zetasql {

// A visitor which collects free ResolvedColumnRef that are referenced, but not
// local to this expression.
class ColumnRefVisitor : public ResolvedASTVisitor {
 public:
  ColumnRefVisitor() = default;

 protected:
  absl::Status VisitResolvedColumnRef(const ResolvedColumnRef* node) override;

  absl::Status VisitResolvedSubqueryExpr(
      const ResolvedSubqueryExpr* node) override;

  absl::Status VisitResolvedInlineLambda(
      const ResolvedInlineLambda* node) override;

  absl::Status VisitResolvedWithExpr(const ResolvedWithExpr* node) override;

  bool IsLocalColumn(const ResolvedColumn& column) const {
    return local_columns_.contains(column);
  }

 private:
  // Columns that are local to an expression -- that is they are defined,
  // populated, and consumed fully within the expression -- should not be
  // collected by this code.
  absl::flat_hash_set<ResolvedColumn> local_columns_;
};

// Returns all ResolvedColumnRef's from the tree under `node` that are
// references to free columns. Note that, a "free" column means that it is not
// defined within the tree under `node`.
absl::StatusOr<absl::flat_hash_set<const ResolvedColumnRef*>>
CollectFreeColumnRefs(const ResolvedNode& node);

// Find the ResolvedComputedColumn in a ResolvedProjectScan producing <column>.
// Return NULL if <column> is not computed by <project>.
const ResolvedComputedColumn* FindProjectComputedColumn(
    const ResolvedProjectScan* project, const ResolvedColumn& column);

// Find the ResolvedExpr in a ResolvedProjectScan producing <column>.
// Return NULL if <column> is not computed by <project>.
const ResolvedExpr* FindProjectExpr(const ResolvedProjectScan* project,
                                    const ResolvedColumn& column);

}  // namespace zetasql

#endif  // ZETASQL_RESOLVED_AST_RESOLVED_AST_HELPER_H_
