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

#ifndef ZETASQL_RESOLVED_AST_REWRITE_UTILS_H_
#define ZETASQL_RESOLVED_AST_REWRITE_UTILS_H_

#include <string>

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"

namespace zetasql {

// A mutable ResolvedColumn factory that creates a new ResolvedColumn with a new
// column id on each call. This prevents column id collisions.
//
// Not thread safe.
class ColIdAllocator {
 public:
  // Allocates columns starting above the max seen column id.
  explicit ColIdAllocator(int max_col_id) : max_col_id_(max_col_id) {}

  // Disable copy assignment, pass around mutable references to the same
  // allocator instead.
  ColIdAllocator(const ColIdAllocator&) = delete;
  ColIdAllocator& operator=(const ColIdAllocator&) = delete;

  // Creates a new column, incrementing the counter for next use.
  ResolvedColumn MakeCol(const std::string& table_name,
                         const std::string& col_name, const Type* type);

 private:
  int max_col_id_;
};

// Visitor which finds the largest column id used in the scans of an AST.
//
// This allows allocating columns starting after that column id to avoid
// conflicts.
class ColIdCounter : public ResolvedASTVisitor {
 public:
  // Returns the maximum column id that has been seen.
  // Will return -1 until the visitor visits an AST.
  int max_col_id() { return max_col_id_; }

 private:
  absl::Status DefaultVisit(const ResolvedNode* node) override;
  absl::Status VisitResolvedComputedColumn(
      const ResolvedComputedColumn* node) override;

  // Visits the given column, updating max_col_id_ if a higher one is seen.
  void VisitColumn(const ResolvedColumn& col);

  int max_col_id_ = -1;
};

}  // namespace zetasql

#endif  // ZETASQL_RESOLVED_AST_REWRITE_UTILS_H_
