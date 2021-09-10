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

#ifndef ZETASQL_ANALYZER_EXPR_MATCHING_HELPERS_H_
#define ZETASQL_ANALYZER_EXPR_MATCHING_HELPERS_H_

#include <stddef.h>

#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/statusor.h"

namespace zetasql {

// Checks whether two expressions are equal for the purpose of allowing
// SELECT expr FROM ... GROUP BY expr
// Comparison is done by traversing the ResolvedExpr tree and making sure all
// the nodes are the same, except that volatile functions (i.e. RAND()) are
// never considered equal.
// This function is conservative, i.e. if some nodes or some properties are
// not explicitly checked by it - expressions are considered not the same.
absl::StatusOr<bool> IsSameExpressionForGroupBy(const ResolvedExpr* expr1,
                                                const ResolvedExpr* expr2);

// Hashing function for field paths, which enables faster set lookups and
// insertions.
size_t FieldPathHash(const ResolvedExpr* expr);

enum class FieldPathMatchingOption { kExpression, kFieldPath };

// This function determines whether <field_path1> and <field_path2> are
// generalized path expressions that point to the same field.
//
// If the FieldPathMatchingOption::kExpression option is specified, this
// function returns true if <field_path1> and <field_path2> are interchangeable
// generalized path expressions. This considers specialized field accesses
// (currently, the only such case is PROTO_DEFAULT_IF_NULL) as well as ensures
// the descriptors for any proto types involved come from the same descriptor
// pool. This option guarantees <field_path1> and <field_path2> evaluate to the
// same result.
//
// If the FieldPathMatchingOption::kFieldPath option is specified, this function
// returns true if <field_path1> and <field_path2> read the same field. This
// option does consider whether the has_bit of the field is being accessed by
// both <field_path1> and <field_path2>. However, it does not consider any
// specialized field accesses. Therefore, this option does not guarantee
// <field_path1> and <field_path2> evaluate to the same result.
bool IsSameFieldPath(const ResolvedExpr* field_path1,
                     const ResolvedExpr* field_path2,
                     FieldPathMatchingOption match_option);

// Field path hashing operator for containers.
struct FieldPathHashOperator {
  size_t operator()(const ResolvedExpr* expr) const {
    return FieldPathHash(expr);
  }
};

// Field path equality operator for containers.
struct FieldPathEqualsOperator {
  bool operator()(const ResolvedExpr* expr1, const ResolvedExpr* expr2) const {
    return IsSameFieldPath(expr1, expr2, FieldPathMatchingOption::kFieldPath);
  }
};

// Field path expression equality operator for containers.
struct FieldPathExpressionEqualsOperator {
  bool operator()(const ResolvedExpr* expr1, const ResolvedExpr* expr2) const {
    return IsSameFieldPath(expr1, expr2, FieldPathMatchingOption::kExpression);
  }
};

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_EXPR_MATCHING_HELPERS_H_
