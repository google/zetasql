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

// An enum that represents the result of testing whether two expressions are
// same.
enum class TestIsSameExpressionForGroupByResult {
  // Two expressions are considered equal.
  kEqual,
  // Two expressions are considered different.
  kNotEqual,
  // It's unknown whether two expressions are equal or not. For now, this only
  // happens when the expression kind is not supported for comparison, but is
  // the same, and expression output type is the same.
  kUnknown,
};

// Checks whether two expressions are equal for the purpose of allowing
// SELECT expr FROM ... GROUP BY expr, and group by expression deduplication.
// This is a shorthand of TestIsSameExpressionForGroupBy function to return a
// bool result instead. The function treats the testing result kUnknown value as
// if the two expressions are not equal.
absl::StatusOr<bool> IsSameExpressionForGroupBy(const ResolvedExpr* expr1,
                                                const ResolvedExpr* expr2);

// Checks whether two expressions are equal for the purpose of allowing
// SELECT expr FROM ... GROUP BY expr. This is also used by group-by
// expressions deduplication.
// Comparison is done by traversing the ResolvedExpr tree and making sure all
// the nodes are the same, except that volatile functions (i.e. RAND()) are
// never considered equal.
// The comparison result will be equal or not-equal if two expressions are
// completely checked. If some nodes or properties are not listed in the switch
// cases and not checked, the result will be unknown. Function callers can
// handle the unknown cases separately from the known-false cases.
absl::StatusOr<TestIsSameExpressionForGroupByResult>
TestIsSameExpressionForGroupBy(const ResolvedExpr* expr1,
                               const ResolvedExpr* expr2);

// Checks whether the expression references any non-local and non-correlated
// column.
absl::StatusOr<bool> ExprReferencesNonCorrelatedColumn(
    const ResolvedExpr& expr);

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

// Field path expression equality operator for containers.
struct FieldPathExpressionEqualsOperator {
  bool operator()(const ResolvedExpr* expr1, const ResolvedExpr* expr2) const {
    return IsSameFieldPath(expr1, expr2, FieldPathMatchingOption::kExpression);
  }
};

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_EXPR_MATCHING_HELPERS_H_
