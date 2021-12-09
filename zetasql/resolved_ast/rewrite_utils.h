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

#ifndef ZETASQL_RESOLVED_AST_REWRITE_UTILS_H_
#define ZETASQL_RESOLVED_AST_REWRITE_UTILS_H_

#include <memory>
#include <string>

#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// A mutable ResolvedColumn factory that creates a new ResolvedColumn with a new
// column id on each call. This prevents column id collisions.
//
// Not thread safe.
class ColumnFactory {
 public:
  // Creates columns using column ids starting above the max seen column id.
  //
  // IdString's for column names are allocated from the IdStringPool provided,
  // which must outlive this ColumnFactory object.
  //
  // If 'sequence' is provided, it's used to do the allocations. IDs from the
  // sequence that are not above 'max_col_id' are discarded.
  ColumnFactory(int max_col_id, IdStringPool* id_string_pool,
                zetasql_base::SequenceNumber* sequence = nullptr)
      : max_col_id_(max_col_id),
        id_string_pool_(id_string_pool),
        sequence_(sequence) {
    // The implementation assumes that a nullptr <id_string_pool_> indicates
    // that the ColumnFactory was created with the legacy constructor that uses
    // the global string pool.
    //
    // This check ensures that it is safe to remove this assumption, once the
    // legacy constructor is removed and all callers have been migrated.
    ZETASQL_CHECK(id_string_pool != nullptr);
  }

  // Similar to the above constructor, except allocates column ids on the global
  // string pool.
  //
  // WARNING: Column factories produced by this constructor will leak memory
  // each time a column is created. To avoid this, use the above constructor
  // overload instead and supply an IdStringPool.
  ABSL_DEPRECATED(
      "This constructor will result in a ColumnFactory that leaks "
      "memory. Use overload that consumes an IdStringPool instead")
  explicit ColumnFactory(int max_col_id,
                         zetasql_base::SequenceNumber* sequence = nullptr)
      : max_col_id_(max_col_id),
        id_string_pool_(nullptr),
        sequence_(sequence) {}

  ColumnFactory(const ColumnFactory&) = delete;
  ColumnFactory& operator=(const ColumnFactory&) = delete;

  // Returns the maximum column id that has been allocated.
  int max_column_id() const { return max_col_id_; }

  // Creates a new column, incrementing the counter for next use.
  ResolvedColumn MakeCol(const std::string& table_name,
                         const std::string& col_name, const Type* type);

 private:
  int max_col_id_;
  IdStringPool* id_string_pool_;
  zetasql_base::SequenceNumber* sequence_;
};

// Returns a copy of 'expr' where all references to columns that are not
// internal to 'expr' as correlated. This is useful when moving a scalar
// expression into a new subquery expression.
template <class T>
absl::StatusOr<std::unique_ptr<T>> CorrelateColumnRefs(const T& expr) {
  ZETASQL_ASSIGN_OR_RETURN(auto correlated, CorrelateColumnRefsImpl(expr));
  ZETASQL_RET_CHECK(correlated->template Is<T>());
  return absl::WrapUnique(correlated.release()->template GetAs<T>());
}

// Type-erased implementation details of CorrelateColumnRefs template.
absl::StatusOr<std::unique_ptr<ResolvedExpr>> CorrelateColumnRefsImpl(
    const ResolvedExpr& expr);

// Fills column_refs with a copy of all ResolvedColumnRef nodes under 'node'
// which are not below a subquery.
//
// If `correlate` is true, the column refs are correlated regardless of whether
// or not they are in the original node tree.
absl::Status CollectColumnRefs(
    const ResolvedNode& node,
    std::vector<std::unique_ptr<const ResolvedColumnRef>>* column_refs,
    bool correlate = false);

// A map to keep track of columns that are replaced during an application of
// 'CopyResolvedAstAndRemapColumns'
using ColumnReplacementMap =
    absl::flat_hash_map</*column_in_input=*/ResolvedColumn,
                        /*column_in_output=*/ResolvedColumn>;

// Prforms a deep copy of 'input_tree' replacing all of the ResolvedColumns in
// that tree either with ResolvedColumns as specified by 'column_map' or by new
// columns allocated from 'column_factory' for any column not found in
// 'column_map'.
//
// 'column_map' is both an input and output parameter. As an input parameter,
//     it allows invoking code to specify explicit replacements for certain
//     columns in 'input_tree'. As an output parameter, it returns to invoking
//     code all the columns allocated from 'column_factory' during the copy.
template <class T>
absl::StatusOr<std::unique_ptr<T>> CopyResolvedASTAndRemapColumns(
    const T& input_tree, ColumnFactory& column_factory,
    ColumnReplacementMap& column_map) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedNode> ret,
                   CopyResolvedASTAndRemapColumnsImpl(
                       input_tree, column_factory, column_map));
  ZETASQL_RET_CHECK(ret->Is<T>());
  return absl::WrapUnique(ret.release()->GetAs<T>());
}

// Implementation of the above template.
absl::StatusOr<std::unique_ptr<ResolvedNode>>
CopyResolvedASTAndRemapColumnsImpl(const ResolvedNode& input_tree,
                                   ColumnFactory& column_factory,
                                   ColumnReplacementMap& column_map);

// Contains helper functions that reduce boilerplate in rewriting rules logic
// related to constructing new ResolvedFunctionCall instances.
class FunctionCallBuilder {
 public:
  FunctionCallBuilder(const AnalyzerOptions& analyzer_options, Catalog& catalog)
      : analyzer_options_(analyzer_options), catalog_(catalog) {}

  // Construct ResolvedFunctionCall for IF(<condition>, <then_case>,
  // <else_case>)
  //
  // Requires: condition is a bool returning expression and then_case and
  //           else_case return equal types.
  //
  // The signature for the built-in function "IF" must be available in <catalog>
  // or an error status is returned.
  absl::StatusOr<std::unique_ptr<ResolvedFunctionCall>> If(
      std::unique_ptr<const ResolvedExpr> condition,
      std::unique_ptr<const ResolvedExpr> then_case,
      std::unique_ptr<const ResolvedExpr> else_case);

  // Construct ResolvedFunctionCall for <arg> IS NULL
  //
  // The signature for the built-in function "$is_null" must be available in
  // <catalog> or an error status is returned.
  absl::StatusOr<std::unique_ptr<ResolvedFunctionCall>> IsNull(
      std::unique_ptr<const ResolvedExpr> arg);

 private:
  const AnalyzerOptions& analyzer_options_;
  Catalog& catalog_;
};

}  // namespace zetasql

#endif  // ZETASQL_RESOLVED_AST_REWRITE_UTILS_H_
