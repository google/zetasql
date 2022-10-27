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
#include <vector>

#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "zetasql/base/status_builder.h"
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

  // Creates a new column with an AnnotatedType, incrementing the counter for
  // next use.
  ResolvedColumn MakeCol(const std::string& table_name,
                         const std::string& col_name, AnnotatedType type);

 private:
  int max_col_id_;
  IdStringPool* id_string_pool_;
  zetasql_base::SequenceNumber* sequence_;

  void UpdateMaxColId();
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

// Sorts and removes duplicates from the ResolvedColumnRefs in 'column_refs'.
// This is used in conjunction with 'CollectColumnRefs' to construct an
// appropriate parameter list for a subquery expression. Among other potential
// uses.
void SortUniqueColumnRefs(
    std::vector<std::unique_ptr<const ResolvedColumnRef>>& column_refs);

// Helper that composes 'CollectColumnRefs' and 'SortUniqueColumnRefs'
absl::Status CollectSortUniqueColumnRefs(
    const ResolvedNode& node,
    std::vector<std::unique_ptr<const ResolvedColumnRef>>& column_refs,
    bool correlate = false);

// A map to keep track of columns that are replaced during an application of
// 'CopyResolvedAstAndRemapColumns'
using ColumnReplacementMap =
    absl::flat_hash_map</*column_in_input=*/ResolvedColumn,
                        /*column_in_output=*/ResolvedColumn>;

// Performs a deep copy of 'input_tree' replacing all of the ResolvedColumns in
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

// Helper function used when deep copying a plan. Takes a 'scan' and
// replaces all of its ResolvedColumns, including in child scans recursively.
// Some columns produced by the 'scan' are remapped to new columns based on
// 'target_column_indices' and 'replacement_columns_to_use'. All other columns
// in the 'scan' and its descendants are replaced by new columns allocated by
// 'column_factory'.
//
// 'target_column_indices' corresponds 1:1 with 'replacement_columns_to_use',
// and maps entries in the 'scan' 'column_list()' to the appropriate
// replacement columns.
//
// Columns in 'replacement_columns_to_use' must have been allocated from
// 'column_factory'.
//
// Ultimately, the copied/returned plan will have all column references
// allocated by 'column_factory', either through the explicit remapping or via
// new allocations.
absl::StatusOr<std::unique_ptr<ResolvedScan>> ReplaceScanColumns(
    ColumnFactory& column_factory, const ResolvedScan& scan,
    const std::vector<int>& target_column_indices,
    const std::vector<ResolvedColumn>& replacement_columns_to_use);

// Creates a new set of replacement columns to the given list.
// Useful when replacing columns for a ResolvedExecuteAsRole node.
std::vector<ResolvedColumn> CreateReplacementColumns(
    ColumnFactory& column_factory,
    const std::vector<ResolvedColumn>& column_list);

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

  // Construct a ResolvedFunctionCall for IFERROR(try_expr, handle_expr)
  //
  // Requires: try_expr and handle_expr must return equal types.
  //
  // The signature for the built-in function "IFERROR" must be available in
  // <catalog> or an error status is returned.
  absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>> IfError(
      std::unique_ptr<const ResolvedExpr> try_expr,
      std::unique_ptr<const ResolvedExpr> handle_expr);

  // Constructs a ResolvedFunctionCall for the $make_array function to create an
  // array for a list of elements
  //
  // Requires: Each element in elements must have the same type as array_type
  //
  // The signature for the built-in function "$make_array" must be available in
  // <catalog> or an error status is returned
  absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>> MakeArray(
      const ArrayType* array_type,
      std::vector<std::unique_ptr<ResolvedExpr>>& elements);

  // Constructs a ResolvedFunctionCall for <input> LIKE <pattern>
  //
  // Requires: <input> and <pattern> must have STRING or BYTES and their types
  // must match
  //
  // The signature for the built-in function "$like" must be available in
  // <catalog> or an error status is returned
  absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>> Like(
      std::unique_ptr<ResolvedExpr> input,
      std::unique_ptr<ResolvedExpr> pattern);

  // Constructs the following expression:
  //   CASE
  //     WHEN <conditions[0]> THEN <results[0]>
  //     WHEN <conditions[1]> THEN <results[1]>
  //     ...
  //   ELSE
  //     <else_result>
  //   END;
  //
  // Requires:
  //  - <conditions> and <results> cannot be empty and must be the same length.
  //  - Elements of <conditions> and <results> must not be nullptr.
  //  - Elements of <conditions> must have type BOOL.
  //  - Elements of <results> must have the same type.
  //  - If <else_result> is nullptr, the constructed CASE expression will have
  //      no ELSE clause. Otherwise, <else_result> must have the same type as
  //      elements in <result>.
  //  - The signature for the built-in function "$case_no_value" must be
  //      available in <catalog>.
  absl::StatusOr<std::unique_ptr<const ResolvedExpr>> CaseNoValue(
      std::vector<std::unique_ptr<const ResolvedExpr>> conditions,
      std::vector<std::unique_ptr<const ResolvedExpr>> results,
      std::unique_ptr<const ResolvedExpr> else_result);

  // Constructs a ResolvedFunctionCall for NOT <expression>
  //
  // Requires: The type of <expression> is a BOOL
  //
  // The signature for the built-in function "$not" must be available in
  // <catalog> or an error status is returned
  absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>> Not(
      std::unique_ptr<const ResolvedExpr> expression);

 private:
  const AnalyzerOptions& analyzer_options_;
  Catalog& catalog_;
};

// Contains helper functions for building components of the ResolvedAST when
// rewriting LIKE ANY and LIKE ALL expressions. It will be used in the case:
// <input> LIKE {{ANY|ALL}} <subquery>
class LikeAnyAllSubqueryScanBuilder {
 public:
  LikeAnyAllSubqueryScanBuilder(const AnalyzerOptions* analyzer_options,
                                Catalog* catalog, ColumnFactory* column_factory)
      : analyzer_options_(analyzer_options),
        catalog_(catalog),
        fn_builder_(*analyzer_options, *catalog),
        column_factory_(column_factory) {}

  // Builds the AggregateScan of the ResolvedAST for a
  // <input> LIKE {{ANY|ALL}} <subquery>
  // expression as detailed at (broken link)
  // Maps to:
  // AggregateScan
  //   +-input_scan=SubqueryScan  // User input subquery
  //     +-pattern_col#2=subquery_column
  //   +-like_agg_col#3=AggregateFunctionCall(
  //         LOGICAL_OR/AND(input_expr#1 LIKE pattern_col#2) -> BOOL)
  //           // OR for ANY, AND for ALL
  //   +-null_agg_col#4=AggregateFunctionCall(
  //         LOGICAL_OR(pattern_col#2 IS NULL) -> BOOL)
  // in the ResolvedAST
  absl::StatusOr<std::unique_ptr<ResolvedAggregateScan>> BuildAggregateScan(
      ResolvedColumn& input_column, ResolvedColumn& subquery_column,
      std::unique_ptr<const ResolvedScan> input_scan,
      ResolvedSubqueryExpr::SubqueryType subquery_type);

 private:
  // Constructs a ResolvedAggregateFunctionCall for a LOGICAL_OR/AND function
  // for use in the LIKE ANY/ALL rewriter
  //
  // The signature for the built-in function "logical_or" or "logical_and" must
  // be available in <catalog> or an error status is returned
  absl::StatusOr<std::unique_ptr<const ResolvedAggregateFunctionCall>>
  AggregateLogicalOperation(FunctionSignatureId context_id,
                            std::unique_ptr<const ResolvedExpr> expression);

  const AnalyzerOptions* analyzer_options_;
  Catalog* catalog_;
  FunctionCallBuilder fn_builder_;
  ColumnFactory* column_factory_;
};

bool IsBuiltInFunctionIdEq(const ResolvedFunctionCall* function_call,
                           FunctionSignatureId function_signature_id);

// Generate an Unimplemented error message - if possible, attach a location.
// Note, Rewriters uniquely need this ability, the resolver generally
// has access to parser ASTNode objects, which more reliably have a
// location.
zetasql_base::StatusBuilder MakeUnimplementedErrorAtNode(const ResolvedNode* node);

}  // namespace zetasql

#endif  // ZETASQL_RESOLVED_AST_REWRITE_UTILS_H_
