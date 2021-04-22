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

#include <string>

#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/base/statusor.h"

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

// Returns a copy of 'expr' where all ResolvedColumnRef that are not below
// a subquery are updated to be marked as correlated column refs.
zetasql_base::StatusOr<std::unique_ptr<ResolvedExpr>> CorrelateColumnRefs(
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

}  // namespace zetasql

#endif  // ZETASQL_RESOLVED_AST_REWRITE_UTILS_H_
