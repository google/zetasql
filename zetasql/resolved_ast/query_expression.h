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

#ifndef ZETASQL_RESOLVED_AST_QUERY_EXPRESSION_H_
#define ZETASQL_RESOLVED_AST_QUERY_EXPRESSION_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace zetasql {

// SQLBuilder representation of a SQL query. Holds internal state while
// traversing a ResolvedAST.
class QueryExpression {
 public:
  QueryExpression() {}
  QueryExpression(const QueryExpression&) = delete;
  QueryExpression& operator=(const QueryExpression&) = delete;
  ~QueryExpression() {}

  // Returns true if the clauses necessary to form a SQL query, i.e. select_list
  // or set_op_scan_list, are filled in QueryExpression. Otherwise false.
  bool CanFormSQLQuery() const;

  std::string GetSQLQuery() const;

  // Mutates the QueryExpression, wrapping its previous form as a subquery in
  // the from_ clause, with the given <alias>.
  void Wrap(const std::string& alias);

  // The below TrySet... methods return true if we are able to set the concerned
  // clause in QueryExpression successfully. Otherwise return false with the
  // QueryExpression left unchanged.
  bool TrySetWithClause(
      const std::vector<std::pair<std::string, std::string>>& with_list,
      bool recursive);
  bool TrySetSelectClause(
      const std::vector<std::pair<std::string, std::string>>& select_list,
      const std::string& select_hints);
  bool TrySetFromClause(const std::string& from);
  bool TrySetWhereClause(const std::string& where);
  bool TrySetSetOpScanList(
      std::vector<std::unique_ptr<QueryExpression>>* set_op_scan_list,
      const std::string& set_op_type, const std::string& set_op_modifier,
      const std::string& query_hints);
  bool TrySetGroupByClause(const std::map<int, std::string>& group_by_list,
                           const std::string& group_by_hints,
                           const std::vector<int>& rollup_column_id_list);
  bool TrySetOrderByClause(const std::vector<std::string>& order_by_list,
                           const std::string& order_by_hints);
  bool TrySetLimitClause(const std::string& limit);
  bool TrySetOffsetClause(const std::string& offset);
  bool TrySetWithAnonymizationClause(const std::string& anonymization_options);
  bool TrySetPivotClause(const std::string& pivot);
  bool TrySetUnpivotClause(const std::string& unpivot);

  // The below CanSet... methods return true if filling in the concerned clause
  // in the QueryExpression will succeed (without mutating it or wrapping it as
  // a subquery). Otherwise false.
  bool CanSetWithClause() const;
  bool CanSetSelectClause() const;
  bool CanSetFromClause() const;
  bool CanSetWhereClause() const;
  bool CanSetSetOpScanList() const;
  bool CanSetGroupByClause() const;
  bool CanSetOrderByClause() const;
  bool CanSetLimitClause() const;
  bool CanSetOffsetClause() const;
  bool CanSetWithAnonymizationClause() const;
  bool CanSetPivotClause() const;
  bool CanSetUnpivotClause() const;

  // The below Has... methods return true if the concerned clause is present
  // inside the QueryExpression. Otherwise false.
  bool HasWithClause() const { return !with_list_.empty(); }
  bool HasSelectClause() const { return !select_list_.empty(); }
  bool HasFromClause() const { return !from_.empty(); }
  bool HasWhereClause() const { return !where_.empty(); }
  bool HasSetOpScanList() const { return !set_op_scan_list_.empty(); }
  bool HasGroupByClause() const { return !group_by_list_.empty(); }
  bool HasOrderByClause() const { return !order_by_list_.empty(); }
  bool HasLimitClause() const { return !limit_.empty(); }
  bool HasOffsetClause() const { return !offset_.empty(); }
  bool HasPivotClause() const { return !pivot_.empty(); }
  bool HasUnpivotClause() const { return !unpivot_.empty(); }
  bool HasWithAnonymizationClause() const {
    return !anonymization_options_.empty();
  }

  void ResetSelectClause();

  const std::string FromClause() const { return from_; }

  // Returns an immutable reference to select_list_. For QueryExpression built
  // from a SetOp scan, it returns the select_list_ of its first subquery.
  const std::vector<std::pair<std::string, std::string>>& SelectList() const;

  // Updates the alias for the select column denoted by <select_column_pos> in
  // select_list_. For QueryExpression built from a SetOp scan, it updates the
  // select_list_ of its first subquery.
  void SetAliasForSelectColumn(int select_column_pos, const std::string& alias);

  // Set the AS modifier for the SELECT.  e.g. "AS VALUE".
  void SetSelectAsModifier(const std::string& modifier);

  // Returns a mutable pointer to the group_by_list_ of QueryExpression. Used
  // mostly to update the sql text of the group_by columns to reflect the
  // ordinal position of select clause.
  std::map<int, std::string>* MutableGroupByList() { return &group_by_list_; }

  // Returns a mutable pointer to the from_ clause of QueryExpression. Used
  // while building sql for a sample scan so as to rewrite the from_ clause to
  // include the TABLESAMPLE clause.
  std::string* MutableFromClause() { return &from_; }

  // Returns a mutable pointer to the select_list_ of QueryExpression. Used
  // while building sql for a sample scan that has a WITH WEIGHT clause.
  std::vector<std::pair<std::string, std::string>>* MutableSelectList() {
    return &select_list_;
  }

 private:
  void ClearAllClauses();

  // Fields below define the text associated with different clauses of a SQL
  // query. Some principles:
  // * The text does not include the keyword corresponding to the clause.
  // * If any clause is not present inside the query its corresponding text
  //   would be empty.
  // * GetSQLQuery() will combine these fields into a single SQL query.
  std::vector<
      std::pair<std::string /* with_alias */, std::string /* with_query */>>
      with_list_;
  bool with_recursive_ = false;
  std::vector<std::pair<std::string /* select column */,
                        std::string /* select alias */>>
      select_list_;
  std::string select_as_modifier_;  // "AS TypeName", "AS STRUCT", or "AS VALUE"
  std::string query_hints_;

  std::string from_;
  std::string where_;

  // Contains the keyword corresponding to the set operation (UNION | INTERSECT
  // | EXCEPT).
  std::string set_op_type_;
  // For a set operation, contains either ALL or DISTINCT.
  std::string set_op_modifier_;
  // For QueryExpression of a SetOperationScan, the set_op_scan_list will
  // contain QueryExpression for each of the input queries in the set
  // operation.
  std::vector<std::unique_ptr<QueryExpression>> set_op_scan_list_;

  // We populate the <group_by_list> in two places (in ProjectScan and
  // AggregateScan) based on where the group_by column was computed.
  // This map stores the sql text of the computed column for group_by, i.e.
  // ordinal position of select clause as text if the column was computed in
  // select list, otherwise the text form of the expression if the column was
  // computed in group_by.
  //
  // NOTE: The map is keyed by column_id of the columns created by the
  // AggregateScan. In resolver output, these columns are always allocated
  // sequentially, so this map will preserve the order of the group_by from the
  // initial query.
  std::map<int, std::string> group_by_list_;
  // Column IDs of group by keys in the ROLLUP list. group_by_list_ stores the
  // string representations of these columns. Will be non-empty only if the
  // query used ROLLUP.
  std::vector<int> rollup_column_id_list_;

  std::string group_by_hints_;

  std::vector<std::string> order_by_list_;
  std::string order_by_hints_;

  std::string limit_;
  std::string offset_;

  std::string anonymization_options_;
  std::string pivot_;
  std::string unpivot_;
};

}  // namespace zetasql

#endif  // ZETASQL_RESOLVED_AST_QUERY_EXPRESSION_H_
