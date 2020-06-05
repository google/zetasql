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

#ifndef ZETASQL_ANALYZER_EXPR_RESOLVER_HELPER_H_
#define ZETASQL_ANALYZER_EXPR_RESOLVER_HELPER_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "zetasql/analyzer/name_scope.h"
#include "zetasql/public/function.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include <cstdint>
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/status.h"

namespace zetasql {

class ASTExpression;
class ASTNode;
class QueryResolutionInfo;
struct ExprResolutionInfo;

// Return true if <expr> can be treated like a constant,
// meaning it has the same value for the entire query and has no
// side effects.
//
// Functions like RAND() are not constant.  UDFs may not be constant.
// Functions like CURRENT_TIMESTAMP() are considered constant because they
// always return a constant within the same query.
//
// The current definition uses these rules:
// - literals and parameters are constant
// - column references are not constant
// - scalar functions are constant if FunctionOptions::volatility is
//   IMMUTABLE or STABLE, and if all arguments are constant
// - aggregate and analytic functions are not constant
// - expression subqueries are not constant
// - built-in operators like CAST and CASE and struct field access are
//   constant if all arguments are constant
bool IsConstantExpression(const ResolvedExpr* expr);

// Checks whether two expressions are equal for the purpose of allowing
// SELECT expr FROM ... GROUP BY expr
// Comparison is done by traversing the ResolvedExpr tree and making sure all
// the nodes are the same, except that volatile functions (i.e. RAND()) are
// never considered equal.
// This function is conservative, i.e. if some nodes or some properties are
// not explicitly checked by it - expressions are considered not the same.
// TODO: Make it return absl::Status for better error reporting.
bool IsSameExpressionForGroupBy(const ResolvedExpr* expr1,
                                const ResolvedExpr* expr2);

// SelectColumnState contains state related to an expression in the
// select-list of a query, while it is being resolved.  This is used and
// mutated in multiple passes while resolving the SELECT-list and GROUP BY.
// TODO: Move this to query_resolution_helper.cc, it is more
// query specific than expression specific.
struct SelectColumnState {
  explicit SelectColumnState(const ASTExpression* ast_expr_in,
                             IdString alias_in, bool is_explicit_in,
                             int select_list_position_in)
      : ast_expr(ast_expr_in),
        alias(alias_in),
        is_explicit(is_explicit_in),
        select_list_position(select_list_position_in) {}

  SelectColumnState(const SelectColumnState&) = delete;
  SelectColumnState& operator=(const SelectColumnState&) = delete;

  // Gets the Type of this SELECT list column.  Can return NULL if the
  // related <ast_expr> has not been resolved yet.
  const Type* GetType() const;

  // Returns whether or not this SELECT list column has a pre-GROUP BY
  // column assigned to it.
  bool HasPreGroupByResolvedColumn() const {
    return resolved_pre_group_by_select_column.IsInitialized();
  }

  // Returns a multi-line debug string, where each line is prefixed by <indent>.
  std::string DebugString(absl::string_view indent = "") const;

  // Points at the * if this came from SELECT *.
  const ASTExpression* ast_expr;

  // The alias provided by the user or computed for this column.
  const IdString alias;

  // True if the alias for this column is an explicit name. Generally, explicit
  // names come directly from the query text, and implicit names are those that
  // are generated automatically from something outside the query text, like
  // column names that come from a table schema. Explicitness does not change
  // any scoping behavior except for the final check in strict mode that may
  // raise an error. For more information, please see the beginning of
  // (broken link).
  const bool is_explicit;

  // 0-based position in the SELECT-list after star expansion.
  // Stores -1 when position is not known yet. This never happens for a
  // SelectColumnState stored inside a SelectColumnStateList.
  int select_list_position;

  // Owned ResolvedExpr for this SELECT list column.  If we need a
  // ResolvedComputedColumn for this SELECT column, then ownership of
  // this <resolved_expr> will be transferred to that ResolvedComputedColumn
  // and <resolved_expr> will be set to NULL.
  std::unique_ptr<const ResolvedExpr> resolved_expr;

  // References the related ResolvedComputedColumn for this SELECT list column,
  // if one is needed.  Otherwise it is NULL.  The referenced
  // ResolvedComputedColumn is owned by a column list in QueryResolutionInfo.
  // The reference here is required to allow us to maintain the relationship
  // between this SELECT list column and its related expression for
  // subsequent HAVING and ORDER BY expression analysis.
  // Not owned.
  const ResolvedComputedColumn* resolved_computed_column = nullptr;

  // True if this expression includes aggregation.  Select-list expressions
  // that use aggregation cannot be referenced in GROUP BY.
  bool has_aggregation = false;

  // True if this expression includes analytic functions.
  bool has_analytic = false;

  // If true, this expression is used as a GROUP BY key.
  bool is_group_by_column = false;

  // The output column of this select list item.  It is projected by a scan
  // that computes the related expression.  After the SELECT list has
  // been fully resolved, <resolved_select_column> will be initialized.
  // After it is set, it is used in subsequent expression resolution (SELECT
  // list ordinal references and SELECT list alias references).
  ResolvedColumn resolved_select_column;

  // If set, indicates the pre-GROUP BY version of the column.  Will only
  // be set if the column must be computed before the AggregateScan (so
  // it will not necessarily always be set if is_group_by_column is true).
  ResolvedColumn resolved_pre_group_by_select_column;
};

// This class contains a SelectColumnState for each column in the SELECT list
// and resolves the alias or ordinal references to the SELECT-list column.
class SelectColumnStateList {
 public:
  SelectColumnStateList() {}
  SelectColumnStateList(const SelectColumnStateList&) = delete;
  SelectColumnStateList& operator=(const SelectColumnStateList&) = delete;

  // Creates and returns a SelectColumnState for a new SELECT-list column.
  // 'is_explicit' should be true if 'alias' is an explicit name. Generally,
  // explicit names come directly from the query text, and implicit names are
  // those that are generated automatically from something outside the query
  // text, like column names that come from a table schema. Explicitness does
  // not change any scoping behavior except for the final check in strict mode
  // that may raise an error. For more information, please see the beginning of
  // (broken link).
  SelectColumnState* AddSelectColumn(const ASTExpression* ast_expr,
                                     IdString alias, bool is_explicit);

  // Add an already created SelectColumnState. Takes ownership. If save_mapping
  // is true, saves a mapping from the alias to this SelectColumnState. The
  // mapping is later used for validations performed by
  // FindAndValidateSelectColumnStateByAlias().
  void AddSelectColumn(SelectColumnState* select_column_state);

  // Finds a SELECT-list column by alias. Returns an error if the
  // name is ambiguous or the referenced column contains an aggregate or
  // analytic function that is disallowed as per <expr_resolution_info>.
  // If the name is not found, sets <*select_column_state> to NULL and
  // returns OK.
  absl::Status FindAndValidateSelectColumnStateByAlias(
      const char* clause_name, const ASTNode* ast_location, IdString alias,
      const ExprResolutionInfo* expr_resolution_info,
      const SelectColumnState** select_column_state) const;

  // Finds a SELECT-list column by ordinal. Returns an error if
  // the ordinal number is out of the valid range or the referenced column
  // contains an aggregate or analytic function that is disallowed as per
  // <expr_resolution_info>.
  absl::Status FindAndValidateSelectColumnStateByOrdinal(
      const std::string& expr_description, const ASTNode* ast_location,
      const int64_t ordinal, const ExprResolutionInfo* expr_resolution_info,
      const SelectColumnState** select_column_state) const;

  static absl::Status ValidateAggregateAndAnalyticSupport(
      const absl::string_view& column_description, const ASTNode* ast_location,
      const SelectColumnState* select_column_state,
      const ExprResolutionInfo* expr_resolution_info);

  // <select_list_position> is 0-based position after star expansion.
  SelectColumnState* GetSelectColumnState(int select_list_position);
  const SelectColumnState* GetSelectColumnState(int select_list_position) const;

  const std::vector<std::unique_ptr<SelectColumnState>>&
  select_column_state_list() const;

  // Returns a list of output ResolvedColumns, one ResolvedColumn per
  // <select_column_state_list_> entry.  Currently only used when creating an
  // OrderByScan and subsequent ProjectScan, ensuring that all SELECT list
  // columns are produced by those scans.  For those callers, all
  // ResolvedColumns in the list are initialized.
  const ResolvedColumnList resolved_column_list() const;

  // Returns the number of SelectColumnStates.
  int Size() const;

  std::string DebugString() const;

 private:
  std::vector<std::unique_ptr<SelectColumnState>> select_column_state_list_;

  // Map from SELECT-list column aliases (lowercase) to column
  // position in select_column_state_list_. These names can be referenced in
  // GROUP BY, overriding other names in scope. Ambiguous names will be
  // stored as -1.
  std::map<IdString, int, IdStringCaseLess>
      column_alias_to_state_list_position_;
};

// This contains common info needed to resolve and validate an expression.
// It includes both the constant info describing what is allowed while
// resolving the expression and mutable info that returns what it actually
// has. It is passed recursively down through all expressions.
struct ExprResolutionInfo {
  // Construct an ExprResolutionInfo with given resolution context and
  // constraints.  Takes a <name_scope_in> that is used to resolve the
  // expression against, and an <aggregate_name_scope_in> that is used
  // to resolve any expression that is an aggregate function argument.
  // Does not take ownership of <select_column_state_list_in>,
  // <query_resolution_info_in>, or <top_level_ast_expr_in>.
  ExprResolutionInfo(const NameScope* name_scope_in,
                     const NameScope* aggregate_name_scope_in,
                     bool allows_aggregation_in, bool allows_analytic_in,
                     bool use_post_grouping_columns_in,
                     const char* clause_name_in,
                     QueryResolutionInfo* query_resolution_info_in,
                     const ASTExpression* top_level_ast_expr_in = nullptr,
                     IdString column_alias_in = IdString());

  // Construct an ExprResolutionInfo that allows both aggregation and
  // analytic expressions.
  // Does not take ownership of <query_resolution_info_in>.
  // Currently used for initially resolving select list columns, and
  // resolving LIMIT with an empty NameScope, so never resolves against
  // post-grouping columns.
  ExprResolutionInfo(const NameScope* name_scope_in,
                     QueryResolutionInfo* query_resolution_info_in,
                     const ASTExpression* top_level_ast_expr_in = nullptr,
                     IdString column_alias_in = IdString());

  // Construct an ExprResolutionInfo that disallows aggregation and analytic
  // expressions.
  ExprResolutionInfo(const NameScope* name_scope_in,
                     const char* clause_name_in);

  // Construct an ExprResolutionInfo that initializes itself from another
  // ExprResolutionInfo.
  // has_aggregation and has_analytic will be updated in parent on destruction.
  // can_flatten does not propagate.
  // Does not take ownership of <parent>.
  explicit ExprResolutionInfo(ExprResolutionInfo* parent);

  // Construct an ExprResolutionInfo that initializes itself from another
  // ExprResolutionInfo, overriding <name_scope>, <clause_name>, and
  // <allows_analytic>.
  // has_aggregation and has_analytic will be updated in parent on destruction.
  // Does not take ownership of <parent>. <can_flatten> does not propagate.
  ExprResolutionInfo(ExprResolutionInfo* parent, const NameScope* name_scope_in,
                     const char* clause_name_in, bool allows_analytic_in);

  ExprResolutionInfo(const ExprResolutionInfo&) = delete;
  ExprResolutionInfo& operator=(const ExprResolutionInfo&) = delete;

  ~ExprResolutionInfo();

  // Returns whether or not the current expression resolution is happening
  // after DISTINCT.
  bool is_post_distinct() const;

  std::string DebugString() const;

  // Constant info.

  ExprResolutionInfo* const parent = nullptr;

  // NameScope to use while resolving this expression.
  const NameScope* const name_scope = nullptr;

  // NameScope to use while resolving any aggregate function arguments that
  // are in this expression.
  const NameScope* const aggregate_name_scope = nullptr;

  // Indicates whether this expression allows aggregations.
  const bool allows_aggregation;

  // Indicates whether this expression allows analytic functions.
  const bool allows_analytic;

  // <clause_name> is used to generate an error saying aggregation/analytic
  // functions are not allowed in this clause, e.g. "WHERE clause".  It is
  // also used in error messages related to path expression resolution
  // after GROUP BY.
  // This can be empty if both aggregations and analytic functions are
  // allowed, or if there is no clear clause name to use in error messages
  // (for instance when resolving correlated path expressions that are in
  // a subquery's SELECT list but the subquery itself is in the outer
  // query's ORDER BY clause).
  const char* const clause_name;

  // Mutable info.

  // Must be non-NULL if <allows_aggregation> or <allows_analytic>.
  // If non-NULL, <query_resolution_info> gets updated during expression
  // resolution with aggregate and analytic function information present
  // in the expression.  It is unused if aggregate and analytic functions
  // are not allowed in the expression.
  // Not owned.
  QueryResolutionInfo* const query_resolution_info;

  // True if this expression contains an aggregation function.
  bool has_aggregation = false;

  // True if this expression contains an analytic function.
  bool has_analytic = false;

  // True if this expression should be resolved against post-grouping
  // columns.  Gets set to false when resolving arguments of aggregation
  // functions.
  bool use_post_grouping_columns = false;

  // The top-level AST expression being resolved in the current context. This
  // field is set only when resolving SELECT columns. Not owned.
  const ASTExpression* const top_level_ast_expr = nullptr;

  // The column alias of the top-level AST expression in SELECT list, which will
  // be used as the name of the resolved column when the top-level AST
  // expression being resolved is an aggregate or an analytic function. This
  // field is set only when resolving SELECT columns.
  const IdString column_alias = IdString();

  // True if this is a context where flattening of (nested) arrays can happen
  // automatically. For example, in UNNEST we automatically flatten paths
  // through arrays so that they are legal without an explicit FLATTEN.
  bool can_flatten = false;
};

// Get an InputArgumentType for a ResolvedExpr, identifying whether or not it
// is a parameter and pointing at the literal value inside <expr> if
// appropriate.  <expr> must outlive the returned object.
InputArgumentType GetInputArgumentTypeForExpr(const ResolvedExpr* expr);

// Get a list of <InputArgumentType> from a list of <ResolvedExpr>,
// invoking GetInputArgumentTypeForExpr() on each of the <arguments>.
// TODO: Remove in favor of unique_ptr version.
void GetInputArgumentTypesForExprList(
    const std::vector<const ResolvedExpr*>* arguments,
    std::vector<InputArgumentType>* input_arguments);

// Get a list of <InputArgumentType> from a list of <ResolvedExpr>,
// invoking GetInputArgumentTypeForExpr() on each of the <arguments>.
void GetInputArgumentTypesForExprList(
    const std::vector<std::unique_ptr<const ResolvedExpr>>& arguments,
    std::vector<InputArgumentType>* input_arguments);

// Cast vector<const AST_TYPE*> to vector<const ASTNode*>.
template <class AST_TYPE>
std::vector<const ASTNode*> ToLocations(
    absl::Span<const AST_TYPE* const> nodes) {
  std::vector<const ASTNode*> ast_locations;
  ast_locations.reserve(nodes.size());
  for (const AST_TYPE* node : nodes) {
    ast_locations.push_back(node);
  }
  return ast_locations;
}

// This helper class is for resolving table-valued functions. It represents a
// resolved argument to the TVF. The argument can be either a scalar
// expression, in which case <expr> is filled, a relation, in which case
// <scan> and <name_list> are filled (where <name_list> is for <scan>), or a
// machine learning model, in which case <model> is filled.
// The public accessors provide ways to set and retrieve the fields in a type
// safe manner.
class ResolvedTVFArg {
 public:
  void SetExpr(std::unique_ptr<const ResolvedExpr> expr) {
    expr_ = std::move(expr);
    type_ = EXPR;
  }
  void SetScan(std::unique_ptr<const ResolvedScan> scan,
               std::shared_ptr<const NameList> name_list) {
    scan_ = std::move(scan);
    name_list_ = name_list;
    type_ = SCAN;
  }
  void SetModel(std::unique_ptr<const ResolvedModel> model) {
    model_ = std::move(model);
    type_ = MODEL;
  }
  void SetConnection(std::unique_ptr<const ResolvedConnection> connection) {
    connection_ = std::move(connection);
    type_ = CONNECTION;
  }
  void SetDescriptor(std::unique_ptr<const ResolvedDescriptor> descriptor) {
    descriptor_ = std::move(descriptor);
    type_ = DESCRIPTOR;
  }

  bool IsExpr() const { return type_ == EXPR; }
  bool IsScan() const { return type_ == SCAN; }
  bool IsModel() const { return type_ == MODEL; }
  bool IsConnection() const { return type_ == CONNECTION; }
  bool IsDescriptor() const { return type_ == DESCRIPTOR; }

  zetasql_base::StatusOr<const ResolvedExpr*> GetExpr() const {
    ZETASQL_RET_CHECK(IsExpr());
    return expr_.get();
  }
  zetasql_base::StatusOr<const ResolvedScan*> GetScan() const {
    ZETASQL_RET_CHECK(IsScan());
    return scan_.get();
  }
  zetasql_base::StatusOr<const ResolvedModel*> GetModel() const {
    ZETASQL_RET_CHECK(IsModel());
    return model_.get();
  }
  zetasql_base::StatusOr<const ResolvedConnection*> GetConnection() const {
    ZETASQL_RET_CHECK(IsConnection());
    return connection_.get();
  }
  zetasql_base::StatusOr<const ResolvedDescriptor*> GetDescriptor() const {
    ZETASQL_RET_CHECK(IsDescriptor());
    return descriptor_.get();
  }
  zetasql_base::StatusOr<std::shared_ptr<const NameList>> GetNameList() const {
    ZETASQL_RET_CHECK(IsScan());
    return name_list_;
  }

  zetasql_base::StatusOr<std::unique_ptr<const ResolvedExpr>> MoveExpr() {
    ZETASQL_RET_CHECK(IsExpr());
    return std::move(expr_);
  }
  zetasql_base::StatusOr<std::unique_ptr<const ResolvedScan>> MoveScan() {
    ZETASQL_RET_CHECK(IsScan());
    return std::move(scan_);
  }
  zetasql_base::StatusOr<std::unique_ptr<const ResolvedModel>> MoveModel() {
    ZETASQL_RET_CHECK(IsModel());
    return std::move(model_);
  }
  zetasql_base::StatusOr<std::unique_ptr<const ResolvedConnection>> MoveConnection() {
    ZETASQL_RET_CHECK(IsConnection());
    return std::move(connection_);
  }
  zetasql_base::StatusOr<std::unique_ptr<const ResolvedDescriptor>> MoveDescriptor() {
    ZETASQL_RET_CHECK(IsDescriptor());
    return std::move(descriptor_);
  }

 private:
  enum {
    UNDEFINED,
    EXPR,
    SCAN,
    MODEL,
    CONNECTION,
    DESCRIPTOR
  } type_ = UNDEFINED;
  std::unique_ptr<const ResolvedExpr> expr_;
  std::unique_ptr<const ResolvedScan> scan_;
  std::unique_ptr<const ResolvedModel> model_;
  std::unique_ptr<const ResolvedConnection> connection_;
  std::unique_ptr<const ResolvedDescriptor> descriptor_;
  std::shared_ptr<const NameList> name_list_;
};

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_EXPR_RESOLVER_HELPER_H_
