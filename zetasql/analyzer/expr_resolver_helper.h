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

#ifndef ZETASQL_ANALYZER_EXPR_RESOLVER_HELPER_H_
#define ZETASQL_ANALYZER_EXPR_RESOLVER_HELPER_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/id_string.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/statusor.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"

namespace zetasql {

class QueryResolutionInfo;
struct ExprResolutionInfo;

// Return true if <expr> can be treated like a constant,
// meaning it has the same value for the entire query and has no
// side effects.
//
// This shouldn't fail, except for unimplemented cases.
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
absl::StatusOr<bool> IsConstantExpression(const ResolvedExpr* expr);

// Helper for representing if we're allowed to flatten (ie: allowed to dot into
// the fields of a proto/struct/json array), and if so, if we're already in a
// ResolvedFlatten from having previously done so.
//
// Flattening is allowed:
// - inside an explicit FLATTEN(...) call
// - in an UNNEST
//
// Once we're allowed to flatten and we do a field access on an array, then we
// being constructing a ResolvedFlatten. The FlattenState tracks the current
// ResolvedFlatten we're building and whether it's active or not.
//
// This allows us to differentiate:
//    FLATTEN(a.b.c)[OFFSET(0)]
// from
//    FLATTEN(a.b.c[OFFSET(0)])
//
// In both cases, the input expression to the offset is the ResolvedFlatten
// but in the former case (no active flatten), the offset should be on the
// result, whereas in the latter the offset should be on the active flatten.
class FlattenState {
 public:
  FlattenState() = default;
  FlattenState(const FlattenState&) = delete;
  FlattenState& operator=(const FlattenState&) = delete;

  ~FlattenState() {
    if (parent_ != nullptr) parent_->active_flatten_ = active_flatten_;
  }

  // Sets the parent for this FlattenState. This copies state from the parent,
  // but also causes the active flatten to propagate back to parent after
  // destruction so a child ExprResolutionInfo can essentially share
  // FlattenState.
  void SetParent(FlattenState* parent) {
    ZETASQL_DCHECK(parent_ == nullptr) << "Parent shouldn't be set more than once";
    ZETASQL_CHECK(parent);
    parent_ = parent;
    can_flatten_ = parent->can_flatten_;
    active_flatten_ = parent->active_flatten_;
  }

  // Helper to allow restoring original `can_flatten` state when desired.
  class Restorer {
   public:
    ~Restorer() {
      if (can_flatten_) *can_flatten_ = original_can_flatten_;
    }
    void Activate(bool* can_flatten) {
      can_flatten_ = can_flatten;
      original_can_flatten_ = *can_flatten;
    }
   private:
    bool* can_flatten_ = nullptr;
    bool original_can_flatten_;
  };
  bool can_flatten() const { return can_flatten_; }
  void set_can_flatten(bool can_flatten, Restorer* restorer) {
    restorer->Activate(&can_flatten_);
    can_flatten_ = can_flatten;
  }

  ResolvedFlatten* active_flatten() const { return active_flatten_; }
  void set_active_flatten(ResolvedFlatten* flatten) {
    active_flatten_ = flatten;
  }

 private:
  FlattenState* parent_ = nullptr;
  bool can_flatten_ = false;
  ResolvedFlatten* active_flatten_ = nullptr;
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

  // Context around if we can flatten and if we're currently actively doing so.
  // See FlattenState for details.
  FlattenState flatten_state;
};

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

  absl::StatusOr<const ResolvedExpr*> GetExpr() const {
    ZETASQL_RET_CHECK(IsExpr());
    return expr_.get();
  }
  absl::StatusOr<const ResolvedScan*> GetScan() const {
    ZETASQL_RET_CHECK(IsScan());
    return scan_.get();
  }
  absl::StatusOr<const ResolvedModel*> GetModel() const {
    ZETASQL_RET_CHECK(IsModel());
    return model_.get();
  }
  absl::StatusOr<const ResolvedConnection*> GetConnection() const {
    ZETASQL_RET_CHECK(IsConnection());
    return connection_.get();
  }
  absl::StatusOr<const ResolvedDescriptor*> GetDescriptor() const {
    ZETASQL_RET_CHECK(IsDescriptor());
    return descriptor_.get();
  }
  absl::StatusOr<std::shared_ptr<const NameList>> GetNameList() const {
    ZETASQL_RET_CHECK(IsScan());
    return name_list_;
  }

  absl::StatusOr<std::unique_ptr<const ResolvedExpr>> MoveExpr() {
    ZETASQL_RET_CHECK(IsExpr());
    return std::move(expr_);
  }
  absl::StatusOr<std::unique_ptr<const ResolvedScan>> MoveScan() {
    ZETASQL_RET_CHECK(IsScan());
    return std::move(scan_);
  }
  absl::StatusOr<std::unique_ptr<const ResolvedModel>> MoveModel() {
    ZETASQL_RET_CHECK(IsModel());
    return std::move(model_);
  }
  absl::StatusOr<std::unique_ptr<const ResolvedConnection>> MoveConnection() {
    ZETASQL_RET_CHECK(IsConnection());
    return std::move(connection_);
  }
  absl::StatusOr<std::unique_ptr<const ResolvedDescriptor>> MoveDescriptor() {
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
