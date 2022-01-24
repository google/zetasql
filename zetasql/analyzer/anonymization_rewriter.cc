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

#include "zetasql/analyzer/anonymization_rewriter.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "zetasql/analyzer/expr_matching_helpers.h"
#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/anon_function.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/make_node_vector.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/types/span.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

struct WithEntryRewriteState;

// Rewrites a given AST that includes a ResolvedAnonymizedAggregateScan to use
// the semantics defined in https://arxiv.org/abs/1909.01917 and
// (broken link).
//
// Overview of the rewrite process:
// 1. This class is invoked on an AST node, blindly copying everything until a
//    ResolvedAnonymizedAggregateScan (anon node) is hit
// 2. Every column in the anon node's column list is recorded in a map entry
//    with a freshly allocated column of the same type in the entry's value
//    (the intermediate columns)
// 3. The per-user ResolvedAggregateScan is created using this map:
//   a. The original anon node's input scan is validated to partition by $uid,
//      and project the $uid column up to the top column list
//   b. The projected $uid column is added to the GROUP BY list if not already
//      included.
//   c. Each ANON_* function call in the anon node is re-resolved to the
//      appropriate per-user aggregate function, e.g. ANON_SUM(expr)->SUM(expr)
//   d. For each aggregate or group by column in the anon node, the column set
//      in the per-user scan's column list is the appropriate intermediate
//      column looked up in the column map
// 4. If kappa is specified, a partioned-by-$uid ResolvedSampleScan is
//    inserted to limit the number of groups that a user can contribute to.
//    While kappa is optional, for most queries with a GROUP BY clause in the
//    ResolvedAnonymizedAggregationScan it MUST be specified for the resulting
//    query to provide correct epsilon-delta differential privacy.
// 5. The final cross-user ResolvedAnonymizedAggregateScan is created:
//   a. The input scan is set to the (possibly sampled) per-user scan
//   b. The first argument for each ANON_* function call in the anon node is
//      re-resolved to point to the appropriate intermediate column
//   c. A k-threshold computing ANON_COUNT(*) function call is added
//
// If we consider the scans in the original AST as a linked list as:
//
// cross_user_transform
//  -> ResolvedAnonymizedAggregateScan
//    -> per_user_transform
//
// Then the above operations can be thought of as inserting a pair of new list
// nodes:
//
// cross_user_transform
//  -> ResolvedAnonymizedAggregateScan
//    -> ResolvedSampleScan (optional)
//      -> ResolvedAggregateScan
//        -> per_user_transform
//
// Where the new ResolvedAggregateScan is the per-user aggregate scan, and
// the optional ResolvedSampleScan uses kappa to restrict the number of groups
// a user can contribute to (for more information on kappa, see
// (broken link)).
class RewriterVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  RewriterVisitor(ColumnFactory* allocator, TypeFactory* type_factory,
                  Resolver* resolver,
                  RewriteForAnonymizationOutput::TableScanToAnonAggrScanMap&
                      table_scan_to_anon_aggr_scan_map)
      : allocator_(allocator),
        type_factory_(type_factory),
        resolver_(resolver),
        table_scan_to_anon_aggr_scan_map_(table_scan_to_anon_aggr_scan_map) {}

 private:
  absl::StatusOr<std::unique_ptr<ResolvedAggregateScan>>
  RewriteInnerAggregateScan(
      const ResolvedAnonymizedAggregateScan* node,
      std::map<ResolvedColumn, ResolvedColumn>* injected_col_map,
      ResolvedColumn* uid_column);

  // Create the cross-user k-threshold function call
  absl::Status MakeKThresholdFunctionColumn(
      double privacy_budget_weight,
      std::unique_ptr<ResolvedComputedColumn>* out);

  absl::Status VisitResolvedAnonymizedAggregateScan(
      const ResolvedAnonymizedAggregateScan* node) override;
  absl::Status VisitResolvedWithScan(const ResolvedWithScan* node) override;
  absl::Status VisitResolvedProjectScan(
      const ResolvedProjectScan* node) override;

  ColumnFactory* allocator_;  // unowned
  TypeFactory* type_factory_;  // unowned
  Resolver* resolver_;         // unowned
  RewriteForAnonymizationOutput::TableScanToAnonAggrScanMap&
      table_scan_to_anon_aggr_scan_map_;
  std::vector<const ResolvedTableScan*> resolved_table_scans_;  // unowned
  std::vector<std::unique_ptr<WithEntryRewriteState>> with_entries_;
};

// Use the resolver to create a new function call using resolved arguments. The
// calling code must ensure that the arguments can always be coerced and
// resolved to a valid function. Any returned status is an internal error.
absl::StatusOr<std::unique_ptr<ResolvedExpr>> ResolveFunctionCall(
    const std::string& function_name,
    std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
    Resolver* resolver) {
  // In order for the resolver to provide error locations, it needs ASTNode
  // locations from the original SQL. However, the functions in these
  // transforms do not necessarily appear in the SQL so they have no locations.
  // Any errors produced here are internal errors, so error locations are not
  // meaningful and we use location stubs instead.
  ASTFunctionCall dummy_ast_function;
  FakeASTNode dummy_ast_location;
  std::vector<const ASTNode*> dummy_arg_locations(arguments.size(),
                                                  &dummy_ast_location);

  // Stub out query/expr resolution info structs. This is ok because we aren't
  // doing any actual resolution here (so we don't need NameScopes, etc.). We
  // are just transforming a function call, and creating a new
  // ResolvedFunctionCall with already-resolved arguments.
  NameScope empty_name_scope;
  QueryResolutionInfo query_resolution_info(resolver);
  ExprResolutionInfo expr_resolution_info(
      &empty_name_scope, &empty_name_scope, /*allows_aggregation_in=*/true,
      /*allows_analytic_in=*/false, /*use_post_grouping_columns_in=*/false,
      /*clause_name_in=*/"", &query_resolution_info);

  std::unique_ptr<const ResolvedExpr> result;
  absl::Status status = resolver->ResolveFunctionCallWithResolvedArguments(
      &dummy_ast_function, dummy_arg_locations, function_name,
      std::move(arguments), /*named_arguments=*/{}, &expr_resolution_info,
      &result);

  // We expect that the caller passes valid/coercible arguments. An error only
  // occurs if that contract is violated, so this is an internal error.
  ZETASQL_RET_CHECK(status.ok()) << status;

  // The resolver inserts the actual function call for aggregate functions
  // into query_resolution_info, so we need to extract it if applicable.
  if (query_resolution_info.aggregate_columns_to_compute().size() == 1) {
    std::unique_ptr<ResolvedComputedColumn> col =
        absl::WrapUnique(const_cast<ResolvedComputedColumn*>(
            query_resolution_info.release_aggregate_columns_to_compute()
                .front()
                .release()));
    result = col->release_expr();
  }
  return absl::WrapUnique(const_cast<ResolvedExpr*>(result.release()));
}

std::unique_ptr<ResolvedColumnRef> MakeColRef(const ResolvedColumn& col) {
  return MakeResolvedColumnRef(col.type(), col, /*is_correlated=*/false);
}

zetasql_base::StatusBuilder MakeSqlErrorAtNode(const ResolvedNode& node) {
  zetasql_base::StatusBuilder builder = MakeSqlError();
  const auto* parse_location = node.GetParseLocationRangeOrNULL();
  if (parse_location != nullptr) {
    builder.Attach(parse_location->start().ToInternalErrorLocation());
  }
  return builder;
}

absl::Status MaybeAttachParseLocation(absl::Status status,
                                      const ResolvedNode& node) {
  const auto* parse_location = node.GetParseLocationRangeOrNULL();
  if (!status.ok() &&
      !zetasql::internal::HasPayloadWithType<InternalErrorLocation>(status) &&
      parse_location != nullptr) {
    zetasql::internal::AttachPayload(
        &status, parse_location->start().ToInternalErrorLocation());
  }
  return status;
}

// Given a call to an ANON_* function, resolve a concrete function signature for
// the matching per-user aggregate call. For example,
// ANON_COUNT(expr, 0, 1) -> COUNT(expr)
absl::StatusOr<std::unique_ptr<ResolvedExpr>>
ResolveInnerAggregateFunctionCallForAnonFunction(
    const ResolvedAggregateFunctionCall* node,
    std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
    Resolver* resolver, ResolvedColumn* order_by_column,
    ColumnFactory* allocator) {
  // We are rewriting ANON_VAR_POP/ANON_STDDEV_POP/ANON_PERCENTILE_CONT to
  // per-user aggregation ARRAY_AGG(expr IGNORE NULLS ORDER BY rand() LIMIT 5).
  // The limit of 5 is proposed to be consistent with the current Penumbra
  // implementation, and at some point we may want to make this configurable.
  // For more information, see (broken link).
  static constexpr int kPerUserArrayAggLimit = 5;
  if (!node->function()->Is<AnonFunction>()) {
    return MakeSqlErrorAtNode(*node)
           << "Unsupported function in SELECT WITH ANONYMIZATION select "
              "list: "
           << node->function()->SQLName();
  }

  if (node->function()->GetGroup() == Function::kZetaSQLFunctionGroupName &&
      node->signature().context_id() ==
          FunctionSignatureId::FN_ANON_COUNT_STAR) {
    // COUNT(*) doesn't take any arguments.
    arguments.clear();
  } else {
    arguments.resize(1);
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedExpr> result,
      ResolveFunctionCall(
          node->function()->GetAs<AnonFunction>()->GetPartialAggregateName(),
          std::move(arguments), resolver));

  // If the anon function is ANON_VAR_POP/ANON_STDDEV_POP/ANON_PERCENTILE_CONT,
  // we allocate a new column "$orderbycol1" and set the limit as 5.
  if (node->function()->GetGroup() == Function::kZetaSQLFunctionGroupName &&
      (node->signature().context_id() ==
           FunctionSignatureId::FN_ANON_VAR_POP_DOUBLE ||
       node->signature().context_id() ==
           FunctionSignatureId::FN_ANON_STDDEV_POP_DOUBLE ||
       node->signature().context_id() ==
           FunctionSignatureId::FN_ANON_PERCENTILE_CONT_DOUBLE)) {
    if (!order_by_column->IsInitialized()) {
      *order_by_column =
          allocator->MakeCol("$orderby", "$orderbycol1", types::DoubleType());
    }
    std::unique_ptr<const ResolvedColumnRef> resolved_column_ref =
        MakeColRef(*order_by_column);
    std::unique_ptr<const ResolvedOrderByItem> resolved_order_by_item =
        MakeResolvedOrderByItem(std::move(resolved_column_ref), nullptr,
                                /*is_descending=*/false,
                                ResolvedOrderByItemEnums::ORDER_UNSPECIFIED);

    ResolvedAggregateFunctionCall* resolved_aggregate_function_call =
        result->GetAs<ResolvedAggregateFunctionCall>();
    resolved_aggregate_function_call->add_order_by_item_list(
        std::move(resolved_order_by_item));
    resolved_aggregate_function_call->set_null_handling_modifier(
        ResolvedNonScalarFunctionCallBaseEnums::IGNORE_NULLS);
    resolved_aggregate_function_call->set_limit(
        MakeResolvedLiteral(Value::Int64(kPerUserArrayAggLimit)));
  }
  return result;
}

// Rewrites the aggregate and group by list for the inner per-user aggregate
// scan. Replaces all function calls with their non-ANON_* versions, and sets
// the output column for each ComputedColumn to the corresponding intermediate
// column in the <injected_col_map>.
class InnerAggregateListRewriterVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  InnerAggregateListRewriterVisitor(
      std::map<ResolvedColumn, ResolvedColumn>* injected_col_map,
      ColumnFactory* allocator, Resolver* resolver)
      : injected_col_map_(injected_col_map),
        allocator_(allocator),
        resolver_(resolver) {}

  const ResolvedColumn& order_by_column() { return order_by_column_; }

 private:
  absl::Status VisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall* node) override {
    // Blindly copy the argument list.
    ZETASQL_ASSIGN_OR_RETURN(std::vector<std::unique_ptr<ResolvedExpr>> argument_list,
                     ProcessNodeList(node->argument_list()));

    // Trim the arg list and resolve the per-user aggregate function.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> result,
                     ResolveInnerAggregateFunctionCallForAnonFunction(
                         node,
                         // This is expecting unique_ptr to be const.
                         // std::vector<std::unique_ptr<__const__ ResolvedExpr>>
                         {std::make_move_iterator(argument_list.begin()),
                          std::make_move_iterator(argument_list.end())},
                         resolver_, &order_by_column_, allocator_));
    ZETASQL_RET_CHECK_EQ(result->node_kind(), RESOLVED_AGGREGATE_FUNCTION_CALL)
        << result->DebugString();
    PushNodeToStack(std::move(result));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedComputedColumn(
      const ResolvedComputedColumn* node) override {
    // Rewrite the output column to point to the mapped column.
    ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedComputedColumn(node));
    ResolvedComputedColumn* col =
        GetUnownedTopOfStack<ResolvedComputedColumn>();

    // Create a column to splice together the per-user and cross-user
    // aggregate/groupby lists, then update the copied computed column and place
    // our new column in the replacement map.
    const ResolvedColumn& old_column = node->column();
    const ResolvedColumn injected_column = allocator_->MakeCol(
        old_column.table_name(), old_column.name() + "_partial",
        col->expr()->type());
    injected_col_map_->emplace(old_column, injected_column);
    col->set_column(injected_column);
    return absl::OkStatus();
  }

  std::map<ResolvedColumn, ResolvedColumn>* injected_col_map_;
  ColumnFactory* allocator_;
  Resolver* resolver_;
  ResolvedColumn order_by_column_;
};

// Given a call to an ANON_* function, resolve an aggregate function call for
// use in the outer cross-user aggregation scan. This function will always be an
// ANON_* function, and the first argument will always point to the appropriate
// column produced by the per-user scan (target_column).
absl::StatusOr<std::unique_ptr<ResolvedExpr>>
ResolveOuterAggregateFunctionCallForAnonFunction(
    const ResolvedAggregateFunctionCall* node,
    const ResolvedColumn& target_column,
    std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
    Resolver* resolver) {
  // Most ANON_* functions don't require special handling.
  std::string target = node->function()->Name();
  // But ANON_COUNT(*) and ANON_COUNT(expr) require special handling. Note that
  // we implement ANON_COUNT(*) and ANON_COUNT(expr) using ANON_SUM(expr) in the
  // outer cross-user aggregation scan.
  // ANON_COUNT(*) is therefore effectively ANON_SUM(COUNT(*))
  if (node->function()->GetGroup() == Function::kZetaSQLFunctionGroupName) {
    switch (node->signature().context_id()) {
      case FunctionSignatureId::FN_ANON_COUNT_STAR:
        target = "anon_sum";
        // Insert a dummy 'expr' column here, the original call will not include
        // one because we are rewriting ANON_COUNT(*) to ANON_SUM(expr). The
        // actual column reference will be set below.
        arguments.insert(arguments.begin(), nullptr);
        break;
      case FunctionSignatureId::FN_ANON_COUNT:
        target = "anon_sum";
        break;
    }
  }
  // The first argument will _always_ point to the partially aggregated column
  // produced by the corresponding function call in the per-user scan.
  arguments[0] = MakeColRef(target_column);

  return ResolveFunctionCall(target, std::move(arguments), resolver);
}

// Rewrites the aggregate list for the outer cross-user aggregate scan. Replaces
// each ANON_* function call with a matching ANON_* function call, but pointing
// the first argument to the appropriate intermediate column produced by the
// per-user aggregate scan.
class OuterAggregateListRewriterVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  OuterAggregateListRewriterVisitor(
      const std::map<ResolvedColumn, ResolvedColumn>& injected_col_map,
      Resolver* resolver)
      : injected_col_map_(injected_col_map), resolver_(resolver) {}

 private:
  absl::Status VisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall* node) override {
    ZETASQL_ASSIGN_OR_RETURN(std::vector<std::unique_ptr<ResolvedExpr>> argument_list,
                     ProcessNodeList(node->argument_list()));

    // Resolve the new cross-user ANON_* function call.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> result,
                     ResolveOuterAggregateFunctionCallForAnonFunction(
                         node, injected_col_map_.at(current_column_),
                         // This is expecting unique_ptr to be const.
                         // std::vector<std::unique_ptr<__const__ ResolvedExpr>>
                         {std::make_move_iterator(argument_list.begin()),
                          std::make_move_iterator(argument_list.end())},
                         resolver_));
    ZETASQL_RET_CHECK_EQ(result->node_kind(), RESOLVED_AGGREGATE_FUNCTION_CALL)
        << result->DebugString();

    PushNodeToStack(std::move(result));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedComputedColumn(
      const ResolvedComputedColumn* node) override {
    // This function is in practice the class entry point. We need to record
    // what the current output column is so that we can look the appropriate
    // intermediate column up in the map.
    current_column_ = node->column();
    return CopyVisitResolvedComputedColumn(node);
  }

  const std::map<ResolvedColumn, ResolvedColumn>& injected_col_map_;
  ResolvedColumn current_column_;
  Resolver* resolver_;
};

// A helper for JoinExprIncludesUid, returns true if at least one argument of
// the function call is a column ref referring to left_uid, and the same for
// right_uid.
bool FunctionReferencesUid(const ResolvedFunctionCall* call,
                           const ResolvedColumn& left_uid,
                           const ResolvedColumn& right_uid) {
  bool left_referenced = false;
  bool right_referenced = false;
  for (const std::unique_ptr<const ResolvedExpr>& argument :
       call->argument_list()) {
    if (argument->node_kind() != RESOLVED_COLUMN_REF) continue;
    const ResolvedColumnRef* ref = argument->GetAs<ResolvedColumnRef>();
    left_referenced |= (ref->column() == left_uid);
    right_referenced |= (ref->column() == right_uid);
  }
  return left_referenced && right_referenced;
}

// A helper function for checking if a join expression between two tables
// containing user data meets our requirements for joining on the $uid column in
// each table.
//
// Returns true IFF join_expr contains a top level AND function, or an AND
// function nested inside another AND function (arbitrarily deep), that contains
// an EQUAL function that satisfies FunctionReferencesUid.
//
// This excludes a number of logically equivalent join expressions
// (e.g. !(left != right)), but that's fine, we want queries to be intentional.
bool JoinExprIncludesUid(const ResolvedExpr* join_expr,
                         const ResolvedColumn& left_uid,
                         const ResolvedColumn& right_uid) {
  if (join_expr->node_kind() != RESOLVED_FUNCTION_CALL) {
    return false;
  }
  const ResolvedFunctionCall* call = join_expr->GetAs<ResolvedFunctionCall>();
  const Function* function = call->function();
  if (!function->IsScalar() || !function->IsZetaSQLBuiltin()) {
    return false;
  }
  switch (call->signature().context_id()) {
    case FN_AND:
      for (const std::unique_ptr<const ResolvedExpr>& argument :
           call->argument_list()) {
        if (JoinExprIncludesUid(argument.get(), left_uid, right_uid)) {
          return true;
        }
      }
      break;
    case FN_EQUAL:
      if (FunctionReferencesUid(call, left_uid, right_uid)) {
        return true;
      }
      break;
  }
  return false;
}

// This class is used by VisitResolvedTVFScan to validate that none of the TVF
// argument trees contain nodes that are not supported yet as TVF arguments.
//
// The current implementation does not support subqueries that have
// anonymization, where we will need to recursively call the rewriter on
// these (sub)queries.
class TVFArgumentValidatorVisitor : public ResolvedASTVisitor {
 public:
  explicit TVFArgumentValidatorVisitor(const std::string& tvf_name)
      : tvf_name_(tvf_name) {}

  absl::Status VisitResolvedAnonymizedAggregateScan(
      const ResolvedAnonymizedAggregateScan* node) override {
    return MakeSqlErrorAtNode(*node)
           << "TVF arguments do not support SELECT WITH ANONYMIZATION queries";
  }

  absl::Status VisitResolvedProjectScan(
      const ResolvedProjectScan* node) override {
    return MaybeAttachParseLocation(
        ResolvedASTVisitor::VisitResolvedProjectScan(node), *node);
  }

 private:
  const std::string tvf_name_;
};

std::string FieldPathExpressionToString(const ResolvedExpr* expr) {
  std::vector<std::string> field_path;
  while (expr != nullptr) {
    switch (expr->node_kind()) {
      case RESOLVED_GET_PROTO_FIELD: {
        auto* node = expr->GetAs<ResolvedGetProtoField>();
        field_path.emplace_back(node->field_descriptor()->name());
        expr = node->expr();
        break;
      }
      case RESOLVED_GET_STRUCT_FIELD: {
        auto* node = expr->GetAs<ResolvedGetStructField>();
        field_path.emplace_back(
            node->expr()->type()->AsStruct()->field(node->field_idx()).name);
        expr = node->expr();
        break;
      }
      case RESOLVED_COLUMN_REF: {
        std::string name = expr->GetAs<ResolvedColumnRef>()->column().name();
        if (!IsInternalAlias(name)) {
          field_path.emplace_back(std::move(name));
        }
        expr = nullptr;
        break;
      }
      default:
        // Node types other than RESOLVED_GET_PROTO_FIELD /
        // RESOLVED_GET_STRUCT_FIELD / RESOLVED_COLUMN_REF should never show up
        // in a $uid column path expression.
        return "<INVALID>";
    }
  }
  return absl::StrJoin(field_path.rbegin(), field_path.rend(), ".");
}

// Wraps the ResolvedColumn for a given $uid column during AST rewrite. Also
// tracks an optional alias for the column, this improves error messages with
// aliased tables.
struct UidColumnState {
  void InitFromValueTable(const ResolvedComputedColumn* projected_userid_column,
                          std::string value_table_alias) {
    column = projected_userid_column->column();
    alias = std::move(value_table_alias);
    value_table_uid = projected_userid_column;
  }

  void Clear() {
    column.Clear();
    alias.clear();
    value_table_uid = nullptr;
    subquery_expr_level = 0;
  }

  bool SetColumn(const zetasql::ResolvedColumn& col) {
    if (subquery_expr_level > 0) {
      return false;
    }
    column = col;
    return true;
  }

  bool SetColumn(const zetasql::ResolvedColumn& col,
                 const std::string& new_alias) {
    if (subquery_expr_level > 0) {
      return false;
    }
    SetColumn(col);
    alias = new_alias;
    return true;
  }

  // Returns an alias qualified (if specified) user visible name for the $uid
  // column to be returned in validation error messages.
  std::string ToString() const {
    const std::string alias_prefix =
        absl::StrCat(alias.empty() ? "" : absl::StrCat(alias, "."));
    if (!IsInternalAlias(column.name())) {
      return absl::StrCat(alias_prefix, column.name());
    } else if (value_table_uid != nullptr) {
      return absl::StrCat(alias_prefix,
                          FieldPathExpressionToString(value_table_uid->expr()));
    } else {
      return "";
    }
  }

  // If the uid column is derived from a value table we insert a
  // ResolvedProjectScan that extracts the uid column from the table row object.
  // But existing references to the uid column in the query (like in a group by
  // list) will reference a semantically equivalent but distinct column. This
  // function replaces these semantically equivalent computed columns with
  // column references to the 'canonical' uid column.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
  SubstituteUidComputedColumn(
      std::vector<std::unique_ptr<const ResolvedComputedColumn>> expr_list) {
    if (subquery_expr_level > 0 || value_table_uid == nullptr) return expr_list;
    for (auto& col : expr_list) {
      if (IsSameFieldPath(col->expr(), value_table_uid->expr(),
                          FieldPathMatchingOption::kExpression)) {
        col = MakeResolvedComputedColumn(col->column(), MakeColRef(column));
        column = col->column();
      }
    }

    return expr_list;
  }

  // A column declared as the $uid column in a table or TVF schema definition.
  // This gets passed up the AST during the rewriting process to validate the
  // query, and gets replaced with computed columns as needed for joins and
  // nested aggregations.
  ResolvedColumn column;

  // <alias> is only used for clarifying error messages, it's only set to a non
  // empty string for table scan clauses like '... FROM Table as t' so that we
  // can display error messages related to the $uid column as 't.userid' rather
  // than 'userid' or 'Table.userid'. It has no impact on the actual rewriting
  // logic.
  std::string alias;

  // In the subquery expression, the `current_uid_` should not be updated.
  int subquery_expr_level = 0;

 private:
  const ResolvedComputedColumn* value_table_uid = nullptr;
};

// Tracks the lazily-rewritten state of a ResolvedWithEntry. The original AST
// must outlive instances of this struct.
struct WithEntryRewriteState {
  // References the WITH entry in the original AST, always set.
  const ResolvedWithEntry& original_entry;

  // Contains the rewritten AST for this WITH entry, but only if it's been
  // rewritten.
  const ResolvedWithEntry* rewritten_entry;
  std::unique_ptr<const ResolvedWithEntry> rewritten_entry_owned;

  // Contains the $uid column state for this WITH entry IFF it's been rewritten
  // AND it reads from a table, TVF, or another WITH entry that reads user data.
  std::optional<UidColumnState> rewritten_uid;
};

// Rewrites the rest of the per-user scan, propagating the AnonymizationInfo()
// userid (aka $uid column) from the base private table scan to the top node
// returned.
//
// This visitor may only be invoked on a scan that is a transitive child of a
// ResolvedAnonymizedAggregateScan. uid_column() will return an error if the
// subtree represented by that scan does not contain a table or TVF that
// contains user data (AnonymizationInfo).
class PerUserRewriterVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  explicit PerUserRewriterVisitor(
      ColumnFactory* allocator, TypeFactory* type_factory, Resolver* resolver,
      std::vector<const ResolvedTableScan*>& resolved_table_scans,
      std::vector<std::unique_ptr<WithEntryRewriteState>>& with_entries)
      : allocator_(allocator),
        type_factory_(type_factory),
        resolver_(resolver),
        resolved_table_scans_(resolved_table_scans),
        with_entries_(with_entries) {}

  std::optional<ResolvedColumn> uid_column() const {
    if (current_uid_.column.IsInitialized()) {
      return current_uid_.column;
    } else {
      return std::nullopt;
    }
  }

 private:
  absl::Status ProjectValueTableScanRowValueIfNeeded(
      ResolvedTableScan* copy,
      const Column* value_table_value_column,
      ResolvedColumn* value_table_value_resolved_column) {
    for (int i = 0; i < copy->column_list_size(); ++i) {
      int j = copy->column_index_list(i);
      if (value_table_value_column == copy->table()->GetColumn(j)) {
        // The current scan already produces the value table value column
        // that we want to extract from, so we can leave the scan node
        // as is.
        *value_table_value_resolved_column = copy->column_list(i);
        return absl::OkStatus();
      }
    }

    // Make a new ResolvedColumn for the value table value column and
    // add it to the table scan's column list.
    *value_table_value_resolved_column =
        allocator_->MakeCol("$table_scan", "$value",
                            value_table_value_column->GetType());
    copy->add_column_list(*value_table_value_resolved_column);
    int table_col_idx = -1;
    for (int idx = 0; idx < copy->table()->NumColumns(); ++idx) {
      if (value_table_value_column == copy->table()->GetColumn(idx)) {
        table_col_idx = idx;
        break;
      }
    }
    ZETASQL_RET_CHECK_GE(table_col_idx, 0);
    ZETASQL_RET_CHECK_LT(table_col_idx, copy->table()->NumColumns());
    copy->add_column_index_list(table_col_idx);

    return absl::OkStatus();
  }

  absl::StatusOr<std::unique_ptr<ResolvedComputedColumn>>
  MakeGetFieldComputedColumn(
      absl::Span<const std::string> userid_column_name_path,
      const ResolvedColumn& value_table_value_resolved_column) {
    const std::string& userid_column_name =
        IdentifierPathToString(userid_column_name_path);
    ResolvedColumn userid_column = value_table_value_resolved_column;
    std::unique_ptr<const ResolvedExpr> resolved_expr_to_ref =
        MakeColRef(value_table_value_resolved_column);

    if (value_table_value_resolved_column.type()->IsStruct()) {
      const StructType* struct_type =
          value_table_value_resolved_column.type()->AsStruct();

      for (const std::string& userid_column_field : userid_column_name_path) {
        ZETASQL_RET_CHECK_NE(struct_type, nullptr) << userid_column_name;
        int found_idx = -1;
        bool is_ambiguous = false;
        const StructField* struct_field = struct_type->FindField(
            userid_column_field, &is_ambiguous, &found_idx);
        ZETASQL_RET_CHECK_NE(struct_field, nullptr) << userid_column_name;
        ZETASQL_RET_CHECK(!is_ambiguous) << userid_column_name;
        struct_type = struct_field->type->AsStruct();

        std::unique_ptr<ResolvedExpr> get_userid_field_expr =
            MakeResolvedGetStructField(
                struct_field->type, std::move(resolved_expr_to_ref), found_idx);

        userid_column = allocator_->MakeCol(
            "$project", absl::StrCat("$", userid_column_field),
            get_userid_field_expr->type());
        resolved_expr_to_ref = std::move(get_userid_field_expr);
      }

    } else {
      const google::protobuf::Descriptor* descriptor =
          value_table_value_resolved_column.type()->AsProto()->descriptor();

      for (const std::string& userid_column_field : userid_column_name_path) {
        ZETASQL_RET_CHECK_NE(descriptor, nullptr) << userid_column_name;
        const google::protobuf::FieldDescriptor* field =
            ProtoType::FindFieldByNameIgnoreCase(descriptor,
                                                 userid_column_field);
        ZETASQL_RET_CHECK_NE(field, nullptr) << userid_column_name;
        descriptor = field->message_type();

        const Type* field_type;
        ZETASQL_RETURN_IF_ERROR(type_factory_->GetProtoFieldType(field, &field_type));

        Value default_value;
        ZETASQL_RETURN_IF_ERROR(
            GetProtoFieldDefault(ProtoFieldDefaultOptions::FromFieldAndLanguage(
                                     field, resolver_->language()),
                                 field, field_type, &default_value));

        // Note that we use 'return_default_value_when_unset' as false here
        // because it indicates behavior for when the parent message is unset,
        // not when the extracted field is unset (whose behavior depends on the
        // field annotations, e.g., use_field_defaults).
        std::unique_ptr<ResolvedExpr> get_userid_field_expr =
            MakeResolvedGetProtoField(
                field_type, std::move(resolved_expr_to_ref), field,
                default_value,
                /*get_has_bit=*/false, ProtoType::GetFormatAnnotation(field),
                /*return_default_value_when_unset=*/false);
        userid_column = allocator_->MakeCol(
            "$project", absl::StrCat("$", userid_column_field),
            get_userid_field_expr->type());

        resolved_expr_to_ref = std::move(get_userid_field_expr);
      }
    }
    return MakeResolvedComputedColumn(userid_column,
                                      std::move(resolved_expr_to_ref));
  }

  absl::Status VisitResolvedTableScan(const ResolvedTableScan* node) override {
    ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedTableScan(node));
    ResolvedTableScan* copy = GetUnownedTopOfStack<ResolvedTableScan>();

    if (!copy->table()->SupportsAnonymization()) {
      return absl::OkStatus();
    }

    if (current_uid_.subquery_expr_level > 0) {
      return MakeSqlErrorAtNode(*node)
             << "Reading the table " << copy->table()->Name()
             << " containing user data in expression subqueries is not allowed";
    }

    // There exists an authoritative $uid column in the underlying table.
    //
    // For value tables, the Column itself doesn't exist in the table,
    // but its Column Name identifies the $uid field name of the value table
    // Value.
    ZETASQL_RET_CHECK(copy->table()->GetAnonymizationInfo().has_value());
    // Save the table alias with the $uid column. If the table doesn't have an
    // alias, copy->alias() returns an empty string and the $uid column alias
    // gets cleared.
    current_uid_.alias = copy->alias();
    const Column* table_col = copy->table()
                                  ->GetAnonymizationInfo()
                                  .value()
                                  .GetUserIdInfo()
                                  .get_column();
    resolved_table_scans_.push_back(copy);
    if (table_col != nullptr) {
      // The userid column is an actual physical column from the table, so
      // find it and make sure it's part of the table's output column list.
      //
      // For each ResolvedColumn column_list[i], the matching table column is
      // table->GetColumn(column_index_list[i])
      for (int i = 0; i < copy->column_list_size(); ++i) {
        int j = copy->column_index_list(i);
        if (table_col == copy->table()->GetColumn(j)) {
          // If the original query selects the $uid column, reuse it.
          current_uid_.SetColumn(copy->column_list(i));
          ZETASQL_RETURN_IF_ERROR(ValidateUidColumnSupportsGrouping(*node));
          return absl::OkStatus();
        }
      }

      if (current_uid_.SetColumn(allocator_->MakeCol(copy->table()->Name(),
                                                     table_col->Name(),
                                                     table_col->GetType()))) {
        copy->add_column_list(current_uid_.column);

        int table_col_id = -1;
        for (int i = 0; i < copy->table()->NumColumns(); ++i) {
          if (table_col == copy->table()->GetColumn(i)) {
            table_col_id = i;
          }
        }
        ZETASQL_RET_CHECK_NE(table_col_id, -1);
        copy->add_column_index_list(table_col_id);
      }
    } else {
      // The userid column is identified by the column name.  This case
      // happens when the table is a value table, and the userid column is
      // derived from the value table's value.
      //
      // In this case, the $uid column is derived by fetching the
      // proper struct/proto field from the table value type.  We create
      // a new Project node on top of the input scan node that projects
      // all of the scan columns, along with one new column that is the
      // GetProto/StructField expression to extract the userid column.

      // First, ensure that the Table's row value is projected from the scan
      // (it may not be projected, for instance, if the full original query
      // is just ANON_COUNT(*)).
      //
      // As per the Table contract, value tables require their first column
      // (column 0) to be the value table value column.
      ZETASQL_RET_CHECK_GE(copy->table()->NumColumns(), 1);
      const Column* value_table_value_column = copy->table()->GetColumn(0);
      ZETASQL_RET_CHECK_NE(value_table_value_column, nullptr) << copy->table()->Name();
      ZETASQL_RET_CHECK(value_table_value_column->GetType()->IsStruct() ||
                value_table_value_column->GetType()->IsProto());

      ResolvedColumn value_table_value_resolved_column;
      ZETASQL_RETURN_IF_ERROR(ProjectValueTableScanRowValueIfNeeded(
          copy, value_table_value_column, &value_table_value_resolved_column));

      ZETASQL_RET_CHECK(value_table_value_resolved_column.IsInitialized())
          << value_table_value_resolved_column.DebugString();

      // Build an expression to extract the userid column from the
      // value table row value.
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<ResolvedComputedColumn> projected_userid_column,
          MakeGetFieldComputedColumn(copy->table()
                                         ->GetAnonymizationInfo()
                                         .value()
                                         .UserIdColumnNamePath(),
                                     value_table_value_resolved_column));

      current_uid_.InitFromValueTable(projected_userid_column.get(),
                                      copy->alias());

      // Create a new Project node that projects the extracted userid
      // field from the table's row (proto or struct) value.
      std::vector<ResolvedColumn> project_column_list_with_userid =
          copy->column_list();
      project_column_list_with_userid.emplace_back(current_uid_.column);

      PushNodeToStack(MakeResolvedProjectScan(
          project_column_list_with_userid,
          MakeNodeVector(std::move(projected_userid_column)),
          ConsumeTopOfStack<ResolvedScan>()));
    }
    ZETASQL_RETURN_IF_ERROR(ValidateUidColumnSupportsGrouping(*node));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedTVFScan(const ResolvedTVFScan* node) override {
    // We do not currently allow TVF arguments to contain anonymization,
    // because we are not invoking the rewriter on the TVF arguments yet.
    for (const std::unique_ptr<const ResolvedFunctionArgument>& arg :
         node->argument_list()) {
      TVFArgumentValidatorVisitor visitor(node->tvf()->FullName());
      ZETASQL_RETURN_IF_ERROR(arg->Accept(&visitor));
    }

    {
      ResolvedASTDeepCopyVisitor copy_visitor;
      ZETASQL_RETURN_IF_ERROR(node->Accept(&copy_visitor));
      ZETASQL_ASSIGN_OR_RETURN(auto copy,
                       copy_visitor.ConsumeRootNode<ResolvedTVFScan>());
      PushNodeToStack(std::move(copy));
    }
    ResolvedTVFScan* copy = GetUnownedTopOfStack<ResolvedTVFScan>();

    // The TVF doesn't produce user data or an anonymization userid column, so
    // we can return early.
    //
    // TODO: Figure out how we can take an early exit without the
    // copy. Does this method take ownership of <node>? Can we effectively
    // push it back onto the top of the stack (which requires a non-const
    // std::unique_ptr<ResolvedNode>)?  I tried creating a non-const unique_ptr
    // and that failed with what looked like a double free condition.  It's
    // unclear at present what the contracts are and how we can avoid the
    // needless copy.
    if (!copy->signature()->SupportsAnonymization()) {
      return absl::OkStatus();
    }

    if (copy->signature()->result_schema().is_value_table()) {
      ZETASQL_RET_CHECK_EQ(copy->signature()->result_schema().num_columns(), 1);
      const std::optional<const AnonymizationInfo> anonymization_info =
          copy->signature()->GetAnonymizationInfo();
      ZETASQL_RET_CHECK(anonymization_info.has_value());

      ResolvedColumn value_column;
      // Check if the value table column is already being projected.
      if (copy->column_list_size() > 0) {
        ZETASQL_RET_CHECK_EQ(copy->column_list_size(), 1);
        value_column = copy->column_list(0);
      } else {
        // Create and project the column of the entire proto.
        value_column = allocator_->MakeCol(
            copy->tvf()->Name(), "$value",
            copy->signature()->result_schema().column(0).type);
        copy->mutable_column_list()->push_back(value_column);
        copy->mutable_column_index_list()->push_back(0);
      }

      // Build an expression to extract the userid column from the
      // value table row value.
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<ResolvedComputedColumn> projected_userid_column,
          MakeGetFieldComputedColumn(anonymization_info->UserIdColumnNamePath(),
                                     value_column));

      current_uid_.InitFromValueTable(projected_userid_column.get(),
                                      copy->alias());

      std::vector<ResolvedColumn> project_column_list_with_userid =
          copy->column_list();
      project_column_list_with_userid.emplace_back(current_uid_.column);

      PushNodeToStack(MakeResolvedProjectScan(
          project_column_list_with_userid,
          MakeNodeVector(std::move(projected_userid_column)),
          ConsumeTopOfStack<ResolvedScan>()));

      ZETASQL_RETURN_IF_ERROR(ValidateUidColumnSupportsGrouping(*node));
      return absl::OkStatus();
    }

    if (copy->signature()
            ->GetAnonymizationInfo()
            ->UserIdColumnNamePath()
            .size() > 1) {
      return MakeSqlErrorAtNode(*node)
             << "Nested user IDs are not currently supported for TVFs (in TVF "
             << copy->tvf()->FullName() << ")";
    }
    // Since we got to here, the TVF produces a userid column so we must ensure
    // that the column is projected for use in the anonymized aggregation.
    const std::string& userid_column_name = copy->signature()
                                                ->GetAnonymizationInfo()
                                                ->GetUserIdInfo()
                                                .get_column_name();

    // Check if the $uid column is already being projected.
    for (int i = 0; i < copy->column_list_size(); ++i) {
      // Look up the schema column name in the index list.
      const std::string& result_column_name =
          copy->signature()
              ->result_schema()
              .column(copy->column_index_list(i))
              .name;
      if (result_column_name == userid_column_name) {
        // Already projected, we're done.
        current_uid_.SetColumn(copy->column_list(i), copy->alias());
        return absl::OkStatus();
      }
    }

    // We need to project the $uid column. Look it up by name in the TVF schema
    // to get type information and record it in column_index_list.
    int tvf_userid_column_index = -1;
    for (int i = 0; i < copy->signature()->result_schema().num_columns(); ++i) {
      if (userid_column_name ==
          copy->signature()->result_schema().column(i).name) {
        tvf_userid_column_index = i;
        break;
      }
    }
    // Engines should normally validate the userid column when creating/adding
    // the TVF to the catalog whenever possible. However, this is not possible
    // in all cases - for example for templated TVFs where the output schema is
    // unknown until call time. So we produce a user-facing error message in
    // this case.
    if (tvf_userid_column_index == -1) {
      return MakeSqlErrorAtNode(*node)
             << "The anonymization userid column " << userid_column_name
             << " defined for TVF " << copy->tvf()->FullName()
             << " was not found in the output schema of the TVF";
    }

    // Create and project the new $uid column.
    ResolvedColumn uid_column =
        allocator_->MakeCol(copy->tvf()->Name(), userid_column_name,
                            copy->signature()
                                ->result_schema()
                                .column(tvf_userid_column_index)
                                .type);

    // Per the ResolvedTVFScan contract:
    //   <column_list> is a set of ResolvedColumns created by this scan.
    //   These output columns match positionally with the columns in the output
    //   schema of <signature>
    // To satisfy this contract we must also insert the $uid column
    // positionally. The target location is at the first value in
    // column_index_list that is greater than tvf_userid_column_index (because
    // it is positional the indices must be ordered).
    int userid_column_insertion_index = 0;
    for (int i = 0; i < copy->column_index_list_size(); ++i) {
      if (copy->column_index_list(i) > tvf_userid_column_index) {
        userid_column_insertion_index = i;
        break;
      }
    }

    copy->mutable_column_list()->insert(
        copy->column_list().begin() + userid_column_insertion_index,
        uid_column);
    copy->mutable_column_index_list()->insert(
        copy->column_index_list().begin() + userid_column_insertion_index,
        tvf_userid_column_index);
    current_uid_.SetColumn(uid_column, copy->alias());

    return absl::OkStatus();
  }

  absl::Status VisitResolvedWithRefScan(
      const ResolvedWithRefScan* node) override {
    // No $uid column should have been encountered before now
    ZETASQL_RET_CHECK(!current_uid_.column.IsInitialized());

    // Lookup the referenced WITH entry
    auto it = std::find_if(
        with_entries_.begin(), with_entries_.end(),
        [node](const std::unique_ptr<WithEntryRewriteState>& entry) {
          return node->with_query_name() ==
                 entry->original_entry.with_query_name();
        });
    ZETASQL_RET_CHECK(it != with_entries_.end())
        << "Failed to find WITH entry " << node->with_query_name();
    WithEntryRewriteState& entry = **it;

    if (entry.rewritten_entry == nullptr) {
      // This entry hasn't been rewritten yet, rewrite it as if it was just a
      // nested subquery.
      ZETASQL_ASSIGN_OR_RETURN(entry.rewritten_entry_owned,
                       ProcessNode(&entry.original_entry));
      // VisitResolvedWithEntry sets 'entry.rewritten_entry'
      ZETASQL_RET_CHECK_EQ(entry.rewritten_entry, entry.rewritten_entry_owned.get())
          << "Invalid rewrite state for " << node->with_query_name();
    }

    ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedWithRefScan(node));
    if (entry.rewritten_uid && entry.rewritten_uid->column.IsInitialized()) {
      // The WITH entry contained a reference to user data, use its $uid column.
      auto* copy = GetUnownedTopOfStack<ResolvedWithRefScan>();
      // Update $uid column reference. The column_list in the
      // ResolvedWithRefScan matches positionally with the column_list in the
      // ResolvedWithEntry. But if the WithEntry explicitly selects columns and
      // does not include the $uid column, ResolvedWithRefScan will have one
      // less column.
      for (int i = 0;
           i < entry.rewritten_entry->with_subquery()->column_list().size() &&
           i < copy->column_list().size();
           ++i) {
        if (entry.rewritten_entry->with_subquery()
                ->column_list(i)
                .column_id() == entry.rewritten_uid->column.column_id()) {
          current_uid_.SetColumn(copy->column_list(i), "");
          return absl::OkStatus();
        }
      }
    }
    return absl::OkStatus();
  }
  absl::Status VisitResolvedWithEntry(const ResolvedWithEntry* node) override {
    // No $uid column should have been encountered before now
    ZETASQL_RET_CHECK(!current_uid_.column.IsInitialized());
    ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedWithEntry(node));
    // Check if this entry is recorded in 'with_entries_', record the rewritten
    // result and $uid column if so.
    for (auto& entry : with_entries_) {
      if (node->with_query_name() == entry->original_entry.with_query_name()) {
        ZETASQL_RET_CHECK(entry->rewritten_entry == nullptr)
            << "WITH entry has already been rewritten: "
            << node->with_query_name();
        entry->rewritten_entry = GetUnownedTopOfStack<ResolvedWithEntry>();
        entry->rewritten_uid = std::move(current_uid_);
        current_uid_.Clear();
        return absl::OkStatus();
      }
    }
    // Record this entry and corresponding rewrite state for use by
    // VisitResolvedWithRefScan.
    with_entries_.emplace_back(new WithEntryRewriteState{
        .original_entry = *node,
        .rewritten_entry = GetUnownedTopOfStack<ResolvedWithEntry>(),
        .rewritten_uid = std::move(current_uid_)});
    current_uid_.Clear();
    return absl::OkStatus();
  }

  absl::Status VisitResolvedJoinScan(const ResolvedJoinScan* node) override {
    // No $uid column should have been encountered before now
    ZETASQL_RET_CHECK(!current_uid_.column.IsInitialized());

    // Make a simple copy of the join node that we can swap the left and right
    // scans out of later.
    ResolvedASTDeepCopyVisitor join_visitor;
    ZETASQL_RETURN_IF_ERROR(node->Accept(&join_visitor));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedJoinScan> owned_copy,
                     join_visitor.ConsumeRootNode<ResolvedJoinScan>());
    PushNodeToStack(std::move(owned_copy));
    ResolvedJoinScan* copy = GetUnownedTopOfStack<ResolvedJoinScan>();

    // Rewrite and copy the left scan.
    PerUserRewriterVisitor left_visitor(allocator_, type_factory_, resolver_,
                                        resolved_table_scans_, with_entries_);
    ZETASQL_RETURN_IF_ERROR(node->left_scan()->Accept(&left_visitor));
    const ResolvedColumn& left_uid = left_visitor.current_uid_.column;
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedScan> left_scan,
                     left_visitor.ConsumeRootNode<ResolvedScan>());
    copy->set_left_scan(std::move(left_scan));

    // Rewrite and copy the right scan.
    PerUserRewriterVisitor right_visitor(allocator_, type_factory_, resolver_,
                                         resolved_table_scans_, with_entries_);
    ZETASQL_RETURN_IF_ERROR(node->right_scan()->Accept(&right_visitor));
    const ResolvedColumn& right_uid = right_visitor.current_uid_.column;
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedScan> right_scan,
                     right_visitor.ConsumeRootNode<ResolvedScan>());
    copy->set_right_scan(std::move(right_scan));

    if (!left_uid.IsInitialized() && !right_uid.IsInitialized()) {
      // Two non-private tables
      // Nothing needs to be done
      return absl::OkStatus();
    } else if (left_uid.IsInitialized() && right_uid.IsInitialized()) {
      // Two private tables
      // Both tables have a $uid column, so we add AND Left.$uid = Right.$uid
      // to the join clause after checking that the types are equal and
      // comparable
      // TODO: Revisit if we want to allow $uid type coercion
      if (!left_uid.type()->Equals(right_uid.type())) {
        return MakeSqlErrorAtNode(*copy) << absl::StrCat(
                   "Joining two tables containing private data requires "
                   "matching user id column types, instead got ",
                   Type::TypeKindToString(left_uid.type()->kind(),
                                          resolver_->language().product_mode()),
                   " and ",
                   Type::TypeKindToString(
                       right_uid.type()->kind(),
                       resolver_->language().product_mode()));
      }
      if (!left_uid.type()->SupportsEquality(resolver_->language())) {
        return MakeSqlErrorAtNode(*copy) << absl::StrCat(
                   "Joining two tables containing private data requires "
                   "the user id column types to support equality comparison, "
                   "instead got ",
                   Type::TypeKindToString(
                       left_uid.type()->kind(),
                       resolver_->language().product_mode()));
      }

      // Reject joins with either missing join expressions, or join
      // expressions that don't join on $uid
      // TODO: also support uid constraints with a WHERE clause,
      // for example this query:
      //   select anon_count(*)
      //   from t1, t2
      //   where t1.uid = t2.uid;
      if (copy->join_expr() == nullptr) {
        return MakeSqlErrorAtNode(*copy) << absl::StrCat(
                   "Joins between tables containing private data must "
                   "explicitly join on the user id column in each table",
                   FormatJoinUidError(", add 'ON %s=%s'",
                                      left_visitor.current_uid_,
                                      right_visitor.current_uid_));
      }
      if (!JoinExprIncludesUid(copy->join_expr(), left_uid, right_uid)) {
        return MakeSqlErrorAtNode(*copy->join_expr()) << absl::StrCat(
                   "Joins between tables containing private data must also "
                   "explicitly join on the user id column in each table",
                   FormatJoinUidError(
                       ", add 'AND %s=%s' to the join ON expression",
                       left_visitor.current_uid_, right_visitor.current_uid_));
      }
    }

    // At this point, we are either joining two private tables and Left.$uid
    // and Right.$uid are both valid, or joining a private table against a
    // non-private table and exactly one of {Left.$uid, Right.$uid} are valid.
    //
    // Now we want to check if a valid $uid column is being projected, and add
    // an appropriate one based on the join type if not.
    // INNER JOIN: project either Left.$uid or Right.$uid
    // LEFT JOIN:  project (and require) Left.$uid
    // RIGHT JOIN: project (and require) Right.$uid
    // FULL JOIN:  require Left.$uid and Right.$uid, project
    //             COALESCE(Left.$uid, Right.$uid)
    current_uid_.column.Clear();

    switch (node->join_type()) {
      case ResolvedJoinScan::INNER:
        // If both join inputs have a $uid column then project the $uid from
        // either of them.  Otherwise project the $uid column from the join
        // input that contains it.
        for (const ResolvedColumn& col : copy->column_list()) {
          if (col == left_uid || col == right_uid) {
            current_uid_.SetColumn(col);
            return absl::OkStatus();
          }
        }
        // We are not currently projecting either the Left or Right $uid
        // column.
        if (current_uid_.SetColumn(left_uid.IsInitialized() ? left_uid
                                                            : right_uid)) {
          copy->add_column_list(current_uid_.column);
        }
        return absl::OkStatus();

      case ResolvedJoinScan::LEFT:
        // We must project the $uid from the Left table in a left outer join,
        // otherwise we end up with rows with NULL $uid.
        if (!left_uid.IsInitialized()) {
          return MakeSqlErrorAtNode(*copy->left_scan())
                 << "The left table in a LEFT OUTER join must contain user "
                    "data";
        }
        for (const ResolvedColumn& col : copy->column_list()) {
          if (col == left_uid) {
            current_uid_.SetColumn(col);
            return absl::OkStatus();
          }
        }
        if (current_uid_.SetColumn(left_uid)) {
          copy->add_column_list(current_uid_.column);
        }
        return absl::OkStatus();

      case ResolvedJoinScan::RIGHT:
        // We must project the $uid from the Right table in a right outer
        // join, otherwise we end up with rows with NULL $uid.
        if (!right_uid.IsInitialized()) {
          return MakeSqlErrorAtNode(*copy->right_scan())
                 << "The right table in a RIGHT OUTER join must contain user "
                    "data";
        }
        for (const ResolvedColumn& col : copy->column_list()) {
          if (col == right_uid) {
            current_uid_.SetColumn(col);
            return absl::OkStatus();
          }
        }
        if (current_uid_.SetColumn(right_uid)) {
          copy->add_column_list(current_uid_.column);
        }
        return absl::OkStatus();

      case ResolvedJoinScan::FULL:
        // Full outer joins require both tables to have an attached $uid. We
        // project COALESCE(Left.$uid, Right.$uid) because up to one of the
        // $uid columns may be null for each output row.
        if (!left_uid.IsInitialized() || !right_uid.IsInitialized()) {
          return MakeSqlErrorAtNode(left_uid.IsInitialized()
                                        ? *copy->right_scan()
                                        : *copy->left_scan())
                 << "Both tables in a FULL OUTER join must contain user "
                    "data";
        }

        // Full outer join, the result $uid column is
        // COALESCE(Left.$uid, Right.$uid).
        // TODO: This generated column is an internal name and
        // isn't selectable by the end user, this makes full outer joins
        // unusable in nested queries. Improve either error messages or change
        // query semantics around full outer joins to fix this usability gap.
        std::vector<ResolvedColumn> wrapped_column_list = copy->column_list();
        copy->add_column_list(left_uid);
        copy->add_column_list(right_uid);

        std::vector<std::unique_ptr<const ResolvedExpr>> arguments;
        arguments.emplace_back(MakeColRef(left_uid));
        arguments.emplace_back(MakeColRef(right_uid));
        ZETASQL_ASSIGN_OR_RETURN(
            std::unique_ptr<ResolvedExpr> coalesced_uid_function,
            ResolveFunctionCall("coalesce", std::move(arguments), resolver_));

        ResolvedColumn uid_column = allocator_->MakeCol(
            "$join", "$uid", coalesced_uid_function->type());
        auto coalesced_uid_column = MakeResolvedComputedColumn(
            uid_column, std::move(coalesced_uid_function));
        if (current_uid_.SetColumn(coalesced_uid_column->column())) {
          wrapped_column_list.emplace_back(current_uid_.column);
        }

        PushNodeToStack(MakeResolvedProjectScan(
            wrapped_column_list,
            MakeNodeVector(std::move(coalesced_uid_column)),
            ConsumeTopOfStack<ResolvedScan>()));

        return absl::OkStatus();
    }
  }

  // Nested AggregateScans require special handling. The differential privacy
  // spec requires that each such scan GROUPs BY the $uid column. But GROUP BY
  // columns are implemented as computed columns in ZetaSQL, so we need to
  // inspect the group by list and update 'current_uid_column_' with the new
  // ResolvedColumn.
  absl::Status VisitResolvedAggregateScan(
      const ResolvedAggregateScan* node) override {
    ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedAggregateScan(node));
    if (!current_uid_.column.IsInitialized()) {
      // Table doesn't contain any private data, so do nothing.
      return absl::OkStatus();
    }

    ResolvedAggregateScan* copy = GetUnownedTopOfStack<ResolvedAggregateScan>();

    // If the source table is a value table the uid column refs will be
    // GetProtoField or GetStructField expressions, replace them with ColumnRef
    // expressions.
    copy->set_group_by_list(current_uid_.SubstituteUidComputedColumn(
        copy->release_group_by_list()));

    // AggregateScan nodes in the per-user transform must always group by
    // $uid. Check if we already do so, and add a group by element if not.
    ResolvedColumn group_by_uid_col;
    for (const auto& col : copy->group_by_list()) {
      if (col->expr()->node_kind() != zetasql::RESOLVED_COLUMN_REF) {
        // Even if 'group by $uid+0' is equivalent to 'group by $uid', these
        // kind of operations are hard to verify so let's ignore them.
        continue;
      }
      const ResolvedColumn& grouped_by_column =
          col->expr()->GetAs<ResolvedColumnRef>()->column();
      if (grouped_by_column.column_id() == current_uid_.column.column_id()) {
        group_by_uid_col = col->column();
        break;
      }
    }

    if (group_by_uid_col.IsInitialized()) {
      // Point current_uid_column_ to the updated group by column, and verify
      // that the original query projected it.
      if (current_uid_.SetColumn(group_by_uid_col)) {
        for (const ResolvedColumn& col : copy->column_list()) {
          if (col == current_uid_.column) {
            // Explicitly projecting a column removes the alias.
            current_uid_.alias = "";
            return absl::OkStatus();
          }
        }
      }
    }
    return absl::OkStatus();
  }

  // For nested projection operations, we require the query to explicitly
  // project $uid.
  absl::Status VisitResolvedProjectScan(
      const ResolvedProjectScan* node) override {
    ZETASQL_RETURN_IF_ERROR(
        MaybeAttachParseLocation(CopyVisitResolvedProjectScan(node), *node));

    if (!current_uid_.column.IsInitialized() ||
        current_uid_.subquery_expr_level > 0) {
      return absl::OkStatus();
    }
    auto* copy = GetUnownedTopOfStack<ResolvedProjectScan>();

    // If the source table is a value table the uid column refs will be
    // GetProtoField or GetStructField expressions, replace them with ColumnRef
    // expressions.
    copy->set_expr_list(
        current_uid_.SubstituteUidComputedColumn(copy->release_expr_list()));

    for (const ResolvedColumn& col : copy->column_list()) {
      if (col.column_id() == current_uid_.column.column_id()) {
        // Explicitly projecting a column removes the alias.
        current_uid_.alias = "";
        return absl::OkStatus();
      }
    }

    // TODO: Ensure that the $uid column name in the error message
    // is appropriately alias/qualified.
    return MakeSqlErrorAtNode(*copy) << absl::StrFormat(
               "Subqueries of anonymization queries must explicitly "
               "SELECT the userid column '%s'",
               current_uid_.ToString());
  }

  absl::Status VisitResolvedSubqueryExpr(
      const ResolvedSubqueryExpr* node) override {
    // In the subquery expression, if the uid is in the columns list of the
    // TableScan, the `current_uid_` will be set, but cannot be accessed outside
    // the subquery, which is not correct. In our design, the TableScan in the
    // subquery expression should not update the `current_uid_`.
    current_uid_.subquery_expr_level++;
    ZETASQL_RETURN_IF_ERROR(
        ResolvedASTDeepCopyVisitor::CopyVisitResolvedSubqueryExpr(node));
    current_uid_.subquery_expr_level--;
    return absl::OkStatus();
  }

  absl::Status VisitResolvedAnonymizedAggregateScan(
      const ResolvedAnonymizedAggregateScan* node) override {
    if (current_uid_.subquery_expr_level > 0) {
      return MakeSqlErrorAtNode(*node)
             << "Nested anonymization query is not implemented yet";
    }
    ZETASQL_RETURN_IF_ERROR(
        ResolvedASTDeepCopyVisitor::CopyVisitResolvedAnonymizedAggregateScan(
            node));
    return absl::OkStatus();
  }

  /////////////////////////////////////////////////////////////////////////////
  // For these scans, the $uid column can be implicitly projected
  /////////////////////////////////////////////////////////////////////////////
#define PROJECT_UID(resolved_scan)                                        \
  absl::Status Visit##resolved_scan(const resolved_scan* node) override { \
    ZETASQL_RETURN_IF_ERROR(CopyVisit##resolved_scan(node));                      \
    if (!current_uid_.column.IsInitialized()) {                           \
      return absl::OkStatus();                                            \
    }                                                                     \
    auto* scan = GetUnownedTopOfStack<resolved_scan>();                   \
    for (const ResolvedColumn& col : scan->column_list()) {               \
      if (col.column_id() == current_uid_.column.column_id()) {           \
        return absl::OkStatus();                                          \
      }                                                                   \
    }                                                                     \
    if (current_uid_.subquery_expr_level == 0) {                          \
      scan->add_column_list(current_uid_.column);                         \
    }                                                                     \
    return absl::OkStatus();                                              \
  }
  PROJECT_UID(ResolvedArrayScan);
  PROJECT_UID(ResolvedSingleRowScan);
  PROJECT_UID(ResolvedFilterScan);
  PROJECT_UID(ResolvedOrderByScan);
  PROJECT_UID(ResolvedLimitOffsetScan);
#undef PROJECT_UID

  /////////////////////////////////////////////////////////////////////////////
  // As of now unsupported per-user scans
  // TODO: Provide a user-friendly error message
  /////////////////////////////////////////////////////////////////////////////
#define UNSUPPORTED(resolved_scan)                                         \
  absl::Status Visit##resolved_scan(const resolved_scan* node) override {  \
    return MakeSqlErrorAtNode(*node)                                       \
           << "Unsupported scan type inside of SELECT WITH ANONYMIZATION " \
              "from clause: " #resolved_scan;                              \
  }
  UNSUPPORTED(ResolvedSetOperationScan);
  UNSUPPORTED(ResolvedAnalyticScan);
  UNSUPPORTED(ResolvedSampleScan);
  UNSUPPORTED(ResolvedRelationArgumentScan);
  UNSUPPORTED(ResolvedRecursiveScan);
  UNSUPPORTED(ResolvedRecursiveRefScan);
#undef UNSUPPORTED

  // Join errors are special cased because:
  // 1) they reference uid columns from two different table subqueries
  // 2) we want to suggest table names as implicit aliases, when helpful
  static std::string FormatJoinUidError(
      const absl::FormatSpec<std::string, std::string>& format_str,
      UidColumnState column1, UidColumnState column2) {
    if (IsInternalAlias(column1.column.name()) ||
        IsInternalAlias(column2.column.name())) {
      return "";
    }
    // Use full table names as uid aliases where doing so reduces ambiguity:
    // 1) the tables must have different names
    // 2) the uid columns must have the same name
    // 3) the query doesn't specify a table alias
    if (column1.column.table_name() != column2.column.table_name() &&
        column1.column.name() == column2.column.name()) {
      if (column1.alias.empty()) column1.alias = column1.column.table_name();
      if (column2.alias.empty()) column2.alias = column2.column.table_name();
    }
    return absl::StrFormat(format_str, column1.ToString(), column2.ToString());
  }

  absl::Status ValidateUidColumnSupportsGrouping(const ResolvedNode& node) {
    if (!current_uid_.column.type()->SupportsGrouping(resolver_->language())) {
      return MakeSqlErrorAtNode(node)
             << "User id columns must support grouping, instead got type "
             << Type::TypeKindToString(current_uid_.column.type()->kind(),
                                       resolver_->language().product_mode());
    }
    return absl::OkStatus();
  }

  ColumnFactory* allocator_;   // unowned
  TypeFactory* type_factory_;  // unowned
  Resolver* resolver_;         // unowned
  std::vector<const ResolvedTableScan*>& resolved_table_scans_;  // unowned
  std::vector<std::unique_ptr<WithEntryRewriteState>>&
      with_entries_;  // unowned

  UidColumnState current_uid_;
};

absl::StatusOr<std::unique_ptr<ResolvedAggregateScan>>
RewriterVisitor::RewriteInnerAggregateScan(
    const ResolvedAnonymizedAggregateScan* node,
    std::map<ResolvedColumn, ResolvedColumn>* injected_col_map,
    ResolvedColumn* uid_column) {
  // Construct a deep copy of the per-user transform, rewriting along to the
  // way to group by and project $uid to the top.
  PerUserRewriterVisitor per_user_visitor(allocator_, type_factory_, resolver_,
                                          resolved_table_scans_, with_entries_);
  ZETASQL_RETURN_IF_ERROR(node->input_scan()->Accept(&per_user_visitor));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedScan> input_scan,
                   per_user_visitor.ConsumeRootNode<ResolvedScan>());

  // Rewrite the aggregate list to change ANON_* functions to their per-user
  // aggregate alternatives (e.g. ANON_SUM->SUM).
  //
  // This also changes the output column of each function to the appropriate
  // intermediate column, as dictated by injected_col_map.
  InnerAggregateListRewriterVisitor inner_rewriter_visitor(
      injected_col_map, allocator_, resolver_);
  std::vector<std::unique_ptr<ResolvedComputedColumn>> inner_aggregate_list;
  for (const auto& col : node->aggregate_list()) {
    ZETASQL_RETURN_IF_ERROR(col->Accept(&inner_rewriter_visitor));
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<ResolvedComputedColumn> unique_ptr_node,
        inner_rewriter_visitor.ConsumeRootNode<ResolvedComputedColumn>());
    inner_aggregate_list.emplace_back(std::move(unique_ptr_node));
  }

  // Rewrite the GROUP BY list to change each output column to the appropriate
  // intermediate column, as dictated by injected_col_map.
  //
  // Any complex GROUP BY transforms/computed columns will be included here
  // (e.g. GROUP BY col1 + col2).
  std::vector<std::unique_ptr<ResolvedComputedColumn>> inner_group_by_list;
  for (const auto& col : node->group_by_list()) {
    ZETASQL_RETURN_IF_ERROR(col->Accept(&inner_rewriter_visitor));
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<ResolvedComputedColumn> unique_ptr_node,
        inner_rewriter_visitor.ConsumeRootNode<ResolvedComputedColumn>());
    inner_group_by_list.emplace_back(std::move(unique_ptr_node));
  }

  // Always group by the $uid column produced by the per-user visitor.
  std::optional<ResolvedColumn> inner_uid_column =
      per_user_visitor.uid_column();
  if (!inner_uid_column.has_value()) {
    const std::string or_tvf_string(
        resolver_->language().LanguageFeatureEnabled(
            FEATURE_TABLE_VALUED_FUNCTIONS)
            ? "or TVF "
            : "");
    return MakeSqlErrorAtNode(*node)
           << "A SELECT WITH ANONYMIZATION query must query "
              "at least one table "
           << or_tvf_string << "containing user data";
  }

  // This is validated by PerUserRewriterVisitor.
  ZETASQL_RET_CHECK(inner_uid_column->type()->SupportsGrouping(resolver_->language()));

  *uid_column =
      allocator_->MakeCol("$group_by", "$uid", inner_uid_column->type()),
  inner_group_by_list.emplace_back(
      MakeResolvedComputedColumn(*uid_column, MakeColRef(*inner_uid_column)));

  // Collect an updated column list, the new list will be entirely disjoint
  // from the original due to intermediate column id rewriting.
  std::vector<ResolvedColumn> new_column_list;
  new_column_list.reserve(inner_aggregate_list.size() +
                          inner_group_by_list.size());
  for (const auto& column : inner_aggregate_list) {
    new_column_list.push_back(column->column());
  }
  for (const auto& column : inner_group_by_list) {
    new_column_list.push_back(column->column());
  }

  // We need to rewrite the ANON_VAR_POP/ANON_STDDEV_POP/ANON_PERCENTILE_CONT's
  // InnerAggregateScan to ARRAY_AGG(expr ORDER BY rand() LIMIT 5).
  // We allocated an `order_by_column` in the InnerAggregateListRewriter, the
  // `order_by_column` will be rand(). Then we can use the new_project_scan as
  // the input_scan of ResolvedAggregateScan.
  if (inner_rewriter_visitor.order_by_column().IsInitialized()) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> rand_function,
                     ResolveFunctionCall("rand", {}, resolver_));
    std::vector<std::unique_ptr<ResolvedComputedColumn>> order_by_expr_list;
    std::unique_ptr<ResolvedComputedColumn> rand_expr =
        MakeResolvedComputedColumn(inner_rewriter_visitor.order_by_column(),
                                   std::move(rand_function));
    ZETASQL_RET_CHECK(rand_expr != nullptr);
    order_by_expr_list.emplace_back(std::move(rand_expr));

    ResolvedColumnList wrapper_column_list = input_scan->column_list();
    for (const auto& computed_column : order_by_expr_list) {
      wrapper_column_list.push_back(computed_column->column());
    }
    std::unique_ptr<const ResolvedScan> new_project_scan =
        MakeResolvedProjectScan(wrapper_column_list,
                                std::move(order_by_expr_list),
                                std::move(input_scan));

    input_scan = std::move(new_project_scan);
  }
  return MakeResolvedAggregateScan(new_column_list, std::move(input_scan),
                                   std::move(inner_group_by_list),
                                   std::move(inner_aggregate_list), {}, {});
}

absl::Status RewriterVisitor::MakeKThresholdFunctionColumn(
    double privacy_budget_weight,
    std::unique_ptr<ResolvedComputedColumn>* out) {
  std::vector<std::unique_ptr<const ResolvedExpr>> argument_list;
  // Create function call argument list logically equivalent to:
  //   ANON_SUM(1 CLAMPED BETWEEN 0 AND 1)
  argument_list.emplace_back(MakeResolvedLiteral(Value::Int64(1)));
  argument_list.emplace_back(MakeResolvedLiteral(Value::Int64(0)));
  argument_list.emplace_back(MakeResolvedLiteral(Value::Int64(1)));

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedExpr> call,
      ResolveFunctionCall("anon_sum", std::move(argument_list), resolver_));
  ZETASQL_RET_CHECK_EQ(call->node_kind(), RESOLVED_AGGREGATE_FUNCTION_CALL)
      << call->DebugString();
  ResolvedColumn uid_column =
      allocator_->MakeCol("$anon", "$k_threshold_col", call->type());
  *out = MakeResolvedComputedColumn(uid_column, std::move(call));
  return absl::OkStatus();
}

absl::Status RewriterVisitor::VisitResolvedAnonymizedAggregateScan(
    const ResolvedAnonymizedAggregateScan* node) {
  // This map is populated when the per-user aggregate list is resolved. It maps
  // the existing columns in `node->column_list()` to the new intermediate
  // columns that splice together the per-user and cross-user aggregate/groupby
  // lists.
  std::map<ResolvedColumn, ResolvedColumn> injected_col_map;

  // Look for kappa in the options.
  const Value* kappa_value = nullptr;
  for (const auto& option : node->anonymization_option_list()) {
    if (zetasql_base::CaseEqual(option->name(), "kappa")) {
      if (kappa_value != nullptr) {
        return MakeSqlErrorAtNode(*option)
               << "Anonymization option kappa must only be set once";
      }
      if (option->value()->node_kind() == RESOLVED_LITERAL &&
          option->value()->GetAs<ResolvedLiteral>()->type()->IsInt64()) {
        kappa_value = &option->value()->GetAs<ResolvedLiteral>()->value();
        // The privacy libraries only support int32_t kappa, so produce an
        // error if the kappa value does not fit in that range.
        if (kappa_value->int64_value() < 1 ||
            kappa_value->int64_value() > std::numeric_limits<int32_t>::max()) {
          return MakeSqlErrorAtNode(*option)
                 << "Anonymization option kappa must be an INT64 literal "
                    "between "
                 << "1 and " << std::numeric_limits<int32_t>::max();
        }
      } else {
        return MakeSqlErrorAtNode(*option)
               << "Anonymization option kappa must be an INT64 literal between "
               << "1 and " << std::numeric_limits<int32_t>::max();
      }
    }
  }

  ResolvedColumn uid_column;
  // Create the per-user aggregate scan, and populate the column map.
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedScan> input_scan,
      RewriteInnerAggregateScan(node, &injected_col_map, &uid_column));

  // Inject a SampleScan if kappa is present, in order to provide epsilon-delta
  // differential privacy in the presence of a GROUP BY clause.
  if (kappa_value != nullptr) {
    std::vector<std::unique_ptr<const ResolvedExpr>> partition_by_list;
    partition_by_list.push_back(MakeColRef(uid_column));
    const std::vector<ResolvedColumn>& column_list = input_scan->column_list();
    input_scan = MakeResolvedSampleScan(
        column_list, std::move(input_scan),
        /*method=*/"RESERVOIR", MakeResolvedLiteral(*kappa_value),
        ResolvedSampleScan::ROWS, /*repeatable_argument=*/nullptr,
        /*weight_column=*/nullptr, std::move(partition_by_list));
  }

  // Rewrite the outer aggregate list, changing the first argument of each
  // ANON_* function to refer to their appropriate per-user column.
  OuterAggregateListRewriterVisitor outer_rewriter_visitor(injected_col_map,
                                                           resolver_);
  std::vector<std::unique_ptr<ResolvedComputedColumn>> outer_aggregate_list;
  for (const auto& col : node->aggregate_list()) {
    ZETASQL_RETURN_IF_ERROR(col->Accept(&outer_rewriter_visitor));
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<ResolvedComputedColumn> unique_ptr_node,
        outer_rewriter_visitor.ConsumeRootNode<ResolvedComputedColumn>());
    outer_aggregate_list.emplace_back(std::move(unique_ptr_node));
  }

  // Create a new k-threshold function because the existing call is unusable.
  // By default, we use a k-threshold of 1.
  std::unique_ptr<ResolvedComputedColumn> k_threshold_col;
  ZETASQL_RETURN_IF_ERROR(MakeKThresholdFunctionColumn(1, &k_threshold_col));
  std::unique_ptr<ResolvedColumnRef> k_threshold_colref =
      MakeColRef(k_threshold_col->column());
  outer_aggregate_list.emplace_back(std::move(k_threshold_col));

  // GROUP BY columns in the cross-user scan are always simple column
  // references to the intermediate columns. Any computed columns are handled
  // in the per-user scan.
  std::vector<std::unique_ptr<ResolvedComputedColumn>> outer_group_by_list;
  for (const std::unique_ptr<const ResolvedComputedColumn>& group_by :
       node->group_by_list()) {
    outer_group_by_list.emplace_back(MakeResolvedComputedColumn(
        group_by->column(),
        MakeColRef(injected_col_map.at(group_by->column()))));
  }

  // Copy the options for the new anonymized aggregate scan.
  std::vector<std::unique_ptr<const ResolvedOption>>
      resolved_anonymization_options;
  for (const std::unique_ptr<const ResolvedOption>& option :
           node->anonymization_option_list()) {
    ResolvedASTDeepCopyVisitor deep_copy_visitor;
    ZETASQL_RETURN_IF_ERROR(option->Accept(&deep_copy_visitor));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedOption> option_copy,
                     deep_copy_visitor.ConsumeRootNode<ResolvedOption>());
    resolved_anonymization_options.push_back(std::move(option_copy));
  }
  auto result = MakeResolvedAnonymizedAggregateScan(
      node->column_list(), std::move(input_scan),
      std::move(outer_group_by_list), std::move(outer_aggregate_list),
      std::move(k_threshold_colref), std::move(resolved_anonymization_options));
  for (auto resolved_table_scan : resolved_table_scans_) {
    table_scan_to_anon_aggr_scan_map_.emplace(resolved_table_scan,
                                              result.get());
  }
  resolved_table_scans_.clear();
  PushNodeToStack(std::move(result));
  return absl::OkStatus();
}

// The default behavior of ResolvedASTDeepCopyVisitor copies the WITH entries
// before copying the subquery. This is backwards, we need to know if a WITH
// entry is referenced inside a SELECT WITH ANONYMIZATION node to know how it
// should be copied. Instead, WithScans are rewritten as follows:
//
// 1. Collect a list of all (at this point un-rewritten) WITH entries.
// 2. Traverse and copy the WithScan subquery, providing the WITH entries list
//    to the PerUserRewriterVisitor when a SELECT WITH ANONYMIZATION node is
//    encountered.
// 3. When a ResolvedWithRefScan is encountered during the per-user rewriting
//    stage, begin rewriting the referenced WITH entry subquery. This can
//    repeat recursively for nested WITH entries.
// 4. Nested ResolvedWithScans inside of a SELECT WITH ANONYMIZATION node are
//    rewritten immediately by PerUserRewriterVisitor and recorded into the WITH
//    entries list.
// 5. Copy non-rewritten-at-this-point WITH entries, they weren't referenced
//    during the per-user rewriting stage and don't need special handling.
absl::Status RewriterVisitor::VisitResolvedWithScan(
    const ResolvedWithScan* node) {
  // Remember the offset for the with_entry_list_size() number of nodes we add
  // to the list of all WITH entries, those are the ones we need to add back to
  // with_entry_list() after rewriting.
  std::size_t local_with_entries_offset = with_entries_.size();
  for (const std::unique_ptr<const ResolvedWithEntry>& entry :
       node->with_entry_list()) {
    with_entries_.emplace_back(new WithEntryRewriteState(
        {.original_entry = *entry, .rewritten_entry = nullptr}));
  }
  // Copy the subquery. This will visit and copy referenced WITH entries.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedScan> subquery,
                   ProcessNode(node->query()));

  // Extract (and rewrite if needed) the WITH entries belonging to this node
  // out of the WITH entries list.
  std::vector<std::unique_ptr<const ResolvedWithEntry>> copied_entries;
  for (std::size_t i = local_with_entries_offset;
       i < local_with_entries_offset + node->with_entry_list_size(); ++i) {
    WithEntryRewriteState& entry = *with_entries_[i];
    if (entry.rewritten_entry == nullptr) {
      // Copy unreferenced WITH entries.
      ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedWithEntry(&entry.original_entry));
      entry.rewritten_entry_owned = ConsumeTopOfStack<ResolvedWithEntry>();
      entry.rewritten_entry = entry.rewritten_entry_owned.get();
    }
    copied_entries.emplace_back(std::move(entry.rewritten_entry_owned));
  }
  ZETASQL_RET_CHECK_EQ(copied_entries.size(), node->with_entry_list_size());

  // Copy the with scan now that we have the subquery and WITH entry list
  // copied.
  auto copy =
      MakeResolvedWithScan(node->column_list(), std::move(copied_entries),
                           std::move(subquery), node->recursive());

  // Copy node members that aren't constructor arguments.
  ZETASQL_ASSIGN_OR_RETURN(std::vector<std::unique_ptr<ResolvedOption>> hint_list,
                   ProcessNodeList(node->hint_list()));
  for (std::unique_ptr<ResolvedOption>& hint : hint_list) {
    copy->add_hint_list(std::move(hint));
  }
  copy->set_is_ordered(node->is_ordered());
  const auto parse_location = node->GetParseLocationRangeOrNULL();
  if (parse_location != nullptr) {
    copy->SetParseLocationRange(*parse_location);
  }

  // Add the non-abstract node to the stack.
  PushNodeToStack(std::move(copy));
  return absl::OkStatus();
}

absl::Status RewriterVisitor::VisitResolvedProjectScan(
    const ResolvedProjectScan* node) {
  return MaybeAttachParseLocation(CopyVisitResolvedProjectScan(node), *node);
}

absl::StatusOr<std::unique_ptr<const ResolvedNode>> RewriteInternal(
    const ResolvedNode& tree, AnalyzerOptions options,
    ColumnFactory& column_factory, Catalog& catalog, TypeFactory& type_factory,
    RewriteForAnonymizationOutput::TableScanToAnonAggrScanMap&
        table_scan_to_anon_aggr_scan_map) {
  options.CreateDefaultArenasIfNotSet();

  Resolver resolver(&catalog, &type_factory, &options);
  // The fresh resolver needs to be reset to initialize internal state before
  // use. We can use an empty SQL string because we aren't resolving a query,
  // we are just using the resolver to help resolve function calls from the
  // catalog.
  // Normally if errors are encountered during the function resolving process
  // the resolver also returns error locations based on the query string. We
  // don't have this issue because the calling code ensures that the resolve
  // calls do not return errors during normal use. We construct bogus
  // locations when resolving functions so that the resolver doesn't segfault
  // if an error is encountered, the bogus location information is ok because
  // these errors should only be raised during development in this file.
  resolver.Reset("");

  RewriterVisitor rewriter(&column_factory, &type_factory, &resolver,
                           table_scan_to_anon_aggr_scan_map);
  ZETASQL_RETURN_IF_ERROR(tree.Accept(&rewriter));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedNode> node,
                   rewriter.ConsumeRootNode<ResolvedNode>());
  return node;
}

}  // namespace

class AnonymizationRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.AllArenasAreInitialized());
    ColumnFactory column_factory(0, options.id_string_pool().get(),
                                 options.column_id_sequence_number());
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedNode> node,
        RewriteInternal(
            input, options, column_factory, catalog, type_factory,
            output_properties
                .resolved_table_scan_to_anonymized_aggregate_scan_map));
    return node;
  }

  std::string Name() const override { return "AnonymizationRewriter"; }
};

absl::StatusOr<RewriteForAnonymizationOutput>
RewriteForAnonymization(const ResolvedNode& query, Catalog* catalog,
                        TypeFactory* type_factory,
                        const AnalyzerOptions& analyzer_options,
                        ColumnFactory& column_factory) {
  RewriteForAnonymizationOutput result;
  ZETASQL_ASSIGN_OR_RETURN(
      result.node,
      RewriteInternal(query, analyzer_options, column_factory, *catalog,
                      *type_factory, result.table_scan_to_anon_aggr_scan_map));
  return result;
}

const Rewriter* GetAnonymizationRewriter() {
  static const Rewriter* kRewriter = new AnonymizationRewriter;
  return kRewriter;
}

}  // namespace zetasql
