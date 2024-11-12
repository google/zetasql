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

#include "zetasql/analyzer/graph_query_resolver.h"

#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/analyzer/analytic_function_resolver.h"
#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/function_resolver.h"
#include "zetasql/analyzer/graph_label_expr_resolver.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/analyzer/resolver_common_inl.h"
#include "zetasql/analyzer/set_operation_resolver_base.h"
#include "zetasql/common/graph_element_utils.h"
#include "zetasql/common/internal_analyzer_output_properties.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/property_graph.h"
#include "zetasql/public/select_with_mode.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/graph_element_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_builder.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/target_syntax.h"
#include "absl/algorithm/container.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "zetasql/base/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/stl_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

STATIC_IDSTRING(kGraphTableName, "$graph_table");
STATIC_IDSTRING(kElementTableName, "$element_table");
STATIC_IDSTRING(kPathScanName, "$path_scan");
STATIC_IDSTRING(kHeadColumnName, "$head");
STATIC_IDSTRING(kTailColumnName, "$tail");
STATIC_IDSTRING(kGraphSetOperation, "$graph_set_op");
STATIC_IDSTRING(kOffsetAlias, "offset");
STATIC_IDSTRING(kDefaultPathColumnName, "$path");

namespace {

ResolvedGraphEdgeScan::EdgeOrientation GetResolvedEdgeOrientation(
    ASTGraphEdgePattern::EdgeOrientation ast_edge_orientation) {
  switch (ast_edge_orientation) {
    case ASTGraphEdgePattern::ANY:
      return ResolvedGraphEdgeScan::ANY;
    case ASTGraphEdgePattern::LEFT:
      return ResolvedGraphEdgeScan::LEFT;
    case ASTGraphEdgePattern::RIGHT:
      return ResolvedGraphEdgeScan::RIGHT;
    default:
      ABSL_DCHECK(false) << "Unexpected edge orientation: " << ast_edge_orientation;
      return ResolvedGraphEdgeScan::ANY;
  }
}

ResolvedGraphPathMode::PathMode GetResolvedPathMode(
    const ASTGraphPathMode* path_mode) {
  if (path_mode == nullptr) {
    return ResolvedGraphPathMode::PATH_MODE_UNSPECIFIED;
  }
  switch (path_mode->path_mode()) {
    case ASTGraphPathMode::PATH_MODE_UNSPECIFIED:
      return ResolvedGraphPathMode::PATH_MODE_UNSPECIFIED;
    case ASTGraphPathMode::WALK:
      return ResolvedGraphPathMode::WALK;
    case ASTGraphPathMode::TRAIL:
      return ResolvedGraphPathMode::TRAIL;
    case ASTGraphPathMode::SIMPLE:
      return ResolvedGraphPathMode::SIMPLE;
    case ASTGraphPathMode::ACYCLIC:
      return ResolvedGraphPathMode::ACYCLIC;
    default:
      ABSL_LOG(ERROR) << "Unexpected path mode: " << path_mode;
      return ResolvedGraphPathMode::PATH_MODE_UNSPECIFIED;
  }
}

// Returns error if <name_list> derived from <location> contains any ambiguous
// name target; otherwise return ok.
// TODO: Make this part of GraphTableNamedVariables.
absl::Status CheckNoAmbiguousNameTarget(const ASTNode& location,
                                        const NameList& name_list) {
  for (const NamedColumn& named_column : name_list.columns()) {
    if (NameTarget target; name_list.LookupName(named_column.name(), &target) &&
                           target.IsAmbiguous()) {
      return MakeSqlErrorAt(&location)
             << "Ambiguous name: " << ToIdentifierLiteral(named_column.name());
    }
  }
  return absl::OkStatus();
}

// Map from identifier to resolved label expr.
using LabelExprMap =
    IdStringHashMapCase<std::unique_ptr<const ResolvedGraphLabelExpr>>;

GraphElementTable::Kind GetElementTableKind(
    const ASTGraphElementPattern& ast_element_pattern) {
  return ast_element_pattern.Is<ASTGraphNodePattern>()
             ? GraphElementTable::Kind::kNode
             : GraphElementTable::Kind::kEdge;
}

// Utility class for resolving label expressions.
class GraphLabelExprResolver {
 public:
  static absl::StatusOr<LabelExprMap> ResolveNamedVariables(
      const ASTGraphPattern* graph_pattern,
      std::function<
          absl::StatusOr<std::unique_ptr<const ResolvedGraphLabelExpr>>(
              const ASTGraphLabelFilter*, GraphElementTable::Kind)>
          label_expr_resolver) {
    GraphLabelExprResolver collector(std::move(label_expr_resolver));
    return collector.Visit(graph_pattern);
  }

 private:
  explicit GraphLabelExprResolver(
      std::function<
          absl::StatusOr<std::unique_ptr<const ResolvedGraphLabelExpr>>(
              const ASTGraphLabelFilter*, GraphElementTable::Kind)>
          label_expr_resolver)
      : label_expr_resolver_(std::move(label_expr_resolver)) {}

  absl::StatusOr<LabelExprMap> Visit(
      const ASTGraphElementPattern* ast_element_pattern) const {
    LabelExprMap map;
    const ASTGraphElementPatternFiller* filler = ast_element_pattern->filler();
    // Ignore anonymous variables as they are mutually distinct, they don't
    // have explicit identifiers and will not be multiply-declared variables.
    // They will be resolved on demand.
    if (filler == nullptr || filler->variable_name() == nullptr) {
      return map;
    }
    const ASTIdentifier* element_var = filler->variable_name();
    const ASTGraphLabelFilter* ast_label_filter = filler->label_filter();
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedGraphLabelExpr> label_expr,
        label_expr_resolver_(ast_label_filter,
                             GetElementTableKind(*ast_element_pattern)));
    map.emplace(element_var->GetAsIdString(), std::move(label_expr));
    return map;
  }

  absl::StatusOr<LabelExprMap> Visit(
      const ASTGraphPattern* graph_pattern) const {
    std::vector<LabelExprMap> results;
    results.reserve(graph_pattern->paths().size());
    for (const auto* path : graph_pattern->paths()) {
      ZETASQL_ASSIGN_OR_RETURN(LabelExprMap result, Visit(path));
      results.push_back(std::move(result));
    }
    return MergeLabelMaps(ResolvedGraphLabelNaryExpr::AND,
                          absl::MakeSpan(results));
  }

  absl::StatusOr<LabelExprMap> Visit(
      const ASTGraphPathPattern* path_pattern) const {
    std::vector<LabelExprMap> results;
    results.reserve(path_pattern->input_pattern_list().size());
    for (const auto* ast : path_pattern->input_pattern_list()) {
      switch (ast->node_kind()) {
        case AST_GRAPH_NODE_PATTERN:
        case AST_GRAPH_EDGE_PATTERN: {
          ZETASQL_ASSIGN_OR_RETURN(LabelExprMap result,
                           Visit(ast->GetAsOrDie<ASTGraphElementPattern>()));
          results.push_back(std::move(result));
          break;
        }
        case AST_GRAPH_PATH_PATTERN: {
          ZETASQL_ASSIGN_OR_RETURN(LabelExprMap result,
                           Visit(ast->GetAsOrDie<ASTGraphPathPattern>()));
          results.push_back(std::move(result));
          break;
        }
        default:
          return MakeSqlErrorAt(ast) << "Unsupported ast node";
      }
    }
    // Merge label maps because there can be multiply-declared variables.
    // Uses `AND` for pattern concatenation. Other composition could use a
    // different operation (for example, pattern alternation will use OR).
    return MergeLabelMaps(ResolvedGraphLabelNaryExpr::AND,
                          absl::MakeSpan(results));
  }

  absl::StatusOr<LabelExprMap> MergeLabelMaps(
      ResolvedGraphLabelNaryExpr::GraphLogicalOpType op,
      absl::Span<LabelExprMap> sub_maps) const {
    ZETASQL_RET_CHECK(!sub_maps.empty());
    IdStringHashMapCase<
        std::vector<std::unique_ptr<const ResolvedGraphLabelExpr>>>
        label_expr_by_names;
    for (LabelExprMap& sub_map : sub_maps) {
      for (auto& [name, label_expr] : sub_map) {
        label_expr_by_names[name].push_back(std::move(label_expr));
      }
    }

    LabelExprMap map;
    for (auto& [name, label_exprs] : label_expr_by_names) {
      ZETASQL_RET_CHECK(!label_exprs.empty());
      if (label_exprs.size() == 1) {
        map[name] = std::move(label_exprs.front());
      } else {
        ZETASQL_ASSIGN_OR_RETURN(map[name],
                         ResolvedGraphLabelNaryExprBuilder()
                             .set_op(op)
                             .set_operand_list(std::move(label_exprs))
                             .Build());
      }
    }
    return map;
  }

 private:
  const std::function<
      absl::StatusOr<std::unique_ptr<const ResolvedGraphLabelExpr>>(
          const ASTGraphLabelFilter*, GraphElementTable::Kind)>
      label_expr_resolver_;
};

// Collect multiply declared names of columns in `name_list` in reverse order.
void CollectReversedMultiplyDeclaredNames(
    const NameListPtr& name_list,
    std::vector<IdString>& multiply_declared_variables,
    IdStringHashMapCase<ResolvedColumnList>& variable_declarations,
    std::vector<const NamedColumn*>& reversed_name_list) {
  for (auto it = name_list->columns().rbegin();
       it != name_list->columns().rend(); ++it) {
    const NamedColumn& named_column = *it;
    const auto& [iter, inserted] = variable_declarations.emplace(
        named_column.name(), ResolvedColumnList{named_column.column()});
    if (inserted) {
      reversed_name_list.push_back(&named_column);
    } else {
      if (iter->second.size() == 1) {
        multiply_declared_variables.push_back(named_column.name());
      }
      iter->second.push_back(named_column.column());
    }
  }
}

// Ensure that the last GQL return operator does not return any columns
// whose type is or contains a graph element.
absl::Status CheckReturnNoGraphTypedCols(
    const ResolvedColumnList& graph_table_columns,
    const ASTGqlOperator* return_op) {
  ZETASQL_RET_CHECK(return_op->Is<ASTGqlReturn>());
  const ASTSelectList* return_item_list =
      return_op->GetAsOrDie<ASTGqlReturn>()->select()->select_list();
  ZETASQL_RET_CHECK(return_item_list != nullptr);
  absl::Span<const ASTSelectColumn* const> select_items =
      return_item_list->columns();
  ZETASQL_RET_CHECK_GE(select_items.size(), 1);

  int star_idx = -1;
  int num_stars = 0;
  for (int i = 0; i < select_items.size(); ++i) {
    const ASTSelectColumn* column = select_items[i];
    if (column->expression()->node_kind() == AST_STAR) {
      num_stars++;
      star_idx = i;
    }
  }
  ZETASQL_RET_CHECK_LE(num_stars, 1) << "Should have been caught during resolution";

  int offset =
      static_cast<int>(graph_table_columns.size() - select_items.size());
  auto select_idx_lambda = [star_idx, offset](int i) {
    if (i < star_idx) {
      return i;
    } else if (i > star_idx + offset) {
      return i - offset;
    } else {
      return star_idx;
    }
  };
  for (int i = 0; i < graph_table_columns.size(); ++i) {
    if (TypeIsOrContainsGraphElement(graph_table_columns[i].type())) {
      int select_idx = num_stars == 0 ? i : select_idx_lambda(i);
      ZETASQL_RET_CHECK_GE(select_idx, 0);
      ZETASQL_RET_CHECK_LT(select_idx, select_items.size());
      return MakeSqlErrorAt(select_items[select_idx])
             << "Returning graph-typed column is not supported";
    }
  }
  return absl::OkStatus();
}

absl::Status CheckReturnStarIsStandalone(const ASTSelectList* select_list) {
  if (select_list->columns().size() == 1 &&
      select_list->columns().front()->expression()->node_kind() == AST_STAR) {
    return absl::OkStatus();
  } else {
    return MakeSqlErrorAt(select_list)
           << "* cannot be combined with other return items";
  }
}

// Creates a name scope that returns error message built by `error_msg_fn`
// when names in `disallowed_name_lists` are referenced.
template <typename ErrorMessageFn>
absl::StatusOr<std::unique_ptr<NameScope>> CreateNameScopeWithDisallowList(
    const NameScope* external_scope,
    absl::Span<const NameList* const> disallowed_name_lists,
    const ErrorMessageFn& error_msg_fn) {
  IdStringHashMapCase<NameTarget> error_name_targets;
  for (const auto& disallowed_name_list : disallowed_name_lists) {
    for (const NamedColumn& column : disallowed_name_list->columns()) {
      NameTarget name_target(column.column(), column.is_explicit());
      name_target.SetAccessError(NameTarget::EXPLICIT_COLUMN,
                                 error_msg_fn(column.name()));
      zetasql_base::InsertIfNotPresent(&error_name_targets, column.name(), name_target);
    }
  }
  NameScope empty_scope(external_scope);
  std::unique_ptr<NameScope> scope;
  ZETASQL_RETURN_IF_ERROR(empty_scope.CopyNameScopeWithOverridingNameTargets(
      error_name_targets, &scope));
  return scope;
}
}  // namespace

// Returns true if the 'ast_node' is a quantified pattern.
bool IsQuantified(const ASTNode* ast_node) {
  const auto* pattern = ast_node->GetAsOrNull<ASTGraphPathBase>();
  return pattern != nullptr && pattern->quantifier() != nullptr;
}

// Validates the AST node of a top level path pattern.
absl::Status ValidateTopLevelPathPattern(
    const ASTGraphPathPattern* ast_path_pattern) {
  // AST search prefix is usually wrapped around an AST parenthesized path node.
  // When occurring on the same AST path node, it means MATCH (ANY ... ) which
  // is not supported.
  if (ast_path_pattern->parenthesized() &&
      ast_path_pattern->search_prefix() != nullptr) {
    return MakeSqlErrorAt(ast_path_pattern)
           << "Search prefix is not allowed inside a parenthesized path "
              "pattern";
  }
  return absl::OkStatus();
}

absl::Status GraphTableQueryResolver::ValidatePathPattern(
    const ASTGraphPattern* ast_graph_pattern, const NameScope* scope) {
  for (const auto* ast_path_pattern : ast_graph_pattern->paths()) {
    ZETASQL_ASSIGN_OR_RETURN(std::vector<const ASTGraphPathBase*> ast_path_bases,
                     CanonicalizePathPattern(*ast_path_pattern));
    ZETASQL_RETURN_IF_ERROR(ValidateTopLevelPathPattern(ast_path_pattern));
    ZETASQL_ASSIGN_OR_RETURN(
        auto path_rule_result,
        ValidatePathPatternInternal(ast_path_pattern, scope, ast_path_bases));
    // Minimum node count must be checked on the entire path.
    if (path_rule_result.min_node_count == 0) {
      return MakeSqlErrorAt(ast_path_pattern)
             << "Minimum node count of path pattern cannot be 0";
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<GraphTableQueryResolver::PathInfo>
GraphTableQueryResolver::ValidatePathPatternInternal(
    const ASTGraphPathPattern* ast_path_pattern, const NameScope* scope,
    const std::vector<const ASTGraphPathBase*>& ast_path_bases) {
  PathInfo path_stats;

  for (const auto* ast_path_base : ast_path_bases) {
    switch (ast_path_base->node_kind()) {
      // Note: All quantified edges are syntactically transformed into a
      // quantified path before we begin resolution, hence, this case covers
      // both quantified-edges and quantified-paths.
      case AST_GRAPH_PATH_PATTERN: {
        ZETASQL_RET_CHECK(ast_path_base->Is<ASTGraphPathPattern>());
        ZETASQL_ASSIGN_OR_RETURN(std::vector<const ASTGraphPathBase*> ast_subpath_bases,
                         CanonicalizePathPattern(
                             *ast_path_base->GetAs<ASTGraphPathPattern>()));
        ZETASQL_ASSIGN_OR_RETURN(auto subpath_rule_info,
                         ValidatePathPatternInternal(
                             ast_path_base->GetAs<ASTGraphPathPattern>(), scope,
                             ast_subpath_bases));

        path_stats.min_node_count += subpath_rule_info.min_node_count;
        path_stats.min_path_length += subpath_rule_info.min_path_length;
        path_stats.child_is_quantified |= subpath_rule_info.child_is_quantified;
        break;
      }
      case AST_GRAPH_NODE_PATTERN:
        // Path length for a single node is 0.
        ++path_stats.min_node_count;
        break;
      case AST_GRAPH_EDGE_PATTERN: {
        // Path length for a single edge is 1.
        ++path_stats.min_path_length;
        break;
      }
      default:
        ZETASQL_RET_CHECK_FAIL() << "Unsupported ASTNode within a path pattern: "
                         << ast_path_base->node_kind();
    }
  }

  // If a quantifier is provided, attempt to fetch its values and validate them.
  // Accordingly, adjust the parent path's node count and path length values.
  // If this is a nested quantifier, throw an error.
  if (path_stats.child_is_quantified && IsQuantified(ast_path_pattern)) {
    return MakeSqlErrorAt(ast_path_pattern)
           << "Nested quantifiers are not allowed";
  } else if (IsQuantified(ast_path_pattern)) {
    // Minimum path length needs to only be checked for path-primaries within a
    // quantified path pattern.
    if (path_stats.min_path_length == 0) {
      return MakeSqlErrorAt(ast_path_pattern)
             << "Minimum path length of a path-primary within a quantified "
                "path pattern cannot be 0";
    }
    ZETASQL_ASSIGN_OR_RETURN(auto resolved_quantifier,
                     ResolveGraphPathPatternQuantifier(
                         scope, ast_path_pattern->quantifier()));
    const auto* lower_bound_expr = resolved_quantifier->lower_bound();
    int64_t lower_bound = 0;
    if (lower_bound_expr->Is<ResolvedLiteral>()) {
      lower_bound =
          lower_bound_expr->GetAs<ResolvedLiteral>()->value().int64_value();
    } else {
      // If the lower bound was given as a parameter, we cannot perform
      // validation on it, so we assume a lower_bound of 1 to make it pass
      // minimum node count validation.
      lower_bound = 1;
    }
    path_stats.min_node_count *= lower_bound;
    path_stats.min_path_length *= lower_bound;
    path_stats.child_is_quantified = true;
  }
  return path_stats;
}

absl::Status GraphTableQueryResolver::ResolveGraphTableQuery(
    const ASTGraphTableQuery* graph_table_query, const NameScope* scope,
    std::unique_ptr<const ResolvedScan>* output,
    NameListPtr* output_name_list) {
  if (!resolver_->language().LanguageFeatureEnabled(FEATURE_V_1_4_SQL_GRAPH)) {
    return MakeSqlErrorAt(graph_table_query) << "Graph query is not supported";
  }
  ZETASQL_RET_CHECK(output != nullptr);
  ZETASQL_RET_CHECK(output_name_list != nullptr);
  ZETASQL_RET_CHECK(graph_table_query != nullptr);
  ZETASQL_RET_CHECK(graph_table_query->graph_op() != nullptr);
  ZETASQL_ASSIGN_OR_RETURN(auto callback, HandleGraphReference(graph_table_query));
  absl::Cleanup cleanup = [&callback]() { callback(); };

  auto graph_named_variables = CreateEmptyGraphNameLists(graph_table_query);
  ResolvedColumnList graph_table_columns;
  std::unique_ptr<const ResolvedGraphScanBase> resolved_graph_scan;
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      graph_table_column_expr_list;
  if (graph_table_query->graph_op()->Is<ASTGqlOperatorList>()) {
    // GQL syntax, which contains chained GQL operators and no
    // graph_table_shape. see (broken link):gql-graph-table for more details.
    const auto* gql_query =
        graph_table_query->graph_op()->GetAsOrDie<ASTGqlOperatorList>();
    if (!resolver_->language().LanguageFeatureEnabled(
            FEATURE_V_1_4_SQL_GRAPH_ADVANCED_QUERY)) {
      return MakeSqlErrorAt(gql_query)
             << "Graph query with GQL extension is not supported";
    }
    if (graph_table_query->graph_reference() == nullptr) {
      ZETASQL_RET_CHECK(!gql_query->operators().empty());
      ZETASQL_RET_CHECK(gql_query->operators(0)->Is<ASTGqlOperatorList>());
      auto* ops_list =
          gql_query->operators(0)->GetAsOrDie<ASTGqlOperatorList>();
      ZETASQL_RET_CHECK(!ops_list->operators().empty());
      if (ops_list->operators(0)->node_kind() != AST_GQL_MATCH) {
        return MakeSqlErrorAt(gql_query)
               << "Match should be the first operator when the graph reference "
                  "is omitted";
      }
    }

    // 'graph_named_variables' contains an empty working name list to indicate
    // the start of the GQL query.
    ZETASQL_ASSIGN_OR_RETURN(auto result,
                     ResolveGqlLinearQueryList(
                         *gql_query, scope, std::move(graph_named_variables)));
    resolved_graph_scan = std::move(result.resolved_node);
    graph_table_columns = resolved_graph_scan->column_list();
    *output_name_list = result.graph_name_lists->singleton_name_list;
  } else {
    ZETASQL_RET_CHECK(graph_table_query->graph_op()->Is<ASTGqlMatch>());
    auto graph_pattern_name_list = std::make_shared<NameList>();
    const ASTGqlMatch* gql_match =
        graph_table_query->graph_op()->GetAsOrDie<ASTGqlMatch>();
    const ASTGraphPattern* graph_pattern = gql_match->graph_pattern();

    ZETASQL_RET_CHECK(graph_pattern != nullptr);
    ZETASQL_RET_CHECK(!gql_match->optional()) << "Parser doesn't accept optional match";

    ZETASQL_ASSIGN_OR_RETURN(auto result, ResolveGraphPattern(
                                      *graph_pattern, scope,
                                      std::move(graph_named_variables),
                                      /*update_multiply_declared_names=*/true));

    // Capture the output nameslists of the match(es).
    graph_named_variables = std::move(result.graph_name_lists);
    // The result of the match(es) will be a ResolvedGraphScan.
    std::unique_ptr<const ResolvedGraphScan> graph_pattern_scan =
        std::move(result.resolved_node);
    // Resolve graph table shape using the combined namescope.
    const NameScope graph_pattern_scope(
        scope, graph_named_variables->singleton_name_list);
    auto graph_table_shape_name_list = std::make_shared<const NameList>();
    if (graph_table_query->graph_table_shape() == nullptr) {
      if (!resolver_->language().LanguageFeatureEnabled(
              FEATURE_V_1_4_SQL_GRAPH_EXPOSE_GRAPH_ELEMENT)) {
        return MakeSqlErrorAt(graph_table_query)
               << "Graph query without COLUMNS clause is not supported";
      }
      // Resolve optional columns, all named graph elements represent the output
      // Columns with internal names are excluded from the output name list and
      // output column list
      auto named_graph_element_name_list = std::make_shared<NameList>();
      for (const NamedColumn& col :
           graph_named_variables->singleton_name_list->columns()) {
        // TODO: Fix IsInternalAlias behavior for quoted
        // identifiers
        if (IsInternalAlias(col.name())) {
          continue;
        }
        ZETASQL_RETURN_IF_ERROR(named_graph_element_name_list->AddColumn(
            col.name(), col.column(), col.is_explicit()));
        graph_table_columns.push_back(col.column());
      }
      *output_name_list = named_graph_element_name_list;
    } else {
      ZETASQL_RETURN_IF_ERROR(ResolveGraphTableShape(
          graph_table_query->graph_table_shape(), &graph_pattern_scope,
          &graph_table_shape_name_list, &graph_table_columns,
          &graph_table_column_expr_list));
      *output_name_list = graph_table_shape_name_list;
    }

    resolved_graph_scan = std::move(graph_pattern_scan);
  }

  if (graph_table_query->alias() != nullptr) {
    // If alias is provided, add a range variable so that this alias can be
    // referred to.
    ZETASQL_ASSIGN_OR_RETURN(*output_name_list,
                     NameList::AddRangeVariableInWrappingNameList(
                         graph_table_query->alias()->GetAsIdString(),
                         graph_table_query, *output_name_list));
  }

  // Columns produced by GRAPH_TABLE is similar to a SELECT list. They count as
  // referenced and cannot be pruned.
  resolver_->RecordColumnAccess(graph_table_columns);
  ZETASQL_ASSIGN_OR_RETURN(
      *output, ResolvedGraphTableScanBuilder()
                   .set_column_list(graph_table_columns)
                   .set_property_graph(graph_)
                   .set_input_scan(std::move(resolved_graph_scan))
                   .set_shape_expr_list(std::move(graph_table_column_expr_list))
                   .Build());

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<const ResolvedExpr>>
GraphTableQueryResolver::MakeGraphPatternFilterExpr(
    const ASTGraphPattern* graph_pattern,
    const GraphTableNamedVariables& input_named_variables,
    std::unique_ptr<const ResolvedExpr> cross_path_multiply_decl_filter_expr,
    std::unique_ptr<const ResolvedExpr> cross_stmt_multiply_decl_filter_expr,
    const NameScope* scope) {
  std::vector<std::unique_ptr<const ResolvedExpr>> filter_expr_conjuncts;
  if (cross_path_multiply_decl_filter_expr != nullptr) {
    filter_expr_conjuncts.push_back(
        std::move(cross_path_multiply_decl_filter_expr));
  }
  if (cross_stmt_multiply_decl_filter_expr != nullptr) {
    filter_expr_conjuncts.push_back(
        std::move(cross_stmt_multiply_decl_filter_expr));
  }
  if (graph_pattern->where_clause() != nullptr) {
    const NameScope where_clause_scope(
        scope, input_named_variables.singleton_name_list);
    auto query_resolution_info =
        std::make_unique<QueryResolutionInfo>(resolver_);
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedExpr> where_expr,
        ResolveWhereClause(graph_pattern->where_clause(), &where_clause_scope,
                           query_resolution_info.get(),
                           /*allow_analytic=*/false));
    filter_expr_conjuncts.push_back(std::move(where_expr));
  }
  std::unique_ptr<const ResolvedExpr> filter_expr;
  if (!filter_expr_conjuncts.empty()) {
    ZETASQL_RETURN_IF_ERROR(resolver_->MakeAndExpr(
        graph_pattern, std::move(filter_expr_conjuncts), &filter_expr));
  }
  return std::move(filter_expr);
}

absl::Status GraphTableQueryResolver::ResolveGqlGraphPatternQuery(
    const ASTGqlGraphPatternQuery* query, const NameScope* scope,
    std::unique_ptr<const ResolvedScan>* output,
    NameListPtr* output_name_list) {
  if (!resolver_->language().LanguageFeatureEnabled(
          FEATURE_V_1_4_SQL_GRAPH_ADVANCED_QUERY)) {
    return MakeSqlErrorAt(query) << "GQL subquery is not supported";
  }
  ZETASQL_RET_CHECK_NE(output, nullptr);
  ZETASQL_RET_CHECK_NE(output_name_list, nullptr);
  ZETASQL_RET_CHECK_NE(query, nullptr);
  ZETASQL_RET_CHECK_NE(query->graph_pattern(), nullptr);
  ZETASQL_ASSIGN_OR_RETURN(auto callback, HandleGraphReference(query));
  absl::Cleanup cleanup = [&callback]() { callback(); };

  ZETASQL_ASSIGN_OR_RETURN(auto result, ResolveGraphPattern(
                                    *query->graph_pattern(), scope,
                                    CreateEmptyGraphNameLists(query),
                                    /*update_multiply_declared_names=*/true));

  ResolvedColumn column(resolver_->AllocateColumnId(), kGraphTableName,
                        resolver_->MakeIdString("literal_true"),
                        types::BoolType());

  ResolvedColumnList result_col_list{column};
  // RecordColumnAccess to avoid being pruned.
  resolver_->RecordColumnAccess(result_col_list);

  auto name_list = std::make_shared<NameList>();
  ZETASQL_RETURN_IF_ERROR(name_list->AddColumn(column.name_id(), column,
                                       /*is_explicit=*/false));
  *output_name_list = std::move(name_list);

  ZETASQL_ASSIGN_OR_RETURN(
      *output,
      ResolvedGraphTableScanBuilder()
          .set_column_list(result_col_list)
          .set_property_graph(graph_)
          .set_input_scan(
              ResolvedGraphLinearScanBuilder()
                  .set_column_list(result_col_list)
                  .add_scan_list(
                      ResolvedGraphLinearScanBuilder()
                          .set_column_list(result_col_list)
                          .add_scan_list(
                              ToBuilder(std::move(result.resolved_node))
                                  .set_input_scan(
                                      ResolvedSingleRowScanBuilder()))
                          .add_scan_list(
                              ResolvedProjectScanBuilder()
                                  .set_column_list(result_col_list)
                                  .add_expr_list(MakeResolvedComputedColumn(
                                      result_col_list.back(),
                                      MakeResolvedLiteral(Value::Bool(true))))
                                  .set_input_scan(BuildGraphRefScan(
                                      std::move(result.graph_name_lists)
                                          ->singleton_name_list)))))
          .Build());
  return absl::OkStatus();
}

absl::Status GraphTableQueryResolver::ResolveGqlLinearOpsQuery(
    const ASTGqlLinearOpsQuery* query, const NameScope* scope,
    std::unique_ptr<const ResolvedScan>* output,
    NameListPtr* output_name_list) {
  if (!resolver_->language().LanguageFeatureEnabled(
          FEATURE_V_1_4_SQL_GRAPH_ADVANCED_QUERY)) {
    return MakeSqlErrorAt(query) << "GQL subquery is not supported";
  }
  ZETASQL_RET_CHECK_NE(output, nullptr);
  ZETASQL_RET_CHECK_NE(output_name_list, nullptr);
  ZETASQL_RET_CHECK_NE(query, nullptr);
  ZETASQL_RET_CHECK_NE(query->linear_ops(), nullptr);
  ZETASQL_ASSIGN_OR_RETURN(auto callback, HandleGraphReference(query));
  absl::Cleanup cleanup = [&callback]() { callback(); };

  ZETASQL_ASSIGN_OR_RETURN(auto input_scan, ResolvedSingleRowScanBuilder().Build());
  ZETASQL_ASSIGN_OR_RETURN(auto resolved_input,
                   ResolveGqlOperatorList(
                       query->linear_ops()->operators(), scope,
                       {.resolved_node = std::move(input_scan),
                        .graph_name_lists = CreateEmptyGraphNameLists(query)}));

  ResolvedColumn column(resolver_->AllocateColumnId(), kGraphTableName,
                        resolver_->MakeIdString("literal_true"),
                        types::BoolType());

  ResolvedColumnList result_col_list{column};
  // RecordColumnAccess to avoid being pruned.
  resolver_->RecordColumnAccess(result_col_list);

  auto name_list = std::make_shared<NameList>();
  ZETASQL_RETURN_IF_ERROR(name_list->AddColumn(column.name_id(), column,
                                       /*is_explicit=*/false));
  *output_name_list = std::move(name_list);

  ZETASQL_ASSIGN_OR_RETURN(
      *output,
      ResolvedGraphTableScanBuilder()
          .set_column_list(result_col_list)
          .set_property_graph(graph_)
          .set_input_scan(
              ResolvedGraphLinearScanBuilder()
                  .set_column_list(result_col_list)
                  .add_scan_list(
                      ToBuilder(std::move(resolved_input.resolved_node))
                          .set_column_list(result_col_list)
                          .add_scan_list(
                              ResolvedProjectScanBuilder()
                                  .set_column_list(result_col_list)
                                  .add_expr_list(MakeResolvedComputedColumn(
                                      result_col_list.back(),
                                      MakeResolvedLiteral(Value::Bool(true))))
                                  .set_input_scan(BuildGraphRefScan(
                                      resolved_input.graph_name_lists
                                          ->singleton_name_list)))))
          .Build());
  return absl::OkStatus();
}

absl::StatusOr<IdString> GraphTableQueryResolver::GetColumnName(
    const ASTSelectColumn* column) const {
  if (column->alias() != nullptr) {
    return column->alias()->GetAsIdString();
  }

  const auto* column_expr = column->expression();
  if (column_expr->node_kind() != AST_PATH_EXPRESSION) {
    return MakeSqlErrorAt(column)
           << "A name must be explicitly defined for this column";
  }
  return column_expr->GetAsOrDie<ASTPathExpression>()
      ->last_name()
      ->GetAsIdString();
}

IdString GraphTableQueryResolver::ComputeElementAlias(
    const ASTIdentifier* ast_id) {
  if (ast_id != nullptr) {
    return ast_id->GetAsIdString();
  }

  // Arbitrary locally unique name.
  return resolver_->MakeIdString(
      absl::StrCat("$element", ++element_var_count_));
}

template <typename T>
absl::StatusOr<ElementTableSet> GetTablesSatisfyingLabelExpr(
    const PropertyGraph& property_graph,
    const ResolvedGraphLabelExpr* label_expr,
    const absl::flat_hash_set<const T*>& tables) {
  static_assert(
      std::is_base_of<GraphElementTable, T>::value,
      "Type parameter must have base class of type GraphElementTable");
  ElementTableSet matching_element_tables;
  for (const GraphElementTable* table : tables) {
    absl::flat_hash_set<const GraphElementLabel*> label_set;
    ZETASQL_RETURN_IF_ERROR(table->GetLabels(label_set));
    ZETASQL_ASSIGN_OR_RETURN(bool label_expr_satisfied,
                     ElementLabelsSatisfy(label_set, label_expr));
    if (label_expr_satisfied) {
      matching_element_tables.insert(table);
    }
  }
  return matching_element_tables;
}

absl::StatusOr<ElementTableSet> GetMatchingElementTables(
    const PropertyGraph& property_graph,
    const ResolvedGraphLabelExpr* label_expr,
    const GraphElementTable::Kind element_kind) {
  ZETASQL_RET_CHECK_NE(label_expr, nullptr);

  // For each element table in the graph, determine whether its set of exposed
  // labels satisfies the given label expression.
  switch (element_kind) {
    case GraphElementTable::Kind::kNode: {
      absl::flat_hash_set<const GraphNodeTable*> node_tables;
      ZETASQL_RETURN_IF_ERROR(property_graph.GetNodeTables(node_tables));
      return GetTablesSatisfyingLabelExpr(property_graph, label_expr,
                                          node_tables);
    }
    case GraphElementTable::Kind::kEdge: {
      absl::flat_hash_set<const GraphEdgeTable*> edge_tables;
      ZETASQL_RETURN_IF_ERROR(property_graph.GetEdgeTables(edge_tables));
      return GetTablesSatisfyingLabelExpr(property_graph, label_expr,
                                          edge_tables);
    }
  }
}

absl::StatusOr<PropertySet> GetPropertySet(
    const ElementTableSet& element_tables) {
  PropertySet output;
  for (const auto* element_table : element_tables) {
    absl::flat_hash_set<const GraphPropertyDefinition*> properties;
    ZETASQL_RETURN_IF_ERROR(element_table->GetPropertyDefinitions(properties));
    for (const auto* prop : properties) {
      output.insert(&prop->GetDeclaration());
    }
  }

  return output;
}

absl::StatusOr<const GraphElementType*>
GraphTableQueryResolver::MakeGraphElementType(
    const GraphElementTable::Kind table_kind, const PropertySet& properties,
    TypeFactory* type_factory) {
  std::vector<GraphElementType::PropertyType> property_types;
  property_types.reserve(properties.size());
  for (const auto* property : properties) {
    property_types.emplace_back(property->Name(), property->Type());
  }

  GraphElementType::ElementKind element_kind =
      table_kind == GraphElementTable::Kind::kNode
          ? GraphElementType::ElementKind::kNode
          : GraphElementType::ElementKind::kEdge;

  const GraphElementType* type = nullptr;
  // TODO: Should use use the full NamePath()?  (Note: NamePath() needs to be
  //       ready in Spanner first!)
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeGraphElementType(
      std::vector<std::string>{graph_->Name()}, element_kind, property_types,
      &type));
  return type;
}

absl::StatusOr<std::unique_ptr<const ResolvedGraphLabelExpr>>
GraphTableQueryResolver::ResolveGraphLabelFilter(
    const ASTGraphLabelFilter* ast_graph_label_filter,
    const GraphElementTable::Kind element_kind) const {
  // If no label filter is specified, we return the label expression % | !%
  // which matches all element tables even those without labels.
  if (ast_graph_label_filter == nullptr) {
    return ResolvedGraphLabelNaryExprBuilder()
        .set_op(ResolvedGraphLabelNaryExprEnums::OR)
        .add_operand_list(ResolvedGraphWildCardLabelBuilder())
        .add_operand_list(
            ResolvedGraphLabelNaryExprBuilder()
                .set_op(ResolvedGraphLabelNaryExprEnums::NOT)
                .add_operand_list(ResolvedGraphWildCardLabelBuilder()))
        .Build();
  }

  // Construct a set of valid labels. A valid label contained within a label
  // expression belonging to a node or edge pattern must be exposed by
  // some node table or edge table, respectively.
  absl::flat_hash_set<const GraphElementLabel*> valid_labels;
  ZETASQL_RETURN_IF_ERROR(FindAllLabelsApplicableToElementKind(*graph_, element_kind,
                                                       valid_labels));
  return ResolveGraphLabelExpr(ast_graph_label_filter->label_expression(),
                               element_kind, valid_labels, graph_);
}

absl::StatusOr<std::unique_ptr<const ResolvedExpr>>
GraphTableQueryResolver::ResolveWhereClause(
    const ASTWhereClause* ast_where_clause, const NameScope* input_scope,
    QueryResolutionInfo* query_resolution_info, bool allow_analytic) const {
  static constexpr char kWhereClause[] = "WHERE clause";

  if (ast_where_clause == nullptr) {
    return nullptr;
  }

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> output,
                   ResolveHorizontalAggregateExpr(
                       ast_where_clause->expression(), input_scope,
                       query_resolution_info, allow_analytic, kWhereClause));
  ZETASQL_RETURN_IF_ERROR(resolver_->CoerceExprToBool(ast_where_clause->expression(),
                                              kWhereClause, &output));
  return output;
}

void GraphTableQueryResolver::RecordArrayColumnsForPathMode(
    std::vector<std::unique_ptr<const ResolvedGraphPathScan>>& path_scans) {
  for (auto& top_level_scan : path_scans) {
    for (auto [path_mode, path_mode_scan] :
         CollectPathScansForPathMode(*top_level_scan)) {
      switch (path_mode) {
        case ResolvedGraphPathModeEnums::SIMPLE:
        case ResolvedGraphPathModeEnums::ACYCLIC: {
          PathModeScalarToArrayVarMap scalar_to_array_var_map;
          RecordArrayColumnsForPathModeForSinglePathScan(
              path_mode_scan, /*recursion_level=*/0,
              /*record_nodes=*/true, scalar_to_array_var_map);
          break;
        }
        case ResolvedGraphPathModeEnums::TRAIL: {
          PathModeScalarToArrayVarMap scalar_to_array_var_map;
          RecordArrayColumnsForPathModeForSinglePathScan(
              path_mode_scan, /*recursion_level=*/0,
              /*record_nodes=*/false, scalar_to_array_var_map);
          break;
        }
        default:
          // Nothing needs to be recorded for other path modes.
          break;
      }
    }
  }
}

GraphTableQueryResolver::PathModeScansVector
GraphTableQueryResolver::CollectPathScansForPathMode(
    const ResolvedGraphPathScan& path_scan) {
  PathModeScansVector result;
  auto path_mode = path_scan.path_mode() == nullptr
                       ? ResolvedGraphPathModeEnums::PATH_MODE_UNSPECIFIED
                       : path_scan.path_mode()->path_mode();
  if (path_mode != ResolvedGraphPathModeEnums::PATH_MODE_UNSPECIFIED &&
      path_mode != ResolvedGraphPathModeEnums::WALK) {
    result.push_back({path_mode, &path_scan});
  }
  for (const auto& input_scan : path_scan.input_scan_list()) {
    if (input_scan->Is<ResolvedGraphPathScan>()) {
      auto sub_result = CollectPathScansForPathMode(
          *input_scan->GetAs<ResolvedGraphPathScan>());
      result.insert(result.end(), sub_result.begin(), sub_result.end());
    }
  }
  return result;
}

void GraphTableQueryResolver::RecordArrayColumnsForPathModeForSinglePathScan(
    const ResolvedGraphPathScan* path_scan, const int recursion_level,
    bool record_nodes, PathModeScalarToArrayVarMap& scalar_to_array_var_map) {
  // If `scalar_to_group_var[i]` is set to `j`, then column #i in the current
  // GraphPathScan maps to the j-th group variable in `group_variable_list`.
  for (const auto& group_var : path_scan->group_variable_list()) {
    int column_id = group_var->element().column_id();
    scalar_to_array_var_map[column_id] = {recursion_level, &group_var->array()};
  }
  for (auto& scan : path_scan->input_scan_list()) {
    if (scan->Is<ResolvedGraphPathScan>()) {
      RecordArrayColumnsForPathModeForSinglePathScan(
          scan->GetAs<ResolvedGraphPathScan>(), recursion_level + 1,
          record_nodes, scalar_to_array_var_map);
      // Quantified path variables are scalar variables in the GraphPathScan
      // that defines their column map, therefore we don't check node or edge
      // scans when `recursion_level == 0`.
    } else if ((recursion_level > 0) &&
               ((record_nodes && scan->Is<ResolvedGraphNodeScan>()) ||
                (!record_nodes && scan->Is<ResolvedGraphEdgeScan>()))) {
      int scalar_column_id = scan->column_list(0).column_id();
      auto it = scalar_to_array_var_map.find(scalar_column_id);
      if (it != scalar_to_array_var_map.end()) {
        const auto [var_level, array_var] = it->second;
        if (var_level > 0) {
          resolver_->RecordColumnAccess(*array_var);
        }
      }
    }
  }
}

absl::StatusOr<GraphTableQueryResolver::ResolvedGraphWithNameList<
    const ResolvedGraphElementScan>>
GraphTableQueryResolver::ResolveElementPattern(
    const ASTGraphElementPattern& ast_element_pattern,
    const NameScope* input_scope,
    std::unique_ptr<const GraphTableNamedVariables> input_graph_name_lists) {
  // Note about quantifiers:
  // Syntactically, only paths or edges may have an attached quantifier.
  // All quantified edges are syntactically transformed into a quantified path
  // before we begin resolution, hence, we do not need to handle quantified
  // edges in ResolveElementPattern().
  ZETASQL_RET_CHECK(ast_element_pattern.quantifier() == nullptr);

  // Filler can be null for abbreviated edge pattern, semantically its
  // equivalent as an empty filler.
  const ASTGraphElementPatternFiller empty_filler;
  const ASTGraphElementPatternFiller* filler =
      ast_element_pattern.filler() == nullptr ? &empty_filler
                                              : ast_element_pattern.filler();

  // Graph element variable name must be distinct from the ones in a containing
  // GRAPH_TABLE.
  bool has_explicit_name = filler->variable_name() != nullptr;
  if (has_explicit_name) {
    NameTarget existing_name_target;
    if (input_scope->LookupName(filler->variable_name()->GetAsIdString(),
                                &existing_name_target) &&
        existing_name_target.IsColumn() &&
        existing_name_target.column().type()->IsGraphElement()) {
      return MakeSqlErrorAt(filler)
             << "The name "
             << ToSingleQuotedStringLiteral(
                    filler->variable_name()->GetAsStringView())
             << " is already defined; redefining graph element variables in a "
                "subquery is not allowed. To refer to the same "
                "graph element, use a different name and add an explicit "
                "filter that checks for equality";
    }
  }

  const IdString element_alias = ComputeElementAlias(filler->variable_name());
  const GraphElementTable::Kind table_kind =
      GetElementTableKind(ast_element_pattern);

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedGraphLabelExpr> label_expr,
      ResolveGraphElementLabelExpr(filler->variable_name(), table_kind,
                                   filler->label_filter()));

  ZETASQL_ASSIGN_OR_RETURN(
      const ElementTableSet matching_element_tables,
      GetMatchingElementTables(*graph_, label_expr.get(), table_kind));
  ZETASQL_ASSIGN_OR_RETURN(const PropertySet properties,
                   GetPropertySet(matching_element_tables));
  ZETASQL_ASSIGN_OR_RETURN(
      const Type* graph_element_type,
      MakeGraphElementType(table_kind, properties, resolver_->type_factory_));

  ResolvedColumn element_column(resolver_->AllocateColumnId(),
                                /*table_name=*/kElementTableName, element_alias,
                                graph_element_type);
  // Mark element_column as accessed so analyzer does not consider it unused and
  // prune it from the scan. In some cases, element_column may not be used
  // in the resolved graph_table query e.g.:
  //   SELECT 1 FROM KeyValue kv WHERE EXISTS (
  //     SELECT Key FROM GRAPH_TABLE ( aml MATCH (n) COLUMNS (kv.Key))
  //   )
  // However, element_column carries Type information necessary to execute the
  // query.
  resolver_->RecordColumnAccess(element_column);

  auto element_name_list = std::make_shared<NameList>();
  ZETASQL_RETURN_IF_ERROR(element_name_list->AddColumn(element_alias, element_column,
                                               /*is_explicit=*/true));

  // resolve WHERE clause using input_scope and the names produced by this scan.
  const NameScope element_pattern_scope(input_scope, element_name_list);
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedExpr> filter_expr,
      ResolveWhereClause(filler->where_clause(), &element_pattern_scope,
                         /*query_resolution_info=*/nullptr,
                         /*allow_analytic=*/false));

  // Resolve property specification and populate filter_expr if needed. Note
  // an element should have at most one where clause or property specification.
  // Where clause and property specification cannot co-exist for the same
  // element.
  if (filler->property_specification() != nullptr) {
    // Property specification and Where clause cannot co-exist.
    ZETASQL_RET_CHECK(filler->where_clause() == nullptr);
    std::vector<ResolvedColumn> resolved_columns =
        element_name_list->GetResolvedColumns();
    // <element pattern> should have one column in this name list.
    ZETASQL_RET_CHECK(resolved_columns.size() == 1);
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedExpr> property_specification_expr,
        ResolveGraphElementPropertySpecification(
            filler->property_specification(), &element_pattern_scope,
            resolved_columns.at(0)));
    filter_expr = std::move(property_specification_expr);
  }

  // Resolve element hints if they're present.
  std::vector<std::unique_ptr<const ResolvedOption>> element_hints;
  if (filler->hint() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        resolver_->ResolveHintAndAppend(filler->hint(), &element_hints));
  }

  std::vector<const GraphElementTable*> element_table_list = {
      matching_element_tables.begin(), matching_element_tables.end()};
  std::unique_ptr<const ResolvedGraphElementScan> output_scan;
  switch (ast_element_pattern.node_kind()) {
    case AST_GRAPH_NODE_PATTERN: {
      ZETASQL_ASSIGN_OR_RETURN(output_scan, ResolvedGraphNodeScanBuilder()
                                        .set_column_list({element_column})
                                        .set_filter_expr(std::move(filter_expr))
                                        .set_label_expr(std::move(label_expr))
                                        .set_target_element_table_list(
                                            std::move(element_table_list))
                                        .set_hint_list(std::move(element_hints))
                                        .Build());
      break;
    }
    case AST_GRAPH_EDGE_PATTERN: {
      const auto* ast_edge_pattern =
          ast_element_pattern.GetAsOrDie<ASTGraphEdgePattern>();
      std::vector<std::unique_ptr<const ResolvedOption>> lhs_hints;
      std::vector<std::unique_ptr<const ResolvedOption>> rhs_hints;
      // Resolve a path hint on a traversal from a node to an edge if present.
      if (ast_edge_pattern->lhs_hint() != nullptr) {
        ZETASQL_RETURN_IF_ERROR(resolver_->ResolveHintAndAppend(
            ast_edge_pattern->lhs_hint()->hint(), &lhs_hints));
      }
      // Subsequently, resolve a path hint on a traversal from an edge to a node
      // if present.
      if (ast_edge_pattern->rhs_hint() != nullptr) {
        ZETASQL_RETURN_IF_ERROR(resolver_->ResolveHintAndAppend(
            ast_edge_pattern->rhs_hint()->hint(), &rhs_hints));
      }
      ZETASQL_ASSIGN_OR_RETURN(
          output_scan,
          ResolvedGraphEdgeScanBuilder()
              .set_column_list({element_column})
              .set_orientation(
                  GetResolvedEdgeOrientation(ast_edge_pattern->orientation()))
              .set_filter_expr(std::move(filter_expr))
              .set_label_expr(std::move(label_expr))
              .set_target_element_table_list(std::move(element_table_list))
              .set_hint_list(std::move(element_hints))
              .set_lhs_hint_list(std::move(lhs_hints))
              .set_rhs_hint_list(std::move(rhs_hints))
              .Build());
      break;
    }
    default:
      return MakeSqlErrorAt(&ast_element_pattern)
             << "Unexpected node: " << ast_element_pattern.DebugString();
  }
  // Merge the new element's name with the current working graph name lists.
  ZETASQL_ASSIGN_OR_RETURN(auto working_name_lists,
                   CreateGraphNameListsSingletonOnly(&ast_element_pattern,
                                                     element_name_list));
  ZETASQL_ASSIGN_OR_RETURN(auto output_graph_name_lists,
                   MergeGraphNameLists(std::move(input_graph_name_lists),
                                       std::move(working_name_lists)));
  return absl::StatusOr<
      ResolvedGraphWithNameList<const ResolvedGraphElementScan>>(
      {.resolved_node = std::move(output_scan),
       .graph_name_lists = std::move(output_graph_name_lists)});
}

absl::StatusOr<GraphTableQueryResolver::PathPatternListResult>
GraphTableQueryResolver::ResolvePathPatternList(
    const ASTGraphPattern& ast_graph_pattern, const NameScope* input_scope) {
  ZETASQL_RETURN_IF_ERROR(ValidatePathPattern(&ast_graph_pattern, input_scope));
  // Resolves the label expressions in the graph pattern scope.
  ZETASQL_RETURN_IF_ERROR(ResolveGraphLabelExpressions(&ast_graph_pattern));
  PathPatternListResult result;
  result.resolved_scan_vector.reserve(ast_graph_pattern.paths().size());

  auto child_name_lists = CreateEmptyGraphNameLists(&ast_graph_pattern);
  ResolvedColumnList output_col_list;
  // Singleton variable names local to each path that cannot be
  // multiply-declared, mapping to their first declaring path. Group vars are
  // rejected separately in `MakeEqualElementExpr` to get a better error
  // message.
  IdStringHashMapCase<const ASTGraphPathPattern* const>
      multiply_decl_forbidden_names_to_err_location;
  for (const auto& path : ast_graph_pattern.paths()) {
    ZETASQL_ASSIGN_OR_RETURN(
        auto path_result,
        ResolvePathPattern(*path, input_scope, std::move(child_name_lists),
                           /*create_path_column=*/false,
                           &multiply_decl_forbidden_names_to_err_location));

    output_col_list.insert(output_col_list.end(),
                           path_result.resolved_node->column_list().begin(),
                           path_result.resolved_node->column_list().end());
    result.resolved_scan_vector.push_back(std::move(path_result.resolved_node));
    child_name_lists = std::move(path_result.graph_name_lists);
  }

  // Record the column names of array variables of group variables that will be
  // used by the path mode implementation.
  RecordArrayColumnsForPathMode(result.resolved_scan_vector);

  ZETASQL_ASSIGN_OR_RETURN(
      ResolvedGraphWithNameList<const ResolvedExpr> multiply_decl_result,
      ResolveMultiplyDeclaredVariables(
          ast_graph_pattern, *child_name_lists,
          &multiply_decl_forbidden_names_to_err_location));
  result.resolved_column_list = std::move(output_col_list);
  result.filter_expr = std::move(multiply_decl_result.resolved_node);
  result.graph_name_lists = std::move(multiply_decl_result.graph_name_lists);
  return result;
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedGraphScan>>
GraphTableQueryResolver::ResolveGraphPattern(
    const ASTGraphPattern& ast_graph_pattern, const NameScope* input_scope,
    std::unique_ptr<const GraphTableNamedVariables> input_graph_name_lists,
    bool update_multiply_declared_names) {
  // Names imported from the previous statement cannot be accessed
  // from within the pattern. Names can be accessed in the WHERE clause of
  // MATCH.
  ZETASQL_ASSIGN_OR_RETURN(
      auto restricted_scope,
      CreateNameScopeWithDisallowList(
          input_scope,
          {input_graph_name_lists->singleton_name_list.get(),
           input_graph_name_lists->group_name_list.get()},
          [](const IdString& name) {
            return absl::StrCat(
                "Name ", ToSingleQuotedStringLiteral(name.ToStringView()),
                ", defined in the previous statement, can only "
                "be referenced in the outermost WHERE clause of MATCH");
          }));

  ZETASQL_ASSIGN_OR_RETURN(
      auto path_list_result,
      ResolvePathPatternList(ast_graph_pattern, restricted_scope.get()));
  ResolvedColumnList& graph_pattern_scan_column_list =
      path_list_result.resolved_column_list;

  // Resolve cross-statement multiply-declared variables and WHERE clause,
  // preferring the variable occurrence in the graph pattern.
  ZETASQL_ASSIGN_OR_RETURN(auto multiply_decl_result,
                   ResolveMultiplyDeclaredVariables(
                       ast_graph_pattern, *input_graph_name_lists,
                       *path_list_result.graph_name_lists));

  auto& [cross_stmt_multiply_decl_filter_expr, path_list_name_lists] =
      multiply_decl_result;
  ZETASQL_ASSIGN_OR_RETURN(
      auto filter_expr,
      MakeGraphPatternFilterExpr(
          &ast_graph_pattern, *path_list_name_lists.get(),
          std::move(path_list_result.filter_expr),
          std::move(cross_stmt_multiply_decl_filter_expr), input_scope));

  std::vector<std::unique_ptr<const ResolvedGraphPathScan>> input_scan_list =
      std::move(path_list_result.resolved_scan_vector);
  ZETASQL_ASSIGN_OR_RETURN(auto output_scan,
                   ResolvedGraphScanBuilder()
                       .set_column_list(graph_pattern_scan_column_list)
                       .set_input_scan_list(std::move(input_scan_list))
                       .set_filter_expr(std::move(filter_expr))
                       .Build());

  // If we have an OPTIONAL MATCH the new working set is the existing
  // 'input_graph_name_lists' and any new variables in 'path_list_name_lists',
  // keeping the old declaration of multiply-declared names.
  auto final_singleton_list =
      std::move(path_list_name_lists->singleton_name_list);
  if (!update_multiply_declared_names) {
    auto optional_name_list = std::make_shared<NameList>();
    IdStringHashSetCase working_name_columns;
    for (const auto& named_column :
         input_graph_name_lists->singleton_name_list->columns()) {
      ZETASQL_RET_CHECK(working_name_columns.insert(named_column.name()).second);
      ZETASQL_RETURN_IF_ERROR(optional_name_list->AddColumn(named_column.name(),
                                                    named_column.column(),
                                                    /*is_explicit=*/true));
    }

    for (const auto& named_column : final_singleton_list->columns()) {
      if (!working_name_columns.contains(named_column.name())) {
        ZETASQL_RETURN_IF_ERROR(optional_name_list->AddColumn(named_column.name(),
                                                      named_column.column(),
                                                      /*is_explicit=*/true));
      }
    }
    final_singleton_list = std::move(optional_name_list);
  }

  ZETASQL_ASSIGN_OR_RETURN(
      auto output_graph_name_lists,
      CreateGraphNameLists(path_list_name_lists->ast_node,
                           std::move(final_singleton_list),
                           std::move(path_list_name_lists->group_name_list)));

  return {{.resolved_node = std::move(output_scan),
           .graph_name_lists = std::move(output_graph_name_lists)}};
}

absl::StatusOr<std::unique_ptr<const ResolvedGraphPathPatternQuantifier>>
GraphTableQueryResolver::ResolveGraphPathPatternQuantifier(
    const NameScope* name_scope, const ASTQuantifier* ast_quantifier) {
  // Graph quantifiers are never reluctant. The syntax doesn't allow it.
  // It also does not allow symbol quantifiers such as `+` or `*`.
  ZETASQL_RET_CHECK(!ast_quantifier->is_reluctant())
      << "Graph quantifiers are never reluctant.";

  std::unique_ptr<const ResolvedExpr> lower_bound_expr;
  std::unique_ptr<const ResolvedExpr> upper_bound_expr;

  const ASTExpression* ast_lower_quantifier = nullptr;
  const ASTExpression* ast_upper_quantifier = nullptr;
  switch (ast_quantifier->node_kind()) {
    case AST_FIXED_QUANTIFIER: {
      ast_lower_quantifier =
          ast_quantifier->GetAsOrDie<ASTFixedQuantifier>()->bound();
      ast_upper_quantifier = ast_lower_quantifier;
      ZETASQL_RETURN_IF_ERROR(GetQuantifierBoundExpr(ast_lower_quantifier, name_scope,
                                             &lower_bound_expr));

      // The upper bound is identical to the lower one for fixed quantifiers.
      ZETASQL_ASSIGN_OR_RETURN(upper_bound_expr, ResolvedASTDeepCopyVisitor::Copy(
                                             lower_bound_expr.get()));
      break;
    }
    case AST_BOUNDED_QUANTIFIER: {
      // Fetch the mandatory upper bound.
      const auto* bounded_quantifier =
          ast_quantifier->GetAsOrDie<ASTBoundedQuantifier>();

      ast_lower_quantifier = bounded_quantifier->lower_bound()->bound();
      ast_upper_quantifier = bounded_quantifier->upper_bound()->bound();

      ZETASQL_RET_CHECK(ast_upper_quantifier != nullptr);
      ZETASQL_RETURN_IF_ERROR(GetQuantifierBoundExpr(ast_upper_quantifier, name_scope,
                                             &upper_bound_expr));

      // The lower bound is optional. Fetch it if it's present.
      if (ast_lower_quantifier != nullptr) {
        ZETASQL_RETURN_IF_ERROR(GetQuantifierBoundExpr(ast_lower_quantifier, name_scope,
                                               &lower_bound_expr));
      } else {
        // If lower bound is omitted, consider it as an INT64 literal with the
        // value '0'.
        lower_bound_expr = zetasql::MakeResolvedLiteral(
            resolver_->type_factory_->get_int64(), Value::Int64(0));
      }
      break;
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unexpected quantifier type: "
                       << ast_quantifier->DebugString();
  }

  // Validate the lower and upper bounds if they are given as literals.
  // If the bounds are given as parameters, we cannot validate them at query
  // compile time. We simply confirm that they're a constant and expect
  // parameters to be validated at runtime.
  int64_t lower_bound, upper_bound;
  if (lower_bound_expr->Is<ResolvedLiteral>()) {
    lower_bound =
        lower_bound_expr->GetAs<ResolvedLiteral>()->value().int64_value();
    if (lower_bound < 0) {
      return MakeSqlErrorAt(ast_lower_quantifier)
             << "Value of lower bound must be an unsigned integer";
    }
  } else {
    ZETASQL_ASSIGN_OR_RETURN(bool is_lower_constant_expr,
                     IsConstantExpression(lower_bound_expr.get()));
    if (!is_lower_constant_expr) {
      return MakeSqlErrorAt(ast_lower_quantifier)
             << "lower bound expression must be constant";
    }
  }
  if (upper_bound_expr->Is<ResolvedLiteral>()) {
    upper_bound =
        upper_bound_expr->GetAs<ResolvedLiteral>()->value().int64_value();
    if (upper_bound <= 0) {
      return MakeSqlErrorAt(ast_upper_quantifier)
             << "Value of upper bound must be greater than zero";
    }
  } else {
    ZETASQL_ASSIGN_OR_RETURN(bool is_upper_constant_expr,
                     IsConstantExpression(upper_bound_expr.get()));
    if (!is_upper_constant_expr) {
      return MakeSqlErrorAt(ast_upper_quantifier)
             << "upper bound expression must be constant";
    }
  }
  if (lower_bound_expr->Is<ResolvedLiteral>() &&
      upper_bound_expr->Is<ResolvedLiteral>()) {
    if (lower_bound > upper_bound) {
      return MakeSqlErrorAt(ast_lower_quantifier)
             << "Invalid ast_lower_quantifier: lower bound cannot be greater "
                "than upper bound: "
             << lower_bound << " > " << upper_bound;
    }
  }
  return MakeResolvedGraphPathPatternQuantifier(std::move(lower_bound_expr),
                                                std::move(upper_bound_expr));
}

absl::StatusOr<std::unique_ptr<const ResolvedGraphPathMode>>
GraphTableQueryResolver::ResolvePathMode(
    const ASTGraphPathMode* ast_path_mode) {
  std::unique_ptr<const ResolvedGraphPathMode> resolved_path_mode;
  ResolvedGraphPathMode::PathMode path_mode =
      GetResolvedPathMode(ast_path_mode);
  if (path_mode != ResolvedGraphPathMode::PATH_MODE_UNSPECIFIED) {
    if (!resolver_->language().LanguageFeatureEnabled(
            FEATURE_V_1_4_SQL_GRAPH_PATH_MODE)) {
      return MakeSqlErrorAt(ast_path_mode) << "path mode is not supported";
    }
    ZETASQL_ASSIGN_OR_RETURN(
        resolved_path_mode,
        ResolvedGraphPathModeBuilder().set_path_mode(path_mode).Build());
  }
  return resolved_path_mode;
}

absl::StatusOr<std::unique_ptr<const ResolvedGraphPathSearchPrefix>>
ResolvePathSearchPrefix(
    const ASTGraphPathSearchPrefix* ast_path_search_prefix) {
  if (ast_path_search_prefix == nullptr) {
    return nullptr;
  }
  switch (ast_path_search_prefix->type()) {
    case ASTGraphPathSearchPrefix::ANY:
      return ResolvedGraphPathSearchPrefixBuilder()
          .set_type(ResolvedGraphPathSearchPrefix::ANY)
          .Build();
    case ASTGraphPathSearchPrefix::SHORTEST:
      return ResolvedGraphPathSearchPrefixBuilder()
          .set_type(ResolvedGraphPathSearchPrefix::SHORTEST)
          .Build();
    case ASTGraphPathSearchPrefix::ALL:
      // Drop "ALL" because it's a no-op for now.
      return nullptr;
    case ASTGraphPathSearchPrefix::ALL_SHORTEST:
      // Unimplemented for now.
      return MakeSqlErrorAt(ast_path_search_prefix)
             << "ALL SHORTEST search is unimplemented";
    case ASTGraphPathSearchPrefix::PATH_SEARCH_PREFIX_TYPE_UNSPECIFIED:
      ZETASQL_RET_CHECK_FAIL()
          << "The parser should never generate a non-null "
             "ASTGraphPathSearchPrefix node with the prefix UNSPECIFIED";
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unexpected search prefix type: "
                       << ast_path_search_prefix->type();
  }
}

absl::StatusOr<ResolvedColumn> GraphTableQueryResolver::GeneratePathColumn(
    const ASTIdentifier* ast_path_name,
    const ResolvedColumnList& path_scan_column_list,
    const GraphTableNamedVariables* multiply_decl_namelists) {
  InputArgumentTypeSet node_types;
  InputArgumentTypeSet edge_types;
  InputArgumentTypeSet path_types;
  for (const ResolvedColumn& column : path_scan_column_list) {
    const Type* type = column.type();
    if (type->IsArray()) {
      type = type->AsArray()->element_type();
    }
    if (type->IsGraphPath()) {
      path_types.Insert(InputArgumentType(type));
    } else {
      ZETASQL_RET_CHECK(type->IsGraphElement());
      if (type->AsGraphElement()->IsNode()) {
        node_types.Insert(InputArgumentType(type));
      } else {
        edge_types.Insert(InputArgumentType(type));
      }
    }
  }
  const Type* node_type = nullptr;
  ZETASQL_RETURN_IF_ERROR(
      resolver_->coercer_.GetCommonSuperType(node_types, &node_type));
  ZETASQL_RET_CHECK_NE(node_type, nullptr)
      << "Unable to find common node supertype from "
      << node_types.ToString(/*verbose=*/true);
  ZETASQL_RET_CHECK(node_type->IsGraphElement());
  const GraphElementType* edge_type = nullptr;
  if (edge_types.empty()) {
    // There are no edges as part of this path so we need to create an edge
    // type with no properties.
    ZETASQL_RETURN_IF_ERROR(resolver_->type_factory_->MakeGraphElementType(
        node_type->AsGraphElement()->graph_reference(),
        GraphElementType::ElementKind::kEdge,
        /*property_types=*/{}, &edge_type));
  } else {
    const Type* edge_super_type;
    ZETASQL_RETURN_IF_ERROR(
        resolver_->coercer_.GetCommonSuperType(edge_types, &edge_super_type));
    ZETASQL_RET_CHECK(edge_super_type != nullptr)
        << "Unable to find common edge supertype from "
        << edge_types.ToString(/*verbose=*/true);
    ZETASQL_RET_CHECK(edge_super_type->IsGraphElement());
    edge_type = edge_super_type->AsGraphElement();
  }
  const GraphPathType* path_type;
  ZETASQL_RETURN_IF_ERROR(resolver_->type_factory_->MakeGraphPathType(
      node_type->AsGraphElement(), edge_type, &path_type));
  path_types.Insert(InputArgumentType(path_type));
  const Type* path_super_type = nullptr;
  ZETASQL_RETURN_IF_ERROR(
      resolver_->coercer_.GetCommonSuperType(path_types, &path_super_type));
  ZETASQL_RET_CHECK_NE(path_super_type, nullptr)
      << "Unable to find common path supertype from "
      << path_types.ToString(/*verbose=*/true);
  ZETASQL_RET_CHECK(path_super_type->IsGraphPath());
  IdString path_name = kDefaultPathColumnName;
  if (ast_path_name != nullptr) {
    path_name = ast_path_name->GetAsIdString();
  }
  ResolvedColumn result(resolver_->AllocateColumnId(), kGraphTableName,
                        path_name, path_super_type);
  if (ast_path_name != nullptr) {
    ZETASQL_RETURN_IF_ERROR(multiply_decl_namelists->singleton_name_list->AddColumn(
        result.name_id(), result,
        /*is_explicit=*/true));
  }
  return result;
}

absl::StatusOr<GraphTableQueryResolver::ResolvedGraphWithNameList<
    const ResolvedGraphPathScan>>
GraphTableQueryResolver::ResolvePathPattern(
    const ASTGraphPathPattern& ast_path_pattern, const NameScope* input_scope,
    std::unique_ptr<const GraphTableNamedVariables> input_graph_name_lists,
    bool create_path_column,
    IdStringHashMapCase<const ASTGraphPathPattern* const>*
        out_multiply_decl_forbidden_names_to_err_location) {
  RETURN_ERROR_IF_OUT_OF_STACK_SPACE();
  ZETASQL_ASSIGN_OR_RETURN(std::vector<const ASTGraphPathBase*> ast_path_bases,
                   CanonicalizePathPattern(ast_path_pattern));

  std::vector<std::unique_ptr<const ResolvedGraphPathScanBase>> input_scans;
  input_scans.reserve(ast_path_bases.size());
  ResolvedColumnList path_scan_column_list;
  ZETASQL_RET_CHECK(!ast_path_bases.empty());
  if (ast_path_pattern.path_name() != nullptr) {
    create_path_column = true;
    if (!resolver_->language().LanguageFeatureEnabled(
            FEATURE_V_1_4_SQL_GRAPH_PATH_TYPE)) {
      return MakeSqlErrorAt(ast_path_pattern) << "Paths are not supported";
    }
    if (ast_path_pattern.parenthesized()) {
      return MakeSqlErrorAt(ast_path_pattern.path_name())
             << "Path variables cannot be assigned on subpaths";
    }
  }
  auto path_name_lists = CreateEmptyGraphNameLists(&ast_path_pattern);
  for (const auto* ast_path_base : ast_path_bases) {
    ZETASQL_RET_CHECK_NE(ast_path_base, nullptr);
    std::unique_ptr<const ResolvedGraphPathScanBase> path_base_scan;
    if (const auto ast_element_pattern =
            ast_path_base->GetAsOrNull<ASTGraphElementPattern>();
        ast_element_pattern != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(auto element_result,
                       ResolveElementPattern(*ast_element_pattern, input_scope,
                                             std::move(path_name_lists)));
      path_base_scan = std::move(element_result.resolved_node);
      path_name_lists = std::move(element_result.graph_name_lists);
      ZETASQL_RET_CHECK_EQ(path_base_scan->column_list().size(), 1);
    } else {
      ZETASQL_RET_CHECK(ast_path_base->Is<ASTGraphPathPattern>())
          << "ASTGraphPathBase node is unsupported type "
          << ast_path_base->node_kind();
      const auto* ast_path_pattern =
          ast_path_base->GetAsOrDie<ASTGraphPathPattern>();
      if (ast_path_pattern->search_prefix() != nullptr) {
        return MakeSqlErrorAt(ast_path_pattern->search_prefix())
               << "Path search prefix is not supported in a subpath";
      }
      ZETASQL_ASSIGN_OR_RETURN(
          auto subpath_result,
          ResolvePathPattern(*ast_path_pattern, input_scope,
                             std::move(path_name_lists), create_path_column));
      path_base_scan = std::move(subpath_result.resolved_node);
      path_name_lists = std::move(subpath_result.graph_name_lists);
    }
    for (const ResolvedColumn& column : path_base_scan->column_list()) {
      if (!column.type()->IsGraphPath()) {
        path_scan_column_list.push_back(column);
      }
    }
    input_scans.push_back(std::move(path_base_scan));
  }
  std::vector<std::unique_ptr<const ResolvedExpr>> filter_expr_conjuncts;
  ZETASQL_ASSIGN_OR_RETURN(
      auto multiply_decl_result,
      ResolveMultiplyDeclaredVariables(ast_path_pattern, *path_name_lists));

  auto& [filter_expr, multiply_decl_namelists] = multiply_decl_result;

  if (filter_expr != nullptr) {
    filter_expr_conjuncts.push_back(std::move(filter_expr));
  }

  std::optional<ResolvedColumn> path_column;
  if (create_path_column) {
    ZETASQL_ASSIGN_OR_RETURN(
        path_column,
        GeneratePathColumn(ast_path_pattern.path_name(), path_scan_column_list,
                           multiply_decl_namelists.get()));
  }

  const NameScope path_pattern_scope(
      input_scope, multiply_decl_namelists->singleton_name_list);
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedExpr> where_expr,
      ResolveWhereClause(ast_path_pattern.where_clause(), &path_pattern_scope,
                         /*query_resolution_info=*/nullptr,
                         /*allow_analytic=*/false));
  if (where_expr != nullptr) {
    filter_expr_conjuncts.push_back(std::move(where_expr));
  }

  if (!filter_expr_conjuncts.empty()) {
    ZETASQL_RETURN_IF_ERROR(resolver_->MakeAndExpr(
        &ast_path_pattern, std::move(filter_expr_conjuncts), &filter_expr));
  }

  // Resolve any hints between paths if present.
  std::vector<std::unique_ptr<const ResolvedOption>> path_hints;
  if (ast_path_pattern.hint() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        resolver_->ResolveHintAndAppend(ast_path_pattern.hint(), &path_hints));
  }

  if (!resolver_->language().LanguageFeatureEnabled(
          FEATURE_V_1_4_SQL_GRAPH_BOUNDED_PATH_QUANTIFICATION) &&
      IsQuantified(&ast_path_pattern)) {
    return MakeSqlErrorAt(ast_path_pattern)
           << "Graph query with quantifiers is not supported";
  }
  if (ast_path_pattern.path_mode() != nullptr &&
      ast_path_pattern.search_prefix() != nullptr) {
    return MakeSqlErrorAt(ast_path_pattern)
           << "Path pattern with path mode and search prefix is not allowed";
  }

  // Resolve the search prefix locally before merging namelists.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedGraphPathSearchPrefix>
                       resolved_search_prefix,
                   ResolvePathSearchPrefix(ast_path_pattern.search_prefix()));
  // If there is a prefix, collect local singleton variable names in
  // `multiply_decl_namelists` that cannot be multiply-declared. Skip if the
  // path IsQuantified() since it has no singleton names. Another reason to skip
  // is their group var names is not finalized yet (shown as singleton,
  // non-array here) which causes confusion.
  if (resolved_search_prefix != nullptr && !IsQuantified(&ast_path_pattern)) {
    ZETASQL_RET_CHECK_NE(
        resolved_search_prefix->type(),
        ResolvedGraphPathSearchPrefix::PATH_SEARCH_PREFIX_TYPE_UNSPECIFIED);
    ZETASQL_RET_CHECK(out_multiply_decl_forbidden_names_to_err_location != nullptr);
    const IdString head_name_id = path_scan_column_list.front().name_id();
    const IdString tail_name_id = path_scan_column_list.back().name_id();
    // `head_name_id` and `tail_name_id` are both singleton names (not array
    // names from group vars), either from a child's synthetic head / tail or
    // are originally singletons.
    ZETASQL_RET_CHECK(path_scan_column_list.front().type()->IsGraphElement() &&
              path_scan_column_list.front().type()->AsGraphElement()->IsNode());
    ZETASQL_RET_CHECK(path_scan_column_list.back().type()->IsGraphElement() &&
              path_scan_column_list.back().type()->AsGraphElement()->IsNode());
    for (const NamedColumn& named_column :
         multiply_decl_namelists->singleton_name_list->columns()) {
      if (!named_column.column().type()->IsGraphElement()) {
        // Group var arrays are verified separately in `MakeEqualElementExpr` to
        // get a better error message.
        continue;
      }
      const IdString name_id = named_column.name();
      if (name_id != head_name_id && name_id != tail_name_id) {
        out_multiply_decl_forbidden_names_to_err_location->insert(
            {name_id, &ast_path_pattern});
      }
    }
  }

  std::unique_ptr<const ResolvedGraphPathPatternQuantifier> resolved_quantifier;
  // Merge the name lists of the resolved path along with the current working
  // graph name lists.
  std::vector<std::unique_ptr<const ResolvedGraphMakeArrayVariable>>
      new_group_variables;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const GraphTableNamedVariables> output_graph_name_lists,
      MergeGraphNameLists(std::move(input_graph_name_lists),
                          std::move(multiply_decl_namelists),
                          new_group_variables));
  if (IsQuantified(&ast_path_pattern)) {
    ZETASQL_ASSIGN_OR_RETURN(resolved_quantifier,
                     ResolveGraphPathPatternQuantifier(
                         input_scope, ast_path_pattern.quantifier()));

    // If this is a quantified path, output a synthetic 'head' and 'tail'
    // column along with the group variables accessible along the path.
    // For more context: (broken link):bounded-quantified-path-pattern
    const Type* head_type = input_scans.front()->column_list().front().type();
    const Type* tail_type = nullptr;
    for (auto it = input_scans.back()->column_list().rbegin();
         it != input_scans.back()->column_list().rend(); ++it) {
      if (!it->type()->IsGraphPath()) {
        tail_type = it->type();
        break;
      }
    }
    ZETASQL_RET_CHECK(head_type->IsGraphElement());
    ZETASQL_RET_CHECK(head_type->AsGraphElement()->IsNode());
    ZETASQL_RET_CHECK(tail_type != nullptr);
    ZETASQL_RET_CHECK(tail_type->IsGraphElement());
    ZETASQL_RET_CHECK(tail_type->AsGraphElement()->IsNode());
    ResolvedColumn head_column(resolver_->AllocateColumnId(),
                               /*table_name=*/kPathScanName, kHeadColumnName,
                               head_type);
    ResolvedColumn tail_column(resolver_->AllocateColumnId(),
                               /*table_name=*/kPathScanName, kTailColumnName,
                               tail_type);
    // Clear the original column list for this path.
    path_scan_column_list.clear();
    path_scan_column_list.push_back(head_column);
    // Add the group variables to the column list.
    for (const auto& new_group_variable : new_group_variables) {
      path_scan_column_list.push_back(new_group_variable->array());
    }
    path_scan_column_list.push_back(tail_column);

    // Since head/tail are internal columns, they will not be accessed anywhere
    // within the query. Record column accesses for them here so that the
    // resolver does not prune them out.
    resolver_->RecordColumnAccess({head_column, tail_column});
  }
  ZETASQL_ASSIGN_OR_RETURN(auto resolved_path_mode,
                   ResolvePathMode(ast_path_pattern.path_mode()));
  ZETASQL_RET_CHECK_EQ(create_path_column, path_column.has_value());
  if (path_column.has_value()) {
    resolver_->RecordColumnAccess({*path_column});
    path_scan_column_list.push_back(*path_column);
  }
  ResolvedColumn tail = path_scan_column_list.back();
  for (auto it = path_scan_column_list.rbegin();
       it != path_scan_column_list.rend(); ++it) {
    if (!it->type()->IsGraphPath()) {
      tail = *it;
      break;
    }
  }
  auto builder = ResolvedGraphPathScanBuilder()
                     .set_column_list(path_scan_column_list)
                     .set_input_scan_list(std::move(input_scans))
                     .set_filter_expr(std::move(filter_expr))
                     .set_head(path_scan_column_list.front())
                     .set_tail(std::move(tail))
                     .set_path_hint_list(std::move(path_hints))
                     .set_quantifier(std::move(resolved_quantifier))
                     .set_group_variable_list(std::move(new_group_variables))
                     .set_path_mode(std::move(resolved_path_mode))
                     .set_search_prefix(std::move(resolved_search_prefix));
  if (path_column.has_value()) {
    builder.set_path(ResolvedColumnHolderBuilder().set_column(*path_column));
  }

  ZETASQL_ASSIGN_OR_RETURN(auto output_scan, std::move(builder).Build());
  return {{.resolved_node = std::move(output_scan),
           .graph_name_lists = std::move(output_graph_name_lists)}};
}

std::vector<std::unique_ptr<const ResolvedExpr>>
GraphTableQueryResolver::BuildColumnRefs(const ResolvedColumnList& columns) {
  std::vector<std::unique_ptr<const ResolvedExpr>> column_refs;
  column_refs.reserve(columns.size());
  for (const auto& column : columns) {
    column_refs.push_back(resolver_->MakeColumnRef(column));
  }
  return column_refs;
}

absl::StatusOr<std::unique_ptr<const ResolvedExpr>>
GraphTableQueryResolver::MakeEqualElementExpr(
    const ASTNode& ast_location, const IdString variable,
    std::vector<std::unique_ptr<const ResolvedExpr>> exprs) const {
  ZETASQL_RET_CHECK_GE(exprs.size(), 2);
  if (!absl::c_all_of(exprs,
                      [](const std::unique_ptr<const ResolvedExpr>& expr) {
                        return expr->type()->IsGraphElement();
                      })) {
    return MakeSqlErrorAt(ast_location) << "`" << variable.ToStringView()
                                        << "` is not a graph element column "
                                           "and cannot be redeclared";
  }
  std::vector<std::unique_ptr<const ResolvedExpr>> equalities;
  equalities.reserve(exprs.size() - 1);
  for (int i = 1; i < exprs.size(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> lhs,
                     ResolvedASTDeepCopyVisitor::Copy(exprs[0].get()));
    std::unique_ptr<const ResolvedExpr> equal_expr;
    ZETASQL_RETURN_IF_ERROR(resolver_->MakeEqualityComparison(
        &ast_location, std::move(lhs), std::move(exprs[i]), &equal_expr));
    equalities.push_back(std::move(equal_expr));
  }
  std::unique_ptr<const ResolvedExpr> result;
  ZETASQL_RETURN_IF_ERROR(
      resolver_->MakeAndExpr(&ast_location, std::move(equalities), &result));
  return result;
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedExpr>>
GraphTableQueryResolver::ResolveMultiplyDeclaredVariables(
    const ASTNode& ast_location,
    const GraphTableNamedVariables& input_graph_name_lists,
    const IdStringHashMapCase<const ASTGraphPathPattern* const>*
        forbidden_singleton_names_to_err_location) {
  // Stores multiply_declared_variables names in a vector as test cases are
  // sensitive to order, uniqueness is guaranteed by variable_declarations.
  std::vector<IdString> multiply_declared_variables;
  IdStringHashMapCase<ResolvedColumnList> variable_declarations;
  auto singleton_name_list = std::make_shared<NameList>();

  // Confirm that there are no multiply declared group variables.
  ZETASQL_RET_CHECK_OK(CheckNoAmbiguousNameTarget(
      ast_location, *input_graph_name_lists.group_name_list));

  // Finds multiply-declared singleton variables.
  for (const NamedColumn& named_column :
       input_graph_name_lists.singleton_name_list->columns()) {
    const auto& [iter, inserted] = variable_declarations.emplace(
        named_column.name(), ResolvedColumnList{named_column.column()});
    if (inserted) {
      ZETASQL_RETURN_IF_ERROR(singleton_name_list->AddColumn(named_column.name(),
                                                     named_column.column(),
                                                     /*is_explicit=*/true));
    } else {
      if (iter->second.size() == 1) {
        multiply_declared_variables.push_back(named_column.name());
      }
      iter->second.push_back(named_column.column());
    }
  }
  ZETASQL_ASSIGN_OR_RETURN(
      auto output_graph_name_lists,
      CreateGraphNameLists(&ast_location, singleton_name_list,
                           std::move(input_graph_name_lists.group_name_list)));
  if (multiply_declared_variables.empty()) {
    return {{.resolved_node = nullptr,
             .graph_name_lists = std::move(output_graph_name_lists)}};
  }

  // Builds SAME for each group of multiply-declared variables.
  std::vector<std::unique_ptr<const ResolvedExpr>> same_exprs;
  same_exprs.reserve(multiply_declared_variables.size());
  for (const auto& variable : multiply_declared_variables) {
    const ResolvedColumnList& column_list = variable_declarations[variable];
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> same_expr,
                     MakeEqualElementExpr(ast_location, variable,
                                          BuildColumnRefs(column_list)));
    if (forbidden_singleton_names_to_err_location != nullptr &&
        (forbidden_singleton_names_to_err_location->find(variable) !=
         forbidden_singleton_names_to_err_location->end())) {
      // Throw this error when redeclaring singletons. If one of them is
      // a group var, will throw a better msg "group vars cannot be redeclared"
      // in `MakeEqualElementExpr` above.
      const ASTGraphPathPattern* declaring_location =
          forbidden_singleton_names_to_err_location->at(variable);
      return MakeSqlErrorAt(declaring_location)
             << "Variable `" << variable.ToStringView()
             << "` is inside a path under a selection prefix (`"
             << ASTGraphPathSearchPrefixEnums::PathSearchPrefixType_Name(
                    static_cast<int>(
                        declaring_location->search_prefix()->type()))
             << "`). It cannot be reused in other "
                "paths within the same MATCH statement, unless it is the "
                "endpoint node of such paths. Maybe use "
                "separated MATCHes for each path?";
    }
    same_exprs.push_back(std::move(same_expr));
  }
  std::unique_ptr<const ResolvedExpr> output_expr;
  ZETASQL_RETURN_IF_ERROR(resolver_->MakeAndExpr(&ast_location, std::move(same_exprs),
                                         &output_expr));

  return {{.resolved_node = std::move(output_expr),
           .graph_name_lists = std::move(output_graph_name_lists)}};
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedExpr>>
GraphTableQueryResolver::ResolveMultiplyDeclaredVariables(
    const ASTNode& ast_location, const GraphTableNamedVariables& left_name_list,
    const GraphTableNamedVariables& right_name_list) {
  std::vector<IdString> multiply_declared_variables;
  IdStringHashMapCase<ResolvedColumnList> variable_declarations;
  // For each multiply declared singleton variable in both left and right name
  // lists, we output the one that appeared in the right name list due to label
  // inference rules for GQL multiply declared variables in
  // (broken link):gql-linear-comp. We also want to keep the original relative order
  // of names so we scan the 2 lists in reverse order, and reverse the final
  // results back. Doing this in 2 pass since we cannot do random modifications
  // to a NameList.
  std::vector<const NamedColumn*> reversed_name_list;

  // Confirm that there are no ambiguous group variables.
  ZETASQL_RET_CHECK_OK(CheckNoAmbiguousNameTarget(ast_location,
                                          *left_name_list.group_name_list));
  ZETASQL_RET_CHECK_OK(CheckNoAmbiguousNameTarget(ast_location,
                                          *right_name_list.group_name_list));

  CollectReversedMultiplyDeclaredNames(
      right_name_list.singleton_name_list, multiply_declared_variables,
      variable_declarations, reversed_name_list);
  CollectReversedMultiplyDeclaredNames(
      left_name_list.singleton_name_list, multiply_declared_variables,
      variable_declarations, reversed_name_list);

  auto name_list = std::make_shared<NameList>();
  // Reverse the namelist to get the normal order. The final namelist also
  // prefers the right input list for where the multiply declared variable
  // appeared.
  for (auto it = reversed_name_list.rbegin(); it != reversed_name_list.rend();
       ++it) {
    ZETASQL_RETURN_IF_ERROR(name_list->AddColumn((*it)->name(), (*it)->column(),
                                         /*is_explicit=*/true));
  }

  // Merge the group name lists into one list.
  auto final_group_list = std::make_shared<NameList>();
  for (const NamedColumn& group_variable :
       left_name_list.group_name_list->columns()) {
    if (NameTarget unused_name_target;
        right_name_list.group_name_list->LookupName(group_variable.name(),
                                                    &unused_name_target)) {
      return MakeSqlErrorAt(ast_location)
             << "Group variable " << group_variable.name()
             << " is multiply declared";
    }
    if (NameTarget unused_name_target;
        right_name_list.singleton_name_list->LookupName(group_variable.name(),
                                                        &unused_name_target)) {
      return MakeSqlErrorAt(ast_location)
             << "Variable name: " << group_variable.name()
             << " cannot be used in both quantified and unquantified patterns";
    }
  }
  for (const NamedColumn& group_variable :
       right_name_list.group_name_list->columns()) {
    if (NameTarget unused_name_target;
        left_name_list.singleton_name_list->LookupName(group_variable.name(),
                                                       &unused_name_target)) {
      return MakeSqlErrorAt(ast_location)
             << "Variable name: " << group_variable.name()
             << " cannot be used in both quantified and unquantified patterns";
    }
  }
  ZETASQL_RETURN_IF_ERROR(final_group_list->MergeFrom(*left_name_list.group_name_list,
                                              &ast_location));
  ZETASQL_RETURN_IF_ERROR(final_group_list->MergeFrom(*right_name_list.group_name_list,
                                              &ast_location));
  ZETASQL_RET_CHECK_OK(CheckNoAmbiguousNameTarget(ast_location, *final_group_list));

  ZETASQL_ASSIGN_OR_RETURN(auto output_graph_name_lists,
                   CreateGraphNameLists(&ast_location, name_list,
                                        std::move(final_group_list)));
  if (multiply_declared_variables.empty()) {
    return {{.resolved_node = nullptr,
             .graph_name_lists = std::move(output_graph_name_lists)}};
  }

  // Builds SAME for each group of multiply-declared variables.
  std::vector<std::unique_ptr<const ResolvedExpr>> same_exprs;
  same_exprs.reserve(multiply_declared_variables.size());
  for (const IdString& variable : multiply_declared_variables) {
    const ResolvedColumnList& column_list = variable_declarations[variable];
    std::optional<GraphElementType::ElementKind> kind;
    for (const ResolvedColumn& col : column_list) {
      if (col.type()->IsGraphElement()) {
        if (kind.has_value() &&
            *kind != col.type()->AsGraphElement()->element_kind()) {
          return MakeSqlErrorAt(ast_location)
                 << "Cannot multiply declare an element as both a node and an "
                    "edge pattern: `"
                 << variable.ToStringView() << "`";
        }
        kind = col.type()->AsGraphElement()->element_kind();
      }
    }
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> same_expr,
                     MakeEqualElementExpr(ast_location, variable,
                                          BuildColumnRefs(column_list)));
    same_exprs.push_back(std::move(same_expr));
  }
  ResolvedGraphWithNameList<const ResolvedExpr> result;
  ZETASQL_RETURN_IF_ERROR(resolver_->MakeAndExpr(&ast_location, std::move(same_exprs),
                                         &result.resolved_node));
  result.graph_name_lists = std::move(output_graph_name_lists);
  return result;
}

absl::Status GraphTableQueryResolver::ResolveGraphTableShape(
    const ASTSelectList* ast_select_list, const NameScope* input_scope,
    NameListPtr* output_name_list, ResolvedColumnList* output_column_list,
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>* expr_list)
    const {
  ZETASQL_RET_CHECK(ast_select_list != nullptr);
  ZETASQL_RET_CHECK(input_scope != nullptr);
  ZETASQL_RET_CHECK(output_name_list != nullptr);
  ZETASQL_RET_CHECK(output_column_list != nullptr);
  ZETASQL_RET_CHECK(expr_list != nullptr);

  if (ast_select_list->columns().empty()) {
    return MakeSqlErrorAt(ast_select_list) << "COLUMNS cannot be empty";
  }

  const auto output_size = ast_select_list->columns().size();
  expr_list->reserve(output_size);
  output_column_list->reserve(output_size);

  auto graph_table_shape_name_list = std::make_shared<NameList>();
  for (const ASTSelectColumn* col : ast_select_list->columns()) {
    ZETASQL_RETURN_IF_ERROR(ResolveSelectColumn(col, input_scope,
                                        graph_table_shape_name_list.get(),
                                        output_column_list, expr_list));
  }
  for (int i = 0; i < output_column_list->size(); ++i) {
    if (!resolver_->language().LanguageFeatureEnabled(
            FEATURE_V_1_4_SQL_GRAPH_EXPOSE_GRAPH_ELEMENT) &&
        TypeIsOrContainsGraphElement(output_column_list->at(i).type())) {
      return MakeSqlErrorAt(ast_select_list->columns()[i])
             << "Returning graph-typed column is not supported";
    }
  }

  // Report error if the final shape contains ambiguous column names.
  ZETASQL_RETURN_IF_ERROR(CheckNoAmbiguousNameTarget(*ast_select_list,
                                             *graph_table_shape_name_list));
  *output_name_list = std::move(graph_table_shape_name_list);

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<const ResolvedExpr>>
GraphTableQueryResolver::ResolveGraphElementPropertySpecification(
    const ASTGraphPropertySpecification* ast_graph_property_specification,
    const NameScope* input_scope, const ResolvedColumn& element_column) const {
  if (ast_graph_property_specification == nullptr) {
    return nullptr;
  }

  ZETASQL_RET_CHECK(element_column.type() != nullptr &&
            element_column.type()->IsGraphElement());
  const GraphElementType* element_type =
      element_column.type()->AsGraphElement();

  std::vector<std::unique_ptr<const ResolvedExpr>> eq_exprs;
  eq_exprs.reserve(
      ast_graph_property_specification->property_name_and_value().size());
  for (const auto* property_name_and_value :
       ast_graph_property_specification->property_name_and_value()) {
    absl::string_view property_name =
        property_name_and_value->property_name()->GetAsStringView();

    if (element_type->FindPropertyType(property_name) == nullptr) {
      return MakeSqlErrorAt(property_name_and_value->property_name())
             << "Property " << property_name
             << " is not exposed by element type "
             << element_type->DebugString();
    }

    const GraphPropertyDeclaration* prop_dcl;
    ZETASQL_RETURN_IF_ERROR(
        graph_->FindPropertyDeclarationByName(property_name, prop_dcl));
    ZETASQL_RET_CHECK(prop_dcl != nullptr);
    std::unique_ptr<const ResolvedColumnRef> resolved_column_ref =
        resolver_->MakeColumnRef(element_column);
    ZETASQL_ASSIGN_OR_RETURN(
        auto get_property_expr,
        ResolvedGraphGetElementPropertyBuilder()
            .set_type(prop_dcl->Type())
            .set_expr(resolver_->CopyColumnRef(resolved_column_ref.get()))
            .set_property(prop_dcl)
            .Build());
    static constexpr char kPropertyValueExp[] = "Property value expr";
    ExprResolutionInfo expr_resolution_info(input_scope, kPropertyValueExp);
    std::unique_ptr<const ResolvedExpr> value_expr;
    ZETASQL_RETURN_IF_ERROR(resolver_->ResolveExpr(property_name_and_value->value(),
                                           &expr_resolution_info, &value_expr));
    std::unique_ptr<const ResolvedExpr> resolved_eq_expr;
    ZETASQL_RETURN_IF_ERROR(resolver_->MakeEqualityComparison(
        property_name_and_value, std::move(get_property_expr),
        std::move(value_expr), &resolved_eq_expr));
    eq_exprs.push_back(std::move(resolved_eq_expr));
  }

  std::unique_ptr<const ResolvedExpr> filter_expr;
  if (!eq_exprs.empty()) {
    ZETASQL_RETURN_IF_ERROR(resolver_->MakeAndExpr(ast_graph_property_specification,
                                           std::move(eq_exprs), &filter_expr));
  }
  return filter_expr;
}

absl::Status GraphTableQueryResolver::ResolveSelectColumn(
    const ASTSelectColumn* ast_select_column, const NameScope* input_scope,
    NameList* output_name_list, ResolvedColumnList* output_column_list,
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>* expr_list)
    const {
  static constexpr char kGraphTableShape[] = "graph table shape";

  if (ast_select_column->expression()->node_kind() == AST_DOT_STAR) {
    ExprResolutionInfo expr_resolution_info(input_scope, kGraphTableShape);
    std::unique_ptr<const ResolvedExpr> resolved_column_expr;
    ZETASQL_RETURN_IF_ERROR(resolver_->ResolveExpr(
        ast_select_column->expression()->GetAsOrDie<ASTDotStar>()->expr(),
        &expr_resolution_info, &resolved_column_expr));
    return AddPropertiesFromElement(
        ast_select_column, std::move(resolved_column_expr), output_name_list,
        output_column_list, expr_list);
  }

  auto query_resolution_info = std::make_unique<QueryResolutionInfo>(resolver_);
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> resolved_column_expr,
                   ResolveHorizontalAggregateExpr(
                       ast_select_column->expression(), input_scope,
                       query_resolution_info.get(), /*allow_analytic=*/true,
                       kGraphTableShape));
  ZETASQL_ASSIGN_OR_RETURN(const auto column_name, GetColumnName(ast_select_column));
  ResolvedColumn output_column(resolver_->AllocateColumnId(), kGraphTableName,
                               column_name, resolved_column_expr->type());
  ZETASQL_RETURN_IF_ERROR(output_name_list->AddColumn(column_name, output_column,
                                              /*is_explicit=*/true));
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedComputedColumn> computed_column,
      ResolvedComputedColumnBuilder()
          .set_column(output_column)
          .set_expr(std::move(resolved_column_expr))
          .Build());
  expr_list->push_back(std::move(computed_column));
  output_column_list->push_back(std::move(output_column));

  return absl::OkStatus();
}

absl::Status GraphTableQueryResolver::AddPropertiesFromElement(
    const ASTNode* ast_location,
    std::unique_ptr<const ResolvedExpr> resolved_element, NameList* name_list,
    ResolvedColumnList* column_list,
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>* expr_list)
    const {
  if (!resolved_element->type()->IsGraphElement()) {
    return MakeSqlErrorAt(ast_location)
           << "Expects GraphElement type. Found "
           << resolved_element->type()->DebugString();
  }

  // Add all properties exposed by graph element to output.
  const absl::Span<const PropertyType> property_types =
      resolved_element->type()->AsGraphElement()->property_types();
  for (const PropertyType& property_type : property_types) {
    const auto column_name_id = resolver_->MakeIdString(property_type.name);
    const Type* column_type = property_type.value_type;
    const ResolvedColumn output_column(resolver_->AllocateColumnId(),
                                       kGraphTableName, column_name_id,
                                       column_type);

    ZETASQL_RETURN_IF_ERROR(name_list->AddColumn(column_name_id, output_column,
                                         /*is_explicit=*/false));
    const GraphPropertyDeclaration* prop_dcl;
    ZETASQL_RETURN_IF_ERROR(
        graph_->FindPropertyDeclarationByName(property_type.name, prop_dcl));
    ResolvedASTDeepCopyVisitor deep_copy_visitor;
    ZETASQL_RETURN_IF_ERROR(resolved_element->Accept(&deep_copy_visitor));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> resolved_element_copy,
                     deep_copy_visitor.ConsumeRootNode<ResolvedExpr>());
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedGraphGetElementProperty>
                         get_element_property,
                     ResolvedGraphGetElementPropertyBuilder()
                         .set_type(column_type)
                         .set_expr(std::move(resolved_element_copy))
                         .set_property(prop_dcl)
                         .Build());
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedComputedColumn> computed_column,
        ResolvedComputedColumnBuilder()
            .set_column(output_column)
            .set_expr(std::move(get_element_property))
            .Build());
    expr_list->push_back(std::move(computed_column));
    column_list->push_back(std::move(output_column));
  }

  return absl::OkStatus();
}

absl::Status GraphTableQueryResolver::ResolveGraphReference(
    const ASTPathExpression* graph_ref) {
  const absl::Status find_status =
      catalog_->FindPropertyGraph(graph_ref->ToIdentifierVector(), graph_,
                                  resolver_->analyzer_options().find_options());
  if (find_status.code() == absl::StatusCode::kNotFound) {
    return MakeGraphNotFoundSqlError(graph_ref);
  }
  ZETASQL_RETURN_IF_ERROR(find_status);
  ZETASQL_RET_CHECK(graph_ != nullptr);
  resolver_->RecordPropertyGraphRef(graph_);
  return absl::OkStatus();
}

absl::Status GraphTableQueryResolver::ResolveGraphLabelExpressions(
    const ASTGraphPattern* graph_pattern) {
  ZETASQL_ASSIGN_OR_RETURN(
      label_expr_map_,
      GraphLabelExprResolver::ResolveNamedVariables(
          graph_pattern, [this](const ASTGraphLabelFilter* filter,
                                GraphElementTable::Kind element_kind) {
            return ResolveGraphLabelFilter(filter, element_kind);
          }));
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<const ResolvedGraphLabelExpr>>
GraphTableQueryResolver::ResolveGraphElementLabelExpr(
    const ASTIdentifier* ast_element_var, GraphElementTable::Kind element_kind,
    const ASTGraphLabelFilter* ast_label_filter) {
  // Resolves anonymous variables when needed as they are mutually distinct.
  if (ast_element_var == nullptr) {
    return ResolveGraphLabelFilter(ast_label_filter, element_kind);
  }
  const auto itr = label_expr_map_.find(ast_element_var->GetAsIdString());
  ZETASQL_RET_CHECK(itr != label_expr_map_.end());

  // Copies the resolved label expr as the label expr might be used multiple
  // times.
  return ResolvedASTDeepCopyVisitor::Copy<ResolvedGraphLabelExpr>(
      itr->second.get());
}

absl::Status GraphTableQueryResolver::MakeGraphNotFoundSqlError(
    const ASTPathExpression* graph_ref) const {
  std::string error_message;
  absl::StrAppend(&error_message, "Property graph not found: ",
                  graph_ref->ToIdentifierPathString());
  const std::string graph_suggestion =
      catalog_->SuggestPropertyGraph(graph_ref->ToIdentifierVector());
  if (!graph_suggestion.empty()) {
    absl::StrAppend(&error_message, "; Did you mean ", graph_suggestion, "?");
  }
  return MakeSqlErrorAt(graph_ref) << error_message;
}

absl::Status GraphTableQueryResolver::MaybeAddMissingNodePattern(
    const ASTGraphPathBase* left, const ASTGraphPathBase* right,
    std::vector<const ASTGraphPathBase*>& output_patterns) const {
  ZETASQL_RET_CHECK(left != nullptr);
  ZETASQL_RET_CHECK(right != nullptr);
  // Consecutive edges.
  if (left->Is<ASTGraphEdgePattern>() && right->Is<ASTGraphEdgePattern>()) {
    output_patterns.push_back(&empty_node_pattern_);
    return absl::OkStatus();
  }
  // Do nothing if either left or right is a subpath pattern.
  return absl::OkStatus();
}

absl::StatusOr<std::vector<const ASTGraphPathBase*>>
GraphTableQueryResolver::CanonicalizePathPattern(
    const ASTGraphPathPattern& ast_path_pattern) const {
  absl::Span<const ASTGraphPathBase* const> input_patterns =
      ast_path_pattern.input_pattern_list();
  ZETASQL_RET_CHECK(!input_patterns.empty());
  std::vector<const ASTGraphPathBase*> output;

  // Path head.
  if (input_patterns.front()->Is<ASTGraphEdgePattern>()) {
    output.push_back(&empty_node_pattern_);
  }

  output.push_back(input_patterns.front());
  for (const auto* pattern : input_patterns.subspan(1)) {
    ZETASQL_RETURN_IF_ERROR(MaybeAddMissingNodePattern(output.back(), pattern, output));
    output.push_back(pattern);
  }

  // Path tail.
  if (input_patterns.back()->Is<ASTGraphEdgePattern>()) {
    output.push_back(&empty_node_pattern_);
  }
  return output;
}

absl::Status GraphTableQueryResolver::GetQuantifierBoundExpr(
    const ASTExpression* ast_quantifier_bound, const NameScope* input_scope,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) const {
  static constexpr char kGraphQuantifier[] = "graph quantifier";
  ExprResolutionInfo expr_resolution_info(input_scope, kGraphQuantifier);
  ZETASQL_RETURN_IF_ERROR(resolver_->ResolveExpr(
      ast_quantifier_bound, &expr_resolution_info, resolved_expr_out));
  ZETASQL_RETURN_IF_ERROR(resolver_->ValidateParameterOrLiteralAndCoerceToInt64IfNeeded(
      kGraphQuantifier, ast_quantifier_bound, resolved_expr_out));
  return absl::OkStatus();
}

// Build GraphRefScan from input namelist
absl::StatusOr<std::unique_ptr<const ResolvedScan>>
GraphTableQueryResolver::BuildGraphRefScan(const NameListPtr& input_name_list) {
  ZETASQL_RET_CHECK(input_name_list != nullptr);
  ResolvedGraphRefScanBuilder builder;
  for (auto& named_column : input_name_list->columns()) {
    builder.add_column_list(named_column.column());
  }
  return std::move(builder).Build();
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedScan>>
GraphTableQueryResolver::ResolveGqlOperator(
    const ASTGqlOperator* gql_op, const NameScope* external_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  auto& input_scan = inputs.resolved_node;
  ZETASQL_RET_CHECK(input_scan != nullptr);
  switch (gql_op->node_kind()) {
    case AST_GQL_OPERATOR_LIST: {
      ZETASQL_ASSIGN_OR_RETURN(
          auto op_list_result,
          ResolveGqlLinearQuery(*gql_op->GetAsOrDie<ASTGqlOperatorList>(),
                                external_scope, std::move(inputs)));
      // ResolveGqlLinearQuery() returns a result with ResolvedGraphLinearScan.
      // Upcast it to a ResolvedScan before returning it.
      return {{.resolved_node = std::move(op_list_result.resolved_node),
               .graph_name_lists = std::move(op_list_result.graph_name_lists)}};
    }
    case AST_GQL_MATCH: {
      return ResolveGqlMatch(*gql_op->GetAsOrDie<ASTGqlMatch>(), external_scope,
                             std::move(inputs));
    }
    case AST_GQL_LET: {
      return ResolveGqlLet(*gql_op->GetAsOrDie<ASTGqlLet>(), external_scope,
                           std::move(inputs));
    }
    case AST_GQL_FILTER: {
      return ResolveGqlFilter(*gql_op->GetAsOrDie<ASTGqlFilter>(),
                              external_scope, std::move(inputs));
    }
    case AST_GQL_ORDER_BY_AND_PAGE: {
      return ResolveGqlOrderByAndPage(
          *gql_op->GetAsOrDie<ASTGqlOrderByAndPage>(), external_scope,
          std::move(inputs));
    }
    case AST_GQL_WITH: {
      return ResolveGqlWith(*gql_op->GetAsOrDie<ASTGqlWith>(), external_scope,
                            std::move(inputs));
    }
    case AST_GQL_FOR: {
      return ResolveGqlFor(*gql_op->GetAsOrDie<ASTGqlFor>(), external_scope,
                           std::move(inputs));
    }
    case AST_GQL_RETURN: {
      return ResolveGqlReturn(*gql_op->GetAsOrDie<ASTGqlReturn>(),
                              external_scope, std::move(inputs));
    }
    case AST_GQL_SET_OPERATION: {
      return ResolveGqlSetOperation(*gql_op->GetAsOrDie<ASTGqlSetOperation>(),
                                    external_scope, std::move(inputs));
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unexpected query statement " << gql_op->node_kind();
  }
}

// Returns a set of column names produced by the singleton name list.
static absl::StatusOr<ColumnNameSet> ToColumnNameSet(
    const GraphTableNamedVariables* graph_name_lists) {
  ColumnNameSet column_names;
  for (const NamedColumn& named_column :
       graph_name_lists->singleton_name_list->columns()) {
    ZETASQL_RET_CHECK(column_names.insert(named_column.name()).second);
  }
  return column_names;
}

// Returns a map of column name id to original column index.
static absl::StatusOr<ColumnNameIdxMap> ToColumnNameIdxMap(
    const GraphTableNamedVariables* graph_name_lists) {
  ColumnNameIdxMap column_name_idx_map;
  for (int idx = 0; idx < graph_name_lists->singleton_name_list->num_columns();
       ++idx) {
    ZETASQL_RET_CHECK(
        column_name_idx_map
            .insert({graph_name_lists->singleton_name_list->column(idx).name(),
                     idx})
            .second);
  }
  return column_name_idx_map;
}

// Returns a string of column names produced by graph singleton and group name
// lists for user-visible errors.
static std::string GraphNameListsToString(
    const GraphTableNamedVariables* name_lists) {
  return absl::StrCat(
      "[",
      absl::StrJoin(name_lists->singleton_name_list->columns(), ", ",
                    [](std::string* out, const NamedColumn& named_column) {
                      absl::StrAppend(out,
                                      ToIdentifierLiteral(named_column.name()));
                    }),
      name_lists->singleton_name_list->num_columns() > 0 &&
              name_lists->group_name_list->num_columns() > 0
          ? ","
          : "",
      absl::StrJoin(name_lists->group_name_list->columns(), ", ",
                    [](std::string* out, const NamedColumn& named_column) {
                      absl::StrAppend(out,
                                      ToIdentifierLiteral(named_column.name()));
                    }),
      "]");
}

static absl::StatusOr<std::unique_ptr<const GraphTableNamedVariables>>
BuildFinalNameLists(const ResolvedColumnList& final_column_list,
                    const ASTNode* ast_location) {
  auto name_list = std::make_shared<NameList>();
  for (int i = 0; i < final_column_list.size(); ++i) {
    ZETASQL_RETURN_IF_ERROR(name_list->AddColumnMaybeValueTable(
        final_column_list[i].name_id(), final_column_list[i],
        /*is_explicit=*/true, ast_location, /*is_value_table_column=*/false));
  }
  return std::make_unique<GraphTableNamedVariables>(GraphTableNamedVariables{
      ast_location, /*singleton_name_list=*/std::move(name_list),
      /*group_name_list=*/std::make_shared<NameList>()});
}

absl::Status
GraphTableQueryResolver::GraphSetOperationResolver::ValidateGqlSetOperation(
    const ASTGqlSetOperation& set_op) {
  ZETASQL_RET_CHECK_GE(set_op.inputs().size(), 2)
      << "Gql set operation must have at least 2 inputs";
  ZETASQL_RET_CHECK_NE(set_op.metadata(), nullptr);
  ZETASQL_RET_CHECK(!set_op.metadata()->set_operation_metadata_list().empty())
      << "Gql set operation must have metadata";

  ZETASQL_ASSIGN_OR_RETURN(
      ResolvedSetOperationScan::SetOperationType first_op_type,
      GetSetOperationType(set_op.metadata()->set_operation_metadata_list(0)));
  for (const auto* metadata :
       set_op.metadata()->set_operation_metadata_list()) {
    ZETASQL_RET_CHECK_EQ(metadata->hint(), nullptr)
        << "Gql set operation does not support hint";
    ZETASQL_RET_CHECK_EQ(metadata->column_match_mode(), nullptr)
        << "Gql set operation does not support column match mode";
    ZETASQL_RET_CHECK_EQ(metadata->column_propagation_mode(), nullptr)
        << "Gql set operation does not support column propagation mode";
    ZETASQL_RET_CHECK_EQ(metadata->corresponding_by_column_list(), nullptr)
        << "Gql set operation does not support corresponding by";
    ZETASQL_ASSIGN_OR_RETURN(ResolvedSetOperationScan::SetOperationType op_type,
                     GetSetOperationType(metadata));
    if (op_type != first_op_type) {
      return MakeSqlErrorAtLocalNode(metadata)
             << "Gql set operation must have the same set operation type";
    }
  }

  for (const auto* set_op_input : set_op.inputs()) {
    ZETASQL_RET_CHECK(set_op_input->Is<ASTGqlOperatorList>())
        << "Gql set operation must have ASTGqlOperatorList as input";
    for (const auto* op :
         set_op_input->GetAsOrDie<ASTGqlOperatorList>()->operators()) {
      ZETASQL_RET_CHECK(!op->Is<ASTGqlOperatorList>());
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedScan>>
GraphTableQueryResolver::BuildRefForInputScan(
    const ResolvedGraphWithNameList<const ResolvedScan>& input) {
  std::unique_ptr<const ResolvedScan> copied_scan;
  if (input.resolved_node->Is<ResolvedSingleRowScan>()) {
    ZETASQL_ASSIGN_OR_RETURN(copied_scan,
                     zetasql::ResolvedSingleRowScanBuilder().Build());
  } else {
    ZETASQL_ASSIGN_OR_RETURN(
        copied_scan,
        BuildGraphRefScan(input.graph_name_lists->singleton_name_list));
  }
  auto copied_name_lists =
      std::make_unique<GraphTableNamedVariables>(*input.graph_name_lists);
  return ResolvedGraphWithNameList<const ResolvedScan>{
      .resolved_node = std::move(copied_scan),
      .graph_name_lists = std::move(copied_name_lists)};
}

std::vector<std::vector<InputArgumentType>>
GraphTableQueryResolver::GraphSetOperationResolver::BuildColumnTypeLists(
    absl::Span<ResolvedGraphWithNameList<const ResolvedGraphLinearScan>>
        resolved_inputs,
    const ColumnNameIdxMap& column_name_idx_map) {
  std::vector<std::vector<InputArgumentType>> column_type_lists(
      column_name_idx_map.size());

  for (int query_idx = 0; query_idx < resolved_inputs.size(); ++query_idx) {
    const ResolvedScan* return_scan =
        resolved_inputs[query_idx].resolved_node->scan_list().back().get();
    for (auto& col : resolved_inputs[query_idx]
                         .graph_name_lists->singleton_name_list->columns()) {
      int column_idx = column_name_idx_map.at(col.name());
      column_type_lists[column_idx].push_back(
          GetColumnInputArgumentType(col.column(), return_scan));
    }
  }
  return column_type_lists;
}

absl::StatusOr<std::unique_ptr<const ResolvedGraphLinearScan>>
GraphTableQueryResolver::GraphSetOperationResolver::MaybeWrapTypeCastScan(
    const ASTSelect* ast_location,
    ResolvedGraphWithNameList<const ResolvedGraphLinearScan> resolved_input,
    int query_idx, const ResolvedColumnList& target_column_list,
    const ColumnNameIdxMap& column_name_idx_map) {
  ResolvedGraphLinearScanBuilder linear_scan_builder =
      ToBuilder(std::move(resolved_input.resolved_node));
  std::vector<std::unique_ptr<const ResolvedScan>> scan_list =
      linear_scan_builder.release_scan_list();

  // Reorder the linear scan column list to match the target column list.
  ResolvedColumnList scan_column_list(target_column_list.size());
  for (const auto& col :
       resolved_input.graph_name_lists->singleton_name_list->columns()) {
    int column_idx = column_name_idx_map.at(col.name());
    scan_column_list[column_idx] = col.column();
  }

  std::unique_ptr<const ResolvedScan>* return_scan = &scan_list.back();
  ZETASQL_RETURN_IF_ERROR(graph_resolver_->resolver_->CreateWrapperScanWithCasts(
      ast_location, target_column_list,
      graph_resolver_->resolver_->MakeIdString(absl::StrCat(
          kGraphSetOperation.ToStringView(), query_idx + 1, "_cast")),
      return_scan, &scan_column_list));
  return std::move(linear_scan_builder)
      .set_column_list(scan_column_list)
      .set_scan_list(std::move(scan_list))
      .Build();
}

absl::StatusOr<std::vector<std::unique_ptr<const ResolvedSetOperationItem>>>
GraphTableQueryResolver::GraphSetOperationResolver::BuildSetOperationItems(
    absl::Span<ResolvedGraphWithNameList<const ResolvedGraphLinearScan>>
        resolved_inputs,
    const ResolvedColumnList& target_column_list,
    const ColumnNameIdxMap& column_name_idx_map) {
  std::vector<std::unique_ptr<const ResolvedSetOperationItem>>
      resolved_set_op_items;
  resolved_set_op_items.reserve(resolved_inputs.size());
  for (int i = 0; i < resolved_inputs.size(); ++i) {
    // Wrap the last scan (ProjectScan) in GraphLinearScan with another
    // ProjectScan, if casting to supertype is needed.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedGraphLinearScan> linear_scan,
        MaybeWrapTypeCastScan(node_.inputs(i)
                                  ->GetAsOrDie<ASTGqlOperatorList>()
                                  ->operators()
                                  .back()
                                  ->GetAsOrDie<ASTGqlReturn>()
                                  ->select(),
                              std::move(resolved_inputs[i]),
                              /*query_idx=*/i, target_column_list,
                              column_name_idx_map));

    ResolvedColumnList column_list = linear_scan->column_list();
    std::unique_ptr<ResolvedSetOperationItem> item =
        MakeResolvedSetOperationItem(std::move(linear_scan), column_list);
    resolved_set_op_items.push_back(std::move(item));
  }
  return resolved_set_op_items;
}

GraphTableQueryResolver::GraphSetOperationResolver::GraphSetOperationResolver(
    const ASTGqlSetOperation& node, GraphTableQueryResolver* resolver)
    : SetOperationResolverBase(resolver->resolver_->language(),
                               resolver->resolver_->coercer_,
                               *resolver->resolver_->column_factory_),
      node_(node),
      graph_resolver_(resolver) {}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedScan>>
GraphTableQueryResolver::GraphSetOperationResolver::Resolve(
    const NameScope* external_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  ZETASQL_RETURN_IF_ERROR(ValidateGqlSetOperation(node_));
  ZETASQL_ASSIGN_OR_RETURN(
      ResolvedSetOperationScan::SetOperationType op_type,
      GetSetOperationType(node_.metadata()->set_operation_metadata_list(0)));

  std::vector<ResolvedGraphWithNameList<const ResolvedGraphLinearScan>>
      resolved_set_op_inputs;
  for (const auto* set_op_input_node : node_.inputs()) {
    ZETASQL_ASSIGN_OR_RETURN(
        ResolvedGraphWithNameList<const ResolvedScan> set_op_input_scan,
        graph_resolver_->BuildRefForInputScan(inputs));

    // Return one linear scan for each set operation input.
    ZETASQL_ASSIGN_OR_RETURN(
        ResolvedGraphWithNameList<const ResolvedGraphLinearScan> resolved_input,
        graph_resolver_->ResolveGqlLinearQuery(
            *set_op_input_node->GetAsOrDie<ASTGqlOperatorList>(),
            external_scope, std::move(set_op_input_scan)));
    resolved_set_op_inputs.push_back(std::move(resolved_input));
  }

  // Validate that column names across linear queries matches by name in a case
  // insensitive way. And column types satisfy the following requirements:
  //   * Columns with the same name have the same supertype
  //   * Set operation other than UNION ALL must have all column types groupable
  //   * Set operation INTERSECT or EXCEPT must have all column types comparable
  // Note that, GraphLinearScan guarantees that output columns must have name.
  // TODO: b/345302738 - pull shareable components out.
  ZETASQL_ASSIGN_OR_RETURN(
      ColumnNameIdxMap first_query_column_name_idx_map,
      ToColumnNameIdxMap(resolved_set_op_inputs[0].graph_name_lists.get()));
  ColumnNameSet first_query_column_names;
  zetasql_base::InsertKeysFromMap(first_query_column_name_idx_map,
                         &first_query_column_names);
  const ASTNode* error_location =
      node_.metadata()->set_operation_metadata_list(0)->op_type();
  for (int query_idx = 1; query_idx < resolved_set_op_inputs.size();
       ++query_idx) {
    ZETASQL_ASSIGN_OR_RETURN(
        ColumnNameSet column_names,
        ToColumnNameSet(
            resolved_set_op_inputs[query_idx].graph_name_lists.get()));
    if (!zetasql_base::HashSetEquality(first_query_column_names, column_names)) {
      // The error message needs a deterministic order of column names. So we
      // can't use ColumnNameSet here.
      return MakeSqlErrorAtLocalNode(error_location)
             << "GQL set operation requires all input queries to have "
                "identical column names, but the first query has "
             << GraphNameListsToString(
                    resolved_set_op_inputs[0].graph_name_lists.get())
             << " and query " << (query_idx + 1) << " has "
             << GraphNameListsToString(
                    resolved_set_op_inputs[query_idx].graph_name_lists.get());
    }
  }

  // Decide the final column list. The names and order of the columns will
  // respect those of the first query. The column type will be supertype of the
  // columns with the same name.
  std::vector<std::vector<InputArgumentType>> column_type_lists =
      BuildColumnTypeLists(absl::MakeSpan(resolved_set_op_inputs),
                           first_query_column_name_idx_map);
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<const Type*> super_types,
      GetSuperTypesOfSetOperation(
          column_type_lists, node_.metadata()->set_operation_metadata_list(0),
          op_type, /*column_identifier_in_error_string=*/
          [&](int col_idx) -> std::string {
            return resolved_set_op_inputs[0]
                .graph_name_lists->singleton_name_list->column(col_idx)
                .name()
                .ToString();
          }));

  ZETASQL_ASSIGN_OR_RETURN(
      ResolvedColumnList final_column_list,
      BuildFinalColumnList(
          resolved_set_op_inputs[0]
              .graph_name_lists->singleton_name_list->GetColumnNames(),
          super_types, kGraphSetOperation,
          /*record_column_access=*/[&](const ResolvedColumn& col) -> void {
            graph_resolver_->resolver_->RecordColumnAccess(col);
          }));

  ZETASQL_ASSIGN_OR_RETURN(auto resolved_set_op_items,
                   BuildSetOperationItems(
                       absl::MakeSpan(resolved_set_op_inputs),
                       final_column_list, first_query_column_name_idx_map));

  // We set CORRESPONDING here to indicate that columns are matched by name.
  // However, this would have the side effect of requiring components that
  // rely on that field to not associate it with user input, e.g. SQLBuilder.
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedSetOperationScan> set_op_scan,
      ResolvedSetOperationScanBuilder()
          .set_column_list(final_column_list)
          .set_op_type(op_type)
          .set_column_match_mode(ResolvedSetOperationScan::CORRESPONDING)
          .set_column_propagation_mode(ResolvedSetOperationScan::STRICT)
          .set_input_item_list(std::move(resolved_set_op_items))
          .Build());

  ZETASQL_ASSIGN_OR_RETURN(auto final_name_lists,
                   BuildFinalNameLists(final_column_list, &node_));

  return ResolvedGraphWithNameList<const ResolvedScan>(
      {.resolved_node = std::move(set_op_scan),
       .graph_name_lists = std::move(final_name_lists)});
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedScan>>
GraphTableQueryResolver::ResolveGqlSetOperation(
    const ASTGqlSetOperation& set_op, const NameScope* external_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  GraphSetOperationResolver set_op_resolver(set_op, this);
  return set_op_resolver.Resolve(external_scope, std::move(inputs));
}

static bool IsLinearQuery(const ASTGqlOperator* node) {
  return node->Is<ASTGqlOperatorList>();
}

static bool IsCompositeQuery(const ASTGqlOperator* node) {
  return IsLinearQuery(node) || node->Is<ASTGqlSetOperation>();
}

absl::StatusOr<GraphTableQueryResolver::ResolvedGraphWithNameList<
    const ResolvedGraphLinearScan>>
GraphTableQueryResolver::ResolveGqlLinearQueryList(
    const ASTGqlOperatorList& gql_ops_list, const NameScope* external_scope,
    std::unique_ptr<const GraphTableNamedVariables> input_graph_name_lists) {
  // Check that the first 2 levels are linear scans.
  ZETASQL_RET_CHECK(!gql_ops_list.operators().empty());
  for (const auto* composite_op : gql_ops_list.operators()) {
    ZETASQL_RET_CHECK(IsCompositeQuery(composite_op))
        << "GQL top level linear scan must contain ASTGqlOperatorList or "
           "ASTGqlSetOperation as children";
  }
  ZETASQL_ASSIGN_OR_RETURN(auto input_scan,
                   zetasql::ResolvedSingleRowScanBuilder().Build());

  ZETASQL_ASSIGN_OR_RETURN(auto result, ResolveGqlOperatorList(
                                    gql_ops_list.operators(), external_scope,
                                    {.resolved_node = std::move(input_scan),
                                     .graph_name_lists =
                                         std::move(input_graph_name_lists)}));

  if (!resolver_->language().LanguageFeatureEnabled(
          FEATURE_V_1_4_SQL_GRAPH_EXPOSE_GRAPH_ELEMENT)) {
    const ASTGqlOperator* last_op = gql_ops_list.operators().back();
    if (IsLinearQuery(last_op)) {
      ZETASQL_RETURN_IF_ERROR(CheckReturnNoGraphTypedCols(
          result.resolved_node->column_list(),
          last_op->GetAsOrDie<ASTGqlOperatorList>()->operators().back()));
    } else {
      // If the last scan is a composite query, validate that the column list of
      // every resolved linear scan contained satisfies the "no graph element
      // column" constraint.
      for (const auto* linear_op :
           last_op->GetAsOrDie<ASTGqlSetOperation>()->inputs()) {
        ZETASQL_RETURN_IF_ERROR(CheckReturnNoGraphTypedCols(
            result.resolved_node->column_list(),
            linear_op->GetAsOrDie<ASTGqlOperatorList>()->operators().back()));
      }
    }
  }
  return result;
}

absl::Status GraphTableQueryResolver::CheckGqlLinearQuery(
    absl::Span<const ASTGqlOperator* const> primitive_ops) const {
  const auto size = primitive_ops.size();
  ZETASQL_RET_CHECK_GT(size, 0);
  // All ops other than the last one should be one of MATCH,
  // LET, FILTER, WITH, FOR, or ORDER BY and OFFSET/LIMIT.
  for (int i = 0; i < size - 1; ++i) {
    ZETASQL_RET_CHECK(primitive_ops[i]->Is<ASTGqlMatch>() ||
              primitive_ops[i]->Is<ASTGqlLet>() ||
              primitive_ops[i]->Is<ASTGqlOrderByAndPage>() ||
              primitive_ops[i]->Is<ASTGqlFilter>() ||
              primitive_ops[i]->Is<ASTGqlWith>() ||
              primitive_ops[i]->Is<ASTGqlFor>());
  }
  // The last op should be a RETURN.
  ZETASQL_RET_CHECK(primitive_ops[size - 1]->Is<ASTGqlReturn>());

  auto is_only_order_by = [](const ASTGqlOrderByAndPage* op) {
    return op != nullptr && op->order_by() != nullptr && op->page() == nullptr;
  };
  auto is_only_limit = [](const ASTGqlOrderByAndPage* op) {
    return op != nullptr && op->order_by() == nullptr &&
           op->page() != nullptr && op->page()->limit() != nullptr &&
           op->page()->offset() == nullptr;
  };
  auto is_only_offset = [](const ASTGqlOrderByAndPage* op) {
    return op != nullptr && op->order_by() == nullptr &&
           op->page() != nullptr && op->page()->offset() != nullptr &&
           op->page()->limit() == nullptr;
  };

  // We consider "ORDER BY LIMIT OFFSET" op sequences to be an error.
  for (int i = 2; i < primitive_ops.size() - 1; ++i) {
    auto* op0 = primitive_ops[i - 2]->GetAsOrNull<ASTGqlOrderByAndPage>();
    auto* op1 = primitive_ops[i - 1]->GetAsOrNull<ASTGqlOrderByAndPage>();
    auto* op2 = primitive_ops[i]->GetAsOrNull<ASTGqlOrderByAndPage>();

    // If op0 is "ORDER BY" and op1 is "LIMIT" and op2 is "OFFSET" then mark
    // the sequence as error.
    if (is_only_order_by(op0) && is_only_limit(op1) && is_only_offset(op2)) {
      return MakeSqlErrorAt(op2) << "ORDER BY LIMIT OFFSET is not allowed; "
                                    "please use ORDER BY OFFSET LIMIT";
    }
  }

  return absl::OkStatus();
}

absl::StatusOr<GraphTableQueryResolver::ResolvedGraphWithNameList<
    const ResolvedGraphLinearScan>>
GraphTableQueryResolver::ResolveGqlLinearQuery(
    const ASTGqlOperatorList& gql_ops_list, const NameScope* external_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  const absl::Span<const ASTGqlOperator* const> primitive_ops =
      gql_ops_list.operators();
  ZETASQL_RETURN_IF_ERROR(CheckGqlLinearQuery(primitive_ops));

  return ResolveGqlOperatorList(primitive_ops, external_scope,
                                std::move(inputs));
}

absl::StatusOr<GraphTableQueryResolver::ResolvedGraphWithNameList<
    const ResolvedGraphLinearScan>>
GraphTableQueryResolver::ResolveGqlOperatorList(
    absl::Span<const ASTGqlOperator* const> gql_ops,
    const NameScope* external_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  ZETASQL_RET_CHECK(!gql_ops.empty())
      << "GQL linear scan must contain at least one child ASTGqlOperator";
  // Output scan list
  std::vector<std::unique_ptr<const ResolvedScan>> scan_list;
  std::unique_ptr<const ResolvedScan> ref_scan;
  auto op_inputs = std::move(inputs);

  for (const ASTGqlOperator* gql_op : gql_ops) {
    ZETASQL_ASSIGN_OR_RETURN(op_inputs, ResolveGqlOperator(gql_op, external_scope,
                                                   std::move(op_inputs)));
    // Capture the resulting output scan in our scan list.
    scan_list.push_back(std::move(op_inputs.resolved_node));
    // Build a ref scan to the tabular result as the next input scan to GQL ops.
    ZETASQL_ASSIGN_OR_RETURN(
        ref_scan,
        BuildGraphRefScan(op_inputs.graph_name_lists->singleton_name_list));
    op_inputs.resolved_node = std::move(ref_scan);
  }
  auto linear_builder = ResolvedGraphLinearScanBuilder().set_column_list(
      scan_list.back()->column_list());
  ZETASQL_ASSIGN_OR_RETURN(
      auto linear_scan,
      std::move(linear_builder).set_scan_list(std::move(scan_list)).Build());
  return absl::StatusOr<
      ResolvedGraphWithNameList<const ResolvedGraphLinearScan>>(
      {.resolved_node = std::move(linear_scan),
       .graph_name_lists = std::move(op_inputs.graph_name_lists)});
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedScan>>
GraphTableQueryResolver::ResolveGqlMatch(
    const ASTGqlMatch& match_op, const NameScope* input_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  auto& input_scan = inputs.resolved_node;
  auto& input_graph_name_lists = inputs.graph_name_lists;
  ZETASQL_RET_CHECK(input_scan != nullptr);
  ZETASQL_RET_CHECK(match_op.graph_pattern() != nullptr);
  const ASTGraphPattern& ast_graph_pattern = *match_op.graph_pattern();

  ZETASQL_ASSIGN_OR_RETURN(
      auto result,
      ResolveGraphPattern(
          ast_graph_pattern, input_scope,
          std::make_unique<GraphTableNamedVariables>(*input_graph_name_lists),
          /*update_multiply_declared_names=*/!match_op.optional()));

  ResolvedColumnList graph_pattern_scan_column_list =
      result.resolved_node->column_list();

  ResolvedColumnList joined_columns = input_scan->column_list();
  absl::c_move(graph_pattern_scan_column_list,
               std::back_inserter(joined_columns));

  // Resolve match hints if they're present.
  std::vector<std::unique_ptr<const ResolvedOption>> match_hints;
  if (match_op.hint() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        resolver_->ResolveHintAndAppend(match_op.hint(), &match_hints));
  }

  ZETASQL_ASSIGN_OR_RETURN(auto graph_scan, ToBuilder(std::move(result.resolved_node))
                                        .set_column_list(joined_columns)
                                        .set_hint_list(std::move(match_hints))
                                        .set_input_scan(std::move(input_scan))
                                        .set_optional(match_op.optional())
                                        .Build());
  return {{.resolved_node = std::move(graph_scan),
           .graph_name_lists = std::move(result.graph_name_lists)}};
}

absl::StatusOr<std::unique_ptr<const ResolvedExpr>>
GraphTableQueryResolver::ResolveHorizontalAggregateExpr(
    const ASTExpression* expr, const NameScope* local_scope,
    QueryResolutionInfo* query_resolution_info, bool allow_analytic,
    const char* const clause_name) const {
  ExprResolutionInfo expr_resolution_info(
      query_resolution_info, local_scope,
      {.allows_aggregation = false,
       .allows_analytic = allow_analytic,
       .clause_name = clause_name,
       .allows_horizontal_aggregation =
           resolver_->language().LanguageFeatureEnabled(
               FEATURE_V_1_4_SQL_GRAPH_ADVANCED_QUERY)});
  std::unique_ptr<const ResolvedExpr> resolved_expr;
  ZETASQL_RETURN_IF_ERROR(
      resolver_->ResolveExpr(expr, &expr_resolution_info, &resolved_expr));
  return resolved_expr;
}

// Takes the input scan and extends it with one column each for the
// variable definitions in `let_op`s `variable_definition_list()`.
// Produces a ResolvedProjectScan with these additional columns, and
// amends the working name list with names from the variable definitions.
// Names added to the name list do not conflict with names already in the
// list. If any such names are encountered, returns an error.
absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedScan>>
GraphTableQueryResolver::ResolveGqlLet(
    const ASTGqlLet& let_op, const NameScope* external_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  auto& input_scan = inputs.resolved_node;
  auto& input_graph_name_lists = inputs.graph_name_lists;

  const auto definitions =
      let_op.variable_definition_list()->variable_definitions();

  // Initialize a ResolvedColumnList with the input scan's column list.
  // New ResolvedColumns will be added to this list as the variable
  // definition list is processed. This is necessary because the generated
  // ResolvedProjectScan extends the input scan with new columns.
  // As such, it is different from a ResolvedProjectScan that selects
  // from the input scan.
  ResolvedColumnList let_definition_column_list(input_scan->column_list());
  let_definition_column_list.reserve(input_scan->column_list_size() +
                                     definitions.size());

  // Like the column list, the output name list starts of being the
  // same as the initial working name list.
  auto post_let_name_list = input_graph_name_lists->singleton_name_list->Copy();

  std::vector<std::unique_ptr<const ResolvedComputedColumn>> expr_list;
  expr_list.reserve(definitions.size());

  const NameScope local_scope(external_scope,
                              input_graph_name_lists->singleton_name_list);
  // Reuse the node-source annotation string for LET as the clause identifier
  // string since it very conveniently already available here.
  auto query_resolution_info = std::make_unique<QueryResolutionInfo>(resolver_);
  for (const auto& definition : definitions) {
    const auto column_name = definition->identifier()->GetAsIdString();

    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedExpr> resolved_column_expr,
        ResolveHorizontalAggregateExpr(definition->expression(), &local_scope,
                                       query_resolution_info.get(),
                                       /*allow_analytic=*/true, "LET"));

    ResolvedColumn new_column(resolver_->AllocateColumnId(), kGraphTableName,
                              column_name, resolved_column_expr->type());
    let_definition_column_list.push_back(new_column);

    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedComputedColumn> computed_column,
        ResolvedComputedColumnBuilder()
            .set_column(new_column)
            .set_expr(std::move(resolved_column_expr))
            .Build());

    ZETASQL_RETURN_IF_ERROR(post_let_name_list->AddColumn(column_name, new_column,
                                                  /*is_explicit=*/true));

    expr_list.push_back(std::move(computed_column));
  }

  ZETASQL_RET_CHECK(!query_resolution_info->HasAggregation());
  if (query_resolution_info->HasAnalytic()) {
    ZETASQL_RETURN_IF_ERROR(
        query_resolution_info->analytic_resolver()->CreateAnalyticScan(
            query_resolution_info.get(), &input_scan));
  }

  // As of now only singleton variables may be assigned via LET.
  // Create an output graph namelist by preserving the input group variables.
  ZETASQL_ASSIGN_OR_RETURN(
      auto output_graph_name_lists,
      CreateGraphNameLists(&let_op, std::move(post_let_name_list),
                           std::move(input_graph_name_lists->group_name_list)));

  // Report error if the final shape contains ambiguous column names.
  ZETASQL_RETURN_IF_ERROR(CheckNoAmbiguousNameTarget(
      /*location=*/*let_op.variable_definition_list(),
      *output_graph_name_lists->singleton_name_list));

  ZETASQL_ASSIGN_OR_RETURN(auto project_scan,
                   ResolvedProjectScanBuilder()
                       .set_column_list(let_definition_column_list)
                       .set_expr_list(std::move(expr_list))
                       .set_input_scan(std::move(input_scan))
                       .Build());
  return {{.resolved_node = std::move(project_scan),
           .graph_name_lists = std::move(output_graph_name_lists)}};
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedScan>>
GraphTableQueryResolver::ResolveGqlFilter(
    const ASTGqlFilter& filter_op, const NameScope* external_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  const NameScope local_scope(external_scope,
                              inputs.graph_name_lists->singleton_name_list);

  // ResolveWhereClauseAndCreateScan except horizontal aggregations are allowed.
  ZETASQL_RET_CHECK(filter_op.condition() != nullptr);
  auto query_resolution_info = std::make_unique<QueryResolutionInfo>(resolver_);

  // Window functions are allowed in FILTER.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> resolved_where,
                   ResolveWhereClause(filter_op.condition(), &local_scope,
                                      query_resolution_info.get(),
                                      /*allow_analytic=*/true));

  ZETASQL_RET_CHECK(!query_resolution_info->HasAggregation());

  if (query_resolution_info->HasAnalytic()) {
    ZETASQL_RETURN_IF_ERROR(
        query_resolution_info->analytic_resolver()->CreateAnalyticScan(
            query_resolution_info.get(), &inputs.resolved_node));
  }

  std::vector<ResolvedColumn> tmp_column_list =
      inputs.resolved_node->column_list();
  return {{.resolved_node = MakeResolvedFilterScan(
               std::move(tmp_column_list), std::move(inputs.resolved_node),
               std::move(resolved_where)),
           .graph_name_lists = std::move(inputs.graph_name_lists)}};
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedScan>>
GraphTableQueryResolver::ResolveGqlOrderByAndPage(
    const ASTGqlOrderByAndPage& order_by_page_op,
    const NameScope* external_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  ZETASQL_ASSIGN_OR_RETURN(auto order_by_result,
                   ResolveGqlOrderByClause(order_by_page_op.order_by(),
                                           external_scope, std::move(inputs)));

  // Pass the result of the OrderBy to the Page clause
  ZETASQL_ASSIGN_OR_RETURN(
      auto page_result,
      ResolveGqlPageClauses(order_by_page_op.page(), external_scope,
                            std::move(order_by_result)));

  return {{.resolved_node = std::move(page_result.resolved_node),
           .graph_name_lists = std::move(page_result.graph_name_lists)}};
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedScan>>
GraphTableQueryResolver::ResolveGqlFor(
    const ASTGqlFor& for_op, const NameScope* external_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  ZETASQL_RET_CHECK(for_op.identifier() != nullptr);
  ZETASQL_RET_CHECK(for_op.expression() != nullptr);

  // Initialize the output column list and NameList with the input
  // column list and NameList.
  ResolvedColumnList output_column_list(inputs.resolved_node->column_list());
  auto post_for_name_list =
      inputs.graph_name_lists->singleton_name_list->Copy();

  std::vector<std::unique_ptr<const ResolvedExpr>> resolved_array_expr_list;
  std::unique_ptr<const ResolvedExpr> array_expr;
  const NameScope local_scope(external_scope,
                              inputs.graph_name_lists->singleton_name_list);
  ExprResolutionInfo expr_resolution_info(&local_scope, "FOR");
  ZETASQL_RETURN_IF_ERROR(resolver_->ResolveExpr(for_op.expression(),
                                         &expr_resolution_info, &array_expr));
  ZETASQL_RET_CHECK(array_expr != nullptr);
  const Type* array_expr_type = array_expr->type();
  ZETASQL_RET_CHECK(array_expr_type != nullptr);
  if (!array_expr_type->IsArray()) {
    return MakeSqlErrorAt(for_op.expression())
           << "Expression in graph FOR operator must be of type ARRAY ";
  }

  const AnnotationMap* element_annotation = nullptr;
  if (array_expr->type_annotation_map() != nullptr) {
    element_annotation =
        array_expr->type_annotation_map()->AsArrayMap()->element();
  }
  const ResolvedColumn array_element_column(
      resolver_->AllocateColumnId(), kGraphTableName,
      /*name=*/for_op.identifier()->GetAsIdString(),
      AnnotatedType(array_expr_type->AsArray()->element_type(),
                    element_annotation));
  std::vector<ResolvedColumn> resolved_element_column_list;
  resolved_element_column_list.push_back(array_element_column);
  output_column_list.push_back(array_element_column);
  ZETASQL_RETURN_IF_ERROR(post_for_name_list->AddColumn(
      for_op.identifier()->GetAsIdString(), array_element_column,
      /*is_explicit=*/true));
  resolved_array_expr_list.push_back(std::move(array_expr));

  // Resolve WITH OFFSET if present
  std::unique_ptr<ResolvedColumnHolder> array_position_column;
  if (for_op.with_offset() != nullptr) {
    const ASTAlias* with_offset_alias = for_op.with_offset()->alias();
    const IdString offset_alias =
        (with_offset_alias == nullptr ? kOffsetAlias
                                      : with_offset_alias->GetAsIdString());
    const ResolvedColumn column(
        resolver_->AllocateColumnId(), kGraphTableName,
        /*name=*/offset_alias,
        AnnotatedType(resolver_->type_factory_->get_int64(),
                      /*annotation_map=*/nullptr));
    array_position_column = MakeResolvedColumnHolder(column);
    output_column_list.push_back(array_position_column->column());
    ZETASQL_RETURN_IF_ERROR(post_for_name_list->AddColumn(
        offset_alias, array_position_column->column(),
        /*is_explicit=*/true));
  }

  // Create an output graph namelist by preserving the input group variables.
  ZETASQL_ASSIGN_OR_RETURN(auto output_graph_name_lists,
                   CreateGraphNameLists(
                       &for_op, std::move(post_for_name_list),
                       std::move(inputs.graph_name_lists->group_name_list)));

  // Report error if the final shape contains ambiguous column names.
  ZETASQL_RETURN_IF_ERROR(CheckNoAmbiguousNameTarget(
      /*location=*/for_op, *output_graph_name_lists->singleton_name_list));

  return {{.resolved_node = MakeResolvedArrayScan(
               output_column_list, std::move(inputs.resolved_node),
               std::move(resolved_array_expr_list),
               std::move(resolved_element_column_list),
               std::move(array_position_column),
               /*join_expr=*/nullptr, /*is_outer=*/false,
               /*array_zip_mode=*/nullptr),
           .graph_name_lists = std::move(output_graph_name_lists)}};
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedScan>>
GraphTableQueryResolver::ResolveGqlOrderByClause(
    const ASTOrderBy* order_by, const NameScope* external_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  auto& input_scan = inputs.resolved_node;
  auto& input_graph_name_lists = inputs.graph_name_lists;

  if (order_by == nullptr) {
    return inputs;
  }

  const std::unique_ptr<const NameScope> order_by_name_scope(new NameScope(
      external_scope, input_graph_name_lists->singleton_name_list));

  ZETASQL_RETURN_IF_ERROR(resolver_->ResolveOrderBySimple(
      order_by, *input_graph_name_lists->singleton_name_list,
      order_by_name_scope.get(), "GQL standalone ORDER BY",
      Resolver::OrderBySimpleMode::kGql, &input_scan));

  return {{.resolved_node = std::move(input_scan),
           .graph_name_lists = std::move(input_graph_name_lists)}};
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedScan>>
GraphTableQueryResolver::ResolveGqlPageClauses(
    const ASTGqlPage* page, const NameScope* external_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  if ((page == nullptr) ||
      (page->limit() == nullptr && page->offset() == nullptr)) {
    return inputs;
  }

  auto& input_scan = inputs.resolved_node;
  auto& input_graph_name_lists = inputs.graph_name_lists;
  const std::unique_ptr<const NameScope> local_scope(new NameScope(
      external_scope, input_graph_name_lists->singleton_name_list));

  auto* limit = page->limit() != nullptr ? page->limit()->limit() : nullptr;
  auto* offset = page->offset() != nullptr ? page->offset()->offset() : nullptr;
  ZETASQL_RETURN_IF_ERROR(resolver_->ResolveLimitOffsetScan(
      limit, offset, local_scope.get(), &input_scan));

  return {{.resolved_node = std::move(input_scan),
           .graph_name_lists = std::move(input_graph_name_lists)}};
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedScan>>
GraphTableQueryResolver::ResolveGqlWith(
    const ASTGqlWith& with_op, const NameScope* external_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  if (!resolver_->language().LanguageFeatureEnabled(
          FEATURE_V_1_4_SQL_GRAPH_RETURN_EXTENSIONS)) {
    return MakeSqlErrorAt(with_op) << "WITH is not supported";
  }
  const ASTSelectList* return_item_list = with_op.select()->select_list();
  ZETASQL_RET_CHECK(return_item_list != nullptr &&
            !return_item_list->columns().empty());
  auto& input_scan = inputs.resolved_node;
  auto& input_graph_name_lists = inputs.graph_name_lists;
  ZETASQL_RET_CHECK(input_scan != nullptr);

  const ASTSelect* select = with_op.select();
  // distinct is allowed but not an AST node
  ZETASQL_RETURN_IF_ERROR(resolver_->CheckForUnwantedSelectClauseChildNodes(
      select, {select->hint(), select->select_list(), select->group_by()},
      "WITH"));

  const ASTGroupBy* group_by = select->group_by();
  if (group_by != nullptr) {
    for (const ASTGroupingItem* grouping_item : group_by->grouping_items()) {
      if (grouping_item->rollup() || grouping_item->cube() ||
          grouping_item->grouping_set_list()) {
        return MakeSqlErrorAt(select)
               << "WITH does not support ROLLUP, CUBE, or GROUPING SETS";
      }
    }
  }

  bool has_star = absl::c_any_of(
      select->select_list()->columns(), [](const ASTSelectColumn* column) {
        return column->expression()->node_kind() == AST_STAR;
      });
  if (has_star && !resolver_->language().LanguageFeatureEnabled(
                      FEATURE_V_1_4_SQL_GRAPH_RETURN_EXTENSIONS)) {
    ZETASQL_RETURN_IF_ERROR(CheckReturnStarIsStandalone(select->select_list()));
  }

  const NameScope local_scope(external_scope,
                              input_graph_name_lists->singleton_name_list);
  NameListPtr select_after_from_name_list;
  ZETASQL_RETURN_IF_ERROR(resolver_->ResolveSelectAfterFrom(
      select, /*order_by=*/nullptr,
      /*limit_offset=*/nullptr, &local_scope, kGraphTableName,
      SelectForm::kGqlWith, SelectWithMode::NONE,
      /*force_new_columns_for_projected_outputs=*/true,
      /*inferred_type_for_query=*/nullptr, &input_scan,
      input_graph_name_lists->singleton_name_list,
      &select_after_from_name_list));

  auto singleton_name_list = select_after_from_name_list->Copy();
  ZETASQL_ASSIGN_OR_RETURN(
      auto output_graph_name_lists,
      CreateGraphNameLists(&with_op,
                           /*singleton_name_list=*/
                           std::move(singleton_name_list),
                           /*group_name_list=*/
                           std::move(input_graph_name_lists->group_name_list)));
  InternalAnalyzerOutputProperties::MarkTargetSyntax(
      resolver_->analyzer_output_properties_,
      const_cast<ResolvedScan*>(input_scan.get()),
      SQLBuildTargetSyntax::kGqlWith);
  return {{.resolved_node = std::move(input_scan),
           .graph_name_lists = std::move(output_graph_name_lists)}};
}

absl::StatusOr<
    GraphTableQueryResolver::ResolvedGraphWithNameList<const ResolvedScan>>
GraphTableQueryResolver::ResolveGqlReturn(
    const ASTGqlReturn& return_op, const NameScope* external_scope,
    ResolvedGraphWithNameList<const ResolvedScan> inputs) {
  const ASTSelectList* return_item_list = return_op.select()->select_list();
  ZETASQL_RET_CHECK(return_item_list != nullptr &&
            !return_item_list->columns().empty());
  auto& input_scan = inputs.resolved_node;
  auto& input_graph_name_lists = inputs.graph_name_lists;
  ZETASQL_RET_CHECK(input_scan != nullptr);

  const ASTSelect* select = return_op.select();
  // distinct is allowed but not an AST node
  ZETASQL_RETURN_IF_ERROR(resolver_->CheckForUnwantedSelectClauseChildNodes(
      select, {select->hint(), select->select_list(), select->group_by()},
      "RETURN"));

  const ASTGroupBy* group_by = select->group_by();
  if (group_by != nullptr) {
    for (const ASTGroupingItem* grouping_item : group_by->grouping_items()) {
      if (grouping_item->rollup() || grouping_item->cube() ||
          grouping_item->grouping_set_list()) {
        return MakeSqlErrorAt(select)
               << "RETURN does not support ROLLUP, CUBE, or GROUPING SETS";
      }
    }
  }

  bool has_star = absl::c_any_of(
      select->select_list()->columns(), [](const ASTSelectColumn* column) {
        return column->expression()->node_kind() == AST_STAR;
      });
  if (has_star && !resolver_->language().LanguageFeatureEnabled(
                      FEATURE_V_1_4_SQL_GRAPH_RETURN_EXTENSIONS)) {
    ZETASQL_RETURN_IF_ERROR(CheckReturnStarIsStandalone(select->select_list()));
  }

  const NameScope local_scope(external_scope,
                              input_graph_name_lists->singleton_name_list);
  auto* order_by = return_op.order_by_page() != nullptr
                       ? return_op.order_by_page()->order_by()
                       : nullptr;
  auto select_after_from_name_list = std::make_shared<const NameList>();
  ZETASQL_RETURN_IF_ERROR(resolver_->ResolveSelectAfterFrom(
      select, order_by,
      /*limit_offset=*/nullptr, &local_scope, kGraphTableName,
      SelectForm::kGqlReturn, SelectWithMode::NONE,
      /*force_new_columns_for_projected_outputs=*/true,
      /*inferred_type_for_query=*/nullptr, &input_scan,
      input_graph_name_lists->singleton_name_list,
      &select_after_from_name_list));

  auto* page = return_op.order_by_page() != nullptr
                   ? return_op.order_by_page()->page()
                   : nullptr;
  // Note: We MergeFrom() here since we cannot assign/move 'const NameList' to
  // 'NameList'
  auto working_graph_name_lists = CreateEmptyGraphNameLists(&return_op);
  ZETASQL_RETURN_IF_ERROR(working_graph_name_lists->singleton_name_list->MergeFrom(
      *select_after_from_name_list, &return_op));

  ZETASQL_ASSIGN_OR_RETURN(
      auto page_result,
      ResolveGqlPageClauses(
          page, external_scope,
          {.resolved_node = std::move(input_scan),
           .graph_name_lists = std::move(working_graph_name_lists)}));
  ZETASQL_ASSIGN_OR_RETURN(
      auto output_graph_name_lists,
      CreateGraphNameLists(
          &return_op,
          std::move(page_result.graph_name_lists->singleton_name_list),
          std::move(input_graph_name_lists->group_name_list)));

  // Report error if the final shape contains ambiguous column names.
  ZETASQL_RETURN_IF_ERROR(CheckNoAmbiguousNameTarget(
      *return_item_list, *output_graph_name_lists->singleton_name_list));

  return {{.resolved_node = std::move(page_result.resolved_node),
           .graph_name_lists = std::move(output_graph_name_lists)}};
}

absl::StatusOr<GraphTableQueryResolver::CleanupGraphRefCb>
GraphTableQueryResolver::HandleGraphReference(const ASTNode* query) {
  const ASTPathExpression* graph_reference = nullptr;
  if (query->Is<ASTGraphTableQuery>()) {
    graph_reference = query->GetAs<ASTGraphTableQuery>()->graph_reference();
  } else if (query->Is<ASTGqlGraphPatternQuery>()) {
    graph_reference =
        query->GetAs<ASTGqlGraphPatternQuery>()->graph_reference();
  } else if (query->Is<ASTGqlLinearOpsQuery>()) {
    graph_reference = query->GetAs<ASTGqlLinearOpsQuery>()->graph_reference();
  } else {
    return MakeSqlErrorAt(query)
           << "Unexpected node kind for query " << query->GetNodeKindString();
  }
  const bool has_graph_reference = graph_reference != nullptr;
  if (has_graph_reference) {
    ZETASQL_RETURN_IF_ERROR(ResolveGraphReference(graph_reference));
    ZETASQL_RETURN_IF_ERROR(resolver_->PushPropertyGraphContext(graph_));
  } else {
    graph_ = resolver_->GetActivePropertyGraphOrNull();
    if (graph_ == nullptr) {
      return MakeSqlErrorAt(query)
             << "No graph reference found in the current context of graph "
                "subquery. Try starting the graph subquery with the GRAPH "
                "clause";
    }
  }
  return std::function<void()>([has_graph_reference, this]() {
    if (has_graph_reference) {
      resolver_->PopPropertyGraphContext();
    }
  });
}

std::unique_ptr<const GraphTableNamedVariables>
GraphTableQueryResolver::CreateEmptyGraphNameLists(const ASTNode* node) {
  GraphTableNamedVariables result = {.ast_node = node};
  return std::make_unique<const GraphTableNamedVariables>(result);
}

absl::StatusOr<std::unique_ptr<const GraphTableNamedVariables>>
GraphTableQueryResolver::CreateGraphNameListsSingletonOnly(
    const ASTNode* node, std::shared_ptr<NameList> singleton_name_list) {
  GraphTableNamedVariables result = {
      .ast_node = node, .singleton_name_list = std::move(singleton_name_list)};
  ZETASQL_RETURN_IF_ERROR(ValidateGraphNameLists(result));
  return std::make_unique<const GraphTableNamedVariables>(result);
}

absl::StatusOr<std::unique_ptr<const GraphTableNamedVariables>>
GraphTableQueryResolver::CreateGraphNameLists(
    const ASTNode* node, std::shared_ptr<NameList> singleton_name_list,
    std::shared_ptr<NameList> group_name_list) {
  GraphTableNamedVariables result = {
      .ast_node = node,
      .singleton_name_list = std::move(singleton_name_list),
      .group_name_list = std::move(group_name_list)};
  ZETASQL_RETURN_IF_ERROR(ValidateGraphNameLists(result));
  return std::make_unique<const GraphTableNamedVariables>(result);
}

NameListPtr GraphTableQueryResolver::GetOutputSingletonNameList(
    const GraphTableNamedVariables& graph_namelists) {
  // Returns an empty singleton name list when the pattern is quantified.
  return (!IsQuantified(graph_namelists.ast_node))
             ? graph_namelists.singleton_name_list
             : std::make_shared<const NameList>();
}

absl::StatusOr<NameListPtr> GraphTableQueryResolver::GetOutputGroupNameList(
    const GraphTableNamedVariables& graph_namelists,
    std::vector<std::unique_ptr<const ResolvedGraphMakeArrayVariable>>&
        new_group_variables) {
  if (!IsQuantified(graph_namelists.ast_node)) {
    return graph_namelists.group_name_list;
  }

  // Returns both singleton and group variables as group variables if the
  // pattern is quantified.
  auto output_name_list = std::make_shared<NameList>();
  if (resolver_->language().LanguageFeatureEnabled(
          FEATURE_V_1_4_SQL_GRAPH_BOUNDED_PATH_QUANTIFICATION)) {
    for (const NamedColumn& singleton :
         graph_namelists.singleton_name_list->columns()) {
      const ArrayType* array_type;
      ZETASQL_RETURN_IF_ERROR(resolver_->type_factory_->MakeArrayType(
          singleton.column().type(), &array_type));
      ResolvedColumn output_column(resolver_->AllocateColumnId(),
                                   singleton.column().table_name_id(),
                                   singleton.column().name_id(), array_type);
      new_group_variables.push_back(MakeResolvedGraphMakeArrayVariable(
          singleton.column(), output_column));
      ZETASQL_RETURN_IF_ERROR(output_name_list->AddColumn(output_column.name_id(),
                                                  output_column,
                                                  /*is_explicit=*/false));
    }
  } else {
    ZETASQL_RETURN_IF_ERROR(output_name_list->MergeFrom(
        *graph_namelists.singleton_name_list, graph_namelists.ast_node));
  }
  ZETASQL_RETURN_IF_ERROR(output_name_list->MergeFrom(*graph_namelists.group_name_list,
                                              graph_namelists.ast_node));
  return output_name_list;
}

absl::StatusOr<std::unique_ptr<const GraphTableNamedVariables>>
GraphTableQueryResolver::MergeGraphNameLists(
    std::unique_ptr<const GraphTableNamedVariables> parent_namelist,
    std::unique_ptr<const GraphTableNamedVariables> child_namelist,
    std::vector<std::unique_ptr<const ResolvedGraphMakeArrayVariable>>&
        new_group_variables) {
  ZETASQL_ASSIGN_OR_RETURN(
      auto child_group_name_list,
      GetOutputGroupNameList(*child_namelist, new_group_variables));

  // Patterns with nested quantifiers should have been caught earlier.
  ZETASQL_RET_CHECK(!IsQuantified(parent_namelist->ast_node) ||
            child_group_name_list->columns().empty());

  // Merge the output-singleton and output-group names of the 'child_namelist'
  // with the singletons and groups in the 'parent_namelist' respectively.
  auto final_singleton_list = std::make_shared<NameList>();
  ZETASQL_RETURN_IF_ERROR(final_singleton_list->MergeFrom(
      *parent_namelist->singleton_name_list, parent_namelist->ast_node));
  ZETASQL_RETURN_IF_ERROR(final_singleton_list->MergeFrom(
      *GetOutputSingletonNameList(*child_namelist), child_namelist->ast_node));

  // group variables can be thought of as singletons of type array
  for (const std::unique_ptr<const ResolvedGraphMakeArrayVariable>& make_array :
       new_group_variables) {
    ZETASQL_RETURN_IF_ERROR(final_singleton_list->AddColumn(
        make_array->array().name_id(), make_array->array(),
        /*is_explicit=*/true));
  }

  auto final_group_list = std::make_shared<NameList>();
  ZETASQL_RETURN_IF_ERROR(final_group_list->MergeFrom(*parent_namelist->group_name_list,
                                              parent_namelist->ast_node));
  ZETASQL_RETURN_IF_ERROR(final_group_list->MergeFrom(*child_group_name_list,
                                              child_namelist->ast_node));

  return CreateGraphNameLists(parent_namelist->ast_node,
                              std::move(final_singleton_list),
                              std::move(final_group_list));
}

absl::StatusOr<std::unique_ptr<const GraphTableNamedVariables>>
GraphTableQueryResolver::MergeGraphNameLists(
    std::unique_ptr<const GraphTableNamedVariables> parent_namelist,
    std::unique_ptr<const GraphTableNamedVariables> child_namelist) {
  std::vector<std::unique_ptr<const ResolvedGraphMakeArrayVariable>>
      new_group_variables;
  ZETASQL_ASSIGN_OR_RETURN(auto merged, MergeGraphNameLists(std::move(parent_namelist),
                                                    std::move(child_namelist),
                                                    new_group_variables));
  ZETASQL_RET_CHECK(new_group_variables.empty())
      << "This function must not be used in a quantified context";
  return merged;
}

absl::Status GraphTableQueryResolver::ValidateGraphNameLists(
    const GraphTableNamedVariables& graph_namelists) {
  auto node_type = graph_namelists.ast_node;
  auto& singleton_name_list = graph_namelists.singleton_name_list;
  auto& group_name_list = graph_namelists.group_name_list;
  for (const auto& group_variable : group_name_list->columns()) {
    // Validate that no duplicate variable in group name list.
    if (NameTarget name_target;
        group_name_list->LookupName(group_variable.name(), &name_target) &&
        name_target.IsAmbiguous()) {
      return MakeSqlErrorAt(node_type)
             << "Variable name: " << ToIdentifierLiteral(group_variable.name())
             << " is ambiguous group variable in the "
             << node_type->GetNodeKindString();
    }

    if (!resolver_->language().LanguageFeatureEnabled(
            FEATURE_V_1_4_SQL_GRAPH_BOUNDED_PATH_QUANTIFICATION)) {
      // Validate that no variable is in both group and singleton name list when
      // direct access to group variables is not allowed. Otherwise this
      // check will be done while resolving multiply declared variables.
      if (NameTarget name_target; singleton_name_list->LookupName(
              group_variable.name(), &name_target)) {
        return MakeSqlErrorAt(node_type)
               << "Variable name: "
               << ToIdentifierLiteral(group_variable.name())
               << " cannot be used in both quantified pattern and unquantified "
                  "pattern in the same "
               << node_type->GetNodeKindString();
      }
    }
  }
  return absl::OkStatus();
}

}  // namespace zetasql
