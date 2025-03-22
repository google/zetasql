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

#include "zetasql/analyzer/graph_expr_resolver_helper.h"

#include <memory>
#include <utility>
#include <vector>

#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/property_graph.h"
#include "zetasql/public/types/graph_element_type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_builder.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "zetasql/base/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

absl::Status FindAllLabelsApplicableToElementKind(
    const PropertyGraph& property_graph, GraphElementTable::Kind element_kind,
    absl::flat_hash_set<const GraphElementLabel*>& static_labels
) {
  absl::flat_hash_set<const GraphElementTable*> element_tables;
  if (element_kind == GraphElementTable::Kind::kNode) {
    absl::flat_hash_set<const GraphNodeTable*> node_tables;
    ZETASQL_RETURN_IF_ERROR(property_graph.GetNodeTables(node_tables));
    element_tables = absl::flat_hash_set<const GraphElementTable*>(
        {node_tables.begin(), node_tables.end()});
  } else {
    absl::flat_hash_set<const GraphEdgeTable*> edge_tables;
    ZETASQL_RETURN_IF_ERROR(property_graph.GetEdgeTables(edge_tables));
    element_tables = absl::flat_hash_set<const GraphElementTable*>(
        {edge_tables.begin(), edge_tables.end()});
  }

  for (const GraphElementTable* element_table : element_tables) {
    absl::flat_hash_set<const GraphElementLabel*> labels;
    ZETASQL_RETURN_IF_ERROR(element_table->GetLabels(labels));
    static_labels.insert(labels.begin(), labels.end());
  }
  return absl::OkStatus();
}

absl::StatusOr<bool> ElementLabelsSatisfyResolvedGraphLabelExpr(
    absl::flat_hash_set<const GraphElementLabel*> element_labels,
    const ResolvedGraphLabelExpr* label_expr) {
  // Recursively determines whether a set of element labels
  // satisfies the given label expression.
  switch (label_expr->node_kind()) {
    case RESOLVED_GRAPH_LABEL: {
      // Base case: This is a simple, non-wildcard base label, e.g. `Worker`.
      const ResolvedGraphLabel* label = label_expr->GetAs<ResolvedGraphLabel>();
      bool is_valid_label = false;
      return (label->label() != nullptr &&
              element_labels.contains(label->label())) ||
             is_valid_label;
    }
    case RESOLVED_GRAPH_WILD_CARD_LABEL: {
      // Base case: This is the wildcard label % and should return true so long
      // as the set of element labels is non-empty.
      return !element_labels.empty();
    }
    case RESOLVED_GRAPH_LABEL_NARY_EXPR: {
      const ResolvedGraphLabelNaryExpr* label_nary_expr =
          label_expr->GetAs<ResolvedGraphLabelNaryExpr>();
      switch (label_nary_expr->op()) {
        case ResolvedGraphLabelNaryExpr::NOT: {
          // In the case of !, return the simple negation of the recursive
          // function called on the inner expression being negated.
          ZETASQL_RET_CHECK_EQ(label_nary_expr->operand_list().size(), 1);
          ZETASQL_ASSIGN_OR_RETURN(
              bool satisfied,
              ElementLabelsSatisfyResolvedGraphLabelExpr(
                  element_labels,
                  label_nary_expr->operand_list()[0].get()));
          return !satisfied;
        }
        case ResolvedGraphLabelNaryExpr::AND: {
          // In the case of & and |, return the result of applying conjunction
          // or disjunction, respectively, on the result of the recursive calls
          // to each operand in the operand list.
          ZETASQL_RET_CHECK_GE(label_nary_expr->operand_list().size(), 2);
          for (const std::unique_ptr<const ResolvedGraphLabelExpr>& operand :
               label_nary_expr->operand_list()) {
            ZETASQL_ASSIGN_OR_RETURN(
                bool satisfied,
                ElementLabelsSatisfyResolvedGraphLabelExpr(
                    element_labels,
                    operand.get()));
            if (!satisfied) {
              return false;
            }
          }
          return true;
        }
        case ResolvedGraphLabelNaryExpr::OR: {
          ZETASQL_RET_CHECK_GE(label_nary_expr->operand_list().size(), 2);
          for (const std::unique_ptr<const ResolvedGraphLabelExpr>& operand :
               label_nary_expr->operand_list()) {
            ZETASQL_ASSIGN_OR_RETURN(
                bool satisfied,
                ElementLabelsSatisfyResolvedGraphLabelExpr(
                    element_labels,
                    operand.get()));
            if (satisfied) {
              return true;
            }
          }
          return false;
        }
        case ResolvedGraphLabelNaryExpr::OPERATION_TYPE_UNSPECIFIED: {
          ZETASQL_RET_CHECK_FAIL() << "Unexpected graph label operation: "
                           << label_nary_expr->op();
        }
      }
    }
    default: {
      ZETASQL_RET_CHECK_FAIL() << "Unexpected graph label expression: "
                       << label_expr->DebugString();
    }
  }
}

static absl::StatusOr<ResolvedGraphLabelNaryExprEnums_GraphLogicalOpType>
GetGraphLabelExprOp(const ASTGraphLabelOperation* ast_graph_label_expr) {
  switch (ast_graph_label_expr->op_type()) {
    case ASTGraphLabelOperation::NOT: {
      return ResolvedGraphLabelNaryExpr::NOT;
    }
    case ASTGraphLabelOperation::AND: {
      return ResolvedGraphLabelNaryExpr::AND;
    }
    case ASTGraphLabelOperation::OR: {
      return ResolvedGraphLabelNaryExpr::OR;
    }
    case ASTGraphLabelOperation::OPERATION_TYPE_UNSPECIFIED:
      ZETASQL_RET_CHECK_FAIL() << "Operation type unspecified";
  }
}

absl::StatusOr<std::unique_ptr<const ResolvedGraphLabelExpr>>
ResolveGraphLabelExpr(
    const ASTGraphLabelExpression* ast_graph_label_expr,
    const GraphElementTable::Kind element_kind,
    const absl::flat_hash_set<const GraphElementLabel*>& valid_static_labels,
    const PropertyGraph* property_graph
) {
  std::unique_ptr<const ResolvedGraphLabelExpr> output;
  if (ast_graph_label_expr == nullptr) {
    return output;
  }
  switch (ast_graph_label_expr->node_kind()) {
    case AST_GRAPH_WILDCARD_LABEL:
      return ResolvedGraphWildCardLabelBuilder().Build();
    case AST_GRAPH_ELEMENT_LABEL: {
      const GraphElementLabel* label = nullptr;
      absl::Status find_status = property_graph->FindLabelByName(
          ast_graph_label_expr->GetAsOrDie<ASTGraphElementLabel>()
              ->name()
              ->GetAsStringView(),
          label);
      if (!find_status.ok()) {
          return MakeSqlErrorAt(ast_graph_label_expr) << find_status.message();
      }
      // If the label was successfully found, but not present in the set of
      // valid static labels, then it must be a label for the wrong element kind
      // e.g. an edge only label in a label expression referring to a node
      // pattern. Report a user error message in this case.
      if (!valid_static_labels.contains(label)) {
        absl::string_view kind_str =
            (element_kind == GraphElementTable::Kind::kNode) ? "node" : "edge";
        absl::string_view correct_kind_str =
            (kind_str == "node") ? "edge" : "node";
        return MakeSqlErrorAt(ast_graph_label_expr) << absl::StrFormat(
                   "Label %s is only valid for %ss, but used here on a %s",
                   label->Name(), correct_kind_str, kind_str);
      }
      ResolvedGraphLabelBuilder builder =
          ResolvedGraphLabelBuilder().set_label(label);
      return std::move(builder).Build();
    }
    case AST_GRAPH_LABEL_OPERATION: {
      std::vector<std::unique_ptr<const ResolvedGraphLabelExpr>> operand_list;
      const ASTGraphLabelOperation* ast_graph_label_operation =
          ast_graph_label_expr->GetAsOrDie<ASTGraphLabelOperation>();
      auto& inputs = ast_graph_label_operation->inputs();
      operand_list.reserve(inputs.size());
      for (const ASTGraphLabelExpression* input : inputs) {
        ZETASQL_ASSIGN_OR_RETURN(
            std::unique_ptr<const ResolvedGraphLabelExpr> next_operand,
            ResolveGraphLabelExpr(
                input, element_kind, valid_static_labels,
                property_graph
                ));
        operand_list.emplace_back(std::move(next_operand));
      }
      ZETASQL_ASSIGN_OR_RETURN(ResolvedGraphLabelNaryExprEnums_GraphLogicalOpType op,
                       GetGraphLabelExprOp(ast_graph_label_operation));
      return ResolvedGraphLabelNaryExprBuilder()
          .set_op(op)
          .set_operand_list(std::move(operand_list))
          .Build();
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unrecognized graph label node type";
  }
}

static absl::StatusOr<std::unique_ptr<const ResolvedLiteral>>
GetResolvedLiteralForPropertyName(absl::string_view property_name) {
  return ResolvedLiteralBuilder()
      .set_value(Value::String(property_name))
      .set_type(types::StringType())
      .set_has_explicit_type(true)
      .Build();
}

absl::StatusOr<std::unique_ptr<const ResolvedGraphGetElementProperty>>
ResolveGraphGetElementProperty(
    const ASTNode* error_location, const PropertyGraph* graph,
    const GraphElementType* element_type, absl::string_view property_name,
    std::unique_ptr<const ResolvedExpr> resolved_lhs) {
  auto builder = ResolvedGraphGetElementPropertyBuilder().set_expr(
      std::move(resolved_lhs));
  std::unique_ptr<const ResolvedGraphGetElementProperty>
      get_element_property_expr;

  if (element_type->FindPropertyType(property_name) == nullptr) {
      return MakeSqlErrorAt(error_location)
             << "Property " << property_name
             << " is not exposed by element type "
             << element_type->DebugString();
  } else {
    const GraphPropertyDeclaration* prop_dcl;
    ZETASQL_RETURN_IF_ERROR(
        graph->FindPropertyDeclarationByName(property_name, prop_dcl));
    ZETASQL_RET_CHECK(prop_dcl != nullptr);

    ZETASQL_ASSIGN_OR_RETURN(get_element_property_expr, std::move(builder)
                                                    .set_type(prop_dcl->Type())
                                                    .set_property(prop_dcl)
                                                    .Build());
  }
  return get_element_property_expr;
}

}  // namespace zetasql
