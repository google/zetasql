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

#ifndef ZETASQL_ANALYZER_GRAPH_EXPR_RESOLVER_HELPER_H_
#define ZETASQL_ANALYZER_GRAPH_EXPR_RESOLVER_HELPER_H_

#include <memory>

#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/property_graph.h"
#include "zetasql/public/types/graph_element_type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql {

// Finds labels defined in `property_graph` applicable to the specified
// `element kind` (node/edge).
absl::Status FindAllLabelsApplicableToElementKind(
    const PropertyGraph& property_graph, GraphElementTable::Kind element_kind,
    absl::flat_hash_set<const GraphElementLabel*>& static_labels
);

// Resolves `ast_graph_label_expr` to a ResolvedGraphLabelExpr within the
// context of `property_graph`.
//
// `valid_static_labels` is a set of static labels applicable to the specified
// `element_kind`. For instance, if `element_kind` is kNode,
// `valid_static_labels` should contain all static node labels in
// `property_graph`.
//
// If a simple label nested in `ast_graph_label_expr` is not in
// `valid_static_labels`, an error is returned.
absl::StatusOr<std::unique_ptr<const ResolvedGraphLabelExpr>>
ResolveGraphLabelExpr(
    const ASTGraphLabelExpression* ast_graph_label_expr,
    GraphElementTable::Kind element_kind,
    const absl::flat_hash_set<const GraphElementLabel*>& valid_static_labels,
    const PropertyGraph* property_graph
);

// Recursively determines whether a given group of labels satisfies the
// given `label_expr`.
absl::StatusOr<bool> ElementLabelsSatisfyResolvedGraphLabelExpr(
    absl::flat_hash_set<const GraphElementLabel*> element_labels,
    const ResolvedGraphLabelExpr* label_expr);

// Resolves a graph element's property access or a property specification using
// `property_name` to a GraphGetElementProperty expression.
absl::StatusOr<std::unique_ptr<const ResolvedGraphGetElementProperty>>
ResolveGraphGetElementProperty(
    const ASTNode* error_location, const PropertyGraph* graph,
    const GraphElementType* element_type, absl::string_view property_name,
    std::unique_ptr<const ResolvedExpr> resolved_lhs);

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_GRAPH_EXPR_RESOLVER_HELPER_H_
