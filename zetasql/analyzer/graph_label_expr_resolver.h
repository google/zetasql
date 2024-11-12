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

#ifndef ZETASQL_ANALYZER_GRAPH_LABEL_EXPR_RESOLVER_H_
#define ZETASQL_ANALYZER_GRAPH_LABEL_EXPR_RESOLVER_H_

#include <memory>

#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/property_graph.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace zetasql {

// Finds labels defined in `property_graph` applicable to the specified
// `element kind` (node/edge) and returns them in `valid_labels`.
absl::Status FindAllLabelsApplicableToElementKind(
    const PropertyGraph& property_graph, GraphElementTable::Kind element_kind,
    absl::flat_hash_set<const GraphElementLabel*>& valid_labels);

// Resolves `ast_graph_label_expr` to a ResolvedGraphLabelExpr within the
// context of `property_graph`.
// `valid_labels` is a set of labels applicable to the specified `element_kind`.
// For instance, if `element_kind` is kNode, `valid_labels` should contain all
// node labels in `property_graph`.
// If a simple label nested in `ast_graph_label_expr` is not in `valid_labels`,
// an error is returned.
absl::StatusOr<std::unique_ptr<const ResolvedGraphLabelExpr>>
ResolveGraphLabelExpr(
    const ASTGraphLabelExpression* ast_graph_label_expr,
    GraphElementTable::Kind element_kind,
    const absl::flat_hash_set<const GraphElementLabel*>& valid_labels,
    const PropertyGraph* property_graph);

// Recursively determines whether a set of `element labels` satisfies the
// given `label_expr`.
absl::StatusOr<bool> ElementLabelsSatisfy(
    absl::flat_hash_set<const GraphElementLabel*> element_labels,
    const ResolvedGraphLabelExpr* label_expr);

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_GRAPH_LABEL_EXPR_RESOLVER_H_
