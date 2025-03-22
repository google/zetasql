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

#ifndef ZETASQL_ANALYZER_GRAPH_STMT_RESOLVER_H_
#define ZETASQL_ANALYZER_GRAPH_STMT_RESOLVER_H_

#include <memory>
#include <vector>

#include "zetasql/analyzer/name_scope.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/property_graph.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/base/case.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace zetasql {

class CreatePropertyGraphStmtBuilder;
class Resolver;

// This class performs resolution for DDL statements related to
// PropertyGraph ZetaSQL analysis. Not thread-safe.
class GraphStmtResolver {
 public:
  explicit GraphStmtResolver(Resolver& resolver, IdStringPool& id_string_pool)
      : resolver_(resolver) {}
  ~GraphStmtResolver() = default;
  GraphStmtResolver(const GraphStmtResolver&) = delete;
  GraphStmtResolver& operator=(const GraphStmtResolver&) = delete;

  // Resolves a CREATE PROPERTY GRAPH statement.
  absl::Status ResolveCreatePropertyGraphStmt(
      const ASTCreatePropertyGraphStatement* ast_stmt,
      std::unique_ptr<ResolvedStatement>* output) const;

 private:
  template <typename T>
  using StringViewHashMapCase =
      absl::flat_hash_map<absl::string_view, T,
                          zetasql_base::StringViewCaseHash,
                          zetasql_base::StringViewCaseEqual>;

  // Resolves `input_table_name` into a corresponding
  // ResolvedTableScan and inserts accessible names into
  // `input_table_scan_name_list`.
  absl::StatusOr<std::unique_ptr<const ResolvedTableScan>> ResolveBaseTable(
      const ASTPathExpression* input_table_name,
      NameListPtr& input_table_scan_name_list) const;

  // Resolves an element table definition `ast_element_table`. As part of
  // resolution, it validates applied labels and property definitions against
  // the global declarations of the graph.
  //
  // `node_table_map` is used to resolve the referenced node tables for edge
  // tables.
  absl::StatusOr<std::unique_ptr<const ResolvedGraphElementTable>>
  ResolveGraphElementTable(
      const ASTGraphElementTable* ast_element_table,
      GraphElementTable::Kind element_kind,
      const StringViewHashMapCase<const ResolvedGraphElementTable*>&
          node_table_map,
      std::vector<std::unique_ptr<const ResolvedGraphElementLabel>>&
          output_labels,
      std::vector<std::unique_ptr<const ResolvedGraphPropertyDeclaration>>&
          output_property_decls) const;

  // Resolves the `ast_node_table_ref` into a ResolvedGraphNodeTableReference.
  //
  // `node_table_map` is used to resolve the referenced node tables.
  absl::StatusOr<std::unique_ptr<const ResolvedGraphNodeTableReference>>
  ResolveGraphNodeTableReference(
      const ASTGraphNodeTableReference* ast_node_table_ref,
      const ResolvedTableScan& edge_table_scan,
      const StringViewHashMapCase<const ResolvedGraphElementTable*>&
          node_table_map) const;

  // Resolves the `ast_label_and_properties` into a label and a list of
  // properties.
  //
  // `element_table_alias` is used as the default label name.
  absl::StatusOr<std::unique_ptr<const ResolvedGraphElementLabel>>
  ResolveLabelAndProperties(
      const ASTGraphElementLabelAndProperties* ast_label_and_properties,
      IdString element_table_alias, const ResolvedTableScan& base_table_scan,
      const NameScope* input_scope,
      std::vector<std::unique_ptr<const ResolvedGraphPropertyDeclaration>>&
          output_property_decls,
      std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>&
          output_property_defs) const;

  // Resolves `ast_properties` into a list of ResolvedGraphPropertyDefinitions.
  absl::StatusOr<
      std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>>
  ResolveGraphProperties(const ASTGraphProperties* ast_properties,
                         const ResolvedTableScan& base_table_scan,
                         const NameScope* input_scope) const;

  // Resolves `properties` into a list of ResolvedGraphPropertyDefinitions.
  // Used by ResolveGraphProperties.
  absl::StatusOr<
      std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>>
  ResolveGraphPropertyList(const ASTNode* ast_location,
                           absl::Span<const ASTSelectColumn* const> properties,
                           const ResolvedTableScan& base_table_scan,
                           const NameScope* input_scope) const;

  // Resolves all columns from `base_table_scan` into a list of
  // ResolvedGraphPropertyDefinitions: excluding the ones specified in
  // `all_except_column_list`.
  //
  // Used by ResolveGraphProperties to resolve the
  // PROPERTIES ALL COLUMNS [EXCEPT (...)] syntax.
  absl::StatusOr<
      std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>>
  ResolveGraphPropertiesAllColumns(
      const ASTNode* ast_location, const ASTColumnList* all_except_column_list,
      const ResolvedTableScan& base_table_scan) const;

  Resolver& resolver_;
};

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_GRAPH_STMT_RESOLVER_H_
