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

#include "zetasql/analyzer/graph_stmt_resolver.h"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/analyzer/resolver_common_inl.h"
#include "zetasql/common/errors.h"
#include "zetasql/parser/ast_node.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/property_graph.h"
#include "zetasql/public/strings.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_builder.h"
#include "zetasql/resolved_ast/resolved_ast_comparator.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/base/case.h"
#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

using StringViewHashSetCase =
    absl::flat_hash_set<absl::string_view, zetasql_base::StringViewCaseHash,
                        zetasql_base::StringViewCaseEqual>;

template <typename T>
using StringViewHashMapCase =
    absl::flat_hash_map<absl::string_view, T, zetasql_base::StringViewCaseHash,
                        zetasql_base::StringViewCaseEqual>;

// Moves `data` to `to`.
template <typename T1, typename T2>
absl::Status AppendUniquePtr(absl::StatusOr<std::unique_ptr<const T1>> data,
                             std::vector<std::unique_ptr<const T2>>& to) {
  static_assert(std::is_convertible<T1*, T2*>::value,
                "The type of `data` must be convertible to type of `to` in "
                "order for `data` to be appended to `to`");
  ZETASQL_RETURN_IF_ERROR(data.status());
  to.push_back(std::move(data).value());
  return absl::OkStatus();
}

// Gets the name of property declaration: either explicitly via alias or
// implicitly from the expression itself.
absl::StatusOr<std::string> GetPropertyDeclarationName(
    const ASTSelectColumn* property) {
  if (property->alias() != nullptr) {
    return property->alias()->GetAsString();
  }
  // without alias, expression must be a column name (ASTPathExpression with
  // length of 1)
  const ASTPathExpression* column_name =
      property->expression()->GetAsOrNull<ASTPathExpression>();
  if (column_name == nullptr || column_name->num_names() != 1) {
    return MakeSqlErrorAt(property->expression())
           << "Without `AS` alias, the property expression must be a simple "
              "reference to a column name";
  }
  return column_name->last_name()->GetAsString();
}

// Resolves column list by catalog columns.
absl::StatusOr<std::vector<std::unique_ptr<const ResolvedExpr>>>
ResolveColumnList(absl::Span<const Column* const> catalog_columns,
                  const ResolvedTableScan& table_scan) {
  absl::flat_hash_map<const Column*, int> column_index;
  for (int i = 0; i < table_scan.column_index_list_size(); ++i) {
    column_index.emplace(
        table_scan.table()->GetColumn(table_scan.column_index_list(i)), i);
  }

  std::vector<std::unique_ptr<const ResolvedExpr>> col_refs;
  col_refs.reserve(catalog_columns.size());
  for (const Column* column : catalog_columns) {
    auto iter = column_index.find(column);
    ZETASQL_RET_CHECK(iter != column_index.end());
    const ResolvedColumn& resolved_col = table_scan.column_list(iter->second);
    ZETASQL_RETURN_IF_ERROR(AppendUniquePtr(ResolvedColumnRefBuilder()
                                        .set_column(resolved_col)
                                        .set_type(resolved_col.type())
                                        .set_is_correlated(false)
                                        .Build(),
                                    col_refs));
  }
  return col_refs;
}

// Resolves column list by column names.
absl::StatusOr<std::vector<std::unique_ptr<const ResolvedExpr>>>
ResolveColumnList(absl::Span<const ASTIdentifier* const> identifiers,
                  const ResolvedTableScan& table_scan) {
  std::vector<const Column*> catalog_cols;
  catalog_cols.reserve(identifiers.size());
  const Table* table = table_scan.table();
  for (const auto* identifier : identifiers) {
    const Column* column = table->FindColumnByName(identifier->GetAsString());
    if (column == nullptr) {
      return MakeSqlErrorAt(identifier)
             << "Column "
             << ToSingleQuotedStringLiteral(identifier->GetAsStringView())
             << " not found in table "
             << ToSingleQuotedStringLiteral(table->FullName());
    }
    catalog_cols.push_back(column);
  }
  return ResolveColumnList(catalog_cols, table_scan);
}

// Resolves column list by column indices.
absl::StatusOr<std::vector<std::unique_ptr<const ResolvedExpr>>>
ResolveColumnList(absl::Span<const int> catalog_column_indices,
                  const ResolvedTableScan& table_scan) {
  std::vector<const Column*> catalog_cols;
  catalog_cols.reserve(catalog_column_indices.size());
  const Table* table = table_scan.table();
  for (const int index : catalog_column_indices) {
    const Column* column = table->GetColumn(index);
    ZETASQL_RET_CHECK(column != nullptr);
    catalog_cols.push_back(column);
  }
  return ResolveColumnList(catalog_cols, table_scan);
}

// Resolves the element keys, either from explicit key clause or implicitly
// from the underlying table's primary keys, into column references of
// `table_scan`.
absl::StatusOr<std::vector<std::unique_ptr<const ResolvedExpr>>>
ResolveKeyColumns(const ASTNode* ast_location,
                  const GraphElementTable::Kind element_kind,
                  const ASTColumnList* input_ast,
                  const ResolvedTableScan& table_scan) {
  // Explicit key clause.
  if (input_ast != nullptr) {
    return ResolveColumnList(input_ast->identifiers(), table_scan);
  }
  // No key clause: implicitly the underlying primary keys.
  if (table_scan.table()->PrimaryKey().has_value()) {
    return ResolveColumnList(*table_scan.table()->PrimaryKey(), table_scan);
  }
  return MakeSqlErrorAt(ast_location) << absl::StrFormat(
             "The %s table %s does not have primary key "
             "defined; graph element table keys must be explicitly defined",
             element_kind == GraphElementTable::Kind::kNode ? "node" : "edge",
             ToSingleQuotedStringLiteral(table_scan.table()->FullName()));
}

// Finds the node table with given `identifier` from `node_table_map`.
absl::StatusOr<const ResolvedGraphElementTable*> FindNodeTable(
    const ASTIdentifier* identifier,
    const StringViewHashMapCase<const ResolvedGraphElementTable*>&
        node_table_map) {
  auto iter = node_table_map.find(identifier->GetAsStringView());
  if (iter == node_table_map.end()) {
    return MakeSqlErrorAt(identifier)
           << "The referenced node table "
           << ToSingleQuotedStringLiteral(identifier->GetAsStringView())
           << " is not defined in the property graph";
  }
  return iter->second;
}

// Copies all `nodes`.
template <typename T>
absl::StatusOr<std::vector<std::unique_ptr<const T>>> CopyAll(
    const std::vector<std::unique_ptr<const T>>& nodes) {
  std::vector<std::unique_ptr<const T>> copied_nodes;
  copied_nodes.reserve(nodes.size());
  for (const auto& node : nodes) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const T> node_copy,
                     ResolvedASTDeepCopyVisitor::Copy(node.get()));
    copied_nodes.push_back(std::move(node_copy));
  }
  return copied_nodes;
}

// Validates `input_ast` does not contain duplicate names: ok when `input_ast`
// is null which means the list is empty.
absl::Status ValidateColumnsAreUnique(const ASTColumnList* input_ast) {
  if (input_ast == nullptr) {
    return absl::OkStatus();
  }
  StringViewHashSetCase unique_names;
  for (const auto& col : input_ast->identifiers()) {
    if (!unique_names.insert(col->GetAsStringView()).second) {
      return MakeSqlErrorAt(col)
             << "Duplicate column "
             << ToSingleQuotedStringLiteral(col->GetAsStringView())
             << " in the referenced column list";
    }
  }
  return absl::OkStatus();
}

// Validates no duplicate label within element table.
absl::Status ValidateNoDuplicateLabelForElementTable(
    const ASTNode* ast_location, absl::string_view element_table_name,
    absl::Span<const std::string> label_list) {
  StringViewHashSetCase unique_labels;
  for (const std::string& label_name : label_list) {
    if (!unique_labels.insert(label_name).second) {
      return MakeSqlErrorAt(ast_location)
             << "Duplicate label name "
             << ToSingleQuotedStringLiteral(label_name)
             << " in the same element table "
             << ToSingleQuotedStringLiteral(element_table_name);
    }
  }
  return absl::OkStatus();
}

// Validates `create_stmt` against the CatalogObjectType existence in `catalog`.
// For example, ValidateCreationAgainstCatalog<Table>: validates against
// the table names.
template <typename CatalogObjectType>
absl::Status ValidateCreationAgainstCatalog(
    const ASTNode* ast_location, const ResolvedCreateStatement& create_stmt,
    Catalog& catalog, const Catalog::FindOptions& options,
    absl::string_view err_message) {
  const CatalogObjectType* object;
  absl::Status find_object_status =
      catalog.FindObject(create_stmt.name_path(), &object, options);
  // Report ok if no object with given name exists in catalog.
  if (absl::IsNotFound(find_object_status)) {
    return absl::OkStatus();
  }

  // Report other catalog error as it is.
  ZETASQL_RETURN_IF_ERROR(find_object_status);

  // Existence is permitted when the create mode allows it.
  if (create_stmt.create_mode() == ResolvedCreateStatement::CREATE_OR_REPLACE ||
      create_stmt.create_mode() ==
          ResolvedCreateStatement::CREATE_IF_NOT_EXISTS) {
    return absl::OkStatus();
  }

  // Otherwise, reports error if object with such name already exists.
  return MakeSqlErrorAt(ast_location) << err_message;
}

// Validates element tables all have distinct names and outputs the names to
// `existing_identifiers`.
absl::Status ValidateNoDuplicateElementTable(
    const ASTGraphElementTableList* ast_table_list,
    absl::Span<const std::unique_ptr<const ResolvedGraphElementTable>>
        element_tables,
    const GraphElementTable::Kind element_kind,
    StringViewHashSetCase& existing_identifiers) {
  if (ast_table_list == nullptr) {
    ZETASQL_RET_CHECK(element_tables.empty());
    return absl::OkStatus();
  }
  absl::Span<const ASTGraphElementTable* const> ast_locations =
      ast_table_list->element_tables();
  ZETASQL_RET_CHECK_EQ(ast_locations.size(), element_tables.size());
  for (size_t i = 0; i < element_tables.size(); ++i) {
    const auto& element_table = element_tables.at(i);
    const auto [_, inserted] =
        existing_identifiers.insert(element_table->alias());
    if (!inserted) {
      return MakeSqlErrorAt(ast_locations.at(i)) << absl::StrFormat(
                 "The %s table %s is defined more than once; use "
                 "a unique name",
                 element_kind == GraphElementTable::Kind::kNode ? "node"
                                                                : "edge",
                 ToSingleQuotedStringLiteral(element_table->alias()));
    }
  }
  return absl::OkStatus();
}

// Validates a property definition pair: they must have the same expression
// within the same element table. Used by element table resolution.
absl::Status ValidateIdentical(const ResolvedGraphPropertyDefinition& def1,
                               const ResolvedGraphPropertyDefinition& def2) {
  // TODO: find a way to compare two equivalent ResolvedAst
  // like `a+b` and `b+a`.
  ZETASQL_ASSIGN_OR_RETURN(const bool same, ResolvedASTComparator::CompareResolvedAST(
                                        def1.expr(), def2.expr()));
  if (!same) {
    ZETASQL_RET_CHECK(def1.GetParseLocationOrNULL() != nullptr);
    return MakeSqlErrorAtPoint(def1.GetParseLocationOrNULL()->start())
           << "Property "
           << ToSingleQuotedStringLiteral(def1.property_declaration_name())
           << " has more than one definition in the element table; use the "
              "same property definition or assign different property names";
  }
  return absl::OkStatus();
}

static std::string QuotedStringCommaSeparatedRep(
    const std::vector<std::string>& strings) {
  return ToSingleQuotedStringLiteral(
      absl::StrCat("[", absl::StrJoin(strings, ", "), "]"));
}

// Validates a label pair: they must have the same property declaration list.
absl::Status ValidateIdentical(const ResolvedGraphElementLabel& label1,
                               const ResolvedGraphElementLabel& label2) {
  StringViewHashSetCase properties;
  for (const auto& property_name : label1.property_declaration_name_list()) {
    ZETASQL_RET_CHECK(properties.emplace(property_name).second);
  }
  ZETASQL_RET_CHECK(label1.GetParseLocationOrNULL() != nullptr);
  if (properties.size() != label2.property_declaration_name_list().size()) {
    return MakeSqlErrorAtPoint(label1.GetParseLocationOrNULL()->start())
           << absl::StrFormat(
                  "The label %s is defined with different properties. One "
                  "definition of the label has %d property declarations: %s. "
                  "Another definition has  %d property declarations: %s. You "
                  "need to use the same set of property declarations under the "
                  "same label",
                  ToSingleQuotedStringLiteral(label1.name()),
                  label1.property_declaration_name_list_size(),
                  QuotedStringCommaSeparatedRep(
                      label1.property_declaration_name_list()),
                  label2.property_declaration_name_list_size(),
                  QuotedStringCommaSeparatedRep(
                      label2.property_declaration_name_list()));
  }

  std::vector<std::string> missing_properties;
  for (const auto& property : label2.property_declaration_name_list()) {
    if (!properties.contains(property)) {
      missing_properties.push_back(property);
    }
  }
  if (!missing_properties.empty()) {
    return MakeSqlErrorAtPoint(label1.GetParseLocationOrNULL()->start())
           << absl::StrFormat(
                  "The label %s is defined with different property "
                  "declarations. There is one instance of this label defined "
                  "with properties of %s. Another instance is defined with "
                  "properties of %s. You need to use the same set of property "
                  "names under the same label",
                  ToSingleQuotedStringLiteral(label1.name()),
                  QuotedStringCommaSeparatedRep(
                      label1.property_declaration_name_list()),
                  QuotedStringCommaSeparatedRep(
                      label2.property_declaration_name_list()));
  }
  return absl::OkStatus();
}

// Validates a property declaration pair: they must have the same type.
absl::Status ValidateIdentical(
    const ResolvedGraphPropertyDeclaration& declaration1,
    const ResolvedGraphPropertyDeclaration& declaration2) {
  if (!declaration1.type()->Equals(declaration2.type())) {
    ZETASQL_RET_CHECK(declaration1.GetParseLocationOrNULL() != nullptr);
    return MakeSqlErrorAtPoint(declaration1.GetParseLocationOrNULL()->start())
           << absl::StrFormat(
                  "The property declaration of %s has type conflicts. There is "
                  "an existing declaration of type %s. There is a conflicting "
                  "one of type %s",
                  ToSingleQuotedStringLiteral(declaration1.name()),
                  declaration1.type()->TypeName(PRODUCT_EXTERNAL),
                  declaration2.type()->TypeName(PRODUCT_EXTERNAL));
  }
  return absl::OkStatus();
}

// Dedupes the nodes from `resolved_nodes` so that the remaining nodes in
// `resolved_nodes` all have distinct names which is computed from
// `get_name_func`. Along deduplication, also ensures the nodes with the same
// name are identical by calling ValidateIdentical.
template <typename T, typename GetNameFunc>
absl::Status Dedupe(std::vector<std::unique_ptr<const T>>& resolved_nodes,
                    const GetNameFunc& get_name_func) {
  StringViewHashMapCase<const T*> node_map;
  std::vector<std::unique_ptr<const T>> unique_nodes;
  for (auto& node : resolved_nodes) {
    const auto& [iter, inserted] =
        node_map.emplace(std::invoke(get_name_func, *node), node.get());
    if (inserted) {
      unique_nodes.push_back(std::move(node));
    } else {
      ZETASQL_RETURN_IF_ERROR(ValidateIdentical(*node, *iter->second));
    }
  }
  resolved_nodes = std::move(unique_nodes);
  return absl::OkStatus();
}

// Validates ResolvedGraphElementLabel.
absl::Status ValidateElementLabel(
    const ASTGraphElementLabelAndProperties* input_ast,
    const ResolvedGraphElementLabel& label) {
  // Element label level validation:
  // No duplicate property name within a label.
  StringViewHashSetCase unique_properties;
  for (const std::string& property_name :
       label.property_declaration_name_list()) {
    if (!unique_properties.insert(property_name).second) {
      return MakeSqlErrorAt(input_ast)
             << "Duplicate property name "
             << ToSingleQuotedStringLiteral(property_name)
             << " in the same label "
             << ToSingleQuotedStringLiteral(label.name());
    }
  }
  return absl::OkStatus();
}

// Validates ResolvedGraphTableReference.
absl::Status ValidateNodeTableReference(
    const ASTGraphNodeTableReference* input_ast,
    const ResolvedGraphNodeTableReference& node_table_ref) {
  // Node table reference level validation.
  // 1) Validates node table columns are all distinct.
  ZETASQL_RETURN_IF_ERROR(ValidateColumnsAreUnique(input_ast->node_table_columns()));

  // 2) Validates edge table columns are all distinct.
  ZETASQL_RETURN_IF_ERROR(ValidateColumnsAreUnique(input_ast->edge_table_columns()));

  const std::vector<std::unique_ptr<const ResolvedExpr>>& node_refs =
      node_table_ref.node_table_column_list();
  const std::vector<std::unique_ptr<const ResolvedExpr>>& edge_refs =
      node_table_ref.edge_table_column_list();
  ZETASQL_RET_CHECK(!node_refs.empty());
  ZETASQL_RET_CHECK(!edge_refs.empty());

  // 3) Validates node and edge table columns are of the same number.
  if (node_refs.size() != edge_refs.size()) {
    std::string error_message = absl::Substitute(
        "The number of referencing columns in the edge table does not "
        "match that of referenced columns in the node table; the former has a "
        "size of $0 but the latter has a size of $1",
        edge_refs.size(), node_refs.size());
    return MakeSqlErrorAt(input_ast) << error_message;
  }

  // 4) Validates node and edge table column types are consistent.
  if (!absl::c_equal(node_refs, edge_refs,
                     [](const auto& node_col, const auto& edge_col) {
                       return node_col->type()->Equals(edge_col->type());
                     })) {
    return MakeSqlErrorAt(input_ast)
           << "Data types of the referencing columns in the edge table do "
              "not match those of the referenced columns in the node table";
  }
  return absl::OkStatus();
}

// Validates ResolvedGraphElementTable.
absl::Status ValidateElementTable(
    const ASTGraphElementTable* input_ast,
    const ResolvedGraphElementTable& element_table,
    GraphElementTable::Kind element_kind) {
  // Element table level validation:
  // 1) No duplicate key names;
  ZETASQL_RETURN_IF_ERROR(ValidateColumnsAreUnique(input_ast->key_list()));

  // 2) No duplicate label names;
  ZETASQL_RETURN_IF_ERROR(ValidateNoDuplicateLabelForElementTable(
      input_ast, element_table.alias(), element_table.label_name_list()));

  // 3) Node must not have source/destination node reference and
  //    Edge must have source/destination node reference.
  if (element_kind == GraphElementTable::Kind::kNode) {
    if ((input_ast->source_node_reference() != nullptr ||
         input_ast->dest_node_reference() != nullptr)) {
      return MakeSqlErrorAt(input_ast) << "Node table cannot have source or "
                                          "destination references";
    }
    ZETASQL_RET_CHECK(element_table.source_node_reference() == nullptr);
    ZETASQL_RET_CHECK(element_table.dest_node_reference() == nullptr);
  } else if (element_kind == GraphElementTable::Kind::kEdge) {
    if (input_ast->source_node_reference() == nullptr) {
      return MakeSqlErrorAt(input_ast) << "Edge table must have its "
                                          "source node reference defined";
    }
    if (input_ast->dest_node_reference() == nullptr) {
      return MakeSqlErrorAt(input_ast) << "Edge table must have its "
                                          "destination node reference defined";
    }
    ZETASQL_RET_CHECK(element_table.source_node_reference() != nullptr);
    ZETASQL_RET_CHECK(element_table.dest_node_reference() != nullptr);
  }

  return absl::OkStatus();
}

template <typename ResolvedElement>
std::unique_ptr<const ResolvedElement> GetResolvedElementWithLocation(
    std::unique_ptr<const ResolvedElement> element,
    const ParseLocationRange& location_range) {
  ResolvedElement* element_ptr = const_cast<ResolvedElement*>(element.get());
  element_ptr->SetParseLocationRange(location_range);
  return element;
}

absl::StatusOr<absl::string_view> GetAstNodeSql(const ASTNode* node,
                                                absl::string_view sql) {
  const ParseLocationRange& ast_query_range = node->GetParseLocationRange();
  ZETASQL_RET_CHECK_GE(sql.length(), ast_query_range.end().GetByteOffset()) << sql;
  return absl::ClippedSubstr(sql, ast_query_range.start().GetByteOffset(),
                             ast_query_range.end().GetByteOffset() -
                                 ast_query_range.start().GetByteOffset());
}

}  // namespace

absl::StatusOr<std::unique_ptr<const ResolvedGraphNodeTableReference>>
GraphStmtResolver::ResolveGraphNodeTableReference(
    const ASTGraphNodeTableReference* ast_node_table_ref,
    const ResolvedTableScan& edge_table_scan,
    const StringViewHashMapCase<const ResolvedGraphElementTable*>&
        node_table_map) const {
  ZETASQL_RET_CHECK(ast_node_table_ref != nullptr);
  ZETASQL_ASSIGN_OR_RETURN(const ResolvedGraphElementTable* node_table,
                   FindNodeTable(ast_node_table_ref->node_table_identifier(),
                                 node_table_map));

  std::vector<std::unique_ptr<const ResolvedExpr>> node_refs, edge_refs;

  // Resolves node table columns:
  // - either from explicit column clause or
  // - implicitly from the underlying node table's element keys.
  if (ast_node_table_ref->node_table_columns() != nullptr) {
    ZETASQL_RET_CHECK(node_table->input_scan()->Is<ResolvedTableScan>());
    ZETASQL_ASSIGN_OR_RETURN(
        node_refs, ResolveColumnList(
                       ast_node_table_ref->node_table_columns()->identifiers(),
                       *node_table->input_scan()->GetAs<ResolvedTableScan>()));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(node_refs, CopyAll(node_table->key_list()));
  }

  // Resolves edge table columns.
  if (ast_node_table_ref->edge_table_columns() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        edge_refs, ResolveColumnList(
                       ast_node_table_ref->edge_table_columns()->identifiers(),
                       edge_table_scan));
  }
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedGraphNodeTableReference> node_table_ref,
      ResolvedGraphNodeTableReferenceBuilder()
          .set_node_table_identifier(
              ast_node_table_ref->node_table_identifier()->GetAsString())
          .set_node_table_column_list(std::move(node_refs))
          .set_edge_table_column_list(std::move(edge_refs))
          .Build());

  // Validates node table reference.
  ZETASQL_RETURN_IF_ERROR(
      ValidateNodeTableReference(ast_node_table_ref, *node_table_ref));
  return node_table_ref;
}

// Validates `expr` is allowed as a property value expression. Illegal property
// value expressions:
//   - query expression
//   - functions with non-deterministic output
//   - lambda with arguments (since these will create new ResolvedColumns)
absl::Status ValidatePropertyValueExpr(const ResolvedNode& expr) {
  RETURN_ERROR_IF_OUT_OF_STACK_SPACE();
  std::vector<const ResolvedNode*> nodes;
  expr.GetDescendantsWithKinds(
      {RESOLVED_SUBQUERY_EXPR, RESOLVED_FUNCTION_CALL, RESOLVED_INLINE_LAMBDA},
      &nodes);

  for (const ResolvedNode* node : nodes) {
    switch (node->node_kind()) {
      case RESOLVED_SUBQUERY_EXPR: {
        return MakeSqlError()
               << "Property value expression cannot contain a subquery";
      }
      case RESOLVED_FUNCTION_CALL: {
        const ResolvedFunctionCall* function_call =
            node->GetAs<ResolvedFunctionCall>();
        if (function_call->function()->function_options().volatility ==
            FunctionEnums::VOLATILE) {
          return MakeSqlError()
                 << "Property value expression cannot use volatile or "
                    "non-deterministic functions";
        }
        // GetDescendantsWithKinds stops traversal when a node of matching kind
        // is found. A Function's arguments may also contain subqueries or
        // volatile functions, validate those too.
        for (const std::unique_ptr<const ResolvedExpr>& arg :
             function_call->argument_list()) {
          ZETASQL_RETURN_IF_ERROR(ValidatePropertyValueExpr(*arg));
        }
        for (const std::unique_ptr<const ResolvedFunctionArgument>& arg :
             function_call->generic_argument_list()) {
          ZETASQL_RETURN_IF_ERROR(ValidatePropertyValueExpr(*arg));
        }
        break;
      }
      case RESOLVED_INLINE_LAMBDA: {
        const ResolvedInlineLambda* lambda =
            node->GetAs<ResolvedInlineLambda>();
        if (!lambda->argument_list().empty()) {
          return MakeSqlError() << "Lambda with argument is not supported in "
                                   "Property value expression";
        }
        ZETASQL_RETURN_IF_ERROR(ValidatePropertyValueExpr(*lambda->body()));
        break;
      }
      default:
        ZETASQL_RET_CHECK_FAIL() << "Did not expect to encounter node kind: "
                         << node->node_kind();
    }
  }

  return absl::OkStatus();
}

absl::StatusOr<
    std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>>
GraphStmtResolver::ResolveGraphPropertyList(
    const ASTNode* ast_location,
    absl::Span<const ASTSelectColumn* const> properties,
    const ResolvedTableScan& base_table_scan,
    const NameScope* input_scope) const {
  static constexpr char kPropertiesClause[] = "PROPERTIES clause";
  ExprResolutionInfo expr_resolution_info(input_scope, kPropertiesClause);
  std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>
      property_defs;
  property_defs.reserve(properties.size());
  for (const auto& property : properties) {
    std::unique_ptr<const ResolvedExpr> resolved_expr;
    ZETASQL_RETURN_IF_ERROR(resolver_.ResolveExpr(
        property->expression(), &expr_resolution_info, &resolved_expr));
    ZETASQL_RET_CHECK_NE(resolved_expr->type(), nullptr);
    ZETASQL_RETURN_IF_ERROR(ValidatePropertyValueExpr(*resolved_expr))
        .With(LocationOverride(property->expression()));

    // Extract derived property SQL string from AST.
    ZETASQL_ASSIGN_OR_RETURN(absl::string_view sql,
                     GetAstNodeSql(property->expression(), resolver_.sql_));
    ZETASQL_ASSIGN_OR_RETURN(std::string property_decl_name,
                     GetPropertyDeclarationName(property));
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedGraphPropertyDefinition> property_def,
        ResolvedGraphPropertyDefinitionBuilder()
            .set_expr(std::move(resolved_expr))
            .set_sql(sql)
            .set_property_declaration_name(std::move(property_decl_name))
            .Build());
    property_defs.push_back(GetResolvedElementWithLocation(
        std::move(property_def), property->GetParseLocationRange()));
  }
  return property_defs;
}

absl::StatusOr<
    std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>>
GraphStmtResolver::ResolveGraphPropertiesAllColumns(
    const ASTNode* ast_location, const ASTColumnList* all_except_column_list,
    const ResolvedTableScan& base_table_scan) const {
  if (resolver_.language().LanguageFeatureEnabled(
          zetasql::FEATURE_PROPERTY_GRAPH_ENFORCE_EXPLICIT_PROPERTIES)) {
    return MakeSqlErrorAt(ast_location)
           << "Properties list must be explicitly specified using syntax "
              "PROPERTIES (...).";
  }

  const Table* base_table = base_table_scan.table();

  // Figures out the excluded columns.
  absl::flat_hash_set<const Column*> all_except_columns;
  if (all_except_column_list != nullptr) {
    for (const auto& except_column : all_except_column_list->identifiers()) {
      std::string excluded_column_name = except_column->GetAsString();
      const Column* col = base_table->FindColumnByName(excluded_column_name);
      if (col == nullptr) {
        return MakeSqlErrorAt(except_column)
               << "Column " << ToSingleQuotedStringLiteral(excluded_column_name)
               << " in the EXCEPT list is not found in the input table";
      }
      if (!all_except_columns.emplace(col).second) {
        return MakeSqlErrorAt(except_column)
               << "Duplicate column "
               << ToSingleQuotedStringLiteral(excluded_column_name)
               << " is used in the property definition EXCEPT list.";
      }
    }
  }

  // Figures out the included columns: all columns except the excluded ones.
  std::vector<const Column*> catalog_columns;
  for (int i = 0; i < base_table->NumColumns(); ++i) {
    const Column* col = base_table->GetColumn(i);
    if (!all_except_columns.contains(col)) {
      catalog_columns.push_back(col);
    }
  }

  // Resolves all the included columns.
  ZETASQL_ASSIGN_OR_RETURN(std::vector<std::unique_ptr<const ResolvedExpr>> col_refs,
                   ResolveColumnList(catalog_columns, base_table_scan));
  ZETASQL_RET_CHECK_EQ(catalog_columns.size(), col_refs.size());

  // Builds the property definitions.
  std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>
      property_defs;
  property_defs.reserve(col_refs.size());
  for (int i = 0; i < col_refs.size(); ++i) {
    std::string col_name = catalog_columns[i]->Name();
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedGraphPropertyDefinition> property_def,
        ResolvedGraphPropertyDefinitionBuilder()
            .set_expr(std::move(col_refs[i]))
            .set_property_declaration_name(col_name)
            // `col_name` itself may not be a valid sql expression when it's
            // a reserved keyword. In that case, ToIdentifierLiteral will quote
            // the name with backticks to make it a valid sql expression.
            //
            // Note that this is not needed for property declaration name above
            // which should stick to the original name. When used in query,
            // either the user or any query generation tool needs to quote it.
            .set_sql(ToIdentifierLiteral(col_name))
            .Build());
    property_defs.push_back(GetResolvedElementWithLocation(
        std::move(property_def), ast_location->GetParseLocationRange()));
  }
  return property_defs;
}

absl::StatusOr<
    std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>>
GraphStmtResolver::ResolveGraphProperties(
    const ASTGraphProperties* ast_properties,
    const ResolvedTableScan& base_table_scan,
    const NameScope* input_scope) const {
  if (ast_properties->no_properties()) {
    // NO PROPERTIES
    ZETASQL_RET_CHECK_EQ(ast_properties->derived_property_list(), nullptr);
    ZETASQL_RET_CHECK_EQ(ast_properties->all_except_columns(), nullptr);
    std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>> output;
    return output;
  }

  if (ast_properties->derived_property_list() != nullptr) {
    // PROPERTIES(<derived_property_list>)
    ZETASQL_RET_CHECK_EQ(ast_properties->all_except_columns(), nullptr);
    return ResolveGraphPropertyList(
        ast_properties, ast_properties->derived_property_list()->columns(),
        base_table_scan, input_scope);
  }

  // ALL PROPERTIES [EXCEPT(<all_except_columns>)]
  return ResolveGraphPropertiesAllColumns(
      ast_properties, ast_properties->all_except_columns(), base_table_scan);
}

absl::StatusOr<std::unique_ptr<const ResolvedGraphElementLabel>>
GraphStmtResolver::ResolveLabelAndProperties(
    const ASTGraphElementLabelAndProperties* ast_label_and_properties,
    IdString element_table_alias, const ResolvedTableScan& base_table_scan,
    const NameScope* input_scope,
    std::vector<std::unique_ptr<const ResolvedGraphPropertyDeclaration>>&
        output_property_decls,
    std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>&
        output_property_defs) const {
  // Resolves label name: either explicitly defined or implicitly the element
  // table alias.
  IdString label_name =
      ast_label_and_properties->label_name() == nullptr
          ? element_table_alias
          : ast_label_and_properties->label_name()->GetAsIdString();

  // Resolves property declarations and definitions.
  ZETASQL_RET_CHECK(ast_label_and_properties->properties() != nullptr);
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>
          property_defs,
      ResolveGraphProperties(ast_label_and_properties->properties(),
                             base_table_scan, input_scope));

  std::vector<std::string> property_declaration_names;
  output_property_decls.reserve(output_property_decls.size() +
                                property_defs.size());
  property_declaration_names.reserve(property_defs.size());
  for (const auto& property_def : property_defs) {
    // Each property definition corresponds to a property declaration.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedGraphPropertyDeclaration> property_decl,
        ResolvedGraphPropertyDeclarationBuilder()
            .set_name(property_def->property_declaration_name())
            .set_type(property_def->expr()->type())
            .Build());
    property_declaration_names.push_back(
        property_def->property_declaration_name());
    ZETASQL_RET_CHECK_NE(property_def->GetParseLocationRangeOrNULL(), nullptr);
    output_property_decls.push_back(GetResolvedElementWithLocation(
        std::move(property_decl),
        *property_def->GetParseLocationRangeOrNULL()));
  }

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedGraphElementLabel> label,
                   ResolvedGraphElementLabelBuilder()
                       .set_name(label_name.ToString())
                       .set_property_declaration_name_list(
                           std::move(property_declaration_names))
                       .Build());

  label = GetResolvedElementWithLocation(
      std::move(label), ast_label_and_properties->GetParseLocationRange());
  absl::c_move(property_defs, std::back_inserter(output_property_defs));

  // Validates label.
  ZETASQL_RETURN_IF_ERROR(ValidateElementLabel(ast_label_and_properties, *label));
  return label;
}

absl::StatusOr<std::unique_ptr<const ResolvedTableScan>>
GraphStmtResolver::ResolveBaseTable(
    const ASTPathExpression* input_table_name,
    NameListPtr& input_table_scan_name_list) const {
  const IdString input_table_alias = GetAliasForExpression(input_table_name);

  std::unique_ptr<const ResolvedTableScan> resolved_table_scan;

  // NOTE: We build `output_column_name_list` instead of `output_name_list`
  // because `output_name_list` contains a range variable (from the table name)
  // that could potential override a column with the same name.
  NameListPtr table_scan_name_list;
  ZETASQL_RETURN_IF_ERROR(resolver_.ResolvePathExpressionAsTableScan(
      input_table_name, input_table_alias,
      /*has_explicit_alias=*/false,
      /*alias_location=*/input_table_name, /*hints=*/nullptr,
      /*for_system_time=*/nullptr, resolver_.empty_name_scope_.get(),
      /*remaining_names=*/nullptr, &resolved_table_scan,
      /*output_name_list=*/&table_scan_name_list,
      /*output_column_name_list=*/&input_table_scan_name_list,
      resolver_.resolved_columns_from_table_scans_));
  // keep the column ref for validation
  resolver_.RecordColumnAccess(resolved_table_scan->column_list());
  return resolved_table_scan;
}

absl::StatusOr<std::unique_ptr<const ResolvedGraphElementTable>>
GraphStmtResolver::ResolveGraphElementTable(
    const ASTGraphElementTable* ast_element_table,
    const GraphElementTable::Kind element_kind,
    const StringViewHashMapCase<const ResolvedGraphElementTable*>&
        node_table_map,
    std::vector<std::unique_ptr<const ResolvedGraphElementLabel>>&
        output_labels,
    std::vector<std::unique_ptr<const ResolvedGraphPropertyDeclaration>>&
        output_property_decls) const {
  // Resolves the underlying table.
  NameListPtr table_scan_name_list;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedTableScan> table_scan,
      ResolveBaseTable(ast_element_table->name(), table_scan_name_list));

  // Resolves the element table key clause.
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<const ResolvedExpr>> key_list,
      ResolveKeyColumns(ast_element_table, element_kind,
                        ast_element_table->key_list(), *table_scan));

  // Resolves element table alias: either from explicit alias or implicitly
  // the qualified identifier.
  const IdString alias = ast_element_table->alias() == nullptr
                             ? GetAliasForExpression(ast_element_table->name())
                             : ast_element_table->alias()->GetAsIdString();

  std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>
      property_defs;

  // Resolves labels and properties.
  std::vector<std::string> label_names;
  ZETASQL_RET_CHECK(ast_element_table->label_properties_list() != nullptr);
  const auto& label_properties_list =
      ast_element_table->label_properties_list()->label_properties_list();
  ZETASQL_RET_CHECK(!label_properties_list.empty());
  label_names.reserve(label_properties_list.size());
  const NameScope input_table_name_scope(*table_scan_name_list);
  for (const auto* ast_label_and_props : label_properties_list) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedGraphElementLabel> label,
        ResolveLabelAndProperties(ast_label_and_props, alias, *table_scan,
                                  &input_table_name_scope,
                                  output_property_decls, property_defs));
    label_names.push_back(label->name());
    output_labels.push_back(std::move(label));
  }

  std::unique_ptr<const ResolvedGraphNodeTableReference> source_node_ref,
      dest_node_ref;

  // Resolves source node table reference if specified.
  if (ast_element_table->source_node_reference() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(source_node_ref,
                     ResolveGraphNodeTableReference(
                         ast_element_table->source_node_reference(),
                         *table_scan, node_table_map));
  }

  // Resolves destination node table reference if specified.
  if (ast_element_table->dest_node_reference() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        dest_node_ref,
        ResolveGraphNodeTableReference(ast_element_table->dest_node_reference(),
                                       *table_scan, node_table_map));
  }

  // Dedupe property definitions at element table level.
  ZETASQL_RETURN_IF_ERROR(
      Dedupe(property_defs,
             &ResolvedGraphPropertyDefinition::property_declaration_name));
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedGraphElementTable> element_table,
      ResolvedGraphElementTableBuilder()
          .set_alias(alias.ToString())
          .set_key_list(std::move(key_list))
          .set_input_scan(std::move(table_scan))
          .set_label_name_list(std::move(label_names))
          .set_property_definition_list(std::move(property_defs))
          .set_source_node_reference(std::move(source_node_ref))
          .set_dest_node_reference(std::move(dest_node_ref))
          .Build());
  // Validates ResolvedGraphElementTable.
  ZETASQL_RETURN_IF_ERROR(
      ValidateElementTable(ast_element_table, *element_table, element_kind));
  return element_table;
}

absl::Status GraphStmtResolver::ResolveCreatePropertyGraphStmt(
    const ASTCreatePropertyGraphStatement* ast_stmt,
    std::unique_ptr<ResolvedStatement>* output) const {
  // Resolves general settings and options.
  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;
  ZETASQL_RETURN_IF_ERROR(resolver_.ResolveCreateStatementOptions(
      ast_stmt, "CREATE PROPERTY GRAPH", &create_scope, &create_mode));

  std::vector<std::unique_ptr<const ResolvedOption>> option_list;
  if (ast_stmt->options_list() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(resolver_.ResolveOptionsList(
        ast_stmt->options_list(), /*allow_alter_array_operators=*/false,
        &option_list));
  }

  std::vector<std::unique_ptr<const ResolvedGraphElementTable>> node_tables;
  std::vector<std::unique_ptr<const ResolvedGraphElementTable>> edge_tables;
  std::vector<std::unique_ptr<const ResolvedGraphElementLabel>> labels;
  std::vector<std::unique_ptr<const ResolvedGraphPropertyDeclaration>>
      property_decls;

  // Resolves node tables.
  ZETASQL_RET_CHECK(ast_stmt->node_table_list() != nullptr);
  StringViewHashMapCase<const ResolvedGraphElementTable*> node_table_map;
  for (const auto* ast_node_table :
       ast_stmt->node_table_list()->element_tables()) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedGraphElementTable> node_table,
        ResolveGraphElementTable(ast_node_table, GraphElementTable::Kind::kNode,
                                 node_table_map, labels, property_decls));
    node_table_map.emplace(node_table->alias(), node_table.get());
    node_tables.push_back(std::move(node_table));
  }

  // Resolves edge tables if specified.
  if (ast_stmt->edge_table_list() != nullptr) {
    for (const auto* ast_edge_table :
         ast_stmt->edge_table_list()->element_tables()) {
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<const ResolvedGraphElementTable> edge_table,
          ResolveGraphElementTable(ast_edge_table,
                                   GraphElementTable::Kind::kEdge,
                                   node_table_map, labels, property_decls));
      edge_tables.push_back(std::move(edge_table));
    }
  }

  // Validates element table uniqueness at graph level.
  StringViewHashSetCase element_table_names;
  ZETASQL_RETURN_IF_ERROR(ValidateNoDuplicateElementTable(
      ast_stmt->node_table_list(), node_tables, GraphElementTable::Kind::kNode,
      element_table_names));
  ZETASQL_RETURN_IF_ERROR(ValidateNoDuplicateElementTable(
      ast_stmt->edge_table_list(), edge_tables, GraphElementTable::Kind::kEdge,
      element_table_names));
  // Dedupe property declarations at graph level.
  ZETASQL_RETURN_IF_ERROR(
      Dedupe(property_decls, &ResolvedGraphPropertyDeclaration::name));
  // Dedupe labels at graph level.
  ZETASQL_RETURN_IF_ERROR(Dedupe(labels, &ResolvedGraphElementLabel::name));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedCreatePropertyGraphStmt>
                       create_property_graph_stmt,
                   ResolvedCreatePropertyGraphStmtBuilder()
                       .set_name_path(ast_stmt->name()->ToIdentifierVector())
                       .set_create_scope(create_scope)
                       .set_create_mode(create_mode)
                       .set_node_table_list(std::move(node_tables))
                       .set_edge_table_list(std::move(edge_tables))
                       .set_label_list(std::move(labels))
                       .set_property_declaration_list(std::move(property_decls))
                       .set_option_list(std::move(option_list))
                       .Build());

  // Validates name collision against property graphs and tables.
  ZETASQL_RETURN_IF_ERROR((ValidateCreationAgainstCatalog<PropertyGraph>(
      ast_stmt->name(), *create_property_graph_stmt, *resolver_.catalog_,
      resolver_.analyzer_options_.find_options(),
      "A property graph with the same name is already defined. "
      "Please use a different name")));
  ZETASQL_RETURN_IF_ERROR((ValidateCreationAgainstCatalog<Table>(
      ast_stmt->name(), *create_property_graph_stmt, *resolver_.catalog_,
      resolver_.analyzer_options_.find_options(),
      "The property graph's name is used by a table under the same name. "
      "Please use a different name")));
  *output = absl::WrapUnique(const_cast<ResolvedCreatePropertyGraphStmt*>(
      create_property_graph_stmt.release()));

  return absl::OkStatus();
}

}  // namespace zetasql
