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




#ifndef ZETASQL_PARSER_PARSE_TREE_MANUAL_H_
#define ZETASQL_PARSER_PARSE_TREE_MANUAL_H_

#include <stack>

#include "zetasql/base/logging.h"
#include "zetasql/parser/ast_node.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree_decls.h"  
#include "zetasql/parser/parse_tree_generated.h"
#include "zetasql/parser/visit_result.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/type.pb.h"

// This header file has definitions for hand-maintained subclasses of ASTNode.
// It should not be included directly, instead always include parse_tree.h.
namespace zetasql {

// This is a fake ASTNode implementation that exists only for tests,
// which may need to pass an ASTNode* to some methods.
class FakeASTNode final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FAKE;

  FakeASTNode() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override {
    ZETASQL_LOG(FATAL) << "FakeASTNode does not support Accept";
  }
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override {
    ZETASQL_LOG(FATAL) << "FakeASTNode does not support Accept";
  }

  void InitFields() final {
    {
      FieldLoader fl(this);  // Triggers check that there were no children.
    }
    set_start_location(ParseLocationPoint::FromByteOffset("fake_filename", 7));
    set_end_location(ParseLocationPoint::FromByteOffset("fake_filename", 10));
  }
};

class ASTDescriptorColumn final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DESCRIPTOR_COLUMN;

  ASTDescriptorColumn() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  ABSL_MUST_USE_RESULT zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required fields
  const ASTIdentifier* name() const { return name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
  }
  const ASTIdentifier* name_ = nullptr;
};

class ASTDescriptorColumnList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DESCRIPTOR_COLUMN_LIST;

  ASTDescriptorColumnList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  ABSL_MUST_USE_RESULT zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Guaranteed by the parser to never be empty.
  absl::Span<const ASTDescriptorColumn* const> descriptor_column_list() const {
    return descriptor_column_list_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&descriptor_column_list_);
  }
  absl::Span<const ASTDescriptorColumn* const> descriptor_column_list_;
};

// Represents a DROP statement.
class ASTDropStatement final : public ASTDdlStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DROP_STATEMENT;

  ASTDropStatement() : ASTDdlStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // This adds the "if exists" modifier to the node name.
  std::string SingleNodeDebugString() const override;

  enum class DropMode { DROP_MODE_UNSPECIFIED, RESTRICT, CASCADE };

  void set_drop_mode(DropMode drop_mode) { drop_mode_ = drop_mode; }
  const DropMode drop_mode() const { return drop_mode_; }

  void set_schema_object_kind(SchemaObjectKind schema_object_kind) {
    schema_object_kind_ = schema_object_kind;
  }
  const SchemaObjectKind schema_object_kind() const {
    return schema_object_kind_;
  }

  bool is_if_exists() const { return is_if_exists_; }
  void set_is_if_exists(bool value) { is_if_exists_ = value; }

  const ASTPathExpression* name() const { return name_; }

  const ASTPathExpression* GetDdlTarget() const override { return name_; }

  static std::string GetSQLForDropMode(DropMode drop_mode);

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
  }
  const ASTPathExpression* name_ = nullptr;
  SchemaObjectKind schema_object_kind_ =
      SchemaObjectKind::kInvalidSchemaObjectKind;
  DropMode drop_mode_ = DropMode::DROP_MODE_UNSPECIFIED;
  bool is_if_exists_ = false;
};

class ASTUnpivotInItemLabel final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_UNPIVOT_IN_ITEM_LABEL;

  ASTUnpivotInItemLabel() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTLeaf* label() const {
    if (string_label_ != nullptr) {
      return string_label_;
    }
    return int_label_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&string_label_, AST_STRING_LITERAL);
    fl.AddOptional(&int_label_, AST_INT_LITERAL);
  }
  const ASTLeaf* string_label_ = nullptr;
  const ASTLeaf* int_label_ = nullptr;
};

// Represents a DROP ROW ACCESS POLICY statement.
class ASTDropRowAccessPolicyStatement final : public ASTDdlStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_DROP_ROW_ACCESS_POLICY_STATEMENT;

  ASTDropRowAccessPolicyStatement() : ASTDdlStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // This adds the "if exists" modifier to the node name.
  std::string SingleNodeDebugString() const override;

  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  bool is_if_exists() const { return is_if_exists_; }

  const ASTPathExpression* table_name() const { return table_name_; }

  const ASTPathExpression* GetDdlTarget() const override { return name_; }

  const ASTIdentifier* name() const {
    ZETASQL_DCHECK(name_ == nullptr || name_->num_names() == 1);
    return name_ == nullptr ? nullptr : name_->name(0);
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddRequired(&table_name_);
  }
  const ASTPathExpression* name_ = nullptr;
  const ASTPathExpression* table_name_ = nullptr;
  bool is_if_exists_ = false;
};

class ASTDescriptor final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DESCRIPTOR;

  ASTDescriptor() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  ABSL_MUST_USE_RESULT zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTDescriptorColumnList* columns() const { return columns_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&columns_);
  }

  const ASTDescriptorColumnList* columns_ = nullptr;
};


class ASTHint final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_HINT;

  ASTHint() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // This is the @num_shards hint shorthand that can occur anywhere that a
  // hint can occur, prior to @{...} hints.
  // At least one of num_shards_hints is non-NULL or hint_entries is non-empty.
  const ASTIntLiteral* num_shards_hint() const { return num_shards_hint_; }

  const absl::Span<const ASTHintEntry* const>& hint_entries() const {
    return hint_entries_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&num_shards_hint_, AST_INT_LITERAL);
    fl.AddRestAsRepeated(&hint_entries_);
  }

  const ASTIntLiteral* num_shards_hint_ = nullptr;
  absl::Span<const ASTHintEntry* const> hint_entries_;
};

class ASTHintEntry final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_HINT_ENTRY;

  ASTHintEntry() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* qualifier() const { return qualifier_; }
  const ASTIdentifier* name() const { return name_; }

  // Value is always an identifier, literal, or parameter.
  const ASTExpression* value() const { return value_; }

 private:
  void InitFields() final {
    // We need a special case here because we have two children that both have
    // type ASTIdentifier and the first one is optional.
    if (num_children() == 2) {
      FieldLoader fl(this);
      fl.AddRequired(&name_);
      fl.AddRequired(&value_);
    } else {
      FieldLoader fl(this);
      fl.AddRequired(&qualifier_);
      fl.AddRequired(&name_);
      fl.AddRequired(&value_);
    }
  }

  const ASTIdentifier* qualifier_ = nullptr;  // May be NULL
  const ASTIdentifier* name_ = nullptr;
  const ASTExpression* value_ = nullptr;
};

// This is the common superclass of CREATE FUNCTION and CREATE TABLE FUNCTION
// statements. It contains all fields shared between the two types of
// statements, including the function declaration, return type, OPTIONS list,
// and string body (if present).
class ASTCreateFunctionStmtBase : public ASTCreateStatement {
 public:
  explicit ASTCreateFunctionStmtBase(ASTNodeKind kind)
      : ASTCreateStatement(kind) {}

  std::string SingleNodeDebugString() const override;

  enum DeterminismLevel {
    DETERMINISM_UNSPECIFIED = 0,
    DETERMINISTIC,
    NOT_DETERMINISTIC,
    IMMUTABLE,
    STABLE,
    VOLATILE,
  };
  void set_determinism_level(DeterminismLevel level) {
    determinism_level_ = level;
  }
  const DeterminismLevel determinism_level() const {
    return determinism_level_;
  }

  void set_sql_security(SqlSecurity sql_security) {
    sql_security_ = sql_security;
  }
  SqlSecurity sql_security() const { return sql_security_; }

  const ASTFunctionDeclaration* function_declaration() const {
    return function_declaration_;
  }
  const ASTIdentifier* language() const { return language_; }
  const ASTStringLiteral* code() const { return code_; }
  const ASTOptionsList* options_list() const { return options_list_; }

  std::string GetSqlForSqlSecurity() const;
  std::string GetSqlForDeterminismLevel() const;

  const ASTPathExpression* GetDdlTarget() const override {
    return function_declaration_->name();
  }

 protected:
  const ASTFunctionDeclaration* function_declaration_ = nullptr;
  const ASTIdentifier* language_ = nullptr;
  // For external-language functions.
  const ASTStringLiteral* code_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
  SqlSecurity sql_security_;
  DeterminismLevel determinism_level_ =
      DeterminismLevel::DETERMINISM_UNSPECIFIED;
};

// This may represent an "external language" function (e.g., implemented in a
// non-SQL programming language such as JavaScript) or a "sql" function.
// Note that some combinations of field setting can represent functions that are
// not actually valid due to optional members that would be inappropriate for
// one type of function or another; validity of the parsed function must be
// checked by the analyzer.
class ASTCreateFunctionStatement final : public ASTCreateFunctionStmtBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_FUNCTION_STATEMENT;

  ASTCreateFunctionStatement() : ASTCreateFunctionStmtBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_aggregate(bool value) { is_aggregate_ = value; }
  bool is_aggregate() const { return is_aggregate_; }

  const ASTType* return_type() const { return return_type_; }

  const ASTSqlFunctionBody* sql_function_body() const {
    return sql_function_body_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&function_declaration_);
    fl.AddOptionalType(&return_type_);
    fl.AddOptional(&language_, AST_IDENTIFIER);
    fl.AddOptional(&code_, AST_STRING_LITERAL);
    fl.AddOptional(&sql_function_body_, AST_SQL_FUNCTION_BODY);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
  }

  const ASTType* return_type_ = nullptr;
  // For SQL functions.
  const ASTSqlFunctionBody* sql_function_body_ = nullptr;
  bool is_aggregate_ = false;
};

// This represents a table-valued function declaration statement in ZetaSQL,
// using the CREATE TABLE FUNCTION syntax. Note that some combinations of field
// settings can represent functions that are not actually valid, since optional
// members may be inappropriate for one type of function or another; validity of
// the parsed function must be checked by the analyzer.
class ASTCreateTableFunctionStatement final : public ASTCreateFunctionStmtBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_TABLE_FUNCTION_STATEMENT;

  ASTCreateTableFunctionStatement()
      : ASTCreateFunctionStmtBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  const ASTTVFSchema* return_tvf_schema() const {
    return return_tvf_schema_;
  }
  const ASTQuery* query() const { return query_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&function_declaration_);
    fl.AddOptional(&return_tvf_schema_, AST_TVF_SCHEMA);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddOptional(&language_, AST_IDENTIFIER);
    fl.AddOptional(&code_, AST_STRING_LITERAL);
    fl.AddOptional(&query_, AST_QUERY);
  }

  const ASTTVFSchema* return_tvf_schema_ = nullptr;
  const ASTQuery* query_ = nullptr;
};

class ASTCreateTableStmtBase : public ASTCreateStatement {
 public:
  explicit ASTCreateTableStmtBase(const ASTNodeKind kConcreteNodeKind)
      : ASTCreateStatement(kConcreteNodeKind) {}

  const ASTPathExpression* name() const { return name_; }
  const ASTTableElementList* table_element_list() const {
    return table_element_list_;
  }
  const ASTOptionsList* options_list() const { return options_list_; }
  const ASTPathExpression* like_table_name() const { return like_table_name_; }
  const ASTCollate* collate() const { return collate_; }
  const ASTPathExpression* GetDdlTarget() const override { return name_; }

 protected:
  const ASTPathExpression* name_ = nullptr;
  const ASTTableElementList* table_element_list_ = nullptr;  // May be NULL.
  const ASTOptionsList* options_list_ = nullptr;             // May be NULL.
  const ASTPathExpression* like_table_name_ = nullptr;       // May be NULL.
  const ASTCollate* collate_ = nullptr;                       // May be NULL.
};

class ASTCreateTableStatement final : public ASTCreateTableStmtBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CREATE_TABLE_STATEMENT;

  ASTCreateTableStatement() : ASTCreateTableStmtBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTCloneDataSource* clone_data_source() const {
    return clone_data_source_;
  }
  const ASTCopyDataSource* copy_data_source() const {
    return copy_data_source_;
  }
  const ASTPartitionBy* partition_by() const { return partition_by_; }
  const ASTClusterBy* cluster_by() const { return cluster_by_; }
  const ASTQuery* query() const { return query_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddOptional(&table_element_list_, AST_TABLE_ELEMENT_LIST);
    fl.AddOptional(&like_table_name_, AST_PATH_EXPRESSION);
    fl.AddOptional(&clone_data_source_, AST_CLONE_DATA_SOURCE);
    fl.AddOptional(&copy_data_source_, AST_COPY_DATA_SOURCE);
    fl.AddOptional(&collate_, AST_COLLATE);
    fl.AddOptional(&partition_by_, AST_PARTITION_BY);
    fl.AddOptional(&cluster_by_, AST_CLUSTER_BY);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddOptional(&query_, AST_QUERY);
  }

  const ASTCloneDataSource* clone_data_source_ = nullptr;    // May be NULL.
  const ASTCopyDataSource* copy_data_source_ = nullptr;     // May be NULL.
  const ASTPartitionBy* partition_by_ = nullptr;             // May be NULL.
  const ASTClusterBy* cluster_by_ = nullptr;                 // May be NULL.
  const ASTQuery* query_ = nullptr;                          // May be NULL.
};

class ASTCreateEntityStatement final : public ASTCreateStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CREATE_ENTITY_STATEMENT;

  explicit ASTCreateEntityStatement() : ASTCreateStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* type() const { return type_; }
  const ASTPathExpression* name() const { return name_; }
  const ASTOptionsList* options_list() const { return options_list_; }
  const ASTJSONLiteral* json_body() const { return json_body_; }
  const ASTStringLiteral* text_body() const { return text_body_; }

  const ASTPathExpression* GetDdlTarget() const override { return name_; }

 protected:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&type_);
    fl.AddRequired(&name_);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddOptional(&json_body_, AST_JSON_LITERAL);
    fl.AddOptional(&text_body_, AST_STRING_LITERAL);
  }

  const ASTIdentifier* type_ = nullptr;
  const ASTPathExpression* name_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
  const ASTJSONLiteral* json_body_ = nullptr;
  const ASTStringLiteral* text_body_ = nullptr;
};

class ASTCreateRowAccessPolicyStatement final : public ASTCreateStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_ROW_ACCESS_POLICY_STATEMENT;

  ASTCreateRowAccessPolicyStatement() : ASTCreateStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  void set_has_access_keyword(bool value) { has_access_keyword_ = value; }
  bool has_access_keyword() const { return has_access_keyword_; }

  const ASTPathExpression* target_path() const { return target_path_; }
  const ASTGrantToClause* grant_to() const { return grant_to_; }
  const ASTFilterUsingClause* filter_using() const { return filter_using_; }

  const ASTIdentifier* name() const {
    ZETASQL_DCHECK(name_ == nullptr || name_->num_names() == 1);
    return name_ == nullptr ? nullptr : name_->name(0);
  }

  const ASTPathExpression* GetDdlTarget() const override { return name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&target_path_);
    fl.AddOptional(&grant_to_, AST_GRANT_TO_CLAUSE);
    fl.AddRequired(&filter_using_);
    fl.AddOptional(&name_, AST_PATH_EXPRESSION);
  }

  const ASTPathExpression* name_ = nullptr;             // Optional
  const ASTPathExpression* target_path_ = nullptr;      // Required
  const ASTGrantToClause* grant_to_ = nullptr;          // Optional
  const ASTFilterUsingClause* filter_using_ = nullptr;  // Required

  bool has_access_keyword_ = false;
};

class ASTCreateViewStatementBase : public ASTCreateStatement {
 public:
  explicit ASTCreateViewStatementBase(const ASTNodeKind kind)
      : ASTCreateStatement(kind) {}

  std::string GetSqlForSqlSecurity() const;

  const ASTPathExpression* name() const { return name_; }
  const ASTColumnList* column_list() const { return column_list_; }

  void set_sql_security(SqlSecurity sql_security) {
    sql_security_ = sql_security;
  }
  const ASTOptionsList* options_list() const { return options_list_; }
  const ASTQuery* query() const { return query_; }
  SqlSecurity sql_security() const { return sql_security_; }
  void set_recursive(bool recursive) { recursive_ = recursive; }
  bool recursive() const { return recursive_; }

  const ASTPathExpression* GetDdlTarget() const override { return name_; }

 protected:
  void CollectModifiers(std::vector<std::string>* modifiers) const override;

  const ASTPathExpression* name_ = nullptr;
  const ASTColumnList* column_list_ = nullptr;
  SqlSecurity sql_security_;
  const ASTOptionsList* options_list_ = nullptr;
  const ASTQuery* query_ = nullptr;
  bool recursive_ = false;
};

class ASTCreateViewStatement final : public ASTCreateViewStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CREATE_VIEW_STATEMENT;

  ASTCreateViewStatement() : ASTCreateViewStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddOptional(&column_list_, AST_COLUMN_LIST);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddRequired(&query_);
  }
};

// The gen_extra_files.sh can only parse the class definition that is on the
// single line.
// NOLINTBEGIN(whitespace/line_length)
class ASTCreateMaterializedViewStatement final : public ASTCreateViewStatementBase {
// NOLINTEND
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_MATERIALIZED_VIEW_STATEMENT;

  ASTCreateMaterializedViewStatement()
      : ASTCreateViewStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPartitionBy* partition_by() const { return partition_by_; }
  const ASTClusterBy* cluster_by() const { return cluster_by_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddOptional(&column_list_, AST_COLUMN_LIST);
    fl.AddOptional(&partition_by_, AST_PARTITION_BY);
    fl.AddOptional(&cluster_by_, AST_CLUSTER_BY);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddRequired(&query_);
  }

  const ASTPartitionBy* partition_by_ = nullptr;             // May be NULL.
  const ASTClusterBy* cluster_by_ = nullptr;                 // May be NULL.
};

class ASTCreateExternalTableStatement final : public ASTCreateTableStmtBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_EXTERNAL_TABLE_STATEMENT;
  ASTCreateExternalTableStatement()
      : ASTCreateTableStmtBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTWithPartitionColumnsClause* with_partition_columns_clause() const {
    return with_partition_columns_clause_;
  }
  const ASTWithConnectionClause* with_connection_clause() const {
    return with_connection_clause_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddOptional(&table_element_list_, AST_TABLE_ELEMENT_LIST);
    fl.AddOptional(&like_table_name_, AST_PATH_EXPRESSION);
    fl.AddOptional(&collate_, AST_COLLATE);
    fl.AddOptional(&with_partition_columns_clause_,
                   AST_WITH_PARTITION_COLUMNS_CLAUSE);
    fl.AddOptional(&with_connection_clause_, AST_WITH_CONNECTION_CLAUSE);
    fl.AddRequired(&options_list_);
  }

  const ASTWithPartitionColumnsClause* with_partition_columns_clause_ =
      nullptr;  // May be NULL.
  const ASTWithConnectionClause* with_connection_clause_ =
      nullptr;  // May be NULL.
};

// A column schema identifies the column type and the column annotations.
// The annotations consist of the column attributes and the column options.
//
// This class is used only in column definitions of CREATE TABLE statements,
// and is unrelated to CREATE SCHEMA despite the usage of the overloaded term
// "schema".
//
// The hierarchy of column schema is similar to the type hierarchy.
// The annotations can be applied on struct fields or array elements, for
// example, as in STRUCT<x INT64 NOT NULL, y STRING OPTIONS(foo="bar")>.
// In this case, some column attributes, such as PRIMARY KEY and HIDDEN, are
// disallowed as field attributes.
class ASTColumnSchema : public ASTNode {
 public:
  explicit ASTColumnSchema(ASTNodeKind kind) : ASTNode(kind) {}
  const ASTTypeParameterList* type_parameters() const {
    return type_parameters_;
  }
  const ASTColumnAttributeList* attributes() const {
    return attributes_;
  }
  const ASTGeneratedColumnInfo* generated_column_info() const {
    return generated_column_info_;
  }
  const ASTExpression* default_expression() const {
    return default_expression_;
  }
  const ASTCollate* collate() const { return collate_; }
  const ASTOptionsList* options_list() const { return options_list_; }

  // Helper method that returns true if the attributes()->values() contains an
  // ASTColumnAttribute with the node->kind() equal to 'node_kind'.
  bool ContainsAttribute(ASTNodeKind node_kind) const;

  template <typename T>
  std::vector<const T*> FindAttributes(ASTNodeKind node_kind) const {
    std::vector<const T*> found;
    if (attributes() == nullptr) {
      return found;
    }
    for (const ASTColumnAttribute* attribute : attributes()->values()) {
      if (attribute->node_kind() == node_kind) {
        found.push_back(static_cast<const T*>(attribute));
      }
    }
    return found;
  }

 protected:
  const ASTTypeParameterList** mutable_type_parameters_ptr() {
    return &type_parameters_;
  }
  const ASTColumnAttributeList** mutable_attributes_ptr() {
    return &attributes_;
  }
  const ASTGeneratedColumnInfo** mutable_generated_column_info_ptr() {
    return &generated_column_info_;
  }
  const ASTOptionsList** mutable_options_list_ptr() {
    return &options_list_;
  }

  const ASTExpression** mutable_default_expression_ptr() {
    return &default_expression_;
  }
  const ASTCollate** mutable_collate_ptr() {
    return &collate_;
  }

 private:
  const ASTTypeParameterList* type_parameters_ = nullptr;
  const ASTColumnAttributeList* attributes_ = nullptr;
  const ASTGeneratedColumnInfo* generated_column_info_ = nullptr;
  const ASTExpression* default_expression_ = nullptr;
  const ASTCollate* collate_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
};

class ASTSimpleColumnSchema final : public ASTColumnSchema {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SIMPLE_COLUMN_SCHEMA;

  ASTSimpleColumnSchema() : ASTColumnSchema(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* type_name() const { return type_name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&type_name_);
    fl.AddOptional(mutable_type_parameters_ptr(), AST_TYPE_PARAMETER_LIST);
    fl.AddOptional(mutable_generated_column_info_ptr(),
                   AST_GENERATED_COLUMN_INFO);
    fl.AddOptionalExpression(mutable_default_expression_ptr());
    fl.AddOptional(mutable_collate_ptr(), AST_COLLATE);
    fl.AddOptional(mutable_attributes_ptr(), AST_COLUMN_ATTRIBUTE_LIST);
    fl.AddOptional(mutable_options_list_ptr(), AST_OPTIONS_LIST);
  }

  const ASTPathExpression* type_name_ = nullptr;
};

class ASTArrayColumnSchema final : public ASTColumnSchema {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ARRAY_COLUMN_SCHEMA;

  ASTArrayColumnSchema() : ASTColumnSchema(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTColumnSchema* element_schema() const { return element_schema_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&element_schema_);
    fl.AddOptional(mutable_type_parameters_ptr(), AST_TYPE_PARAMETER_LIST);
    fl.AddOptional(mutable_generated_column_info_ptr(),
                   AST_GENERATED_COLUMN_INFO);
    fl.AddOptionalExpression(mutable_default_expression_ptr());
    fl.AddOptional(mutable_collate_ptr(), AST_COLLATE);
    fl.AddOptional(mutable_attributes_ptr(), AST_COLUMN_ATTRIBUTE_LIST);
    fl.AddOptional(mutable_options_list_ptr(), AST_OPTIONS_LIST);
  }

  const ASTColumnSchema* element_schema_ = nullptr;
};

class ASTStructColumnSchema final : public ASTColumnSchema {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_STRUCT_COLUMN_SCHEMA;

  ASTStructColumnSchema() : ASTColumnSchema(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTStructColumnField* const>& struct_fields() const {
    return struct_fields_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRepeatedWhileIsNodeKind(&struct_fields_, AST_STRUCT_COLUMN_FIELD);
    fl.AddOptional(mutable_type_parameters_ptr(), AST_TYPE_PARAMETER_LIST);
    fl.AddOptional(mutable_generated_column_info_ptr(),
                   AST_GENERATED_COLUMN_INFO);
    fl.AddOptionalExpression(mutable_default_expression_ptr());
    fl.AddOptional(mutable_collate_ptr(), AST_COLLATE);
    fl.AddOptional(mutable_attributes_ptr(), AST_COLUMN_ATTRIBUTE_LIST);
    fl.AddOptional(mutable_options_list_ptr(), AST_OPTIONS_LIST);
  }

  absl::Span<const ASTStructColumnField* const> struct_fields_;
};

class ASTInferredTypeColumnSchema final : public ASTColumnSchema {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_INFERRED_TYPE_COLUMN_SCHEMA;

  ASTInferredTypeColumnSchema() : ASTColumnSchema(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(mutable_generated_column_info_ptr());
    fl.AddOptional(mutable_attributes_ptr(), AST_COLUMN_ATTRIBUTE_LIST);
    fl.AddOptional(mutable_options_list_ptr(), AST_OPTIONS_LIST);
  }
};

// Base class for constraints, including primary key, foreign key and check
// constraints.
class ASTTableConstraint : public ASTTableElement {
 public:
  explicit ASTTableConstraint(ASTNodeKind kind) : ASTTableElement(kind) {}

  const ASTIdentifier* constraint_name() const {
    return constraint_name_;
  }

 protected:
  const ASTIdentifier* constraint_name_ = nullptr;
};

class ASTPrimaryKey final : public ASTTableConstraint {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_PRIMARY_KEY;

  ASTPrimaryKey() : ASTTableConstraint(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  void set_enforced(bool enforced) { enforced_ = enforced; }
  bool enforced() const { return enforced_; }

  const ASTColumnList* column_list() const { return column_list_; }
  const ASTOptionsList* options_list() const { return options_list_; }

  std::string SingleNodeDebugString() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&column_list_, AST_COLUMN_LIST);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddOptional(&constraint_name_, AST_IDENTIFIER);
  }

  const ASTColumnList* column_list_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
  bool enforced_ = true;
};

class ASTForeignKey final : public ASTTableConstraint {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FOREIGN_KEY;

  ASTForeignKey() : ASTTableConstraint(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTColumnList* column_list() const { return column_list_; }
  const ASTForeignKeyReference* reference() const { return reference_; }
  const ASTOptionsList* options_list() const { return options_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&column_list_);
    fl.AddRequired(&reference_);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddOptional(&constraint_name_, AST_IDENTIFIER);
  }

  const ASTColumnList* column_list_ = nullptr;
  const ASTForeignKeyReference* reference_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
};

class ASTCheckConstraint final : public ASTTableConstraint {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CHECK_CONSTRAINT;

  ASTCheckConstraint() : ASTTableConstraint(kConcreteNodeKind) {}

  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_enforced(bool is_enforced) { is_enforced_ = is_enforced; }
  const bool is_enforced() const { return is_enforced_; }

  const ASTExpression* expression() const { return expression_; }
  const ASTOptionsList* options_list() const { return options_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddOptional(&constraint_name_, AST_IDENTIFIER);
  }

  const ASTExpression* expression_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
  bool is_enforced_ = true;
};

class ASTPrivilege final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_PRIVILEGE;

  ASTPrivilege() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* privilege_action() const { return privilege_action_; }
  const ASTColumnList* column_list() const { return column_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&privilege_action_);
    fl.AddOptional(&column_list_, AST_COLUMN_LIST);
  }

  const ASTIdentifier* privilege_action_ = nullptr;  // Required
  const ASTColumnList* column_list_ = nullptr;       // Optional
};

// Represents privileges to be granted or revoked. It can be either or a
// non-empty list of ASTPrivilege, or "ALL PRIVILEGES" in which case the list
// will be empty.
class ASTPrivileges final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_PRIVILEGES;

  ASTPrivileges() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTPrivilege* const>& privileges() const {
    return privileges_;
  }

  bool is_all_privileges() const {
    // Empty Span means ALL PRIVILEGES.
    return privileges_.empty();
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&privileges_);
  }

  absl::Span<const ASTPrivilege* const> privileges_;
};

class ASTGranteeList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_GRANTEE_LIST;

  ASTGranteeList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTExpression* const>& grantee_list() const {
    return grantee_list_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&grantee_list_);
  }

  // An ASTGranteeList element may either be a string literal or
  // parameter.
  absl::Span<const ASTExpression* const> grantee_list_;
};

class ASTGrantStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_GRANT_STATEMENT;

  ASTGrantStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPrivileges* privileges() const { return privileges_; }
  const ASTIdentifier* target_type() const { return target_type_; }
  const ASTPathExpression* target_path() const { return target_path_; }
  const ASTGranteeList* grantee_list() const { return grantee_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&privileges_);
    fl.AddOptional(&target_type_, AST_IDENTIFIER);
    fl.AddRequired(&target_path_);
    fl.AddRequired(&grantee_list_);
  }

  const ASTPrivileges* privileges_ = nullptr;       // Required
  const ASTIdentifier* target_type_ = nullptr;      // Optional
  const ASTPathExpression* target_path_ = nullptr;  // Required
  const ASTGranteeList* grantee_list_ = nullptr;    // Required
};

class ASTRevokeStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_REVOKE_STATEMENT;

  ASTRevokeStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPrivileges* privileges() const { return privileges_; }
  const ASTIdentifier* target_type() const { return target_type_; }
  const ASTPathExpression* target_path() const { return target_path_; }
  const ASTGranteeList* grantee_list() const { return grantee_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&privileges_);
    fl.AddOptional(&target_type_, AST_IDENTIFIER);
    fl.AddRequired(&target_path_);
    fl.AddRequired(&grantee_list_);
  }

  const ASTPrivileges* privileges_ = nullptr;
  const ASTIdentifier* target_type_ = nullptr;
  const ASTPathExpression* target_path_ = nullptr;
  const ASTGranteeList* grantee_list_ = nullptr;
};

class ASTRepeatableClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_REPEATABLE_CLAUSE;

  ASTRepeatableClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* argument() const { return argument_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&argument_);
  }

  const ASTExpression* argument_ = nullptr;  // Required
};

class ASTFilterFieldsArg final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FILTER_FIELDS_ARG;

  ASTFilterFieldsArg() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  enum FilterType {
    NOT_SET,
    INCLUDE,
    EXCLUDE,
  };
  void set_filter_type(FilterType filter_type) { filter_type_ = filter_type; }
  FilterType filter_type() const { return filter_type_; }

  const ASTGeneralizedPathExpression* path_expression() const {
    return path_expression_;
  }

  std::string GetSQLForOperator() const;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&path_expression_);
  }

  const ASTGeneralizedPathExpression* path_expression_ = nullptr;
  FilterType filter_type_ = NOT_SET;
};

class ASTFilterFieldsExpression final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FILTER_FIELDS_EXPRESSION;

  ASTFilterFieldsExpression() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expr() const { return expr_; }

  const absl::Span<const ASTFilterFieldsArg* const>& arguments() const {
    return arguments_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expr_);
    fl.AddRestAsRepeated(&arguments_);
  }

  const ASTExpression* expr_ = nullptr;  // Required
  absl::Span<const ASTFilterFieldsArg* const> arguments_;
};

class ASTReplaceFieldsArg final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_REPLACE_FIELDS_ARG;

  ASTReplaceFieldsArg() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }

  const ASTGeneralizedPathExpression* path_expression() const {
    return path_expression_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
    fl.AddRequired(&path_expression_);
  }

  const ASTExpression* expression_ = nullptr;
  const ASTGeneralizedPathExpression* path_expression_ = nullptr;
};

class ASTReplaceFieldsExpression final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_REPLACE_FIELDS_EXPRESSION;

  ASTReplaceFieldsExpression() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expr() const { return expr_; }

  const absl::Span<const ASTReplaceFieldsArg* const>& arguments() const {
    return arguments_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expr_);
    fl.AddRestAsRepeated(&arguments_);
  }

  const ASTExpression* expr_ = nullptr;  // Required
  absl::Span<const ASTReplaceFieldsArg* const> arguments_;
};

class ASTSampleSize final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SAMPLE_SIZE;

  ASTSampleSize() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  enum Unit { NOT_SET, ROWS, PERCENT };

  // Returns the token kind corresponding to the sample-size unit, i.e.
  // parser::ROWS or parser::PERCENT.
  void set_unit(Unit unit) { unit_ = unit; }
  Unit unit() const { return unit_; }

  const ASTExpression* size() const { return size_; }
  const ASTPartitionBy* partition_by() const { return partition_by_; }

  // Returns the SQL keyword for the sample-size unit, i.e. "ROWS" or "PERCENT".
  std::string GetSQLForUnit() const;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&size_);
    fl.AddOptional(&partition_by_, AST_PARTITION_BY);
  }

  const ASTExpression* size_ = nullptr;           // Required
  // Can only be non-NULL when 'unit_' is parser::ROWS.
  const ASTPartitionBy* partition_by_ = nullptr;  // Optional
  Unit unit_ = NOT_SET;
};

class ASTWithWeight final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WITH_WEIGHT;

  ASTWithWeight() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // alias may be NULL.
  const ASTAlias* alias() const { return alias_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&alias_, AST_ALIAS);
  }

  const ASTAlias* alias_ = nullptr;  // Optional
};

class ASTSampleSuffix final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SAMPLE_SUFFIX;

  ASTSampleSuffix() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // weight and repeat may be NULL.
  const ASTWithWeight* weight() const { return weight_; }
  const ASTRepeatableClause* repeat() const { return repeat_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&weight_, AST_WITH_WEIGHT);
    fl.AddOptional(&repeat_, AST_REPEATABLE_CLAUSE);
  }

  const ASTWithWeight* weight_ = nullptr;        // Optional
  const ASTRepeatableClause* repeat_ = nullptr;  // Optional
};

class ASTSampleClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SAMPLE_CLAUSE;

  ASTSampleClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* sample_method() const { return sample_method_; }
  const ASTSampleSize* sample_size() const { return sample_size_; }
  const ASTSampleSuffix* sample_suffix() const { return sample_suffix_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&sample_method_);
    fl.AddRequired(&sample_size_);
    fl.AddOptional(&sample_suffix_, AST_SAMPLE_SUFFIX);
  }

  const ASTIdentifier* sample_method_ = nullptr;    // Required
  const ASTSampleSize* sample_size_ = nullptr;      // Required
  const ASTSampleSuffix* sample_suffix_ = nullptr;  // Optional
};

// Common parent for all actions in ALTER statements
class ASTAlterAction : public ASTNode {
 public:
  explicit ASTAlterAction(ASTNodeKind kind) : ASTNode(kind) {}
  virtual std::string GetSQLForAlterAction() const = 0;
};

// ALTER action for "SET OPTIONS ()" clause
class ASTSetOptionsAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SET_OPTIONS_ACTION;

  ASTSetOptionsAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTOptionsList* options_list() const { return options_list_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&options_list_);
  }

  const ASTOptionsList* options_list_ = nullptr;
};

// ALTER action for "SET AS" clause
class ASTSetAsAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SET_AS_ACTION;

  ASTSetAsAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTJSONLiteral* json_body() const { return json_body_; }
  const ASTStringLiteral* text_body() const { return text_body_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&json_body_, AST_JSON_LITERAL);
    fl.AddOptional(&text_body_, AST_STRING_LITERAL);
  }

  const ASTJSONLiteral* json_body_ = nullptr;
  const ASTStringLiteral* text_body_ = nullptr;
};

// ALTER table action for "ADD CONSTRAINT" clause
class ASTAddConstraintAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ADD_CONSTRAINT_ACTION;

  ASTAddConstraintAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_if_not_exists(bool value) { is_if_not_exists_ = value; }
  bool is_if_not_exists() const { return is_if_not_exists_; }

  const ASTTableConstraint* constraint() const { return constraint_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&constraint_);
  }

  const ASTTableConstraint* constraint_ = nullptr;
  bool is_if_not_exists_ = false;
};

// ALTER table action for "DROP PRIMARY KEY" clause
class ASTDropPrimaryKeyAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DROP_PRIMARY_KEY_ACTION;

  ASTDropPrimaryKeyAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  bool is_if_exists() const { return is_if_exists_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {}

  bool is_if_exists_ = false;
};

// ALTER table action for "DROP CONSTRAINT" clause
class ASTDropConstraintAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DROP_CONSTRAINT_ACTION;

  ASTDropConstraintAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  bool is_if_exists() const { return is_if_exists_; }

  const ASTIdentifier* constraint_name() const { return constraint_name_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&constraint_name_);
  }

  const ASTIdentifier* constraint_name_ = nullptr;
  bool is_if_exists_ = false;
};

// ALTER table action for "ALTER CONSTRAINT identifier [NOT] ENFORCED" clause
class ASTAlterConstraintEnforcementAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ALTER_CONSTRAINT_ENFORCEMENT_ACTION;

  ASTAlterConstraintEnforcementAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  bool is_if_exists() const { return is_if_exists_; }

  void set_is_enforced(bool enforced) { is_enforced_ = enforced; }
  bool is_enforced() const { return is_enforced_; }

  const ASTIdentifier* constraint_name() const { return constraint_name_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&constraint_name_);
  }

  const ASTIdentifier* constraint_name_ = nullptr;
  bool is_if_exists_ = false;
  bool is_enforced_ = true;
};

// ALTER table action for "ALTER CONSTRAINT identifier SET OPTIONS" clause
class ASTAlterConstraintSetOptionsAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ALTER_CONSTRAINT_SET_OPTIONS_ACTION;

  ASTAlterConstraintSetOptionsAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  bool is_if_exists() const { return is_if_exists_; }

  const ASTIdentifier* constraint_name() const { return constraint_name_; }
  const ASTOptionsList* options_list() const { return options_list_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&constraint_name_);
    fl.AddRequired(&options_list_);
  }

  const ASTIdentifier* constraint_name_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
  bool is_if_exists_ = false;
};

// ALTER table action for "ADD COLUMN" clause
class ASTAddColumnAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ADD_COLUMN_ACTION;

  ASTAddColumnAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_if_not_exists(bool value) { is_if_not_exists_ = value; }
  bool is_if_not_exists() const { return is_if_not_exists_; }

  const ASTColumnDefinition* column_definition() const {
    return column_definition_;
  }
  // Optional children.
  const ASTColumnPosition* column_position() const { return column_position_; }
  const ASTExpression* fill_expression() const { return fill_expression_; }


  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&column_definition_);
    fl.AddOptional(&column_position_, AST_COLUMN_POSITION);
    fl.AddOptionalExpression(&fill_expression_);
  }

  const ASTColumnDefinition* column_definition_ = nullptr;
  const ASTColumnPosition* column_position_ = nullptr;
  const ASTExpression* fill_expression_ = nullptr;
  bool is_if_not_exists_ = false;
};

// ALTER table action for "DROP COLUMN" clause
class ASTDropColumnAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DROP_COLUMN_ACTION;

  ASTDropColumnAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  bool is_if_exists() const { return is_if_exists_; }

  const ASTIdentifier* column_name() const { return column_name_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&column_name_);
  }

  const ASTIdentifier* column_name_ = nullptr;
  bool is_if_exists_ = false;
};

// ALTER table action for "RENAME COLUMN" clause
class ASTRenameColumnAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_RENAME_COLUMN_ACTION;

  ASTRenameColumnAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  bool is_if_exists() const { return is_if_exists_; }

  const ASTIdentifier* column_name() const { return column_name_; }
  const ASTIdentifier* new_column_name() const { return new_column_name_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&column_name_);
    fl.AddRequired(&new_column_name_);
  }

  const ASTIdentifier* column_name_ = nullptr;
  const ASTIdentifier* new_column_name_ = nullptr;
  bool is_if_exists_ = false;
};

// ALTER table action for "ALTER COLUMN SET TYPE" clause
class ASTAlterColumnTypeAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ALTER_COLUMN_TYPE_ACTION;

  ASTAlterColumnTypeAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  bool is_if_exists() const { return is_if_exists_; }

  const ASTIdentifier* column_name() const { return column_name_; }
  const ASTColumnSchema* schema() const { return schema_; }
  const ASTCollate* collate() const { return collate_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&column_name_);
    fl.AddRequired(&schema_);
    fl.AddOptional(&collate_, AST_COLLATE);
  }

  const ASTIdentifier* column_name_ = nullptr;
  const ASTColumnSchema* schema_ = nullptr;
  const ASTCollate* collate_ = nullptr;
  bool is_if_exists_ = false;
};

// ALTER table action for "ALTER COLUMN SET OPTIONS" clause
class ASTAlterColumnOptionsAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ALTER_COLUMN_OPTIONS_ACTION;

  ASTAlterColumnOptionsAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  bool is_if_exists() const { return is_if_exists_; }

  const ASTIdentifier* column_name() const { return column_name_; }
  const ASTOptionsList* options_list() const { return options_list_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&column_name_);
    fl.AddRequired(&options_list_);
  }

  const ASTIdentifier* column_name_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
  bool is_if_exists_ = false;
};

// ALTER table action for "ALTER COLUMN DROP NOT NULL" clause
class ASTAlterColumnDropNotNullAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ALTER_COLUMN_DROP_NOT_NULL_ACTION;

  ASTAlterColumnDropNotNullAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  bool is_if_exists() const { return is_if_exists_; }

  const ASTIdentifier* column_name() const { return column_name_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&column_name_);
  }

  const ASTIdentifier* column_name_ = nullptr;
  bool is_if_exists_ = false;
};

// ALTER ROW ACCESS POLICY action for "GRANT TO (<grantee_list>)" or "TO
// <grantee_list>" clause, also used by CREATE ROW ACCESS POLICY
class ASTGrantToClause final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_GRANT_TO_CLAUSE;

  ASTGrantToClause() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string GetSQLForAlterAction() const override;

  void set_has_grant_keyword_and_parens(bool value) {
    has_grant_keyword_and_parens_ = value;
  }
  bool has_grant_keyword_and_parens() const {
    return has_grant_keyword_and_parens_;
  }

  const ASTGranteeList* grantee_list() const { return grantee_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&grantee_list_);
  }

  const ASTGranteeList* grantee_list_ = nullptr;  // Required.
  bool has_grant_keyword_and_parens_ = false;
};

// ALTER ROW ACCESS POLICY action for "[FILTER] USING (<expression>)" clause,
// also used by CREATE ROW ACCESS POLICY
class ASTFilterUsingClause final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FILTER_USING_CLAUSE;

  ASTFilterUsingClause() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  void set_has_filter_keyword(bool value) { has_filter_keyword_ = value; }
  bool has_filter_keyword() const { return has_filter_keyword_; }

  const ASTExpression* predicate() const { return predicate_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&predicate_);
  }

  const ASTExpression* predicate_ = nullptr;  // Required.
  bool has_filter_keyword_ = false;
};

// ALTER ROW ACCESS POLICY action for "REVOKE FROM (<grantee_list>)|ALL" clause
class ASTRevokeFromClause final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_REVOKE_FROM_CLAUSE;

  ASTRevokeFromClause() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  void set_is_revoke_from_all(bool value) { is_revoke_from_all_ = value; }
  bool is_revoke_from_all() const { return is_revoke_from_all_; }

  const ASTGranteeList* revoke_from_list() const { return revoke_from_list_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&revoke_from_list_, AST_GRANTEE_LIST);
  }

  const ASTGranteeList* revoke_from_list_ = nullptr;  // Optional.
  bool is_revoke_from_all_ = false;
};

// ALTER ROW ACCESS POLICY action for "RENAME TO <new_name>" clause,
// and ALTER table action for "RENAME TO" clause.
class ASTRenameToClause final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_RENAME_TO_CLAUSE;

  ASTRenameToClause() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* new_name() const { return new_name_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&new_name_);
  }

  const ASTPathExpression* new_name_ = nullptr;  // Required
};

// ALTER action for "SET COLLATE ()" clause
class ASTSetCollateClause final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SET_COLLATE_CLAUSE;

  ASTSetCollateClause() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTCollate* collate() const { return collate_; }

  std::string GetSQLForAlterAction() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&collate_);
  }

  const ASTCollate* collate_ = nullptr;
};

class ASTAlterActionList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ALTER_ACTION_LIST;

  ASTAlterActionList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTAlterAction* const>& actions() const {
    return actions_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&actions_);
  }

  absl::Span<const ASTAlterAction* const> actions_;
};

// Common parent class for ALTER statement, e.g., ALTER TABLE/ALTER VIEW
class ASTAlterStatementBase : public ASTDdlStatement {
 public:
  explicit ASTAlterStatementBase(ASTNodeKind kind) : ASTDdlStatement(kind) {}

  // This adds the "if exists" modifier to the node name.
  std::string SingleNodeDebugString() const override;

  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  bool is_if_exists() const { return is_if_exists_; }

  const ASTPathExpression* path() const { return path_; }
  const ASTAlterActionList* action_list() const { return action_list_; }

  const ASTPathExpression* GetDdlTarget() const override { return path_; }
  bool IsAlterStatement() const override { return true; }

 protected:
  void InitPathAndAlterActions(FieldLoader* field_loader) {
    field_loader->AddRequired(&path_);
    field_loader->AddRequired(&action_list_);
  }

 private:
  const ASTPathExpression* path_ = nullptr;
  const ASTAlterActionList* action_list_ = nullptr;
  bool is_if_exists_ = false;
};

class ASTAlterDatabaseStatement final : public ASTAlterStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ALTER_DATABASE_STATEMENT;

  ASTAlterDatabaseStatement() : ASTAlterStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    InitPathAndAlterActions(&fl);
  }
};

class ASTAlterSchemaStatement final : public ASTAlterStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ALTER_SCHEMA_STATEMENT;

  ASTAlterSchemaStatement() : ASTAlterStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  const ASTCollate* collate() const { return collate_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    InitPathAndAlterActions(&fl);
    fl.AddOptional(&collate_, AST_COLLATE);
  }
  const ASTCollate* collate_ = nullptr;
};

class ASTAlterTableStatement final : public ASTAlterStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ALTER_TABLE_STATEMENT;

  ASTAlterTableStatement() : ASTAlterStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  const ASTCollate* collate() const { return collate_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    InitPathAndAlterActions(&fl);
    fl.AddOptional(&collate_, AST_COLLATE);
  }
  const ASTCollate* collate_ = nullptr;
};

class ASTAlterViewStatement final : public ASTAlterStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ALTER_VIEW_STATEMENT;

  ASTAlterViewStatement() : ASTAlterStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    InitPathAndAlterActions(&fl);
  }
};

class ASTAlterMaterializedViewStatement final : public ASTAlterStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ALTER_MATERIALIZED_VIEW_STATEMENT;

  ASTAlterMaterializedViewStatement()
      : ASTAlterStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    InitPathAndAlterActions(&fl);
  }
};

class ASTAlterRowAccessPolicyStatement final : public ASTAlterStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ALTER_ROW_ACCESS_POLICY_STATEMENT;

  ASTAlterRowAccessPolicyStatement()
      : ASTAlterStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required fields.
  const ASTIdentifier* name() const { return name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    InitPathAndAlterActions(&fl);
  }

  const ASTIdentifier* name_ = nullptr;  // Required
};

class ASTAlterAllRowAccessPoliciesStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ALTER_ALL_ROW_ACCESS_POLICIES_STATEMENT;

  ASTAlterAllRowAccessPoliciesStatement()
      : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTAlterAction* alter_action() const {
    return alter_action_;
  }
  const ASTPathExpression* table_name_path() const { return table_name_path_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&table_name_path_);
    fl.AddRequired(&alter_action_);
  }

  const ASTAlterAction* alter_action_ = nullptr;  // Required
  const ASTPathExpression* table_name_path_ = nullptr;  // Required
};

class ASTAlterEntityStatement final : public ASTAlterStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ALTER_ENTITY_STATEMENT;

  ASTAlterEntityStatement() : ASTAlterStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* type() const { return type_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&type_);
    InitPathAndAlterActions(&fl);
  }

  const ASTIdentifier* type_ = nullptr;
};

class ASTForeignKeyActions final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FOREIGN_KEY_ACTIONS;

  ASTForeignKeyActions() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  enum Action { NO_ACTION, RESTRICT, CASCADE, SET_NULL };

  void set_update_action(Action action) { update_action_ = action; }
  const Action update_action() const { return update_action_; }
  void set_delete_action(Action action) { delete_action_ = action; }
  const Action delete_action() const { return delete_action_; }

  static std::string GetSQLForAction(Action action);

 private:
  void InitFields() final {
    FieldLoader fl(this);
  }

  Action update_action_ = NO_ACTION;
  Action delete_action_ = NO_ACTION;
};

class ASTForeignKeyReference final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FOREIGN_KEY_REFERENCE;

  ASTForeignKeyReference() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  enum Match { SIMPLE, FULL, NOT_DISTINCT };

  void set_match(Match match) { match_ = match; }
  const Match match() const { return match_; }

  void set_enforced(bool enforced) { enforced_ = enforced; }
  bool enforced() const { return enforced_; }

  const ASTPathExpression* table_name() const { return table_name_; }
  const ASTColumnList* column_list() const { return column_list_; }
  const ASTForeignKeyActions* actions() const { return actions_; }

  std::string GetSQLForMatch() const;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&table_name_);
    fl.AddRequired(&column_list_);
    fl.AddRequired(&actions_);
  }

  const ASTPathExpression* table_name_ = nullptr;
  const ASTColumnList* column_list_ = nullptr;
  const ASTForeignKeyActions* actions_ = nullptr;
  Match match_ = SIMPLE;
  bool enforced_ = true;
};

// A top-level script.
class ASTScript final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SCRIPT;

  ASTScript() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTStatementList* statement_list_node() const {
    return statement_list_;
  }

  absl::Span<const ASTStatement* const> statement_list() const {
    return statement_list_->statement_list();
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&statement_list_);
  }
  const ASTStatementList* statement_list_ = nullptr;
};

// Represents an ELSEIF clause in an IF statement.
class ASTElseifClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ELSEIF_CLAUSE;

  ASTElseifClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required fields
  const ASTExpression* condition() const { return condition_; }
  const ASTStatementList* body() const { return body_; }

  // Returns the ASTIfStatement that this ASTElseifClause belongs to.
  const ASTIfStatement* if_stmt() const {
    return parent()->parent()->GetAsOrDie<ASTIfStatement>();
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&condition_);
    fl.AddRequired(&body_);
  }

  const ASTExpression* condition_ = nullptr;  // Required
  const ASTStatementList* body_ = nullptr;    // Required
};

// Represents a list of ELSEIF clauses.  Note that this list is never empty,
// as the grammar will not create an ASTElseifClauseList object unless there
// exists at least one ELSEIF clause.
class ASTElseifClauseList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ELSEIF_CLAUSE_LIST;

  ASTElseifClauseList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTElseifClause* const>& elseif_clauses() const {
    return elseif_clauses_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&elseif_clauses_);
  }
  absl::Span<const ASTElseifClause* const> elseif_clauses_;
};

class ASTIfStatement final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_IF_STATEMENT;

  ASTIfStatement() : ASTScriptStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required fields.
  const ASTExpression* condition() const { return condition_; }
  const ASTStatementList* then_list() const { return then_list_; }

  // Optional; nullptr if no ELSEIF clauses are specified.  If present, the
  // list will never be empty.
  const ASTElseifClauseList* elseif_clauses() const { return elseif_clauses_; }

  // Optional; nullptr if no ELSE clause is specified
  const ASTStatementList* else_list() const { return else_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&condition_);
    fl.AddRequired(&then_list_);
    fl.AddOptional(&elseif_clauses_, AST_ELSEIF_CLAUSE_LIST);
    fl.AddOptional(&else_list_, AST_STATEMENT_LIST);
  }

  const ASTExpression* condition_ = nullptr;     // Required
  const ASTStatementList* then_list_ = nullptr;  // Required
  const ASTElseifClauseList* elseif_clauses_ = nullptr;  // Required
  const ASTStatementList* else_list_ = nullptr;  // Optional
};

// Represents a WHEN...THEN clause in a CASE statement.
class ASTWhenThenClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WHEN_THEN_CLAUSE;

  ASTWhenThenClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required fields
  const ASTExpression* condition() const { return condition_; }
  const ASTStatementList* body() const { return body_; }

  // Returns the ASTCaseNoPrefixStatement/ASTCaseWithPrefixStatement that this
  // ASTWhenThenClause belongs to.
  const ASTScriptStatement* case_stmt() const {
    return parent()->parent()->GetAsOrDie<ASTScriptStatement>();
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&condition_);
    fl.AddRequired(&body_);
  }

  const ASTExpression* condition_ = nullptr;  // Required
  const ASTStatementList* body_ = nullptr;    // Required
};

// Represents a list of WHEN...THEN clauses. Note that this list is never empty,
// as the grammar mandates that there is at least one WHEN...THEN clause in
// a CASE statement.
class ASTWhenThenClauseList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WHEN_THEN_CLAUSE_LIST;

  ASTWhenThenClauseList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTWhenThenClause* const>& when_then_clauses() const {
    return when_then_clauses_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&when_then_clauses_);
  }
  absl::Span<const ASTWhenThenClause* const> when_then_clauses_;
};

class ASTCaseStatement final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CASE_STATEMENT;

  ASTCaseStatement() : ASTScriptStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required field.
  const ASTWhenThenClauseList* when_then_clauses() const {
    return when_then_clauses_;
  }

  // Optional; nullptr if not specified
  const ASTExpression* expression() const { return expression_; }
  const ASTStatementList* else_list() const { return else_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptionalExpression(&expression_);
    fl.AddRequired(&when_then_clauses_);
    fl.AddOptional(&else_list_, AST_STATEMENT_LIST);
  }

  const ASTExpression* expression_ = nullptr;     // Optional
  const ASTWhenThenClauseList* when_then_clauses_ = nullptr;  // Required
  const ASTStatementList* else_list_ = nullptr;  // Optional
};

class ASTRaiseStatement final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_RAISE_STATEMENT;

  ASTRaiseStatement() : ASTScriptStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* message() const { return message_; }

  // A RAISE statement rethrows an existing exception, as opposed to creating
  // a new exception, when none of the properties are set.  Currently, the only
  // property is the message.  However, for future proofing, as more properties
  // get added to RAISE later, code should call this function to check for a
  // rethrow, rather than checking for the presence of a message, directly.
  bool is_rethrow() const { return message_ == nullptr; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptionalExpression(&message_);
  }
  const ASTExpression* message_ = nullptr;  // Optional
};

class ASTExceptionHandler final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_EXCEPTION_HANDLER;

  ASTExceptionHandler() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required field; even an empty block still contains an empty statement list.
  const ASTStatementList* statement_list() const { return statement_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&statement_list_);
  }
  const ASTStatementList* statement_list_ = nullptr;
};

// Represents a list of exception handlers in a block.  Currently restricted
// to one element, but may contain multiple elements in the future, once there
// are multiple error codes for a block to catch.
class ASTExceptionHandlerList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_EXCEPTION_HANDLER_LIST;

  ASTExceptionHandlerList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  absl::Span<const ASTExceptionHandler* const> exception_handler_list() const {
    return exception_handler_list_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&exception_handler_list_);
  }

  absl::Span<const ASTExceptionHandler* const> exception_handler_list_;
};

class ASTBeginEndBlock final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_BEGIN_END_BLOCK;
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  ASTBeginEndBlock() : ASTScriptStatement(kConcreteNodeKind) {}

  const ASTStatementList* statement_list_node() const {
    return statement_list_node_;
  }

  absl::Span<const ASTStatement* const> statement_list() const {
    return statement_list_node_->statement_list();
  }

  bool has_exception_handler() const {
    return handler_list_ != nullptr &&
           !handler_list_->exception_handler_list().empty();
  }

  // Optional; nullptr indicates a BEGIN block without an EXCEPTION clause.
  const ASTExceptionHandlerList* handler_list() const { return handler_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&statement_list_node_);
    fl.AddOptional(&handler_list_, AST_EXCEPTION_HANDLER_LIST);
  }
  // Required, never null.
  const ASTStatementList* statement_list_node_ = nullptr;

  const ASTExceptionHandlerList* handler_list_ = nullptr;  // Optional
};

class ASTIdentifierList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_IDENTIFIER_LIST;

  ASTIdentifierList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Guaranteed by the parser to never be empty.
  absl::Span<const ASTIdentifier* const> identifier_list() const {
    return identifier_list_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&identifier_list_);
  }
  absl::Span<const ASTIdentifier* const> identifier_list_;
};

class ASTVariableDeclaration final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_VARIABLE_DECLARATION;

  ASTVariableDeclaration() : ASTScriptStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required fields
  const ASTIdentifierList* variable_list() const { return variable_list_; }

  // Optional fields; at least one of <type> and <default_value> must be
  // present.
  const ASTType* type() const { return type_; }
  const ASTExpression* default_value() const { return default_value_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&variable_list_);
    fl.AddOptionalType(&type_);
    fl.AddOptionalExpression(&default_value_);
  }
  const ASTIdentifierList* variable_list_ = nullptr;
  const ASTType* type_ = nullptr;
  const ASTExpression* default_value_ = nullptr;  // Optional
};

// Base class for all loop statements (loop/end loop, while, foreach, etc.).
// Every loop has a body.
class ASTLoopStatement : public ASTScriptStatement {
 public:
  explicit ASTLoopStatement(ASTNodeKind kind) : ASTScriptStatement(kind) {}

  // Required fields
  const ASTStatementList* body() const { return body_; }

  bool IsLoopStatement() const override { return true; }

 protected:
  void InitBodyField(FieldLoader* field_loader) {
    field_loader->AddRequired(&body_);
  }

 private:
  const ASTStatementList* body_ = nullptr;
};

// Represents either:
//  - LOOP...END LOOP (if condition is nullptr).  This is semantically
//      equivelant to WHILE(true)...END WHILE.
//  - WHILE...END WHILE (if condition is not nullptr)
//
class ASTWhileStatement final : public ASTLoopStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WHILE_STATEMENT;

  ASTWhileStatement() : ASTLoopStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // The <condition> is optional.  A null <condition> indicates a
  // LOOP...END LOOP construct.
  const ASTExpression* condition() const { return condition_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptionalExpression(&condition_);
    InitBodyField(&fl);
  }

  const ASTExpression* condition_ = nullptr;
};

// Represents UNTIL in a REPEAT statement.
class ASTUntilClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_UNTIL_CLAUSE;

  ASTUntilClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Returns the ASTRepeatStatement that this ASTUntilClause belongs to.
  const ASTRepeatStatement* repeat_stmt() const {
    return parent()->GetAsOrDie<ASTRepeatStatement>();
  }

  // Required field
  const ASTExpression* condition() const { return condition_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&condition_);
  }

  const ASTExpression* condition_ = nullptr;
};

// Represents the statement REPEAT...UNTIL...END REPEAT.
// This is conceptually also called do-while.
class ASTRepeatStatement final : public ASTLoopStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_REPEAT_STATEMENT;

  ASTRepeatStatement() : ASTLoopStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required field.
  const ASTUntilClause* until_clause() const { return until_clause_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    InitBodyField(&fl);
    fl.AddRequired(&until_clause_);
  }

  const ASTUntilClause* until_clause_ = nullptr;
};

// Represents the statement FOR...IN...DO...END FOR.
// This is conceptually also called for-each.
class ASTForInStatement final : public ASTLoopStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FOR_IN_STATEMENT;

  ASTForInStatement() : ASTLoopStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* variable() const { return variable_; }
  const ASTQuery* query() const { return query_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&variable_);
    fl.AddRequired(&query_);
    InitBodyField(&fl);
  }

  const ASTIdentifier* variable_ = nullptr;
  const ASTQuery* query_ = nullptr;
};

// Base class shared by break and continue statements.
class ASTBreakContinueStatement : public ASTScriptStatement {
 public:
  enum BreakContinueKeyword { BREAK, LEAVE, CONTINUE, ITERATE };

  ASTBreakContinueStatement(ASTNodeKind node_kind, BreakContinueKeyword keyword)
      : ASTScriptStatement(node_kind), keyword_(keyword) {}

  void set_keyword(BreakContinueKeyword keyword) { keyword_ = keyword; }
  BreakContinueKeyword keyword() const { return keyword_; }

  // Returns text representing the keyword used for this BREAK/CONINUE
  // statement.  All letters are in uppercase.
  absl::string_view GetKeywordText() const {
    switch (keyword_) {
      case BREAK:
        return "BREAK";
      case LEAVE:
        return "LEAVE";
      case CONTINUE:
        return "CONTINUE";
      case ITERATE:
        return "ITERATE";
    }
  }

 private:
  void InitFields() final {}
  BreakContinueKeyword keyword_;
};

class ASTBreakStatement final : public ASTBreakContinueStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_BREAK_STATEMENT;

  ASTBreakStatement() : ASTBreakContinueStatement(kConcreteNodeKind, BREAK) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
};

class ASTContinueStatement final : public ASTBreakContinueStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CONTINUE_STATEMENT;

  ASTContinueStatement()
      : ASTBreakContinueStatement(kConcreteNodeKind, CONTINUE) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
};

class ASTReturnStatement final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_RETURN_STATEMENT;

  ASTReturnStatement() : ASTScriptStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {}
};

// A statement which assigns to a single variable from an expression.
// Example:
//   SET x = 3;
class ASTSingleAssignment final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SINGLE_ASSIGNMENT;

  ASTSingleAssignment() : ASTScriptStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* variable() const { return variable_; }
  const ASTExpression* expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&variable_);
    fl.AddRequired(&expression_);
  }

  const ASTIdentifier* variable_ = nullptr;
  const ASTExpression* expression_ = nullptr;
};

// A statement which assigns to a query parameter from an expression.
// Example:
//   SET @x = 3;
class ASTParameterAssignment final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_PARAMETER_ASSIGNMENT;

  ASTParameterAssignment() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTParameterExpr* parameter() const { return parameter_; }
  const ASTExpression* expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&parameter_);
    fl.AddRequired(&expression_);
  }

  const ASTParameterExpr* parameter_ = nullptr;
  const ASTExpression* expression_ = nullptr;
};

// A statement which assigns to a system variable from an expression.
// Example:
//   SET @@x = 3;
class ASTSystemVariableAssignment final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_SYSTEM_VARIABLE_ASSIGNMENT;

  ASTSystemVariableAssignment() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTSystemVariableExpr* system_variable() const {
    return system_variable_;
  }
  const ASTExpression* expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&system_variable_);
    fl.AddRequired(&expression_);
  }

  const ASTSystemVariableExpr* system_variable_ = nullptr;
  const ASTExpression* expression_ = nullptr;
};

// A statement which assigns multiple variables to fields in a struct,
// which each variable assigned to one field.
// Example:
//   SET (x, y) = (5, 10);
class ASTAssignmentFromStruct final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ASSIGNMENT_FROM_STRUCT;

  ASTAssignmentFromStruct() : ASTScriptStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifierList* variables() const { return variables_; }
  const ASTExpression* struct_expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&variables_);
    fl.AddRequired(&expression_);
  }

  const ASTIdentifierList* variables_ = nullptr;  // Required, not NULL
  const ASTExpression* expression_ = nullptr;  // Required, not NULL
};

class ASTExecuteIntoClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_EXECUTE_INTO_CLAUSE;

  ASTExecuteIntoClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifierList* identifiers() const { return identifiers_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&identifiers_);
  }

  const ASTIdentifierList* identifiers_ = nullptr;
};

class ASTExecuteUsingArgument final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_EXECUTE_USING_ARGUMENT;

  ASTExecuteUsingArgument() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }
  // Optional. Absent if this argument is positional. Present if it is named.
  const ASTAlias* alias() const { return alias_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
    fl.AddOptional(&alias_, AST_ALIAS);
  }

  const ASTExpression* expression_ = nullptr;
  const ASTAlias* alias_ = nullptr;
};

class ASTExecuteUsingClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_EXECUTE_USING_CLAUSE;

  ASTExecuteUsingClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  absl::Span<const ASTExecuteUsingArgument* const> arguments() const {
    return arguments_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&arguments_);
  }

  absl::Span<const ASTExecuteUsingArgument* const> arguments_;
};

class ASTExecuteImmediateStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_EXECUTE_IMMEDIATE_STATEMENT;

  ASTExecuteImmediateStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* sql() const { return sql_; }
  const ASTExecuteIntoClause* into_clause() const { return into_clause_; }
  const ASTExecuteUsingClause* using_clause() const { return using_clause_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&sql_);
    fl.AddOptional(&into_clause_, AST_EXECUTE_INTO_CLAUSE);
    fl.AddOptional(&using_clause_, AST_EXECUTE_USING_CLAUSE);
  }

  const ASTExpression* sql_ = nullptr;
  const ASTExecuteIntoClause* into_clause_ = nullptr;
  const ASTExecuteUsingClause* using_clause_ = nullptr;
};

inline IdString ASTAlias::GetAsIdString() const {
  return identifier()->GetAsIdString();
}

namespace parse_tree_internal {

// Concrete types (the 'leaves' of the hierarchy) must be constructible.
template <typename T>
using EnableIfConcrete =
    typename std::enable_if<std::is_constructible<T>::value, int>::type;

// Non Concrete types (internal nodes) of the hierarchy must _not_ be
// constructible.
template <typename T>
using EnableIfNotConcrete =
    typename std::enable_if<!std::is_constructible<T>::value, int>::type;

// GetAsOrNull implementation optimized for concrete types.  We assume that all
// concrete types define:
//   static constexpr Type kConcreteNodeKind;
//
// This allows us to avoid invoking dynamic_cast.
template <typename T, typename MaybeConstRoot, EnableIfConcrete<T> = 0>
inline T* GetAsOrNullImpl(MaybeConstRoot* n) {
  if (n->node_kind() == T::kConcreteNodeKind) {
    return static_cast<T*>(n);
  } else {
    return nullptr;
  }
}

// GetAsOrNull implemented simply with dynamic_cast.  This is used for
// intermediate nodes (such as ASTExpression).
template <typename T, typename MaybeConstRoot, EnableIfNotConcrete<T> = 0>
inline T* GetAsOrNullImpl(MaybeConstRoot* r) {
  // Note, if this proves too slow, it could be implemented with ancestor enums
  // sets.
  return dynamic_cast<T*>(r);
}

}  // namespace parse_tree_internal

template <typename NodeType>
inline const NodeType* ASTNode::GetAsOrNull() const {
  static_assert(std::is_base_of<ASTNode, NodeType>::value,
                "NodeType must be a member of the ASTNode class hierarchy");
  return parse_tree_internal::GetAsOrNullImpl<const NodeType, const ASTNode>(
      this);
}

template <typename NodeType>
inline NodeType* ASTNode::GetAsOrNull() {
  static_assert(std::is_base_of<ASTNode, NodeType>::value,
                "NodeType must be a member of the ASTNode class hierarchy");
  return parse_tree_internal::GetAsOrNullImpl<NodeType, ASTNode>(this);
}

}  // namespace zetasql

#endif  // ZETASQL_PARSER_PARSE_TREE_MANUAL_H_
