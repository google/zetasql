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

#ifndef ZETASQL_ANALYZER_RESOLVER_H_
#define ZETASQL_ANALYZER_RESOLVER_H_

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <stack>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "zetasql/base/atomic_sequence_num.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/analyzer/column_cycle_detector.h"
#include "zetasql/analyzer/container_hash_equals.h"
#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/deprecation_warning.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/base/case.h"
#include "absl/base/attributes.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "absl/types/variant.h"

namespace zetasql {

class FunctionArgumentInfo;
class FunctionResolver;
class QueryResolutionInfo;
class SelectColumnStateList;
class ExtendedCompositeCastEvaluator;
struct ColumnReplacements;
struct OrderByItemInfo;
struct SelectColumnState;

// This class contains most of the implementation of ZetaSQL analysis.
// The functions here generally traverse the AST nodes recursively,
// constructing and returning the Resolved AST nodes bottom-up.  For
// a more detailed overview, see (broken link).
// Not thread-safe.
//
// NOTE: Because this class is so large, the implementation is split up
// by category across multiple cc files:
//   resolver.cc            Common and shared methods
//   resolver_alter_stmt.cc ALTER TABLE statements
//   resolver_dml.cc        DML
//   resolver_expr.cc       Expressions
//   resolver_query.cc      SELECT statements, things that make Scans
//   resolver_stmt.cc       Statements (except DML)
class Resolver {
 public:
  // <*analyzer_options> should outlive the constructed Resolver. It must have
  // all arenas initialized.
  Resolver(Catalog* catalog, TypeFactory* type_factory,
           const AnalyzerOptions* analyzer_options);
  Resolver(const Resolver&) = delete;
  Resolver& operator=(const Resolver&) = delete;
  ~Resolver();

  // Resolve a parsed ASTStatement to a ResolvedStatement.
  // This fails if the statement is not of a type accepted by
  // LanguageOptions.SupportsStatementKind().
  // <sql> contains the text at which the ASTStatement points.
  absl::Status ResolveStatement(
      absl::string_view sql, const ASTStatement* statement,
      std::unique_ptr<const ResolvedStatement>* output);

  // Resolve a standalone expression outside a query.
  // <sql> contains the text at which the ASTExpression points.
  absl::Status ResolveStandaloneExpr(
      absl::string_view sql, const ASTExpression* ast_expr,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Resolve a parsed ASTExpression to a ResolvedExpr in the context of a
  // function call. Unlike ResolveExpr, this method accepts maps from the
  // argument names to their types for arguments in <function_arguments>.
  // <expr_resolution_info> is used for resolving the function call.
  //
  // TODO: Provide an overload that takes a FunctionArgumentInfo
  //     directly and deprecate this one.
  absl::Status ResolveExprWithFunctionArguments(
      absl::string_view sql, const ASTExpression* ast_expr,
      IdStringHashMapCase<std::unique_ptr<ResolvedArgumentRef>>*
          function_arguments,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* output);

  // Resolve the ASTQueryStatement associated with a SQL TVF.  The TVF's
  // arguments are passed in through <function_arguments> (for scalar arguments)
  // and <function_table_arguments> (for table-valued arguments). Takes
  // ownership of all pointers in these arguments. If <specified_output_schema>
  // is present, calls the CheckSQLBodyReturnTypesAndCoerceIfNeeded method to
  // enforce that the schema returned by the function body matches the expected
  // schema, adding a coercion or returning an error if necessary.
  //
  // TODO: Provide an overload that takes a FunctionArgumentInfo
  //     directly and deprecate this one.
  absl::Status ResolveQueryStatementWithFunctionArguments(
      absl::string_view sql, const ASTQueryStatement* query_stmt,
      const absl::optional<TVFRelation>& specified_output_schema,
      bool allow_query_parameters,
      IdStringHashMapCase<std::unique_ptr<ResolvedArgumentRef>>*
          function_arguments,
      IdStringHashMapCase<TVFRelation>* function_table_arguments,
      std::unique_ptr<const ResolvedStatement>* output_stmt,
      std::shared_ptr<const NameList>* output_name_list);

  // If a CREATE TABLE FUNCTION statement contains RETURNS TABLE to explicitly
  // specify the output schema for the function's output table, this method
  // compares it against the schema actually returned by the SQL body (if
  // present).
  //
  // If the required schema includes a column name that is not returned from the
  // SQL body, or the matching column name has a type that is not equal or
  // implicitly coercible to the required type, this method returns an error.
  // Note that the column order is not relevant: this method matches the columns
  // in the explicitly-specified schema against the output columns if the query
  // in the SQL body by name.
  //
  // Otherwise, if the names and types of columns do not match exactly, this
  // method adds a new projection to perform the necessary type coercion and/or
  // column dropping so that the names and types match from the result of the
  // projection.
  //
  // If the explicitly-specified schema is a value table, then this method only
  // checks that the query in the SQL body returns one column of a type that is
  // equal or implicitly coercible to the value-table type.
  absl::Status CheckSQLBodyReturnTypesAndCoerceIfNeeded(
      const ASTNode* statement_location,
      const TVFRelation& return_tvf_relation,
      const NameList* tvf_body_name_list,
      std::unique_ptr<const ResolvedScan>* resolved_query,
      std::vector<std::unique_ptr<const ResolvedOutputColumn>>*
          resolved_output_column_list);

  // Generate a customer error message when coercion is found to not be allowed
  // in one of the CoerceExprTo functions.
  using CoercionErrorMessageFunction = std::function<std::string(
      absl::string_view target_type_name, absl::string_view actual_type_name)>;

  // The different kinds of coercion supported by CoerceExprTo function family.
  // Using an enum makes calls to these functions clear and concise.
  enum CoercionMode {
    kImplicitAssignment,
    kExplicitCoercion,
    kImplicitCoercion,
  };

  // Given a resolved expression <resolved_expr>, along with the AST that
  // generated it (<ast_location>), coerces the expression to <target_type>,
  // replacing <resolved_expr> with the modified result. If the expression is
  // already the correct type, it is simply left in place, without modification.
  // If the expression cannot be coerced, an error is emitted. Errors are
  // returned with InternalErrorLocation.
  //
  // <kind> configures the Coercer to signal which kind of coercion is should
  // validate.
  //
  // The <make_error> function is called to generate an error message strinng if
  // <resolved_expr> is not coercible to <target_type> using coercion mode
  // <mode>. Two more overloads of this function take simple error message
  // template or no error message argument (in which case a generic template is
  // used).
  ABSL_DEPRECATED(
      "Use CoerceExprToType function with <annotated_target_type> argument.")
  // TODO: Refactor and remove the deprecated function in a quick
  // follow up.
  absl::Status CoerceExprToType(
      const ASTNode* ast_location, const Type* target_type, CoercionMode mode,
      CoercionErrorMessageFunction make_error,
      std::unique_ptr<const ResolvedExpr>* resolved_expr) const;

  // Same as the previous method but <annotated_target_type> is used to contain
  // both target type and its annotation information.
  absl::Status CoerceExprToType(
      const ASTNode* ast_location, AnnotatedType annotated_target_type,
      CoercionMode mode, CoercionErrorMessageFunction make_error,
      std::unique_ptr<const ResolvedExpr>* resolved_expr) const;

  // Similar to the above function, but provides an error message template
  // instead of an error message construction function. When only the type names
  // of the <target_type> and the argument (<resolved_expr>) type are needed,
  // this is more concise than specifying a full function. The name of
  // <target_type> will replace '$0' and the name of the argument type will
  // replace $1.
  ABSL_DEPRECATED(
      "Use CoerceExprToType function with <annotated_target_type> argument.")
  absl::Status CoerceExprToType(
      const ASTNode* ast_location, const Type* target_type, CoercionMode mode,
      absl::string_view error_template,
      std::unique_ptr<const ResolvedExpr>* resolved_expr) const;

  // Same as the previous method but <annotated_target_type> is used to contain
  // both target type and its annotation information.
  absl::Status CoerceExprToType(
      const ASTNode* ast_location, AnnotatedType annotated_target_type,
      CoercionMode mode, absl::string_view error_template,
      std::unique_ptr<const ResolvedExpr>* resolved_expr) const;

  // Similar to CoerceExprToType above but using a generic error message when
  // <resolved_expr> cannot be coerced to <target_type>.
  ABSL_DEPRECATED(
      "Use CoerceExprToType function with <annotated_target_type> argument.")
  absl::Status CoerceExprToType(
      const ASTNode* ast_location, const Type* target_type,  CoercionMode mode,
      std::unique_ptr<const ResolvedExpr>* resolved_expr) const;

  // Same as the previous method but <annotated_target_type> is used to contain
  // both target type and its annotation information.
  absl::Status CoerceExprToType(
      const ASTNode* ast_location, AnnotatedType annotated_target_type,
      CoercionMode mode,
      std::unique_ptr<const ResolvedExpr>* resolved_expr) const;

  // Similar to the above function, but coerces to BOOL type.
  // There is no <assignment_semantics> parameter, since assignment semantics
  // do not matter when coercing to type BOOL.
  absl::Status CoerceExprToBool(
      const ASTNode* ast_location, absl::string_view clause_name,
      std::unique_ptr<const ResolvedExpr>* resolved_expr) const;

  // Resolve the Type from the <type_name>.
  absl::Status ResolveTypeName(const std::string& type_name, const Type** type);

  // Resolve the Type and TypeParameters from the <type_name>.
  absl::Status ResolveTypeName(const std::string& type_name, const Type** type,
                               TypeParameters* type_params);

  // DEPRECATED: WILL BE REMOVED SOON
  // Attempt to coerce <scan>'s output types to those in <types> using
  // assignment coercion semantics.
  // If no coercion is needed, then <scan> and <output_name_list> are left
  // unmodified.
  // Otherwise, a new projection will be added to <scan> which will perform
  // the necessary type coercions. <output_name_list> will also be updated
  // to match the new <scan>.
  absl::Status CoerceQueryStatementResultToTypes(
      const ASTNode* ast_node, absl::Span<const Type* const> types,
      std::unique_ptr<const ResolvedScan>* scan,
      std::shared_ptr<const NameList>* output_name_list);

  // Return vector of warnings generated by the last input analyzed. These have
  // DeprecationWarning protos attached to them.
  const std::vector<absl::Status>& deprecation_warnings() const {
    return deprecation_warnings_;
  }

  // Return undeclared parameters found the query, and their inferred types.
  const QueryParametersMap& undeclared_parameters() const {
    return undeclared_parameters_;
  }

  // Returns undeclared positional parameters found the query and their inferred
  // types. The index in the vector corresponds with the position of the
  // undeclared parameter--for example, the first element in the vector is the
  // type of the undeclared parameter at position 1 and so on.
  const std::vector<const Type*>& undeclared_positional_parameters() const {
    return undeclared_positional_parameters_;
  }

  const AnalyzerOptions& analyzer_options() const { return analyzer_options_; }
  const LanguageOptions& language() const {
    return analyzer_options_.language();
  }

  const AnalyzerOutputProperties& analyzer_output_properties() const {
    return analyzer_output_properties_;
  }

  // Returns the highest column id that has been allocated.
  int max_column_id() const { return max_column_id_; }

  // Clear state so this can be used to resolve a second statement whose text
  // is contained in <sql>.
  void Reset(absl::string_view sql);

 private:
  // Case-insensitive map of a column name to its position in a list of columns.
  typedef std::map<IdString, int, IdStringCaseLess> ColumnIndexMap;

  // These indicate arguments that require special treatment during resolution,
  // and are related to special syntaxes in the grammar.  The grammar should
  // enforce that the corresponding argument will have the expected ASTNode
  // type.
  enum class SpecialArgumentType {
    // INTERVAL indicates the function argument is an interval, like
    // INTERVAL 5 YEAR.  This is one ASTIntervalExpr node in the AST input, and
    // will be resolved into two arguments, the numeric value ResolvedExpr and
    // the DateTimestampPart enum ResolvedLiteral.
    INTERVAL,

    // DATEPART indicates the function argument is a date part keyword like
    // YEAR.  This is an ASTIdentifier or ASTPathExpression node in the AST
    // input, and will be resolved to a DateTimestampPart enum ResolvedLiteral
    // argument.
    DATEPART,

    // NORMALIZE_MODE indicates that function argument is a normalization
    // mode keyword like NFC. This is an ASTIdentifier node in the AST
    // input, and will be resolved to a NormalizeMode enum ResolvedLiteral
    // argument.
    NORMALIZE_MODE,
  };

  enum class PartitioningKind { PARTITION_BY, CLUSTER_BY };

  static const std::map<int, SpecialArgumentType>* const
      kEmptyArgumentOptionMap;

  // Defined in resolver_query.cc.
  static const IdString& kArrayId;
  static const IdString& kOffsetAlias;
  static const IdString& kWeightAlias;
  static const IdString& kArrayOffsetId;
  static const IdString& kLambdaArgId;
  static const IdString& kWithActionId;

  // Input SQL query text. Set before resolving a statement, expression or
  // type.
  absl::string_view sql_;

  Catalog* catalog_;

  // Internal catalog for looking up system variables.  Content is imported
  // directly from analyzer_options_.system_variables().  This field is
  // initially set to nullptr, and is initialized the first time we encounter a
  // reference to a system variable.
  std::unique_ptr<Catalog> system_variables_catalog_;

  TypeFactory* type_factory_;
  const AnalyzerOptions& analyzer_options_;  // Not owned.
  Coercer coercer_;

  // Shared constant for an empty NameList and NameScope.
  const std::shared_ptr<const NameList> empty_name_list_;
  const std::unique_ptr<NameScope> empty_name_scope_;

  // For resolving functions.
  std::unique_ptr<FunctionResolver> function_resolver_;

  // Pool where IdStrings are allocated.  Copied from AnalyzerOptions.
  IdStringPool* const id_string_pool_;

  // Next unique column_id to allocate.  Pointer may come from AnalyzerOptions.
  zetasql_base::SequenceNumber* next_column_id_sequence_ = nullptr;  // Not owned.
  std::unique_ptr<zetasql_base::SequenceNumber> owned_column_id_sequence_;
  int max_column_id_ = 0;

  // Next unique subquery ID to allocate. Used for display only.
  int next_subquery_id_;

  // Next unique unnest ID to allocate. Used for display only.
  int next_unnest_id_;

  // True if we are analyzing a standalone expression rather than a statement.
  bool analyzing_expression_;

  // Either "PARTITION BY" or "CLUSTER BY" if we are analyzing one of those
  // clauses inside a DDL statement. Used for the error message if we encounter
  // an unsupported expression in the clause.
  const char* analyzing_partition_by_clause_name_ = nullptr;

  // If not empty, we are analyzing a clause that disallows query parameters,
  // such as SQL function body and view body; when encountering query
  // parameters, this field will be used as the error message.
  absl::string_view disallowing_query_parameters_with_error_;

  // For generated columns, 'cycle_detector_' is used for detecting cycles
  // between columns in a create table statement.
  // When 'generated_column_cycle_detector_' is not null,
  // Resolver::ResolvePathExpressionAsExpression() calls
  // 'cycle_detector_->AddDependencyOn(x)' whenever
  // it resolves a column 'x'.
  // The pointer will contain a local variable set in
  // Resolver::ResolveColumnDefinitionList().
  ColumnCycleDetector* generated_column_cycle_detector_ = nullptr;
  // When 'generated_column_cycle_detector_' is not null and
  // ResolvePathExpressionAsExpression() fails to resolve a column, this stores
  // the column name in 'unresolved_column_name_in_generated_column_'. A higher
  // layer can then detect that the generated column it was attempting to
  // resolve has a dependency on 'unresolved_column_name_in_generated_column_'.
  IdString unresolved_column_name_in_generated_column_;

  // True if we are analyzing an expression that is stored and non volatile,
  // either as a generated table column or as an expression stored in an index.
  bool analyzing_nonvolatile_stored_expression_columns_;

  // True if we are analyzing check constraint expression.
  bool analyzing_check_constraint_expression_;

  // When analyzing columns with a default value expression, set to the
  // NameScope containing all column names of the table being analyzed. This is
  // used to generate better error messages when the expression accesses a table
  // column.
  absl::optional<const NameScope*> default_expr_access_error_name_scope_;

  AnalyzerOutputProperties analyzer_output_properties_;

  // Store list of named subqueries currently visible.
  // This is updated as we traverse the query to implement scoping of
  // WITH subquery names.
  struct NamedSubquery {
    NamedSubquery(IdString unique_alias_in, bool is_recursive_in,
                  const ResolvedColumnList& column_list_in,
                  const std::shared_ptr<const NameList>& name_list_in)
        : unique_alias(unique_alias_in),
          is_recursive(is_recursive_in),
          column_list(column_list_in),
          name_list(name_list_in) {}

    NamedSubquery(const NamedSubquery&) = delete;
    NamedSubquery& operator=(const NamedSubquery&) = delete;

    // The globally uniquified alias for this table alias which we will use in
    // the resolved AST.
    const IdString unique_alias;

    // True if references to this subquery should resolve to a
    // ResolvedRecursiveRefScan, rather than a ResolvedWithRefScan.
    bool is_recursive;

    // The columns produced by the table alias.
    // These will be matched 1:1 with newly created columns in future
    // WithRefScan/RecursiveRefScan nodes.
    ResolvedColumnList column_list;

    // The name_list for the columns produced by the WITH subquery.
    // This provides the user-visible column names, which may not map 1:1
    // with column_list.
    // This also includes the is_value_table bit indicating if the WITH subquery
    // produced a value table.
    const std::shared_ptr<const NameList> name_list;
  };

  // Keeps track of all active named subqueries.
  // Key: Subquery name. This is a vector to allow for multi-part recursive view
  //        names, in addition to single-path WITH entry names.
  // Value: Vector of active subqueries with that name, with the innermost
  //        subquery last. This vector is never empty.
  //
  //        Note: While resolving the non-recursive term of a recursive UNION,
  //        a nullptr entry is added to this vector to indicate that any
  //        references to this alias should result in an error.
  absl::flat_hash_map<
      std::vector<IdString>, std::vector<std::unique_ptr<NamedSubquery>>,
      ContainerHash<std::vector<IdString>, IdStringCaseHash>,
      ContainerEquals<std::vector<IdString>, IdStringCaseEqualFunc>>
      named_subquery_map_;

  // Stores additional information about each ResolvedRecursiveRefScan node
  // needed by the resolver, but not persisted in the tree.
  struct RecursiveRefScanInfo {
    // ASTPathExpression representing the table reference; used for error
    // reporting only.
    const ASTPathExpression* path;

    // Unique name of the recursive query being referenced. Used to identify
    // cases where an inner WITH alias contains a recursive reference to an
    // outer WITH query. Since such cases always result in an error, this
    // information does not need to be persisted in the resolved tree; by the
    // time the resolver completes, it is guaranteed that every recursive
    // reference points to the innermost ResolvedRecursiveScan.
    IdString recursive_query_unique_name;
  };

  // Stores additional information about each ResolvedRecursiveRefScan node
  // created, which is needed for validation checks later in the resolver, but
  // is not persisted into the resolved AST.
  //
  // All node pointers are owned externally, as part of the resolved tree being
  // generated.
  absl::flat_hash_map<const ResolvedRecursiveRefScan*, RecursiveRefScanInfo>
      recursive_ref_info_;

  void AddNamedSubquery(const std::vector<IdString>& alias,
                        std::unique_ptr<NamedSubquery> named_subquery);
  bool IsPathExpressionStartingFromNamedSubquery(
      const ASTPathExpression* path_expr);

  // Set of unique WITH aliases seen so far.  If there are duplicate WITH
  // aliases in the query (visible in different scopes), we'll give them
  // unique names in the resolved AST.
  IdStringHashSetCase unique_with_alias_names_;

  // Deprecation warnings to return.  The set is keyed on the kind of
  // deprecation warning, and the warning string (not including the location).
  std::set<std::pair<DeprecationWarning::Kind, std::string>>
      unique_deprecation_warnings_;
  std::vector<absl::Status> deprecation_warnings_;

  // Store how columns have actually been referenced in the query.
  // (Note: The bottom-up resolver will initially include all possible columns
  // for each table on each ResolvedTableScan.)
  // Once we analyze the full query, this will be used to prune column_lists of
  // unreferenced columns. It is also used to populate column_access_list, which
  // indicates whether columns were read and/or written. Engines can use this
  // additional information for correct column-level ACL checking.
  std::map<ResolvedColumn, ResolvedStatement::ObjectAccess>
      referenced_column_access_;

  // Contains metadata reguarding any function arguments that are in scope and
  // can be referenced. This pointer is only set when analyzing the SQL body of
  // a function or table function.
  //
  // TODO: Maybe allow argument names in scope for the sake of
  //   the TYPEOF() operator. 'RETURNS TYPEOF(arg)' maybe a valid thing to do.
  const FunctionArgumentInfo* function_argument_info_ = nullptr;

  // Contains undeclared parameters whose type has been inferred from context.
  QueryParametersMap undeclared_parameters_;
  // Contains undeclared positional parameters whose type has been inferred from
  // context.
  std::vector<const Type*> undeclared_positional_parameters_;
  // Maps parse locations to the names or positions of untyped occurrences of
  // undeclared parameters.
  std::map<ParseLocationPoint, absl::variant<std::string, int>>
      untyped_undeclared_parameters_;

  // Status object returned when the stack overflows. Used to avoid
  // RETURN_ERROR, which may end up calling GoogleOnceInit methods on
  // GenericErrorSpace, which in turn would require more stack while the
  // stack is already overflowed.
  static absl::Status* stack_overflow_status_;

  // Maps ResolvedColumns produced by ResolvedTableScans to their source Columns
  // from the Catalog. This can be used to check properties like
  // Column::IsWritableColumn().
  // Note that this is filled in only for ResolvedColumns directly produced in a
  // ResolvedTableScan, not any derived columns.
  absl::flat_hash_map<ResolvedColumn, const Column*, ResolvedColumnHasher>
      resolved_columns_from_table_scans_;

  // Maps resolved floating point literal IDs to their original textual image.
  absl::flat_hash_map<int, std::string> float_literal_images_;
  // Next ID to assign to a float literal. The ID of 0 is reserved for
  // ResolvedLiterals without a cached image.
  int next_float_literal_image_id_ = 1;

  // A list of AnnotationSpec to be used to propagate annotations.
  std::vector<std::unique_ptr<AnnotationSpec>> annotation_specs_;

  // Holds active name lists that are available for GROUP_ROWS() function
  // invoked from inside WITH GROUP_ROWS(...). Contains an entry for each nested
  // usage of WITH GROUP_ROWS syntax. Boolean component is used to track usage
  // of GROUP_ROWS() TVF inside WITH GROUP_ROWS(...). It is enforced that
  // GROUP_ROWS() function should be used at least once.
  struct GroupRowsTvfInput {
    std::shared_ptr<const NameList> name_list;
    bool group_rows_tvf_used = false;
  };
  std::stack<GroupRowsTvfInput> name_lists_for_group_rows_;

  // Resolve the Type and TypeParameters from the <type_name> without resetting
  // the state.
  absl::Status ResolveTypeNameInternal(const std::string& type_name,
                                       const Type** type,
                                       TypeParameters* type_params);

  const FunctionResolver* function_resolver() const {
    return function_resolver_.get();
  }

  // Creates AnnotationSpec based on language feature and analyzer options.
  void InitializeAnnotationSpecs();

  // Checks and propagates annotations through <resolved_node>. If there is SQL
  // error thrown, the error will be attached to the location of <error_node>.
  // <error_node> could be nullptr to indicate there is no suitable location to
  // attach the error.
  absl::Status CheckAndPropagateAnnotations(const ASTNode* error_node,
                                            ResolvedNode* resolved_node);

  int AllocateColumnId();
  IdString AllocateSubqueryName();
  IdString AllocateUnnestName();

  IdString MakeIdString(absl::string_view str) const;

  // Makes a new resolved literal and records its location.
  std::unique_ptr<const ResolvedLiteral> MakeResolvedLiteral(
      const ASTNode* ast_location, const Value& value,
      bool set_has_explicit_type = false) const;

  // Makes a new resolved literal and records its location.
  std::unique_ptr<const ResolvedLiteral> MakeResolvedLiteral(
      const ASTNode* ast_location, const Type* type, const Value& value,
      bool has_explicit_type) const;

  // Makes a new resolved float literal and records its location and original
  // image. The ResolvedLiteral will have a non-zero float_literal_id if the
  // FEATURE_NUMERIC_TYPE language feature is enabled, which associates the
  // float literal with its original image in the float_literal_images_ cache in
  // order to preserve precision for float to numeric coercion.
  std::unique_ptr<const ResolvedLiteral> MakeResolvedFloatLiteral(
      const ASTNode* ast_location, const Type* type, const Value& value,
      bool has_explicit_type, absl::string_view image);

  // Make a new resolved literal without location. Those are essentially
  // constants produced by the resolver, which don't occur in the input string
  // (e.g., NULLs for optional CASE branches) or cannot be replaced by
  // query parameters (e.g., DAY keyword in intervals).
  static std::unique_ptr<const ResolvedLiteral>
  MakeResolvedLiteralWithoutLocation(const Value& value);

  // Propagates any deprecation warnings from the body of the function call
  // corresponding to 'signature'.
  absl::Status AddAdditionalDeprecationWarningsForCalledFunction(
      const ASTNode* ast_location, const FunctionSignature& signature,
      const std::string& function_name, bool is_tvf);

  // Adds a deprecation warning pointing at <ast_location>. If <source_warning>
  // is non-NULL, it is added to the new deprecation warning as an ErrorSource.
  //
  // Skips adding duplicate messages for a given kind of warning.
  absl::Status AddDeprecationWarning(
      const ASTNode* ast_location, DeprecationWarning::Kind kind,
      const std::string& message,
      const FreestandingDeprecationWarning* source_warning = nullptr);

  static void InitStackOverflowStatus();

  static ResolvedColumnList ConcatColumnLists(
      const ResolvedColumnList& left, const ResolvedColumnList& right);

  // Appends the ResolvedColumns in <computed_columns> to those in
  // <column_list>, returning a new ResolvedColumnList.  The returned
  // list is sorted by ResolvedColumn ids.
  // TODO: The sort is not technically required, but it helps match
  // the result plan better against the pre-refactoring plans.
  static ResolvedColumnList ConcatColumnListWithComputedColumnsAndSort(
      const ResolvedColumnList& column_list,
      const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
          computed_columns);

  // Returns the alias of the given column (if not internal). Otherwise returns
  // the column pos (1-based as visible outside).
  // <alias> - assigned alias for the column (if any).
  // <column_pos> - 0-based column position in the query.
  static std::string ColumnAliasOrPosition(IdString alias, int column_pos);

  // Return true if <type>->SupportsGrouping().
  // When return false, also return in "no_grouping_type" the type that does not
  // supports grouping.
  bool TypeSupportsGrouping(const Type* type,
                            std::string* no_grouping_type) const;

  // Resolve an ASTQueryStatement.
  absl::Status ResolveQueryStatement(
      const ASTQueryStatement* query_stmt,
      std::unique_ptr<ResolvedStatement>* output_stmt,
      std::shared_ptr<const NameList>* output_name_list);

  // Resolve the LIKE table name from a generic CREATE statement.
  absl::Status ResolveCreateStatementLikeTableName(
      const ASTPathExpression* like_table_name,
      const IdString& table_name_id_string,
      std::vector<std::unique_ptr<const ResolvedColumnDefinition>>*
          column_definition_list,
      const Table** like_table);

  // Resolve the CreateMode from a generic CREATE statement.
  absl::Status ResolveCreateStatementOptions(
      const ASTCreateStatement* ast_statement, absl::string_view statement_type,
      ResolvedCreateStatement::CreateScope* create_scope,
      ResolvedCreateStatement::CreateMode* create_mode) const;

  // Resolves properties of ASTCreateViewStatementBase.
  // Used by ResolveCreate(|Materialized)ViewStatement functions to resolve
  // parts that are common between logical and materialized views.
  // 'column_definition_list' parameter is set to nullptr for logical views.
  // Other output arguments are always non-nulls.
  absl::Status ResolveCreateViewStatementBaseProperties(
      const ASTCreateViewStatementBase* ast_statement,
      absl::string_view statement_type, absl::string_view object_type,
      std::vector<std::string>* table_name,
      ResolvedCreateStatement::CreateScope* create_scope,
      ResolvedCreateStatement::CreateMode* create_mode,
      ResolvedCreateStatementEnums::SqlSecurity* sql_security,
      std::vector<std::unique_ptr<const ResolvedOption>>* resolved_options,
      std::vector<std::unique_ptr<const ResolvedOutputColumn>>*
          output_column_list,
      std::vector<std::unique_ptr<const ResolvedColumnDefinition>>*
          column_definition_list,
      std::unique_ptr<const ResolvedScan>* query_scan, std::string* view_sql,
      bool* is_value_table, bool* is_recursive);

  // Creates the ResolvedGeneratedColumnInfo from an ASTGeneratedColumnInfo.
  // - <ast_generated_column>: Is a pointer to the Generated Column
  // - <column_name_list>: Contains the names of the columns seen so far
  // so that they can be referenced by generated columns.
  // - opt_type: The optional type of this expression if provided from the
  // syntax.
  // - output: The resolved generated column.
  absl::Status ResolveGeneratedColumnInfo(
      const ASTGeneratedColumnInfo* ast_generated_column,
      const NameList& column_name_list, const Type* opt_type,
      std::unique_ptr<ResolvedGeneratedColumnInfo>* output);

  // Creates a ResolvedExpr from an ASTExpression representing a column
  // default value. The output ResolvedExpr is expected to be a member of
  // ResolvedColumnDefinition.
  // - <ast_column_default>: a pointer to the ASTExpression object holding the
  //   default expression.
  // - <opt_type>: The type of this expression provided from the syntax.
  // - <skip_type_match_check>: when true, skip checking default value type
  //   can be coerced to column type. Mainly used in ALTER COLUMN SET DEFAULT
  //   when the column doesn't exist.
  // - <default_value>: The resolved default value.
  absl::Status ResolveColumnDefaultExpression(
      const ASTExpression* ast_column_default, const Type* opt_type,
      bool skip_type_match_check,
      std::unique_ptr<ResolvedColumnDefaultValue>* default_value);

  // Resolve the column definition list from a CREATE TABLE statement.
  absl::Status ResolveColumnDefinitionList(
      IdString table_name_id_string,
      const absl::Span<const ASTColumnDefinition* const> ast_column_definitions,
      std::vector<std::unique_ptr<const ResolvedColumnDefinition>>*
          column_definition_list,
      ColumnIndexMap* column_indexes);

  // Creates a ResolvedColumnDefinition from an ASTTableElement.
  // Lots of complexity of this function is required because of generated
  // columns. During expression resolution, the resolver might start resolving
  // a referenced column that was not resolved yet.
  // e.g. CREATE TABLE T (a as b, b INT64);
  // When that happens, the resolver will record the pending dependency (in the
  // previous case 'b') and start resolving 'b'. Then it will retry resolving
  // 'a' again.
  //
  // The following data structures allow this to happen efficiently:
  // - <id_to_table_element_map>: Map from name of the column to the
  // ASTTableElement. This is used for finding ASTTableElement when one
  // resolution fails. The ASTColumnDefinition* are not owned.
  // - <id_to_column_def_map>: Map from name of the column to the
  // ResolvedColumnDefinition pointer. It's used for avoiding resolving the same
  // ASTTableElement more than once and also to avoid allocating a new id for a
  // ResolvedColumn. The ResolvedColumnDefinition* are not owned.
  // - <column>: The column definition to resolve.
  // - <table_name_id_string>: The name of the underlying table.
  // - <column_name_list>: Ordered list of visible column names for this column.
  // This list will also be updated with the new column being added by this
  // ResolvedColumnDefinition.
  // Note: This function requires 'generated_column_cycle_detector_' to be
  // non-NULL.
  absl::Status ResolveColumnDefinition(
      const std::unordered_map<IdString, const ASTColumnDefinition*,
                               IdStringHash>& id_to_column_definition_map,
      absl::node_hash_map<IdString,
                          std::unique_ptr<const ResolvedColumnDefinition>,
                          IdStringHash>* id_to_column_def_map,
      const ASTColumnDefinition* column, const IdString& table_name_id_string,
      NameList* column_name_list);

  // Creates a ResolvedColumnDefinition from an ASTColumnDefinition.
  // - <column>: The column definition to resolve.
  // - <table_name_id_string>: The name of the underlying table.
  // - <column_name_list>: Ordered list of visible column names for this column.
  // This list will also be updated with the new column being added by this
  // ResolvedColumnDefinition.
  absl::StatusOr<std::unique_ptr<const ResolvedColumnDefinition>>
  ResolveColumnDefinitionNoCache(const ASTColumnDefinition* column,
                                 const IdString& table_name_id_string,
                                 NameList* column_name_list);

  // Resolves AS SELECT clause for CREATE TABLE/VIEW/MATERIALIZED_VIEW/MODEL
  // statements.
  // The CREATE TABLE/MODEL statement must not have a column definition list
  // (otherwise, use ResolveAndAdaptQueryAndOutputColumns instead).
  // - <query>, <query_scan>, <is_value_table> and <output_column_list> cannot
  //   be null.
  // - <internal_table_name> should be a static IdString such as
  //   kCreateAsId and kViewId; it's used as an alias of the SELECT query.
  // - <explicit_column_list> the list of columns in the formal DDL declaration,
  //   currently for CREATE VIEW v(c1, c2) and CREATE MATERIALIZED VIEW v(...).
  // - <is_recursive_view> is true only for views which are actually recursive.
  //   This affects the resolved tree respresentation.
  // - If <column_definition_list> is not null, then <column_definition_list>
  //   will be populated based on the output column list and
  //   <table_name_id_string> (the name of the table to be created).
  //   Currently, when this is invoked for CREATE VIEW the
  //   <column_definition_list> is null, but for CREATE
  //   TABLE/MATERIALIZED_VIEW/MODEL the <column_definition_list> is non-null.
  absl::Status ResolveQueryAndOutputColumns(
      const ASTQuery* query, absl::string_view object_type,
      bool is_recursive_view,
      const std::vector<IdString>& table_name_id_string,
      IdString internal_table_name, const ASTColumnList* explicit_column_list,
      std::unique_ptr<const ResolvedScan>* query_scan, bool* is_value_table,
      std::vector<std::unique_ptr<const ResolvedOutputColumn>>*
          output_column_list,
      std::vector<std::unique_ptr<const ResolvedColumnDefinition>>*
          column_definition_list);

  // Resolves AS SELECT clause for CREATE TABLE AS SELECT when the SQL query
  // contains a column definition list. CAST might be added to <query_scan>, to
  // ensure that the output types are the same as in <column_definition_list>.
  // No pointer in the arguments can be null.
  absl::Status ResolveAndAdaptQueryAndOutputColumns(
      const ASTQuery* query, const ASTPathExpression* like_table_name,
      const ASTTableElementList* table_element_list,
      absl::Span<const ASTColumnDefinition* const> ast_column_definitions,
      std::vector<std::unique_ptr<const ResolvedColumnDefinition>>&
          column_definition_list,
      std::unique_ptr<const ResolvedScan>* query_scan,
      std::vector<std::unique_ptr<const ResolvedOutputColumn>>*
          output_column_list);

  // Resolves the column schema from a column definition in a CREATE TABLE
  // statement. If annotations is null, it means annotations are disallowed.
  // generated_column_info must not be null if a generated column is present
  // on the ASTColumnSchema. Likewise, default_value must not be null if
  // a default column expression is present on the ASTColumnSchema. At most
  // one of <generated_column_info> and <default_value> may be non-null.
  absl::Status ResolveColumnSchema(
      const ASTColumnSchema* schema, const NameList& column_name_list,
      const Type** resolved_type,
      std::unique_ptr<const ResolvedColumnAnnotations>* annotations,
      std::unique_ptr<ResolvedGeneratedColumnInfo>* generated_column_info,
      std::unique_ptr<ResolvedColumnDefaultValue>* default_value);

  // Validates the ASTColumnAttributeList, in particular looking for
  // duplicate attribute definitions (i.e. "PRIMARY KEY" "PRIMARY KEY").
  // - attribute_list is a pointer because it's an optional construct that can
  // be nullptr.
  absl::Status ValidateColumnAttributeList(
      const ASTColumnAttributeList* attribute_list) const;

  // Resolve the primary key from column definitions.
  absl::Status ResolvePrimaryKey(
      const absl::Span<const ASTTableElement* const> table_elements,
      const ColumnIndexMap& column_indexes,
      std::unique_ptr<ResolvedPrimaryKey>* resolved_primary_key);

  // Resolve the primary key from its AST node and the column indexes of
  // resolved columns.
  absl::Status ResolvePrimaryKey(
      const ColumnIndexMap& column_indexes,
      const ASTPrimaryKey* ast_primary_key,
      std::unique_ptr<ResolvedPrimaryKey>* resolved_primary_key);

  // Resolves the column and table foreign key constraints.
  // - column_indexes: mapping column names to indices in <column_definitions>
  // - <constraint_names>: contains list of constraint names already encountered
  //   so far, for checking uniqueness of new constraint names. The method is
  //   expected to add new constraint names to the list before returning.
  absl::Status ResolveForeignKeys(
      absl::Span<const ASTTableElement* const> ast_table_elements,
      const ColumnIndexMap& column_indexes,
      const std::vector<std::unique_ptr<const ResolvedColumnDefinition>>&
          column_definitions,
      std::set<std::string, zetasql_base::CaseLess>*
          constraint_names,
      std::vector<std::unique_ptr<const ResolvedForeignKey>>* foreign_key_list);

  // Resolves a column foreign key constraint.
  absl::Status ResolveForeignKeyColumnConstraint(
      const ColumnIndexMap& column_indexes,
      const std::vector<const Type*>& column_types,
      const ASTColumnDefinition* ast_column_definition,
      const ASTForeignKeyColumnAttribute* ast_foreign_key,
      std::vector<std::unique_ptr<ResolvedForeignKey>>* resolved_foreign_keys);

  // Resolves a table foreign key constraint.
  absl::Status ResolveForeignKeyTableConstraint(
      const ColumnIndexMap& column_indexes,
      const std::vector<const Type*>& column_types,
      const ASTForeignKey* ast_foreign_key,
      std::vector<std::unique_ptr<ResolvedForeignKey>>* resolved_foreign_keys);

  // Resolves a foreign key's referencing columns and referenced table and
  // columns. <column_indexes> is used to index into <column_types>.
  absl::Status ResolveForeignKeyReference(
      const ColumnIndexMap& column_indexes,
      const std::vector<const Type*>& column_types,
      absl::Span<const ASTIdentifier* const> ast_referencing_column_identifiers,
      const ASTForeignKeyReference* ast_foreign_key_reference,
      ResolvedForeignKey* foreign_key);

  // Resolves ZETASQL_CHECK constraints.
  // - <name_scope>: used for resolving column names in the expression.
  // - <constraint_names>: contains list of constraint names already encountered
  //   so far, for checking uniqueness of new constraint names. The method is
  //   expected to add new constraint names to the list before returning.
  // - <check_constraint_list>: List of ResolvedCheckConstraint created.
  absl::Status ResolveCheckConstraints(
      absl::Span<const ASTTableElement* const> ast_table_elements,
      const NameScope& name_scope,
      std::set<std::string, zetasql_base::CaseLess>*
          constraint_names,
      std::vector<std::unique_ptr<const ResolvedCheckConstraint>>*
          check_constraint_list);

  // Resolves the PARTITION BY or CLUSTER BY expressions of a CREATE
  // TABLE/MATERIALIZED_VIEW statement. <clause_type> is either PARTITION_BY or
  // CLUSTER_BY. <name_scope> and <query_info> are used for name resolution.
  // <partition_by_list_out>, which may be non-empty even in error cases.
  absl::Status ResolveCreateTablePartitionByList(
      absl::Span<const ASTExpression* const> expressions,
      PartitioningKind partitioning_kind, const NameScope& name_scope,
      QueryResolutionInfo* query_info,
      std::vector<std::unique_ptr<const ResolvedExpr>>* partition_by_list_out);

  // Resolve a CREATE INDEX statement.
  absl::Status ResolveCreateIndexStatement(
    const ASTCreateIndexStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output);

  // Validates 'resolved_expr' on an index key or storing clause of an index.
  //
  // 'resolved_columns' stores all the resolved columns in index keys and
  // storing columns. It errors out if the referred column of 'resolved_expr' is
  // already in 'resolved_columns'. If not, the column is inserted into
  // 'resolved_columns' for future usage.
  absl::Status ValidateResolvedExprForCreateIndex(
      const ASTCreateIndexStatement* ast_statement,
      const ASTExpression* ast_expression,
      std::set<IdString, IdStringCaseLess>* resolved_columns,
      const ResolvedExpr* resolved_expr);

  // Validates index key expressions for the search index.
  //
  // The key expression should not have ASC or DESC option; the key expression
  // should not have the null order option.
  // The key expression should only refer to column name until b/180069278 been
  // fixed.
  // TODO: Support alias on the index key expression.
  absl::Status ValidateIndexKeyExpressionForCreateSearchIndex(
      const ASTOrderingExpression& ordering_expression,
      const ResolvedExpr& resolved_expr);

  // A helper that resolves 'unnest_expression_list' for CREATE INDEX statement.
  //
  // 'name_list' is expected to contain the available names from the base table.
  //
  // When this function returns, populates 'name_list', and
  // 'resolved_unnest_items' accordingly.
  absl::Status ResolveIndexUnnestExpressions(
      const ASTIndexUnnestExpressionList* unnest_expression_list,
      NameList* name_list,
      std::vector<std::unique_ptr<const ResolvedUnnestItem>>*
          resolved_unnest_items);

  // Resolve a CREATE TABLE [AS SELECT] statement.
  absl::Status ResolveCreateTableStatement(
      const ASTCreateTableStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Resolve a CREATE MODEL statement.
  absl::Status ResolveCreateModelStatement(
      const ASTCreateModelStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Resolve a CREATE DATABASE statement.
  absl::Status ResolveCreateDatabaseStatement(
      const ASTCreateDatabaseStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Resolve a CREATE SCHEMA statement.
  absl::Status ResolveCreateSchemaStatement(
      const ASTCreateSchemaStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Resolves a CREATE VIEW statement
  absl::Status ResolveCreateViewStatement(
      const ASTCreateViewStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Resolves a CREATE MATERIALIZED VIEW statement
  absl::Status ResolveCreateMaterializedViewStatement(
      const ASTCreateMaterializedViewStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveCreateExternalTableStatement(
      const ASTCreateExternalTableStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Resolves a CREATE SNAPSHOT TABLE statement.
  absl::Status ResolveCreateSnapshotTableStatement(
      const ASTCreateSnapshotTableStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Resolves a CREATE CONSTANT statement.
  absl::Status ResolveCreateConstantStatement(
      const ASTCreateConstantStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Resolves a CREATE FUNCTION or CREATE AGGREGATE FUNCTION statement.
  absl::Status ResolveCreateFunctionStatement(
      const ASTCreateFunctionStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Resolves a CREATE TABLE FUNCTION statement.
  absl::Status ResolveCreateTableFunctionStatement(
      const ASTCreateTableFunctionStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Resolves a CREATE PROCEDURE statement.
  absl::Status ResolveCreateProcedureStatement(
      const ASTCreateProcedureStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // This enum instructs the ResolveTVFSchema method on how to check the
  // properties of the resulting schema object.
  enum class ResolveTVFSchemaCheckPropertiesType {
    // The ResolveTVFSchema method checks if the resulting schema is valid, and
    // if not, returns an error reporting that the schema is invalid for a
    // table-valued argument for a table-valued function.
    INVALID_TABLE_ARGUMENT,

    // The ResolveTVFSchema method checks if the resulting schema is valid, and
    // if not, returns an error reporting that the schema is invalid for a
    // return table for a table-valued function.
    INVALID_OUTPUT_SCHEMA,

    // The ResolveTVFSchema method does not perform either of the above checks.
    SKIP_CHECKS
  };

  // Resolves a table-valued argument or return type for a CREATE TABLE FUNCTION
  // statement. This is only called from the ResolveCreateTableFunctionStatement
  // method. <check_type> indicates how to check the properties of the resulting
  // schema.
  absl::Status ResolveTVFSchema(const ASTTVFSchema* ast_tvf_schema,
                                ResolveTVFSchemaCheckPropertiesType check_type,
                                TVFRelation* tvf_relation);

  // Helper function that returns a customized error for unsupported (templated)
  // argument types in a function declaration.
  absl::Status UnsupportedArgumentError(const ASTFunctionParameter& argument,
                                        const std::string& context);

  // This enum instructs the ResolveFunctionDeclaration method on what kind of
  // function it is currently resolving.
  enum class ResolveFunctionDeclarationType {
    // This is a scalar function that accepts zero or more individual values and
    // returns a single value.
    SCALAR_FUNCTION,

    // This is an aggregate function.
    AGGREGATE_FUNCTION,

    // This is a table-valued function.
    TABLE_FUNCTION,

    // This is a procedure.
    PROCEDURE,
  };

  absl::Status ResolveFunctionDeclaration(
      const ASTFunctionDeclaration* function_declaration,
      ResolveFunctionDeclarationType function_type,
      std::vector<std::string>* function_name, FunctionArgumentInfo* arg_info);

  // Resolves function parameter list and populates <arg_info>.
  absl::Status ResolveFunctionParameters(
      const ASTFunctionParameters* ast_function_parameters,
      ResolveFunctionDeclarationType function_type,
      FunctionArgumentInfo* arg_info);

  // Sets the Resolver::function_argument_info_ variable that signals what
  // argument names are in scope for expression and table name resolution.
  // Returns a cleanup object that resets the Resolver::function_argument_info_
  // variable when it goes out of scope.
  using AutoUnsetArgumentInfo =
      decltype(absl::MakeCleanup(std::function<void()>()));
  AutoUnsetArgumentInfo SetArgumentInfo(const FunctionArgumentInfo* arg_info);

  absl::Status ResolveCreatePrivilegeRestrictionStatement(
      const ASTCreatePrivilegeRestrictionStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveCreateRowAccessPolicyStatement(
      const ASTCreateRowAccessPolicyStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveCloneDataStatement(
      const ASTCloneDataStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveExportDataStatement(
      const ASTExportDataStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveExportModelStatement(
      const ASTExportModelStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveCallStatement(const ASTCallStatement* ast_call,
                                    std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveDefineTableStatement(
      const ASTDefineTableStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveDescribeStatement(
      const ASTDescribeStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveShowStatement(const ASTShowStatement* ast_statement,
                                    std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveBeginStatement(
      const ASTBeginStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveSetTransactionStatement(
      const ASTSetTransactionStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveCommitStatement(
      const ASTCommitStatement* statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveRollbackStatement(
      const ASTRollbackStatement* statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveStartBatchStatement(
      const ASTStartBatchStatement* statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveRunBatchStatement(
      const ASTRunBatchStatement* statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveAbortBatchStatement(
      const ASTAbortBatchStatement* statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveDeleteStatement(
      const ASTDeleteStatement* ast_statement,
      std::unique_ptr<ResolvedDeleteStmt>* output);
  // <target_alias> is the alias of the target, which must be in the topmost
  // scope of <scope>.
  absl::Status ResolveDeleteStatementImpl(
      const ASTDeleteStatement* ast_statement, IdString target_alias,
      const std::shared_ptr<const NameList>& target_name_list,
      const NameScope* scope,
      std::unique_ptr<const ResolvedTableScan> table_scan,
      std::unique_ptr<ResolvedDeleteStmt>* output);

  absl::Status ResolveDropStatement(const ASTDropStatement* ast_statement,
                                    std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveDropFunctionStatement(
      const ASTDropFunctionStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveDropTableFunctionStatement(
      const ASTDropTableFunctionStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveDropPrivilegeRestrictionStatement(
      const ASTDropPrivilegeRestrictionStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveDropRowAccessPolicyStatement(
      const ASTDropRowAccessPolicyStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveDropAllRowAccessPoliciesStatement(
      const ASTDropAllRowAccessPoliciesStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveDropMaterializedViewStatement(
      const ASTDropMaterializedViewStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveDropSnapshotTableStatement(
      const ASTDropSnapshotTableStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveDropSearchIndexStatement(
      const ASTDropSearchIndexStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveDMLTargetTable(
      const ASTPathExpression* target_path, const ASTAlias* target_path_alias,
      IdString* alias,
      std::unique_ptr<const ResolvedTableScan>* resolved_table_scan,
      std::shared_ptr<const NameList>* name_list);

  absl::Status ResolveInsertStatement(
      const ASTInsertStatement* ast_statement,
      std::unique_ptr<ResolvedInsertStmt>* output);
  absl::Status ResolveInsertStatementImpl(
      const ASTInsertStatement* ast_statement, IdString target_alias,
      const std::shared_ptr<const NameList>& target_name_list,
      std::unique_ptr<const ResolvedTableScan> table_scan,
      const ResolvedColumnList& insert_columns,
      const NameScope* nested_scope,  // NULL for non-nested INSERTs.
      std::unique_ptr<ResolvedInsertStmt>* output);

  absl::Status ResolveUpdateStatement(
      const ASTUpdateStatement* ast_statement,
      std::unique_ptr<ResolvedUpdateStmt>* output);
  // Resolves the given UPDATE statement node. The function uses two name
  // scopes: <target_scope> is used to resolve names that should appear as
  // targets in the SET clause and should come from the target table;
  // <update_scope> includes all names that can appear inside the UPDATE
  // statement and it is used to resolve names anywhere outside the target
  // expressions. <target_alias> is the alias of the target, which must be in
  // the topmost scope of both <target_scope> and <update_scope>.
  absl::Status ResolveUpdateStatementImpl(
      const ASTUpdateStatement* ast_statement, bool is_nested,
      IdString target_alias, const NameScope* target_scope,
      const std::shared_ptr<const NameList>& target_name_list,
      const NameScope* update_scope,
      std::unique_ptr<const ResolvedTableScan> table_scan,
      std::unique_ptr<const ResolvedScan> from_scan,
      std::unique_ptr<ResolvedUpdateStmt>* output);

  absl::Status ResolveMergeStatement(
      const ASTMergeStatement* statement,
      std::unique_ptr<ResolvedMergeStmt>* output);
  absl::Status ResolveMergeWhenClauseList(
      const ASTMergeWhenClauseList* when_clause_list,
      const IdStringHashMapCase<ResolvedColumn>* target_table_columns,
      const NameScope* target_name_scope, const NameScope* source_name_scope,
      const NameScope* all_name_scope, const NameList* target_name_list,
      const NameList* source_name_list,
      std::vector<std::unique_ptr<const ResolvedMergeWhen>>*
          resolved_when_clauses);
  absl::Status ResolveMergeUpdateAction(
      const ASTUpdateItemList* update_item_list,
      const NameScope* target_name_scope, const NameScope* all_name_scope,
      std::vector<std::unique_ptr<const ResolvedUpdateItem>>*
          resolved_update_item_list);
  absl::Status ResolveMergeInsertAction(
      const ASTMergeAction* merge_action,
      const IdStringHashMapCase<ResolvedColumn>* target_table_columns,
      const NameScope* target_name_scope, const NameScope* all_name_scope,
      const NameList* target_name_list, const NameList* source_name_list,
      ResolvedColumnList* resolved_insert_column_list,
      std::unique_ptr<const ResolvedInsertRow>* resolved_insert_row);

  absl::Status ResolveTruncateStatement(
      const ASTTruncateStatement* statement,
      std::unique_ptr<ResolvedTruncateStmt>* output);

  absl::Status ResolveGrantStatement(
      const ASTGrantStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveRevokeStatement(
      const ASTRevokeStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveRowAccessPolicyTableAndAlterActions(
      const ASTAlterRowAccessPolicyStatement* ast_statement,
      std::unique_ptr<const ResolvedTableScan>* resolved_table_scan,
      std::vector<std::unique_ptr<const ResolvedAlterAction>>* alter_actions);

  absl::Status ResolveAlterPrivilegeRestrictionStatement(
      const ASTAlterPrivilegeRestrictionStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveAlterRowAccessPolicyStatement(
      const ASTAlterRowAccessPolicyStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveAlterAllRowAccessPoliciesStatement(
      const ASTAlterAllRowAccessPoliciesStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveAlterActions(
      const ASTAlterStatementBase* ast_statement,
      absl::string_view alter_statement_kind,
      std::unique_ptr<ResolvedStatement>* output,
      bool* has_only_set_options_action,
      std::vector<std::unique_ptr<const ResolvedAlterAction>>* alter_actions);

  // <table> can be NULL. If the table does not exist in the catalog, we try
  // to resolve the ALTER statement anyway.
  absl::Status ResolveAddColumnAction(
      IdString table_name_id_string, const Table* table,
      const ASTAddColumnAction* action, IdStringSetCase* new_columns,
      IdStringSetCase* columns_to_drop,
      std::unique_ptr<const ResolvedAlterAction>* alter_action);

  // <table> can be NULL. If the table does not exist in the catalog, we try
  // to resolve the ALTER statement anyway.
  absl::Status ResolveDropColumnAction(
      const Table* table, const ASTDropColumnAction* action,
      IdStringSetCase* new_columns, IdStringSetCase* columns_to_drop,
      std::unique_ptr<const ResolvedAlterAction>* alter_action);

  // <table> can be NULL. If the table does not exist in the catalog, we try
  // to resolve the ALTER statement anyway.
  absl::Status ResolveRenameColumnAction(
      const Table* table, const ASTRenameColumnAction* action,
      IdStringSetCase* columns_to_rename,
      IdStringHashMapCase<IdString>* columns_rename_map,
      std::unique_ptr<const ResolvedAlterAction>* alter_action);

  // <table> can be NULL. If the table does not exist in the catalog, we try
  // to resolve the ALTER statement anyway.
  absl::Status ResolveAlterColumnTypeAction(
      IdString table_name_id_string, const Table* table,
      const ASTAlterColumnTypeAction* action,
      std::unique_ptr<const ResolvedAlterAction>* alter_action);

  // <table> can be NULL. If the table does not exist in the catalog, we try
  // to resolve the ALTER statement anyway.
  absl::Status ResolveAlterColumnOptionsAction(
      const Table* table, const ASTAlterColumnOptionsAction* action,
      std::unique_ptr<const ResolvedAlterAction>* alter_action);

  // <table> can be NULL. If the table does not exist in the catalog, we try
  // to resolve the ALTER statement anyway.
  absl::Status ResolveAlterColumnDropNotNullAction(
      const Table* table, const ASTAlterColumnDropNotNullAction* action,
      std::unique_ptr<const ResolvedAlterAction>* alter_action);

  // <table> can be NULL. If the table does not exist in the catalog, we try
  // to resolve the ALTER statement anyway.
  absl::Status ResolveAlterColumnSetDefaultAction(
      IdString table_name_id_string, const Table* table,
      const ASTAlterColumnSetDefaultAction* action,
      std::unique_ptr<const ResolvedAlterAction>* alter_action);

  // <table> can be NULL. If the table does not exist in the catalog, we try
  // to resolve the ALTER statement anyway.
  absl::Status ResolveAlterColumnDropDefaultAction(
      const Table* table, const ASTAlterColumnDropDefaultAction* action,
      std::unique_ptr<const ResolvedAlterAction>* alter_action);

  absl::Status ResolveSetCollateClause(
      const ASTSetCollateClause* action,
      std::unique_ptr<const ResolvedAlterAction>* alter_action);

  absl::Status ResolveAlterDatabaseStatement(
      const ASTAlterDatabaseStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveAlterSchemaStatement(
      const ASTAlterSchemaStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveAlterTableStatement(
      const ASTAlterTableStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveAlterViewStatement(
      const ASTAlterViewStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveAlterMaterializedViewStatement(
      const ASTAlterMaterializedViewStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveRenameStatement(
      const ASTRenameStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveImportStatement(
      const ASTImportStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveModuleStatement(
      const ASTModuleStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveAnalyzeStatement(
      const ASTAnalyzeStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveAssertStatement(const ASTAssertStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Resolve an ASTQuery ignoring its ASTWithClause.  This is only called from
  // inside ResolveQuery after resolving the with clause if there was one.
  absl::Status ResolveQueryAfterWith(
      const ASTQuery* query,
      const NameScope* scope,
      IdString query_alias,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  // Resolve an ASTQuery (which may have an ASTWithClause).
  //
  // <query_alias> is the table name used internally for the ResolvedColumns
  // produced as output of this query (for display only).
  //
  // <is_outer_query> is true if this is the outermost query, and not any kind
  // of subquery.
  //
  // Side-effect: Updates named_subquery_map_ to reflect WITH aliases currently
  // in scope so WITH references can be resolved inside <query>.
  absl::Status ResolveQuery(
      const ASTQuery* query,
      const NameScope* scope,
      IdString query_alias,
      bool is_outer_query,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  // Resolves a WITH entry.
  // <recursive> is true only when a WITH entry is actually recursive, as
  // opposed to merely belonging to a WITH clause with the RECURSIVE keyword.
  absl::StatusOr<std::unique_ptr<const ResolvedWithEntry>> ResolveWithEntry(
      const ASTWithClauseEntry* with_entry, bool recursive);

  // Called only for the query associated with an actually-recursive WITH
  // entry. Verifies that the query is a UNION and returns the ASTSetOperation
  // node representing that UNION.
  absl::StatusOr<const ASTSetOperation*> GetRecursiveUnion(
      const ASTQuery* query);

  // Resolve an ASTQueryExpression.
  //
  // <query_alias> is the table name used internally for the ResolvedColumns
  // produced as output of this query (for display only).
  //
  // <force_new_columns_for_projected_outputs> indicates whether or not each
  // projected output requires a new column, even if
  // <analyzer_options_.create_new_column_for_each_projected_output()> is not
  // set. Used for the input to a PIVOT clause to ensure that each projection
  // not referenced by the PIVOT clause is treated as a grouping column.
  //
  // This is similar to ResolveQuery, but with no support for order by or limit.
  absl::Status ResolveQueryExpression(
      const ASTQueryExpression* query_expr, const NameScope* scope,
      IdString query_alias, bool force_new_columns_for_projected_outputs,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  // If the query contains a WITH clause, resolves all WITH entries and returns
  // them. Otherwise, just returns an empty vector.
  absl::StatusOr<std::vector<std::unique_ptr<const ResolvedWithEntry>>>
  ResolveWithClauseIfPresent(const ASTQuery* query, bool is_outer_query);

  // Called immediately after resolving the main body of a query. If the query
  // contained a WITH clause, removes the WITH entries from named_subquery_map_
  // and wraps the query scan in a ResolvedWithScan node. Ownership of elements
  // in <with_entries> is transferred to the new ResolvedWithScan node, which
  // replaces <*output>.
  absl::Status FinishResolveWithClauseIfPresent(
      const ASTQuery* query,
      std::vector<std::unique_ptr<const ResolvedWithEntry>> with_entries,
      std::unique_ptr<const ResolvedScan>* output);

  // Resolve an ASTSelect.  Resolves everything within the scope of the related
  // query block, including the FROM, WHERE, GROUP BY, HAVING, and ORDER BY
  // clauses.  The ORDER BY is passed in separately because it binds outside
  // the SELECT in the parser, but since the ORDER BY can reference columns
  // from the FROM clause scope, the ORDER BY clause also resolves in
  // ResolvedSelect().
  //
  // <force_new_columns_for_projected_outputs> indicates whether or not each
  // projected output requires a new column, even if
  // <analyzer_options_.create_new_column_for_each_projected_output()> is not
  // set. Used for the input to a PIVOT clause to ensure that each projection
  // not referenced by the PIVOT clause is treated as a grouping column.
  //
  // <query_alias> is the table name used internally for the ResolvedColumns
  // produced as output of this select query block (for display only).
  absl::Status ResolveSelect(const ASTSelect* select,
                             const ASTOrderBy* order_by,
                             const ASTLimitOffset* limit_offset,
                             const NameScope* external_scope,
                             IdString query_alias,
                             bool force_new_columns_for_projected_outputs,
                             std::unique_ptr<const ResolvedScan>* output,
                             std::shared_ptr<const NameList>* output_name_list);

  // Resolves TableDataSource to a ResolvedScan for copy or clone operation.
  absl::Status ResolveDataSourceForCopyOrClone(
      const ASTTableDataSource* data_source,
      std::unique_ptr<const ResolvedScan>* output);

  // Resolve select list in TRANSFORM clause for model creation.
  absl::Status ResolveModelTransformSelectList(
      const NameScope* input_scope, const ASTSelectList* select_list,
      const std::shared_ptr<const NameList>& input_cols_name_list,
      std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
          transform_list,
      std::vector<std::unique_ptr<const ResolvedOutputColumn>>*
          transform_output_column_list,
      std::vector<std::unique_ptr<const ResolvedAnalyticFunctionGroup>>*
          transform_analytic_function_group_list);

  // Helper function to add grantee to grantee expression list.
  absl::Status AddGranteeToExpressionList(
      const ASTExpression* grantee,
      std::vector<std::unique_ptr<const ResolvedExpr>>* grantee_expr_list);

  // Helper function to add grantee to deprecated grantee list.
  absl::Status AddGranteeToList(const ASTExpression* grantee,
                                std::vector<std::string>* grantee_list);

  // Resolves the grantee list, which only contains string literals and
  // parameters (given the parser rules).  The <ast_grantee_list> may be
  // nullptr for ALTER ROW POLICY statements.  Only one of <grantee_list> or
  // <grantee_expr_list> will be populated, depending on whether the
  // FEATURE_PARAMETERS_IN_GRANTEE_LIST is enabled.
  // TODO: Enable this feature for all customers, and remove the
  // <grantee_list> from this function call.
  absl::Status ResolveGranteeList(
      const ASTGranteeList* ast_grantee_list,
      std::vector<std::string>* grantee_list,
      std::vector<std::unique_ptr<const ResolvedExpr>>* grantee_expr_list);

  absl::Status ResolveExecuteImmediateStatement(
      const ASTExecuteImmediateStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  absl::Status ResolveSystemVariableAssignment(
      const ASTSystemVariableAssignment* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  static absl::Status CreateSelectNamelists(
      const SelectColumnState* select_column_state,
      NameList* post_group_by_alias_name_list,
      NameList* pre_group_by_alias_name_list,
      IdStringHashMapCase<NameTarget>* error_name_targets,
      std::set<IdString, IdStringCaseLess>* select_column_aliases);

  // Analyzes an expression, and if it is logically a path expression (of
  // one or more names) then returns true, along with the 'source_column'
  // where the path expression starts and a 'valid_name_path' that identifies
  // the path name list along with the 'target_column' that the entire path
  // expression resolves to.
  // If the expression is not a path expression then sets 'source_column'
  // to be uninitialized and returns false.
  bool GetSourceColumnAndNamePath(
      const ResolvedExpr* resolved_expr, ResolvedColumn target_column,
      ResolvedColumn* source_column, ValidNamePath* valid_name_path) const;

  // Assign a pre-GROUP BY ResolvedColumn to each SelectColumnState that could
  // be referenced in HAVING or ORDER BY inside an aggregate function.  For
  // example:
  //   SELECT t1.a + 1 as foo
  //   FROM t1
  //   GROUP BY 1
  //   HAVING sum(foo) > 5;
  // Resolving 'foo' in the HAVING clause requires the pre-GROUP BY version
  // of 't1.a + 1'.
  //
  // This includes SELECT columns that do not themselves have aggregation, and
  // that have non-internal aliases.  The assigned ResolvedColumn represents a
  // pre-GROUP BY version of the column/expression.  Additionally, if the
  // SelectColumnState expression needs precomputation (i.e., it is a path
  // expression), then add a new ResolvedComputedColumn for it in
  // <select_list_columns_to_compute_before_aggregation>.
  // Added ResolvedComputedColumns will be precomputed by a ProjectScan
  // before the related AggregateScan.
  absl::Status AnalyzeSelectColumnsToPrecomputeBeforeAggregation(
      QueryResolutionInfo* query_resolution_info);

  // Resolve the WHERE clause expression (which must be non-NULL) and
  // generate a ResolvedFilterScan for it.  The <current_scan> will be
  // wrapped with this new ResolvedFilterScan.
  absl::Status ResolveWhereClauseAndCreateScan(
    const ASTWhereClause* where_clause,
    const NameScope* from_scan_scope,
    std::unique_ptr<const ResolvedScan>* current_scan);

  // Performs first pass analysis on the SELECT list expressions.  This
  // pass includes star and dot-star expansion, and resolves expressions
  // against the FROM clause.  Populates the SelectColumnStateList in
  // <query_resolution_info>, and also records information about referenced
  // and resolved aggregation and analytic functions.
  absl::Status ResolveSelectListExprsFirstPass(
      const ASTSelectList* select_list, const NameScope* from_scan_scope,
      bool has_from_clause,
      const std::shared_ptr<const NameList>& from_clause_name_list,
      QueryResolutionInfo* query_resolution_info);

  // Performs first pass analysis on a SELECT list expression.
  // <ast_select_column_idx> indicates an index into the original ASTSelect
  // list, before any star expansion.
  absl::Status ResolveSelectColumnFirstPass(
      const ASTSelectColumn* ast_select_column,
      const NameScope* from_scan_scope,
      const std::shared_ptr<const NameList>& from_clause_name_list,
      int ast_select_column_idx, bool has_from_clause,
      QueryResolutionInfo* query_resolution_info);

  // Finishes resolving the SelectColumnStateList after first pass
  // analysis.  For each <select_column_state_list> entry, a ResolvedColumn
  // is produced as its output.  Columns that need computing are added
  // to the appropriate list.  Must only be called if there is no grouping
  // or SELECT list aggregation or analytic function present.
  //
  // <force_new_columns_for_projected_outputs> indicates whether or not each
  // projected output requires a new column, even if
  // <analyzer_options_.create_new_column_for_each_projected_output()> is not
  // set. Used for the input to a PIVOT clause to ensure that each projection
  // not referenced by the PIVOT clause is treated as a grouping column.
  void FinalizeSelectColumnStateList(
      const ASTSelectList* ast_select_list, IdString query_alias,
      bool force_new_columns_for_projected_outputs,
      QueryResolutionInfo* query_resolution_info,
      SelectColumnStateList* select_column_state_list);

  // Performs second pass analysis on the SELECT list expressions, re-resolving
  // expressions against GROUP BY scope if necessary.  After this pass, each
  // SelectColumnState has an initialized output ResolvedColumn.
  absl::Status ResolveSelectListExprsSecondPass(
      IdString query_alias,
      const NameScope* group_by_scope,
      std::shared_ptr<NameList>* final_project_name_list,
      QueryResolutionInfo* query_resolution_info);

  // Performs second pass analysis on a SELECT list expression, as indicated
  // by <select_column_state>.
  absl::Status ResolveSelectColumnSecondPass(
      IdString query_alias,
      const NameScope* group_by_scope,
      SelectColumnState* select_column_state,
      std::shared_ptr<NameList>* final_project_name_list,
      QueryResolutionInfo* query_resolution_info);

  // Performs second pass analysis on aggregate and analytic expressions that
  // are indicated by <query_resolution_info>, in either list:
  //   dot_star_columns_with_aggregation_for_second_pass_resolution_
  //   dot_star_columns_with_analytic_for_second_pass_resolution_
  absl::Status ResolveAdditionalExprsSecondPass(
      const NameScope* from_clause_or_group_by_scope,
      QueryResolutionInfo* query_resolution_info);

  // Resolve modifiers for StarWithModifiers or DotStarWithModifiers.
  // Stores the modifier mappings in <column_replacements>.
  // Exactly one of <name_list_for_star> or <type_for_star> must be non-NULL,
  // and is used to check that excluded names actually exist.
  // <scope> is the scope for resolving full expressions in REPLACE.
  absl::Status ResolveSelectStarModifiers(
      const ASTNode* ast_location,
      const ASTStarModifiers* modifiers,
      const NameList* name_list_for_star,
      const Type* type_for_star,
      const NameScope* scope,
      QueryResolutionInfo* query_resolution_info,
      ColumnReplacements* column_replacements);

  // Resolves a Star expression in the SELECT list, producing multiple
  // columns and adding them to SelectColumnStateList in
  // <query_resolution_info>.
  // <ast_select_expr> can be ASTStar or ASTStarWithModifiers.
  absl::Status ResolveSelectStar(
      const ASTExpression* ast_select_expr,
      const std::shared_ptr<const NameList>& from_clause_name_list,
      const NameScope* from_scan_scope,
      bool has_from_clause,
      QueryResolutionInfo* query_resolution_info);

  // Resolves a DotStar expression in the SELECT list, producing multiple
  // columns and adding them to SelectColumnStateList in
  // <query_resolution_info>.
  // If the lhs is a range variable, adds all the columns visible from that
  // range variable.
  // If the lhs is a struct/proto, adds one column for each field.
  // If the lhs is an expression rather than a ColumnRef, a ComputedColumn will
  // be added to <precompute_columns> to materialize the struct/proto before
  // extracting its fields.
  // <ast_dotstar> can be ASTStar or ASTStarWithModifiers.
  absl::Status ResolveSelectDotStar(
      const ASTExpression* ast_dotstar,
      const NameScope* from_scan_scope,
      QueryResolutionInfo* query_resolution_info);

  // Adds all fields of the column referenced by <src_column_ref> to
  // <select_column_state_list>, like we do for 'SELECT column.*'.
  // Copies <src_column_ref>, without taking ownership.  If
  // <src_column_has_aggregation>, then marks the new SelectColumnState as
  // has_aggregation.  If <src_column_has_analytic>, then marks the new
  // SelectColumnState as has_analytic.  If the column has no fields, then if
  // <column_alias_if_no_fields> is non-empty, emits the column itself,
  // and otherwise returns an error.
  absl::Status AddColumnFieldsToSelectList(
      const ASTExpression* ast_expression,
      const ResolvedColumnRef* src_column_ref,
      bool src_column_has_aggregation,
      bool src_column_has_analytic,
      IdString column_alias_if_no_fields,
      const IdStringSetCase* excluded_field_names,
      SelectColumnStateList* select_column_state_list,
      ColumnReplacements* column_replacements = nullptr);

  // Add all columns in <name_list> into <select_column_state_list>, optionally
  // excluding value table fields that have been marked as excluded.
  absl::Status AddNameListToSelectList(
      const ASTExpression* ast_expression,
      const std::shared_ptr<const NameList>& name_list,
      const CorrelatedColumnsSetList& correlated_columns_set_list,
      bool ignore_excluded_value_table_fields,
      SelectColumnStateList* select_column_state_list,
      ColumnReplacements* column_replacements = nullptr);

  // If <resolved_expr> is a resolved path expression (zero or more
  // RESOLVED_GET_*_FIELD expressions over a ResolvedColumnRef) then inserts
  // a new entry into 'query_resolution_info->group_by_valid_field_info_map'
  // with a source ResolvedColumn that is the <resolved_expr> source
  // ResolvedColumnRef column, the name path derived from the <resolved_expr>
  // get_*_field expressions, along with the <target_column>.
  // If <resolved_expr> is not a resolved path expression then has no
  // effect.
  absl::Status CollectResolvedPathExpressionInfoIfRelevant(
      QueryResolutionInfo* query_resolution_info,
      const ResolvedExpr* resolved_expr, ResolvedColumn target_column) const;

  // Resolve the 'SELECT DISTINCT ...' part of the query.
  // Creates a new aggregate scan in <current_scan> (that wraps the input
  // <current_scan>) having GROUP BY on the columns visible in the input scan.
  // Updates <query_resolution_info> with the mapping between pre-distinct and
  // post-distinct versions of columns.
  absl::Status ResolveSelectDistinct(
      const ASTSelect* select,
      SelectColumnStateList* select_column_state_list,
      const NameList* input_name_list,
      std::unique_ptr<const ResolvedScan>* current_scan,
      QueryResolutionInfo* query_resolution_info,
      std::shared_ptr<const NameList>* output_name_list);

  // Resolve the 'SELECT AS {STRUCT | TypeName}' part of a query.
  // Creates a new output_scan that wraps input_scan_in and converts it to
  // the requested type.
  absl::Status ResolveSelectAs(
      const ASTSelectAs* select_as,
      const SelectColumnStateList& select_column_state_list,
      std::unique_ptr<const ResolvedScan> input_scan_in,
      const NameList* input_name_list,
      std::unique_ptr<const ResolvedScan>* output_scan,
      std::shared_ptr<const NameList>* output_name_list);

  // Add a ResolvedProjectScan wrapping <current_scan> and computing
  // <computed_columns> if <computed_columns> is non-empty.
  // <current_scan> will be updated to point at the wrapper scan.
  static void MaybeAddProjectForComputedColumns(
      std::vector<std::unique_ptr<const ResolvedComputedColumn>>
          computed_columns,
      std::unique_ptr<const ResolvedScan>* current_scan);

  // Add all remaining scans for this SELECT query on top of <current_scan>,
  // which already includes the FROM clause scan and WHERE clause scan (if
  // present).  The remaining scans include any necessary scans for
  // grouping/aggregation, HAVING clause filtering, analytic functions,
  // QUALIFY clause filtering, DISTINCT, ORDER BY, LIMIT/OFFSET,
  // a final ProjectScan for the SELECT list output, and HINTs.
  absl::Status AddRemainingScansForSelect(
      const ASTSelect* select, const ASTOrderBy* order_by,
      const ASTLimitOffset* limit_offset,
      const NameScope* having_and_order_by_scope,
      std::unique_ptr<const ResolvedExpr>* resolved_having_expr,
      std::unique_ptr<const ResolvedExpr>* resolved_qualify_expr,
      QueryResolutionInfo* query_resolution_info,
      std::shared_ptr<const NameList>* output_name_list,
      std::unique_ptr<const ResolvedScan>* current_scan);

  // Add a ResolvedAggregateScan wrapping <current_scan> and producing the
  // aggregate expression columns.  Must only be called if an aggregate scan
  // is necessary.  <is_for_select_distinct> indicates this AggregateScan is
  // being added for SELECT DISTINCT, so shouldn't inherit hints from the query.
  absl::Status AddAggregateScan(
      const ASTSelect* select,
      bool is_for_select_distinct,
      QueryResolutionInfo* query_resolution_info,
      std::unique_ptr<const ResolvedScan>* current_scan);

  // Add a ResolvedAnonymizedAggregateScan wrapping <current_scan> and producing
  // the anonymization function call / expression columns.  Must only be called
  // if FEATURE_ANONYMIZATION is enabled and the column list contains
  // anonymization function calls and/or group by columns.
  absl::Status AddAnonymizedAggregateScan(
      const ASTSelect* select, QueryResolutionInfo* query_resolution_info,
      std::unique_ptr<const ResolvedScan>* current_scan);

  // Add a ResolvedAnalyticScan wrapping <current_scan> and producing the
  // analytic function columns.  A ProjectScan will be inserted between the
  // input <current_scan> and ResolvedAnalyticScan if needed.
  // <current_scan> will be updated to point at the wrapper
  // ResolvedAnalyticScan.
  absl::Status AddAnalyticScan(
    const NameScope* having_and_order_by_name_scope,
    QueryResolutionInfo* query_resolution_info,
    std::unique_ptr<const ResolvedScan>* current_scan);

  // Create a new scan wrapping <input_scan_in> converting it to a struct type.
  // If <named_struct_type> is NULL, convert to a new anonymous struct type.
  // If <named_struct_type> is non-NULL, convert to that struct type.
  absl::Status ConvertScanToStruct(
      const ASTNode* ast_location,
      const StructType* named_struct_type,  // May be NULL
      std::unique_ptr<const ResolvedScan> input_scan,
      const NameList* input_name_list,
      std::unique_ptr<const ResolvedScan>* output_scan,
      std::shared_ptr<const NameList>* output_name_list);

  // Creates a STRUCT out of the columns present in <name_list> as its fields.
  absl::Status CreateStructFromNameList(
      const NameList* name_list,
      const CorrelatedColumnsSetList& correlated_column_sets,
      std::unique_ptr<ResolvedComputedColumn>* computed_column);

  class AliasOrASTPathExpression {
   public:
    enum Kind { ALIAS, AST_PATH_EXPRESSION };

    explicit AliasOrASTPathExpression(IdString alias)
        : alias_or_ast_path_expr_(alias) {}

    explicit AliasOrASTPathExpression(const ASTPathExpression* ast_path_expr)
        : alias_or_ast_path_expr_(ast_path_expr) {}

    AliasOrASTPathExpression(const AliasOrASTPathExpression&) = delete;
    AliasOrASTPathExpression& operator=(const AliasOrASTPathExpression&) =
        delete;

    Kind kind() const {
      if (absl::holds_alternative<IdString>(alias_or_ast_path_expr_)) {
        return ALIAS;
      }
      return AST_PATH_EXPRESSION;
    }

    // Requires kind() == ALIAS.
    IdString alias() const {
      return absl::get<IdString>(alias_or_ast_path_expr_);
    }

    // Requires kind() == AST_PATH_EXPRESSION.
    const ASTPathExpression* ast_path_expr() const {
      return absl::get<const ASTPathExpression*>(alias_or_ast_path_expr_);
    }

   private:
    const absl::variant<IdString, const ASTPathExpression*>
        alias_or_ast_path_expr_;
  };

  struct ResolvedBuildProtoArg {
    ResolvedBuildProtoArg(
        const ASTNode* ast_location_in,
        std::unique_ptr<const ResolvedExpr> expr_in,
        std::unique_ptr<AliasOrASTPathExpression> alias_or_ast_path_expr_in)
        : ast_location(ast_location_in),
          expr(std::move(expr_in)),
          alias_or_ast_path_expr(std::move(alias_or_ast_path_expr_in)) {}
    const ASTNode* ast_location;
    std::unique_ptr<const ResolvedExpr> expr;
    std::unique_ptr<const AliasOrASTPathExpression> alias_or_ast_path_expr;
  };

  // Create a ResolvedMakeProto from a type and a vector of arguments.
  // <input_scan> is used only to look up whether some argument expressions
  // may be literals coming from ProjectScans.
  // <argument_description> and <query_description> are the words used to
  // describe those entities in error messages.
  absl::Status ResolveBuildProto(const ASTNode* ast_type_location,
                                 const ProtoType* proto_type,
                                 const ResolvedScan* input_scan,
                                 const std::string& argument_description,
                                 const std::string& query_description,
                                 std::vector<ResolvedBuildProtoArg>* arguments,
                                 std::unique_ptr<const ResolvedExpr>* output);

  // Returns the FieldDescriptor corresponding to <ast_path_expr>. First tries
  // to look up with respect to <descriptor>, and failing that extracts a type
  // name from <ast_path_expr>, looks up the type name, and then looks for the
  // extension field name in that type.
  absl::StatusOr<const google::protobuf::FieldDescriptor*> FindExtensionFieldDescriptor(
      const ASTPathExpression* ast_path_expr,
      const google::protobuf::Descriptor* descriptor);

  // Returns the FieldDescriptor corresponding to a top level field with the
  // given <name>. The field is looked up  with respect to <descriptor>. Returns
  // nullptr if no matching field was found.
  absl::StatusOr<const google::protobuf::FieldDescriptor*> FindFieldDescriptor(
      const ASTNode* ast_name_location, const google::protobuf::Descriptor* descriptor,
      absl::string_view name);

  // Returns a vector of FieldDesciptors that correspond to each of the fields
  // in the path <path_vector>. The first FieldDescriptor in the returned
  // vector is looked up with respect to <root_descriptor>.
  // <path_vector> must only contain nested field extractions.
  absl::Status FindFieldDescriptors(
      absl::Span<const ASTIdentifier* const> path_vector,
      const google::protobuf::Descriptor* root_descriptor,
      std::vector<const google::protobuf::FieldDescriptor*>* field_descriptors);

  // Parses <generalized_path>, filling <struct_path> and/or <field_descriptors>
  // as appropriate, with the struct and proto fields that correspond to each of
  // the fields in the path. The first field is looked up with respect to
  // <root_type>. Both <struct_path> and <field_descriptors> may be populated if
  // <generalized_path> contains accesses to fields of a proto nested within a
  // struct. In this case, when parsing the output vectors, the first part of
  // <generalized_path> corresponds to <struct_path> and the last part to
  // <field_descriptors>. <function_name> is for generating error messages.
  absl::Status FindFieldsFromPathExpression(
      absl::string_view function_name,
      const ASTGeneralizedPathExpression* generalized_path,
      const Type* root_type,
      std::vector<std::pair<int, const StructType::StructField*>>* struct_path,
      std::vector<const google::protobuf::FieldDescriptor*>* field_descriptors);

  // Returns a vector of StructFields and their indexes corresponding to the
  // fields in the path represented by <path_vector>. The first field in the
  // returned vector is looked up with respect to <root_struct>. If a field of
  // proto type is encountered in the path, it will be inserted into
  // <struct_path> and the function will return without examining any further
  // fields in the path.
  absl::Status FindStructFieldPrefix(
      absl::Span<const ASTIdentifier* const> path_vector,
      const StructType* root_struct,
      std::vector<std::pair<int, const StructType::StructField*>>* struct_path);

  // Looks up a proto message type name first in <descriptor_pool> and then in
  // <catalog>. Returns NULL if the type name is not found. If
  // 'return_error_for_non_message' is false, then also returns NULL if the type
  // name is found in <catalog> but is not a proto.
  absl::StatusOr<const google::protobuf::Descriptor*> FindMessageTypeForExtension(
      const ASTPathExpression* ast_path_expr,
      const std::vector<std::string>& type_name_path,
      const google::protobuf::DescriptorPool* descriptor_pool,
      bool return_error_for_non_message);

  // Create a new scan wrapping <input_scan_in> converting it to <proto_type>.
  absl::Status ConvertScanToProto(
      const ASTNode* ast_type_location,
      const SelectColumnStateList& select_column_state_list,
      const ProtoType* proto_type,
      std::unique_ptr<const ResolvedScan> input_scan,
      const NameList* input_name_list,
      std::unique_ptr<const ResolvedScan>* output_scan,
      std::shared_ptr<const NameList>* output_name_list);

  absl::Status ResolveSetOperation(
      const ASTSetOperation* set_operation,
      const NameScope* scope,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  // Visitor to walk the resolver tree of a recursive UNION and verify that
  // recursive references appear only in a supported context.
  class ValidateRecursiveTermVisitor : public ResolvedASTVisitor {
   public:
    ValidateRecursiveTermVisitor(const Resolver* resolver,
                                 IdString recursive_query_name);

   private:
    absl::Status DefaultVisit(const ResolvedNode* node) override;

    absl::Status VisitResolvedAggregateScan(
        const ResolvedAggregateScan* node) override;

    absl::Status VisitResolvedLimitOffsetScan(
        const ResolvedLimitOffsetScan* node) override;

    absl::Status VisitResolvedAnalyticScan(
        const ResolvedAnalyticScan* node) override;

    absl::Status VisitResolvedJoinScan(const ResolvedJoinScan* node) override;

    absl::Status VisitResolvedSubqueryExpr(
        const ResolvedSubqueryExpr* node) override;

    absl::Status VisitResolvedRecursiveRefScan(
        const ResolvedRecursiveRefScan* node) override;

    absl::Status VisitResolvedRecursiveScan(
        const ResolvedRecursiveScan* node) override;

    absl::Status VisitResolvedSampleScan(
        const ResolvedSampleScan* node) override;

    absl::Status VisitResolvedSetOperationScan(
        const ResolvedSetOperationScan* node) override;

    absl::Status VisitResolvedOrderByScan(
        const ResolvedOrderByScan* node) override;

    absl::Status VisitResolvedFunctionArgument(
        const ResolvedFunctionArgument* node) override;

    absl::Status VisitResolvedWithEntry(const ResolvedWithEntry* node) override;

    // Returns either the address of right_operand_of_left_join_count_,
    // left_operand_of_right_join_count_, or full_join_operand_count_,
    // depending on the arguments, or nullptr if none of the above apply.
    //
    // Used to increment or decrement the appropriate join count field when
    // starting and finishing the processing of an operand.
    int* GetJoinCountField(const ResolvedJoinScan::JoinType join_type,
                           bool left_operand);

    // Adjusts the values of the appropriate join count field by <offset>,
    // in response to entering or exiting a join operand.
    void MaybeAdjustJoinCount(const ResolvedJoinScan::JoinType join_type,
                         bool left_operand, int offset);

    const Resolver* resolver_;

    // Name of the recursive table currently being resolved. Used to distinguish
    // between recursive references to that table itself vs. recursive
    // references to some outer table. The latter results in an error, as it
    // is not supported.
    IdString recursive_query_name_;

    // Number of nested WITH entries we are inside of (relative to the recursive
    // term of the recursive query being validated). It is illegal to reference
    // a recursive table through any inner WITH entry.
    int nested_with_entry_count_ = 0;

    // Number of aggregate scans we are inside of.
    int aggregate_scan_count_ = 0;

    // Number of analytic scans we are inside of.
    int analytic_scan_count_ = 0;

    // Number of limit/offset scans we are inside of.
    int limit_offset_scan_count_ = 0;

    // Number of order by scans we are inside of.
    int order_by_scan_count_ = 0;

    // Number of sample sacns we are inside of.
    int sample_scan_count_ = 0;

    // Number of subquery expressions we are inside of.
    int subquery_expr_count_ = 0;

    // Number of times we are inside the right operand of a left join.
    int right_operand_of_left_join_count_ = 0;

    // Number of times we are inside the left operand of a right join.
    int left_operand_of_right_join_count_ = 0;

    // Number of times we are inside any operand of a full join.
    int full_join_operand_count_ = 0;

    // Number of TVF arguments we are inside of.
    int tvf_argument_count_ = 0;

    // Number of EXCEPT clauses we are inside the rhs of.
    int except_clause_count_ = 0;

    // Number of times we are inside of any operand of INTERSECT/UNION/EXCEPT
    // with the DISTINCT modifier.
    int setop_distinct_count_ = 0;

    // True if we've already encountered a recursive reference to the current
    // query. Multiple recursive references to the same query are disallowed.
    bool seen_recursive_reference_ = false;
  };

  // Helper class used to implement ResolveSetOperation().
  class SetOperationResolver {
   public:
    SetOperationResolver(const ASTSetOperation* set_operation,
                         Resolver* resolver);

    // Resolves the ASTSetOperation passed to the constructor, returning the
    // ResolvedScan and NameList in the given output parameters.
    // <scope> represents the name scope used to resolve each of the set items.
    absl::Status Resolve(const NameScope* scope,
                         std::unique_ptr<const ResolvedScan>* output,
                         std::shared_ptr<const NameList>* output_name_list);

    // Resolves the UNION representing a recursive query.
    // <scope>: the NameScope used to resolve the union's components.
    // <recursive_alias>: the name of the alias used in the query to
    //   refer to the recursive table reference.
    // <recursive_query_unique_name>: A unique name to associate with the
    //   recursive query in the resolved tree.
    // <output>: Receives a scan containing the result.
    // <output_name_list>: Receives a NameList containing the columns of the
    //   result.
    absl::Status ResolveRecursive(
        const NameScope* scope, const std::vector<IdString>& recursive_alias,
        const IdString& recursive_query_unique_name,
        std::unique_ptr<const ResolvedScan>* output,
        std::shared_ptr<const NameList>* output_name_list);

   private:
    // Represents the result of resolving one input to the set operation.
    struct ResolvedInputResult {
      std::unique_ptr<ResolvedSetOperationItem> node;
      std::shared_ptr<const NameList> name_list;
    };
    // Resolves a single input into a ResolvedSetOperationItem.
    // <scope> = name scope for resolution
    // <query index> = child index within set_operation_->inputs() of the query
    //   to resolve.
    absl::StatusOr<ResolvedInputResult> ResolveInputQuery(
        const NameScope* scope, int query_index) const;

    // Builds a vector specifying the type of each column for each input scan.
    // After calling:
    //   ZETASQL_ASSIGN_OR_RETURN(column_type_lists, BuildColumnTypeLists(...));
    //
    // column_type_lists[column_idx][scan_idx] specifies the type for the given
    // column index/input index combination.
    absl::StatusOr<std::vector<std::vector<InputArgumentType>>>
    BuildColumnTypeLists(absl::Span<ResolvedInputResult> resolved_inputs) const;

    absl::StatusOr<ResolvedColumnList> BuildColumnLists(
        const std::vector<std::vector<InputArgumentType>>& column_type_lists,
        const NameList& first_item_name_list) const;

    // Modifies <resolved_inputs>, adding a cast if necessary to convert each
    // column to the respective final column type of the set operation.
    absl::Status CreateWrapperScansWithCasts(
        const ResolvedColumnList& column_list,
        absl::Span<std::unique_ptr<ResolvedSetOperationItem>> resolved_inputs)
        const;

    // Builds the final name list for the resolution of the set operation.
    absl::StatusOr<std::shared_ptr<const NameList>> BuildFinalNameList(
        const NameList& first_item_name_list,
        const ResolvedColumnList& final_column_list) const;

    const ASTSetOperation* const set_operation_;
    Resolver* const resolver_;
    const IdString op_type_str_;
  };

  absl::Status ResolveGroupByExprs(
      const ASTGroupBy* group_by,
      const NameScope* from_clause_scope,
      QueryResolutionInfo* query_resolution_info);

  // Allocates a new ResolvedColumn for the post-GROUP BY version of the
  // column and returns it in <group_by_column>.  Resets <resolved_expr>
  // to the original SELECT column expression.  Updates the
  // SelectColumnState to reflect that the corresponding SELECT list
  // column is being grouped by.
  absl::Status HandleGroupBySelectColumn(
      const SelectColumnState* group_by_column_state,
      QueryResolutionInfo* query_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr,
      ResolvedColumn* group_by_column);

  // Allocates a new ResolvedColumn for the post-GROUP BY version of the
  // column and returns it in <group_by_column>.  If the expression is
  // already on the precomputed list (in <query_resolution_info>),
  // updates <resolved_expr> to be a column reference to the precomputed
  // column.
  absl::Status HandleGroupByExpression(
      const ASTExpression* ast_group_by_expr,
      QueryResolutionInfo* query_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr,
      ResolvedColumn* group_by_column);

  absl::Status ResolveQualifyExpr(
      const ASTQualify* qualify, const NameScope* having_and_order_by_scope,
      const NameScope* select_list_and_from_scan_scope,
      QueryResolutionInfo* query_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_qualify_expr);

  absl::Status ResolveHavingExpr(
      const ASTHaving* having, const NameScope* having_and_order_by_scope,
      const NameScope* select_list_and_from_scan_scope,
      QueryResolutionInfo* query_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_having_expr);

  // Ensures that each undeclared parameter got assigned a type.
  absl::Status ValidateUndeclaredParameters(const ResolvedNode* node);

  // Validate and resolves ASTCollate node for collation in columns.
  absl::Status ValidateAndResolveCollate(
      const ASTCollate* ast_collate, const ASTNode* ast_location,
      const Type* column_type,
      std::unique_ptr<const ResolvedExpr>* resolved_collate);

  // Resolves ASTCollate node for default collation in tables and datasets.
  absl::Status ValidateAndResolveDefaultCollate(
      const ASTCollate* ast_collate, const ASTNode* ast_location,
      std::unique_ptr<const ResolvedExpr>* resolved_collate);

  // Validate and resolves ASTCollate node for ORDER BY COLLATE.
  absl::Status ValidateAndResolveOrderByCollate(
      const ASTCollate* ast_collate, const ASTNode* ast_order_by_item_location,
      const Type* order_by_item_column,
      std::unique_ptr<const ResolvedExpr>* resolved_collate);

  // Resolves ASTCollate node.
  absl::Status ResolveCollate(
      const ASTCollate* ast_collate,
      std::unique_ptr<const ResolvedExpr>* resolved_collate);

  // Resolves the ORDER BY expressions and creates columns for them.
  // Populates OrderByItemInfo in
  // <expr_resolution_info>->query_resolution_info, along with the list
  // of computed ORDER BY columns.  <is_post_distinct> indicates that the
  // ORDER BY occurs after DISTINCT, i.e., SELECT DISTINCT ... ORDER BY...
  absl::Status ResolveOrderByExprs(
      const ASTOrderBy* order_by, const NameScope* having_and_order_by_scope,
      const NameScope* select_list_and_from_scan_scope, bool is_post_distinct,
      QueryResolutionInfo* query_resolution_info);

  absl::Status ResolveOrderByAfterSetOperations(
      const ASTOrderBy* order_by, const NameScope* scope,
      std::unique_ptr<const ResolvedScan> input_scan_in,
      std::unique_ptr<const ResolvedScan>* output_scan);

  // Resolves the table name and predicate expression in an ALTER ROW POLICY
  // or CREATE ROW POLICY statement.
  absl::Status ResolveTableAndPredicate(
      const ASTPathExpression* table_path, const ASTExpression* predicate,
      const char* clause_name,
      std::unique_ptr<const ResolvedTableScan>* resolved_table_scan,
      std::unique_ptr<const ResolvedExpr>* resolved_predicate,
      std::string* predicate_str);

  // Create a ResolvedColumn for each ORDER BY item in <order_by_info> that
  // is not supposed to be a reference to a SELECT column (which currently only
  // corresponds to an item that is not an integer literal, and includes
  // the alias references).
  // If the ORDER BY expression is not a column reference or is an outer
  // reference, then create a ResolvedComputedColumn and insert it into
  // <computed_columns>.
  void AddColumnsForOrderByExprs(
      IdString query_alias, std::vector<OrderByItemInfo>* order_by_info,
      std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
          computed_columns);

  // Resolves the lambda with the provided list of <arg_types> and expected
  // lambda type of <body_result_type>.
  // If <body_result_type> is not nullptr, then the result of the body
  // expression will be coerced to <body_result_type> if necessary
  absl::Status ResolveLambda(
      const ASTLambda* ast_lambda, absl::Span<const IdString> arg_names,
      absl::Span<const Type* const> arg_types, const Type* body_result_type,
      bool allow_argument_coercion, const NameScope* name_scope,
      std::unique_ptr<const ResolvedInlineLambda>* resolved_expr_out);

  // Resolves the given LIMIT or OFFSET clause <ast_expr> and stores the
  // resolved expression in <resolved_expr>.
  absl::Status ResolveLimitOrOffsetExpr(
      const ASTExpression* ast_expr,
      const char* clause_name,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr);

  absl::Status ResolveLimitOffsetScan(
      const ASTLimitOffset* limit_offset,
      std::unique_ptr<const ResolvedScan> input_scan_in,
      std::unique_ptr<const ResolvedScan>* output);

  // Translates the enum representing an IGNORE NULLS or RESPECT NULLS modifier.
  ResolvedNonScalarFunctionCallBase::NullHandlingModifier
  ResolveNullHandlingModifier(
      ASTFunctionCall::NullHandlingModifier ast_null_handling_modifier);

  // Resolves the given HAVING MAX or HAVING MIN argument, and stores the
  // result in <resolved_having>.
  absl::Status ResolveHavingModifier(
      const ASTHavingModifier* ast_having_modifier,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedAggregateHavingModifier>* resolved_having);

  // Add a ProjectScan if necessary to make sure that <scan> produces columns
  // with the desired types.
  // <target_column_list> provides the expected column types.
  // <scan_column_list> is the set of columns currently selected, matching
  // positionally with <target_column_list>.
  // If any types don't match, <scan> and <scan_column_list> are mutated,
  // adding a ProjectScan and new columns.
  // <scan_alias> is the table name used internally for new ResolvedColumns in
  // the ProjectScan.
  absl::Status CreateWrapperScanWithCasts(
      const ASTQueryExpression* ast_query,
      const ResolvedColumnList& target_column_list, IdString scan_alias,
      std::unique_ptr<const ResolvedScan>* scan,
      ResolvedColumnList* scan_column_list);

  IdString ComputeSelectColumnAlias(const ASTSelectColumn* ast_select_column,
                                    int column_idx) const;

  // Compute the default alias to use for an expression.
  // This comes from the final identifier used in a path expression.
  // Returns empty string if this node doesn't have a default alias.
  static IdString GetAliasForExpression(const ASTNode* node);

  // Return true if the first identifier on the path is a name that exists in
  // <scope>.
  static bool IsPathExpressionStartingFromScope(
      const ASTPathExpression* expr,
      const NameScope* scope);

  // Return true if <table_ref> should be resolved as an array scan.
  // This happens if it has UNNEST, or it is a path with at least two
  // identifiers where the first comes from <scope>.
  bool ShouldResolveAsArrayScan(const ASTTablePathExpression* table_ref,
                                const NameScope* scope);

  // Return an expression that tests <expr1> and <expr2> for equality.
  absl::Status MakeEqualityComparison(
      const ASTNode* ast_location, std::unique_ptr<const ResolvedExpr> expr1,
      std::unique_ptr<const ResolvedExpr> expr2,
      std::unique_ptr<const ResolvedExpr>* output_expr);

  // Returns a resolved expression that computes NOT of expr.
  // NOTE: expr should resolve to a boolean type.
  absl::Status MakeNotExpr(const ASTNode* ast_location,
                           std::unique_ptr<const ResolvedExpr> expr,
                           ExprResolutionInfo* expr_resolution_info,
                           std::unique_ptr<const ResolvedExpr>* expr_out);

  // Returns a resolved expression computing COALESCE of <columns>.
  absl::Status MakeCoalesceExpr(
      const ASTNode* ast_location, const ResolvedColumnList& columns,
      std::unique_ptr<const ResolvedExpr>* output_expr);

  // Return an expression that combines <exprs> with AND.
  // <exprs> must be non-empty, and each element must have type BOOL.
  // If only one input expr, then returns it without creating an AND.
  absl::Status MakeAndExpr(
      const ASTNode* ast_location,
      std::vector<std::unique_ptr<const ResolvedExpr>> exprs,
      std::unique_ptr<const ResolvedExpr>* output_expr) const;

  // Copies the parse location from the AST to resolved node depending on the
  // value of the analyzer option 'parse_location_record_type()'.
  void MaybeRecordParseLocation(const ASTNode* ast_location,
                                ResolvedNode* resolved_node) const;

  // Copies the parse location from the AST to resolved function call node
  // depending on the value of the analyzer option
  // 'parse_location_record_type()'.
  void MaybeRecordFunctionCallParseLocation(const ASTFunctionCall* ast_location,
                                            ResolvedNode* resolved_node) const;
  void MaybeRecordTVFCallParseLocation(const ASTTVF* ast_location,
                                       ResolvedNode* resolved_node) const;
  // Copies the parse location from the AST to expression subquery node
  // depending on the value of the analyzer option
  // 'parse_location_record_type()'.
  void MaybeRecordExpressionSubqueryParseLocation(
      const ASTExpressionSubquery* ast_expr_subquery,
      ResolvedNode* resolved_node) const;
  // Copies the parse location from the AST to field access resolved node
  // depending on the value of the analyzer option
  // 'parse_location_record_type()'.
  void MaybeRecordFieldAccessParseLocation(
    const ASTNode* ast_path, const ASTIdentifier* ast_field,
    ResolvedNode* resolved_node) const;

  // Copies the locations of the argument name and type (if present) from the
  // 'function_argument' to the 'options'.
  void RecordArgumentParseLocationsIfPresent(
      const ASTFunctionParameter& function_argument,
      FunctionArgumentTypeOptions* options) const;

  // Records the parse locations of name and type of TVF schema column (if
  // present) into 'column'.
  void RecordTVFRelationColumnParseLocationsIfPresent(
      const ASTTVFSchemaColumn& tvf_schema_column, TVFRelation::Column* column);

  // Generate a ResolvedScan for the FROM clause, populating the
  // <output_name_list> with the names visible in the FROM.  If there
  // is no FROM clause, then a ResolvedSingleRowScan will be produced.
  // Performs semantic checking to verify that queries without a FROM
  // clause do not have disallowed features.  For instance, ORDER BY is
  // not allowed if there is no FROM clause.
  absl::Status ResolveFromClauseAndCreateScan(
      const ASTSelect* select, const ASTOrderBy* order_by,
      const NameScope* external_scope,
      std::unique_ptr<const ResolvedScan>* output_scan,
      std::shared_ptr<const NameList>* output_name_list);

  // Resolve an element of a from clause.
  // This could be a table reference, a subquery, or a join.
  // <external_scope> is the scope with nothing from this FROM clause, to be
  // used for parts of the FROM clause that can't see local names.
  // <local_scope> includes all names visible in <external_scope> plus
  // names earlier in the same FROM clause that are visible.
  absl::Status ResolveTableExpression(
      const ASTTableExpression* table_expr,
      const NameScope* external_scope,
      const NameScope* local_scope,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  // Table referenced through a path expression.
  absl::Status ResolveTablePathExpression(
      const ASTTablePathExpression* table_ref,
      const NameScope* scope,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  // Resolve a path expression <path_expr> as a argument of table type within
  // the context of a CREATE TABLE FUNCTION statement. The <path_expr> should
  // exist as a key in the function_table_arguments_ map, and should only
  // comprise a single-part name with exactly one element. The <hint> is
  // optional and may be NULL.
  absl::Status ResolvePathExpressionAsFunctionTableArgument(
      const ASTPathExpression* path_expr,
      const ASTHint* hint,
      IdString alias,
      const ASTNode* ast_location,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  // Table referenced through a subquery.
  absl::Status ResolveTableSubquery(
      const ASTTableSubquery* table_ref,
      const NameScope* scope,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  // Resolve a identifier that is known to resolve to a named subquery
  // (e.g. WITH entry or recursive view).
  absl::Status ResolveNamedSubqueryRef(
      const ASTPathExpression* table_path, const ASTHint* hint,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  // If <ast_join> has a join hint keyword (e.g. HASH JOIN or LOOKUP JOIN),
  // add that hint onto <resolved_scan>.  Called with JoinScan or ArrayScan.
  static absl::Status MaybeAddJoinHintKeyword(const ASTJoin* ast_join,
                                              ResolvedScan* resolved_scan);

  // Resolves the <join_condition> for a USING clause on a join.
  // <name_list_lhs> and <name_list_rhs> are the columns visible in the left and
  // right side input.
  // Adds columns that need to be computed before or after the join to the
  // appropriate computed_column vectors.
  absl::Status ResolveUsing(
      const ASTUsingClause* using_clause, const NameList& name_list_lhs,
      const NameList& name_list_rhs, const ResolvedJoinScan::JoinType join_type,
      bool is_array_scan,
      std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
          lhs_computed_columns,
      std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
          rhs_computed_columns,
      std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
          computed_columns,
      NameList* output_name_list,
      std::unique_ptr<const ResolvedExpr>* join_condition);

  absl::Status ResolveJoin(
      const ASTJoin* join,
      const NameScope* external_scope,
      const NameScope* local_scope,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  absl::Status AddScansForJoin(
      const ASTJoin* join, std::unique_ptr<const ResolvedScan> resolved_lhs,
      std::unique_ptr<const ResolvedScan> resolved_rhs,
      ResolvedJoinScan::JoinType resolved_join_type,
      std::unique_ptr<const ResolvedExpr> join_condition,
      std::vector<std::unique_ptr<const ResolvedComputedColumn>>
          computed_columns,
      std::unique_ptr<const ResolvedScan>* output_scan);

  absl::Status ResolveParenthesizedJoin(
      const ASTParenthesizedJoin* parenthesized_join,
      const NameScope* external_scope,
      const NameScope* local_scope,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  // Resolves a call to a table-valued function (TVF) represented by <ast_tvf>.
  // This returns a new ResolvedTVFScan which contains the name of the function
  // to call and the scalar and table-valued arguments to pass into the call.
  //
  // The steps of resolving this function call proceed in the following order:
  //
  // 1. Check to see if the language option is enabled to support TVF calls in
  //    general. If not, return an error.
  //
  // 2. Get the function name from <ast_tvf> and perform a catalog lookup to see
  //    if a TVF exists with that name. If not, return an error.
  //
  // 3. Resolve each scalar argument as an expression, and resolve each
  //    table-valued argument as a query. This step can result in nested
  //    resolution of stored SQL bodies in templated TVFs or UDFs.
  //
  // 4. Check to see if the TVF's resolved arguments match its function
  //    signature. If not, return an error.
  //
  // 5. If needed, add type coercions for scalar arguments or projections to
  //    rearrange/coerce/drop columns for table-valued arguments. Note that
  //    table-valued arguments are matched on column names, not order.
  //
  // 6. Call the virtual TableValuedFunction::Resolve method to obtain the TVF
  //    output schema based on its input arguments.
  //
  // 7. Build the final ResolvedTVFScan based on the final input arguments and
  //    output schema.
  absl::Status ResolveTVF(
      const ASTTVF* ast_tvf,
      const NameScope* external_scope,
      const NameScope* local_scope,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  absl::StatusOr<ResolvedTVFArg> ResolveTVFArg(
      const ASTTVFArgument* ast_tvf_arg, const NameScope* external_scope,
      const NameScope* local_scope,
      const FunctionArgumentType* function_argument,
      const TableValuedFunction* tvf_catalog_entry, int arg_num,
      std::unordered_map<int, std::unique_ptr<const NameScope>>*
          tvf_table_scope_map);

  static absl::StatusOr<InputArgumentType> GetTVFArgType(
      const ResolvedTVFArg& resolved_tvf_arg);

  // Resolves GROUP_ROWS() TVF in a special way: GROUP_ROWS() expected to be
  // used inside WITH GROUP_ROWS(...) subquery on an aggregate function.
  // GROUP_ROWS() TVF allows subquery to access input rows of the aggregate
  // function and do preprocessing before the final aggregation happens.
  absl::Status ResolveGroupRowsTVF(
      const ASTTVF* ast_tvf, std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* group_rows_name_list);

  // Returns true in <add_projection> if the relation argument of
  // <tvf_signature_arg> at <arg_idx> has a required schema where the number,
  // order, and/or types of columns do not exactly match those in the provided
  // input relation. If so, the CoerceOrRearrangeTVFRelationArgColumns method
  // can construct a projection to produce the column names that the required
  // schema expects.
  absl::Status CheckIfMustCoerceOrRearrangeTVFRelationArgColumns(
      const FunctionArgumentType& tvf_signature_arg, int arg_idx,
      const SignatureMatchResult& signature_match_result,
      const ResolvedTVFArg& resolved_tvf_arg, bool* add_projection);

  // This method adds a ProjectScan on top of a relation argument for a
  // table-valued function relation argument when the function signature
  // specifies a required schema for that argument and the provided number,
  // order, and/or types of columns do not match exactly. This way the engine
  // may consume the provided input columns in the same order as the order of
  // the requested columns, since they match 1:1 after this function returns.
  //
  // This assumes that the signature matching process has already accepted the
  // function arguments and updated the signature match results to indicate
  // which coercions need to be made (if any).
  //
  // <tvf_signature_arg> is the type of the current relation argument to
  // consider.
  //
  // <arg_idx> is the index of that argument in the list of signature
  // arguments, starting at zero.
  //
  // <signature_match_result> contains information obtained from performing the
  // match of the provided TVF arguments against the function signature.
  //
  // <ast_location> is a place in the AST to use for error messages.
  //
  // <resolved_tvf_arg> is an in/out parameter that contains the resolved scan
  // and name list for the relation argument, and this method updates it to
  // contain a projection to perform the coercions.
  absl::Status CoerceOrRearrangeTVFRelationArgColumns(
      const FunctionArgumentType& tvf_signature_arg, int arg_idx,
      const SignatureMatchResult& signature_match_result,
      const ASTNode* ast_location,
      ResolvedTVFArg* resolved_tvf_arg);

  // Resolve a column in the USING clause on one side of the join.
  // <side_name> is "left" or "right", for error messages.
  absl::Status ResolveColumnInUsing(
      const ASTIdentifier* ast_identifier, const NameList& name_list,
      const std::string& side_name, IdString key_name,
      ResolvedColumn* found_column,
      std::unique_ptr<const ResolvedExpr>* compute_expr_for_found_column);

  // Resolve an array scan written as a JOIN or in a FROM clause with comma.
  // This does not handle cases where an array scan is the first thing in
  // the FROM clause.  That could happen for correlated subqueries.
  //
  // <resolved_input_scan> is either NULL or the already resolved scan feeding
  // rows into this array scan. May be mutated if we need to compute columns
  // before the join.
  // <on_condition> is non-NULL if this is a JOIN with an ON clause.
  // <using_clause> is non-NULL if this is a JOIN with a USING clause.
  // <is_outer_scan> is true if this is a LEFT JOIN.
  // <ast_join> is the JOIN node for this array scan, or NULL.
  //
  // ResolveArrayScan may take ownership of <resolved_lhs_scan> and
  // clear the unique_ptr.
  //
  // Preconditions:
  // - First identifier on that path resolves to a name inside scope.
  absl::Status ResolveArrayScan(
      const ASTTablePathExpression* table_ref, const ASTOnClause* on_clause,
      const ASTUsingClause* using_clause, const ASTJoin* ast_join,
      bool is_outer_scan,
      std::unique_ptr<const ResolvedScan>* resolved_input_scan,
      const std::shared_ptr<const NameList>& name_list_input,
      const NameScope* scope,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  // Performs initial resolution of ordering expressions, and distinguishes
  // between select list ordinals and other resolved expressions.
  // The OrderByInfo in <expr_resolution_info>->query_resolution_info is
  // populated with the resolved ORDER BY expression info.
  absl::Status ResolveOrderingExprs(
      absl::Span<const ASTOrderingExpression* const> ordering_expressions,
      ExprResolutionInfo* expr_resolution_info,
      std::vector<OrderByItemInfo>* order_by_info);

  // Resolves the <order_by_info> into <resolved_order_by_items>, which is
  // used for resolving both select ORDER BY clause and ORDER BY arguments
  // in the aggregate functions.
  // Validation is performed to ensure that the ORDER BY expression result
  // types support ordering. For resolving select ORDER BY clause, ensures
  // that the select list ordinal references are within bounds.
  // The returned ResolvedOrderByItem objects are stored in
  // <resolved_order_by_items>.
  absl::Status ResolveOrderByItems(
      const ASTOrderBy* order_by,
      const std::vector<ResolvedColumn>& output_column_list,
      const std::vector<OrderByItemInfo>& order_by_info,
      std::vector<std::unique_ptr<const ResolvedOrderByItem>>*
          resolved_order_by_items);

  // Make a ResolvedOrderByScan from the <order_by_info>, with <input_scan> as
  // a child scan.  Any hints associated with <order_by> are resolved.
  absl::Status MakeResolvedOrderByScan(
    const ASTOrderBy* order_by,
    std::unique_ptr<const ResolvedScan>* input_scan,
    const std::vector<ResolvedColumn>& output_column_list,
    const std::vector<OrderByItemInfo>& order_by_info,
    std::unique_ptr<const ResolvedScan>* output_scan);

  // Make a ResolvedColumnRef for <column>.  Caller owns the returned object.
  // Has side-effect of calling RecordColumnAccess on <column>, so that
  // the access can be recorded if necessary and the ColumnRef will stay valid
  // after pruning.
  ABSL_MUST_USE_RESULT
  std::unique_ptr<ResolvedColumnRef> MakeColumnRef(
      const ResolvedColumn& column, bool is_correlated = false,
      ResolvedStatement::ObjectAccess access_flags = ResolvedStatement::READ);

  // Make a ResolvedColumnRef with correlation if <correlated_columns_sets> is
  // non-empty, or make a ResolvedColumnRef without correlation otherwise.  If
  // creating a ResolvedColumnRef with correlation, returns a
  // ResolvedColumnRef with is_correlated=true and adds <column> to each of
  // the <correlated_columns_sets>.
  // Note that even though <correlated_columns_sets> is a const reference,
  // the items in the list will be mutated.
  std::unique_ptr<ResolvedColumnRef> MakeColumnRefWithCorrelation(
      const ResolvedColumn& column,
      const CorrelatedColumnsSetList& correlated_columns_sets,
      ResolvedStatement::ObjectAccess access_flags = ResolvedStatement::READ);

  // Returns a copy of the <column_ref>.
  ABSL_MUST_USE_RESULT
  static std::unique_ptr<const ResolvedColumnRef> CopyColumnRef(
      const ResolvedColumnRef* column_ref);

  // Resolves an input ResolvedColumn in <resolved_column_ref_expr> to a
  // version of that ResolvedColumn that is available after GROUP BY.
  // Updates <resolved_column_ref_expr> with a visible version of the
  // ResolvedColumn if necessary, and returns an error if the column is
  // not visible after GROUP BY.
  absl::Status ResolveColumnRefExprToPostGroupingColumn(
      const ASTExpression* path_expr, absl::string_view clause_name,
      QueryResolutionInfo* query_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_column_ref_expr);

  // Resolves an expression specified by AST node <ast_expr>, looking up names
  // against <name_scope>, without support for aggregate or analytic functions.
  // If the expression contains aggregate or analytic functions then this method
  // returns an error message, possibly including <clause_name>.
  absl::Status ResolveScalarExpr(
      const ASTExpression* ast_expr,
      const NameScope* name_scope,
      const char* clause_name,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // This is the recursive method that resolves expressions.
  // For scalar-only expressions, ResolveScalarExpr can be used instead.
  //
  // It receives an ExprResolutionInfo object specifying whether aggregate
  // and/or analytic functions are allowed (among other properties) and returns
  // information about the resolved expressions in that same object, including
  // whether aggregate or analytic functions are included in the resolved
  // expression.
  //
  // If aggregate and/or analytic functions are allowed, then the
  // parent_expr_resolution_info must have a non-NULL QueryResolutionInfo.
  // Otherwise, the QueryResolutionInfo can be NULL.
  //
  // Note: If the same ExprResolutionInfo is used across multiple calls, the
  // expressions will be resolved correctly, but the output fields (like
  // has_aggregation) in ExprResolutionInfo will be updated based on all
  // expressions resolved so far.
  absl::Status ResolveExpr(
      const ASTExpression* ast_expr,
      ExprResolutionInfo* parent_expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Validates <json_literal> and returns the JSONValue.
  absl::StatusOr<std::unique_ptr<const ResolvedLiteral>> ResolveJsonLiteral(
      const ASTJSONLiteral* json_literal);

  // Resolve a literal expression. Requires ast_expr->node_kind() to be one of
  // AST_*_LITERAL.
  absl::Status ResolveLiteralExpr(
      const ASTExpression* ast_expr,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status MakeResolvedDateOrTimeLiteral(
      const ASTExpression* ast_expr, const TypeKind type_kind,
      absl::string_view literal_string_value,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ValidateColumnForAggregateOrAnalyticSupport(
      const ResolvedColumn& resolved_column, IdString first_name,
      const ASTPathExpression* path_expr,
      ExprResolutionInfo* expr_resolution_info) const;

  // If there is an in-scope function or table function argument with a name
  // matching the first part of <path_expr>, populates <resolved_expr_out> with
  // a reference to that argument and increments <num_parts_consumed>.
  // Otherwise, does not modify <resolved_expr_out> or <num_parts_consumed>.
  absl::Status MaybeResolvePathExpressionAsFunctionArgumentRef(
      const ASTPathExpression* path_expr,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out,
      int* num_parts_consumed);

  absl::Status ResolvePathExpressionAsExpression(
      const ASTPathExpression* path_expr,
      ExprResolutionInfo* expr_resolution_info,
      ResolvedStatement::ObjectAccess access_flags,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveModel(
      const ASTPathExpression* path_expr,
      std::unique_ptr<const ResolvedModel>* resolved_model);

  absl::Status ResolveConnection(
      const ASTPathExpression* path_expr,
      std::unique_ptr<const ResolvedConnection>* resolved_connection);

  // Performs first pass analysis on descriptor object. This
  // pass includes preserving descriptor column names in ResolvedDescriptor.
  absl::Status ResolveDescriptorFirstPass(
      const ASTDescriptorColumnList* column_list,
      std::unique_ptr<const ResolvedDescriptor>* resolved_descriptor);

  // This method is used when descriptor objects appear in a TVF call. This
  // method resolves descriptor_column_name_list within <resolved_descriptor>
  // from <name_scope>. <name_scope> provides a namescope for the related input
  // table and populates the descriptor_column_list in <resolved_descriptor>.
  // <name_scope> must never be nullptr. <ast_tvf_argument> and
  // <table_argument_offset> are used for error messaging.
  absl::Status FinishResolvingDescriptor(
      const ASTTVFArgument* ast_tvf_argument,
      const std::unique_ptr<const NameScope>& name_scope,
      int table_argument_offset,
      std::unique_ptr<const ResolvedDescriptor>* resolved_descriptor);

  absl::Status ResolveForSystemTimeExpr(
      const ASTForSystemTime* for_system_time,
      std::unique_ptr<const ResolvedExpr>* resolved);

  // Resolves <path_expr> identified as <alias> as a scan from a table in
  // catalog_ (not from the <scope>). Flag <has_explicit_alias> identifies if
  // the alias was explicitly defined in the query or was computed from the
  // expression. Returns the resulting resolved table scan in <output> and
  // <output_name_list>.
  absl::Status ResolvePathExpressionAsTableScan(
      const ASTPathExpression* path_expr, IdString alias,
      bool has_explicit_alias, const ASTNode* alias_location,
      const ASTHint* hints, const ASTForSystemTime* for_system_time,
      const NameScope* scope, std::unique_ptr<const ResolvedTableScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  // Resolves a path expression to a Type.  If <is_single_identifier> then
  // the path expression is treated as a single (quoted) identifier. Otherwise
  // it is treated as a nested (catalog) path expression.
  absl::Status ResolvePathExpressionAsType(const ASTPathExpression* path_expr,
                                           bool is_single_identifier,
                                           const Type** resolved_type) const;

  absl::Status ResolveParameterExpr(
      const ASTParameterExpr* param_expr,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveDotIdentifier(
      const ASTDotIdentifier* dot_identifier,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Options to be used when attempting to resolve a proto field access.
  struct MaybeResolveProtoFieldOptions {
    MaybeResolveProtoFieldOptions() {}

    ~MaybeResolveProtoFieldOptions() {}

    // If true, an error will be returned if the field is not found. If false,
    // then instead of returning an error on field not found, returns OK with a
    // NULL <resolved_expr_out>.
    bool error_if_not_found = true;

    // If <get_has_bit_override> has a value, then the get_has_bit field of the
    // ResolvedProtoField related to <identifier> will be set to this
    // value (without determining if the <identifier> name might be ambiguous).
    // If <get_has_bit_override> does not contain a value, <identifier> will be
    // inspected to determine the field being accessed.
    absl::optional<bool> get_has_bit_override;

    // If true, then any FieldFormat.Format annotations on the field to extract
    // will be ignored. Note that this can change NULL behavior, because for
    // some types (e.g., DATE_DECIMAL), the value 0 decodes to NULL when the
    // annotation is applied. If the field to extract is not a primitive type,
    // the default value of the ResolvedGetProtoField will be NULL.
    bool ignore_format_annotations = false;
  };

  // Try to resolve a proto field access with the options specified by
  // <options>. <resolved_lhs> must have Proto type. On success, <resolved_lhs>
  // will be reset.
  absl::Status MaybeResolveProtoFieldAccess(
      const ASTNode* ast_path_expression, const ASTIdentifier* identifier,
      const MaybeResolveProtoFieldOptions& options,
      std::unique_ptr<const ResolvedExpr> resolved_lhs,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Try to resolve struct field access.  <resolved_lhs> must have Struct type.
  // If <error_if_not_found> is false, then instead of returning an error
  // on field not found, returns OK with a NULL <resolved_expr_out>.
  // On success, <resolved_lhs> will be reset.
  absl::Status MaybeResolveStructFieldAccess(
      const ASTNode* ast_path_expression, const ASTIdentifier* identifier,
      bool error_if_not_found, std::unique_ptr<const ResolvedExpr> resolved_lhs,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Resolves JSON field access.  <resolved_lhs> must have JSON type.
  // On success, <resolved_lhs> will be reset.
  absl::Status ResolveJsonFieldAccess(
      const ASTIdentifier* identifier,
      std::unique_ptr<const ResolvedExpr> resolved_lhs,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveFieldAccess(
      std::unique_ptr<const ResolvedExpr> resolved_lhs,
      const ASTNode* ast_path_expression, const ASTIdentifier* identifier,
      FlattenState* flatten_state,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Resolves a PROTO_DEFAULT_IF_NULL function call to a ResolvedGetProtoField
  // returned in <resolved_expr_out>. <resolved_arguments> must contain a single
  // ResolvedGetProtoField expression representing a non-message proto field
  // access, where the accessed field is not annotated with
  // zetasql.use_defaults=false. Element in <resolved_arguments> is
  // transferred to <resolved_expr_out>.
  absl::Status ResolveProtoDefaultIfNull(
      const ASTNode* ast_location,
      std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  struct ResolveExtensionFieldOptions {
    // Indicates whether the returned ResolvedGetProtoField denotes extraction
    // of the field's value or a bool indicating whether the field has been set.
    bool get_has_bit = false;

    // If true, then any FieldFormat.Format annotations on the extension to
    // extract will be ignored. Note that this can change NULL behavior, because
    // for some types (e.g., DATE_DECIMAL), the value 0 decodes to NULL when the
    // annotation is applied. If the extension to extract is not a primitive
    // type, the default value of the ResolvedGetProtoField will be NULL.
    bool ignore_format_annotations = false;
  };
  absl::Status ResolveExtensionFieldAccess(
      std::unique_ptr<const ResolvedExpr> resolved_lhs,
      const ResolveExtensionFieldOptions& options,
      const ASTPathExpression* ast_path_expr,
      FlattenState* flatten_state,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveDotGeneralizedField(
      const ASTDotGeneralizedField* dot_generalized_field,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveReplaceFieldsExpression(
      const ASTReplaceFieldsExpression* ast_replace_fields,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveSystemVariableExpression(
      const ASTSystemVariableExpr* ast_system_variable_expr,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveUnaryExpr(
      const ASTUnaryExpression* unary_expr,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveBinaryExpr(
      const ASTBinaryExpression* binary_expr,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveBitwiseShiftExpr(
      const ASTBitwiseShiftExpression* bitwise_shift_expr,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveInExpr(
      const ASTInExpression* in_expr, ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveInSubquery(
      const ASTInExpression* in_subquery_expr,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveLikeExpr(
      const ASTLikeExpression* like_expr,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveLikeExprSubquery(
      const ASTLikeExpression* like_subquery_expr,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveBetweenExpr(
      const ASTBetweenExpression* between_expr,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveAndExpr(
      const ASTAndExpr* and_expr, ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveOrExpr(
      const ASTOrExpr* or_expr, ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveExprSubquery(
      const ASTExpressionSubquery* expr_subquery,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveFunctionCall(
      const ASTFunctionCall* ast_function,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveFilterFieldsFunctionCall(
      const ASTFunctionCall* ast_function,
      const std::vector<const ASTExpression*>& function_arguments,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Resolve aggregate function for the first pass. This function also resolves
  // GROUP_ROWS clause if it is present.
  absl::Status ResolveAggregateFunctionCallFirstPass(
      const ASTFunctionCall* ast_function, const Function* function,
      ResolvedFunctionCallBase::ErrorMode error_mode,
      const std::vector<const ASTExpression*>& function_arguments,
      const std::map<int, SpecialArgumentType>& argument_option_map,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveAnalyticFunctionCall(
      const ASTAnalyticFunctionCall* analytic_function_call,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Populates <resolved_date_part> with a ResolvedLiteral that wraps a literal
  // Value of EnumType(functions::DateTimestampPart) corresponding to
  // <date_part_name> and <date_part_arg_name>. If <date_part> is not null, sets
  // it to the resolved date part. <date_part_arg_name> must be empty if and
  // only if 'date_part_arg_ast_location is NULL.
  absl::Status MakeDatePartEnumResolvedLiteralFromNames(
      IdString date_part_name, IdString date_part_arg_name,
      const ASTExpression* date_part_ast_location,
      const ASTExpression* date_part_arg_ast_location,
      std::unique_ptr<const ResolvedExpr>* resolved_date_part,
      functions::DateTimestampPart* date_part);

  absl::Status MakeDatePartEnumResolvedLiteral(
      functions::DateTimestampPart date_part,
      std::unique_ptr<const ResolvedExpr>* resolved_date_part);

  bool IsValidExplicitCast(
      const std::unique_ptr<const ResolvedExpr>& resolved_argument,
      const Type* to_type);

  // Checks whether explicit cast of the <resolved_argument> to the type
  // <to_type> is possible. CheckExplicitCast can return a status that is
  // different from Ok if it gets such error status from a Catalog's
  // FindConversion method or if a Catalog returns a conversion that breaks some
  // of Coercer invariants. If this happens Resolver should abort a resolution
  // request by returning the error status. If cast involves extended types the
  // function for such extended conversion is returned in
  // <extended_type_conversion> argument.
  absl::StatusOr<bool> CheckExplicitCast(
      const ResolvedExpr* resolved_argument, const Type* to_type,
      ExtendedCompositeCastEvaluator* extended_conversion_evaluator);

  absl::Status ResolveExplicitCast(
      const ASTCastExpression* cast, ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Resolves the format clause used in a CAST expression. On success,
  // <resolved_format> contains the resolved format expression, and
  // <resolved_time_zone> contains the resolved time zone expression.
  // If <resolve_cast_to_null> is true, it means that the CAST expression should
  // be resolved to a null expression. For example, this happens when the cast
  // is a safe cast, and the format string is invalid.
  absl::Status ResolveFormatClause(
      const ASTCastExpression* cast, ExprResolutionInfo* expr_resolution_info,
      const std::unique_ptr<const ResolvedExpr>& resolved_argument,
      const Type* resolved_cast_type,
      std::unique_ptr<const ResolvedExpr>* resolved_format,
      std::unique_ptr<const ResolvedExpr>* resolved_time_zone,
      bool* resolve_cast_to_null);

  // Resolves the format or the time zone expression in the format clause in a
  // CAST expression. On success, <resolved_expr> contains the resolved
  // expression. The type of the expression is checked, and if it is not a
  // string, returns an error. <clause_name> is used for formatting error
  // messages.
  absl::Status ResolveFormatOrTimeZoneExpr(
      const ASTExpression* expr,
      ExprResolutionInfo* expr_resolution_info,
      const char* clause_name,
      std::unique_ptr<const ResolvedExpr>* resolved_expr);

  // Resolves a cast from <resolved_argument> to <to_type>.  If the
  // argument is a NULL literal, then converts it to the target type and
  // updates <resolved_argument> with a NULL ResolvedLiteral of the target
  // type.  Otherwise, wraps <resolved_argument> with a new ResolvedCast whose
  // <type_annotation_map> is nullptr. <return_null_on_error> indicates
  // whether the cast should return a NULL value of the <target_type> in case of
  // failures.
  absl::Status ResolveCastWithResolvedArgument(
      const ASTNode* ast_location, const Type* to_type,
      bool return_null_on_error,
      std::unique_ptr<const ResolvedExpr>* resolved_argument);

  // Same as the previous method, but includes <format>, <time_zone> and
  // <type_params>. <to_annotated_type> is used to contain both type and its
  // annotation information. If <format> is specified, it is used as the format
  // string for the cast.
  absl::Status ResolveCastWithResolvedArgument(
      const ASTNode* ast_location, AnnotatedType to_annotated_type,
      std::unique_ptr<const ResolvedExpr> format,
      std::unique_ptr<const ResolvedExpr> time_zone,
      const TypeParameters& type_params,
      bool return_null_on_error,
      std::unique_ptr<const ResolvedExpr>* resolved_argument);

  absl::Status ResolveArrayElement(
      const ASTArrayElement* array_element,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Function names returned by ResolveArrayElement().
  static const char kArrayAtOffset[];
  static const char kArrayAtOrdinal[];
  static const char kProtoMapAtKey[];
  static const char kSafeArrayAtOffset[];
  static const char kSafeArrayAtOrdinal[];
  static const char kSafeProtoMapAtKey[];

  // Function names returned by ResolveNonArrayElement().
  static const char kSubscript[];
  static const char kSubscriptWithOffset[];
  static const char kSubscriptWithOrdinal[];
  static const char kSubscriptWithKey[];

  // Resolves subscript([]) operator for non-array type.
  // Depending on the argument the subscript operator can be resolved to one of
  // the following functions:
  //
  // $subscript_with_key: when wrapper keyword is KEY or SAFE_KEY,
  // $subscript_with_offset: when wrapper keyword is OFFSET or SAFE_OFFSET,
  // $subscript_with_ordinal: when wrapper keyword is ORDINAL or SAFE_ORDINAL
  // $subscript: no wrapper keywords as mentioned in the above cases.
  //
  // With SAFE_* prefix the resolved function_name_path will contain
  // "SAFE" as first element indicating the function runs under SAFE_ERROR_MODE
  // rather than DEFAULT_ERROR_MODE.
  //
  // To be consist with the pointer vs. reference usage in this file, but this
  // function requires all the pointers to be not nullptr.
  absl::Status ResolveNonArraySubscriptElementAccess(
      const ResolvedExpr* resolved_lhs, const ASTExpression* ast_position,
      ExprResolutionInfo* expr_resolution_info,
      std::vector<std::string>* function_name_path,
      const ASTExpression** unwrapped_ast_position_expr,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out,
      std::string* original_wrapper_name);

  // Requires that <resolved_array> is an array and verifies that
  // <ast_position> is an appropriate array element function call (e.g., to
  // OFFSET) and populates <function_name> and <unwrapped_ast_position_expr>
  // accordingly. Also resolves <unwrapped_ast_position_expr> into
  // <resolved_expr_out> and coerces it to the correct type if necessary. For
  // most arrays, this will be an INT64, but for proto maps, it will be the key
  // type of the map. <original_wrapper_name> is populated to corresponding
  // wrapper value of subscript operator.
  absl::Status ResolveArrayElementAccess(
      const ResolvedExpr* resolved_array, const ASTExpression* ast_position,
      ExprResolutionInfo* expr_resolution_info,
      absl::string_view* function_name,
      const ASTExpression** unwrapped_ast_position_expr,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out,
      std::string* original_wrapper_name);

  absl::Status ResolveCaseNoValueExpression(
      const ASTCaseNoValueExpression* case_no_value,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveCaseValueExpression(
      const ASTCaseValueExpression* case_value,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveAssertRowsModified(
      const ASTAssertRowsModified* ast_node,
      std::unique_ptr<const ResolvedAssertRowsModified>* output);

  absl::Status ResolveReturningClause(
      const ASTReturningClause* ast_node, IdString target_alias,
      const std::shared_ptr<const NameList>& from_clause_name_list,
      const NameScope* from_scan_scope,
      std::unique_ptr<const ResolvedReturningClause>* output);

  absl::Status FinishResolvingAggregateFunction(
      const ASTFunctionCall* ast_function_call,
      std::unique_ptr<ResolvedFunctionCall>* resolved_function_call,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedScan> with_group_rows_subquery,
      std::vector<std::unique_ptr<const ResolvedColumnRef>>
          with_group_rows_correlation_references,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveExtractExpression(
    const ASTExtractExpression* extract_expression,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveNewConstructor(
      const ASTNewConstructor* ast_new_constructor,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveArrayConstructor(
      const ASTArrayConstructor* ast_array_constructor,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveStructConstructorWithParens(
      const ASTStructConstructorWithParens* ast_struct_constructor,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveStructConstructorWithKeyword(
      const ASTStructConstructorWithKeyword* ast_struct_constructor,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // When resolving a STRUCT constructor expression, we generally try
  // to resolve it to a STRUCT literal where possible.  If all of the
  // fields are themselves literals, then we resolve this expression
  // to a STRUCT literal unless the STRUCT was not explicitly typed
  // (<ast_struct_type> is nullptr) and either 1) there is an untyped
  // NULL field, or 2) some fields have explicit types and others do
  // not.
  // The resulting STRUCT literal will be marked with has_explicit_type
  // if <ast_struct_type> is non-null or all of its fields were
  // has_explicit_type.
  //
  // Examples of expressions that resolve to STRUCT literals:
  // 1) CAST(NULL AS STRUCT<INT32>)              - has_explicit_type = true
  // 2) CAST((1, 2) AS STRUCT<INT32, INT64>)     - has_explicit_type = true
  // 3) STRUCT<INT64>(4)                         - has_explicit_type = true
  // 4) (1, 2, 3)                                - has_explicit_type = false
  // 5) (cast(1 as int64_t), cast (2 as int32_t))    - has_explicit_type = true
  // 6) (cast(null as int64_t), cast (2 as int32_t)) - has_explicit_type = true
  //
  // Examples of expressions that do not resolve to STRUCT literals:
  // 1) (1, NULL)             - one field is untyped null
  // 2) (1, CAST(3 as INT64)) - fields have different has_explicit_type
  absl::Status ResolveStructConstructorImpl(
      const ASTNode* ast_location, const ASTStructType* ast_struct_type,
      absl::Span<const ASTExpression* const> ast_field_expressions,
      absl::Span<const ASTAlias* const> ast_field_aliases,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // If <date_part> is not null, sets it to the resolved date part.
  absl::Status ResolveDatePartArgument(
      const ASTExpression* date_part_ast_location,
      std::unique_ptr<const ResolvedExpr>* resolved_date_part,
      functions::DateTimestampPart* date_part = nullptr);

  // Defines the accessors that can be used in the EXTRACT function with proto
  // input (e.g. EXTRACT(FIELD(x) from y) where y is a message that defines a
  // field x)
  enum class ProtoExtractionType {
    // HAS determines if a particular field is set in its containing message.
    kHas,

    // FIELD extracts the value of a field from its containing message.
    kField,

    // RAW extracts the value of a field from its containing message without
    // taking any type annotations into consideration. If
    // the field is missing then the field's default value is returned. For
    // message fields, the default value is NULL. If the containing message is
    // NULL, NULL is returned.
    kRaw,
  };

  // Parses <extraction_type_name> and returns the corresponding
  // ProtoExtractionType. An error is returned when the input does not parse to
  // a valid ProtoExtractionType.
  static absl::StatusOr<Resolver::ProtoExtractionType>
  ProtoExtractionTypeFromName(const std::string& extraction_type_name);

  // Returns the string name of the ProtoExtractionType corresponding to
  // <extraction_type>.
  static std::string ProtoExtractionTypeName(
      ProtoExtractionType extraction_type);

  // Resolves an EXTRACT(ACCESSOR(field) FROM proto) call.
  // <field_extraction_type_ast_location> is the ASTNode denoting the
  // ACCESSOR(field) expression. <resolved_proto_input> is the resolved proto
  // to be extracted from. The resultant resolved AST is returned in
  // <resolved_expr_out>.
  absl::Status ResolveProtoExtractExpression(
      const ASTExpression* field_extraction_type_ast_location,
      std::unique_ptr<const ResolvedExpr> resolved_proto_input,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveProtoExtractWithExtractTypeAndField(
      ProtoExtractionType field_extraction_type,
      const ASTPathExpression* field_path,
      std::unique_ptr<const ResolvedExpr> resolved_proto_input,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Resolves the normalize mode represented by <arg> and sets it to the
  // <resolved_expr_out>.
  absl::Status ResolveNormalizeModeArgument(
      const ASTExpression* arg,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  absl::Status ResolveIntervalArgument(
      const ASTExpression* arg,
      ExprResolutionInfo* expr_resolution_info,
      std::vector<std::unique_ptr<const ResolvedExpr>>* resolved_arguments_out,
      std::vector<const ASTExpression*>* ast_arguments_out);

  // Resolves interval expressions:
  // Literal:     INTERVAL '<literal>' <date_part> [ TO <date_part2>]
  // Constructor: INTERVAL <int64_expr> <date_part>
  absl::Status ResolveIntervalExpr(
      const ASTIntervalExpr* interval_expr,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Resolves AST identifier as DateTimestampPart
  absl::StatusOr<functions::DateTimestampPart> ResolveDateTimestampPart(
      const ASTIdentifier* date_part_identifier);

  absl::Status ResolveInsertValuesRow(
      const ASTInsertValuesRow* ast_insert_values_row, const NameScope* scope,
      const ResolvedColumnList& insert_columns,
      std::unique_ptr<const ResolvedInsertRow>* output);

  // Resolves the insert row by referencing all columns of <value_columns>.
  absl::Status ResolveInsertValuesRow(
      const ASTNode* ast_location, const ResolvedColumnList& value_columns,
      const ResolvedColumnList& insert_columns,
      std::unique_ptr<const ResolvedInsertRow>* output);

  // <nested_scope> is NULL for a non-nested INSERT. For a nested INSERT,
  // populates <parameter_list> with any columns in <nested_scope> (whose
  // topmost scope is always the empty scope) that are referenced by <output>.
  absl::Status ResolveInsertQuery(
      const ASTQuery* query, const NameScope* nested_scope,
      const ResolvedColumnList& insert_columns,
      std::unique_ptr<const ResolvedScan>* output,
      ResolvedColumnList* output_column_list,
      std::vector<std::unique_ptr<const ResolvedColumnRef>>* parameter_list);

  // Resolve an expression for a DML INSERT or UPDATE targeted at a column
  // with <annotated_target_type>.  Adds a cast if necessary and possible.  If a
  // cast is impossible, this call returns OK without adding a cast, and relies
  // on the caller to check if the expression type Equals the column type. (The
  // caller can give better error messages with more context.)
  absl::Status ResolveDMLValue(const ASTExpression* ast_value,
                               AnnotatedType annotated_target_type,
                               const NameScope* scope, const char* clause_name,
                               CoercionErrorMessageFunction coercion_err_msg,
                               std::unique_ptr<const ResolvedDMLValue>* output);

  // Similar to above ResolveDMLValue(), but is used by INSERT clause of MERGE,
  // when the value list is omitted by using INSERT ROW. The <referenced_column>
  // is the resolved column from source.
  absl::Status ResolveDMLValue(const ASTNode* ast_location,
                               const ResolvedColumn& referenced_column,
                               AnnotatedType annotated_target_type,
                               CoercionErrorMessageFunction coercion_err_msg,
                               std::unique_ptr<const ResolvedDMLValue>* output);

  // Resolves the given update items corresponding to an UPDATE statement. The
  // function uses two name scopes: <target_scope> is used to resolve names that
  // appear as targets in the SET clause and come from the target table;
  // <update_scope> includes all names that can appear inside expressions in the
  // UPDATE statement, including in the WHERE clause and the right hand side of
  // assignments.
  absl::Status ResolveUpdateItemList(
      const ASTUpdateItemList* ast_update_item_list, bool is_nested,
      const NameScope* target_scope, const NameScope* update_scope,
      std::vector<std::unique_ptr<const ResolvedUpdateItem>>* update_item_list);

  // Stores information about one of the highest-level ResolvedUpdateItem
  // nodes corresponding to an ASTUpdateItemList.
  struct UpdateItemAndLocation {
    std::unique_ptr<ResolvedUpdateItem> resolved_update_item;

    // The target path of one of the ASTUpdateItems corresponding to
    // <resolved_update_item>. (All of those target paths are all the same
    // unless <resolved_update_item> modifies an array element.) Not owned.
    const ASTGeneralizedPathExpression* one_target_path = nullptr;
  };

  // Merges <ast_update_item> with an existing element of <update_items> if
  // possible. Otherwise adds a new corresponding element to <update_items>.
  absl::Status ResolveUpdateItem(
      const ASTUpdateItem* ast_update_item, bool is_nested,
      const NameScope* target_scope, const NameScope* update_scope,
      std::vector<UpdateItemAndLocation>* update_items);

  // Target information for one of the (to be created) ResolvedUpdateItem nodes
  // in a path of ResolvedUpdateItem->ResolvedUpdateArrayItem->
  // ResolvedUpdateItem->ResolvedUpdateArrayItem->...->ResolvedUpdateItem path
  // corresponding to a particular ASTUpdateItem.
  struct UpdateTargetInfo {
    std::unique_ptr<const ResolvedExpr> target;

    // The following fields are only non-NULL if the ResolvedUpdateItem
    // corresponds to an array element modification (i.e., it is not the last
    // ResolvedUpdateItem on the path).

    // Represents the array element being modified.
    std::unique_ptr<const ResolvedColumn> array_element;

    // The 0-based offset of the array being modified.
    std::unique_ptr<const ResolvedExpr> array_offset;

    // The ResolvedColumnRef that is the leaf of the target of the next
    // ResolvedUpdateItem node on the path (which refers to the array element
    // being modified by this node).
    ResolvedColumnRef* array_element_ref = nullptr;  // Not owned.
  };

  // Populates <update_target_infos> according to the ResolvedUpdateItem nodes
  // to create for the 'path' portion of <ast_update_item>. The elements of
  // <update_target_infos> are sorted in root-to-leaf order of the corresponding
  // ResolvedUpdateItem nodes. For example, for
  // a.b[<expr1>].c[<expr2>].d.e.f[<expr3>].g, we end up with 4
  // UpdateTargetInfos, corresponding to
  // - a.b[<expr1>] with <array_element_column> = x1,
  // - x1.c[<expr2>] with <array_element_column> = x2,
  // - x2.d.e.f[<expr3>] with <array_element_column> = x3
  // - x3.g
  absl::Status PopulateUpdateTargetInfos(
      const ASTUpdateItem* ast_update_item, bool is_nested,
      const ASTGeneralizedPathExpression* path,
      ExprResolutionInfo* expr_resolution_info,
      std::vector<UpdateTargetInfo>* update_target_infos);

  // Verifies that the <target> (which must correspond to the first
  // UpdateTargetInfo returned by PopulateUpdateTargetInfos() for a non-nested
  // ASTUpdateItem) is writable.
  absl::Status VerifyUpdateTargetIsWritable(const ASTNode* ast_location,
                                            const ResolvedExpr* target);

  // Returns whether the column is writable.
  absl::StatusOr<bool> IsColumnWritable(const ResolvedColumn& column);

  // Verifies that the <column> is writable by looking into
  // <resolved_columns_from_table_scans_> for the corresponding catalog::Column
  // and checking into the property catalog::Column::IsWritableColumn().
  absl::Status VerifyTableScanColumnIsWritable(const ASTNode* ast_location,
                                               const ResolvedColumn& column,
                                               const char* statement_type);

  // Determines if <ast_update_item> should share the same ResolvedUpdateItem as
  // <update_item>.  Sets <merge> to true if they have the same target. Sets
  // <merge> to false if they have different, non-overlapping targets. Returns
  // an error if they have overlapping or conflicting targets, or if
  // <ast_update_item> violates the nested dml ordering
  // rules. <update_target_infos> is the output of PopulateUpdateTargetInfos()
  // corresponding to <ast_update_item>.
  absl::Status ShouldMergeWithUpdateItem(
      const ASTUpdateItem* ast_update_item,
      const std::vector<UpdateTargetInfo>& update_target_infos,
      const UpdateItemAndLocation& update_item, bool* merge);

  // Merges <ast_input_update_item> into <merged_update_item> (which might be
  // uninitialized). <input_update_target_infos> is the output of
  // PopulateUpdateTargetInfos() corresponding to <ast_update_item>.
  absl::Status MergeWithUpdateItem(
      const NameScope* update_scope, const ASTUpdateItem* ast_input_update_item,
      std::vector<UpdateTargetInfo>* input_update_target_infos,
      UpdateItemAndLocation* merged_update_item);

  // Resolves privileges, validating that the privileges are non-empty. If
  // name_scope is not nullptr, validates that any referenced paths exist in the
  // name scope (e.g. validate that column- and field-level privileges
  // correspond to columns and fields that exist in the name scope of a table).
  // If enable_nested_field_privileges is false, returns an error if any of the
  // given privileges are on nested fields.
  absl::Status ResolvePrivileges(
      const ASTPrivileges* ast_privileges, const NameScope* name_scope,
      bool enable_nested_field_privileges, absl::string_view statement_type,
      std::vector<std::unique_ptr<const ResolvedPrivilege>>* privilege_list);

  // Resolves a sample scan. Adds the name of the weight column to
  // <current_name_list> if WITH WEIGHT is present.
  absl::Status ResolveTablesampleClause(
      const ASTSampleClause* sample_clause,
      std::shared_ptr<const NameList>* current_name_list,
      std::unique_ptr<const ResolvedScan>* current_scan);

  // Common implementation for resolving a single argument of all expressions.
  // Pushes the related ResolvedExpr onto <resolved_arguments>.
  absl::Status ResolveExpressionArgument(
      const ASTExpression* arg,
      ExprResolutionInfo* expr_resolution_info,
      std::vector<std::unique_ptr<const ResolvedExpr>>* resolved_arguments);

  // Common implementation for resolving the children of all expressions.
  // Resolves input <arguments> and returns both <resolved_arguments_out>
  // and parallel vector <ast_arguments_out> (both having the same length).
  // The <argument_option_map> identifies arguments (by index) that require
  // special treatment during resolution (i.e., for INTERVAL and DATEPART).
  // Some AST arguments will expand into more than one resolved argument
  // (e.g., ASTIntervalExpr arguments expand into two resolved arguments).
  absl::Status ResolveExpressionArguments(
      ExprResolutionInfo* expr_resolution_info,
      absl::Span<const ASTExpression* const> arguments,
      const std::map<int, SpecialArgumentType>& argument_option_map,
      std::vector<std::unique_ptr<const ResolvedExpr>>* resolved_arguments_out,
      std::vector<const ASTExpression*>* ast_arguments_out);

  // Common implementation for resolving all functions given resolved input
  // <arguments> and <expected_result_type> (if any, usually needed while
  // resolving cast functions). If <function> is an aggregate function,
  // <ast_location> must be an ASTFunctionCall, and additional validation work
  // is done for aggregate function properties in the ASTFunctionCall, such as
  // distinct and order_by.  After resolving the function call, will add a
  // deprecation warning if either the function itself is deprecated or a
  // deprecated function signature is used.
  absl::Status ResolveFunctionCallWithResolvedArguments(
      const ASTNode* ast_location,
      const std::vector<const ASTNode*>& arg_locations,
      const Function* function, ResolvedFunctionCallBase::ErrorMode error_mode,
      std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments,
      std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedScan> with_group_rows_subquery,
      std::vector<std::unique_ptr<const ResolvedColumnRef>>
          with_group_rows_correlation_references,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // These are the same as previous but they take a (possibly multipart)
  // function name and looks it up in the resolver catalog.
  absl::Status ResolveFunctionCallWithResolvedArguments(
      const ASTNode* ast_location,
      const std::vector<const ASTNode*>& arg_locations,
      const std::vector<std::string>& function_name_path,
      std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments,
      std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Common implementation for resolving arguments in the USING clause of
  // EXECUTE IMMEDIATE statements.
  absl::Status ResolveExecuteImmediateArgument(
      const ASTExecuteUsingArgument* argument, ExprResolutionInfo* expr_info,
      std::unique_ptr<const ResolvedExecuteImmediateArgument>* output);
  // Common implementation for resolving EXECUTE IMMEDIATE statements.
  absl::Status ResolveExecuteImmediateStatement(
      const ASTExecuteImmediateStatement* ast_statement,
      std::unique_ptr<const ResolvedStatement>* output);

  // Resolves a generic CREATE <entity_type> statement.
  absl::Status ResolveCreateEntityStatement(
      const ASTCreateEntityStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Resolves a generic ALTER <entity_type> statement.
  absl::Status ResolveAlterEntityStatement(
      const ASTAlterEntityStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Resolves a generic DROP <entity_type> statement.
  absl::Status ResolveDropEntityStatement(
      const ASTDropEntityStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

 public:
  absl::Status ResolveFunctionCallWithResolvedArguments(
      const ASTNode* ast_location,
      const std::vector<const ASTNode*>& arg_locations,
      absl::string_view function_name,
      std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments,
      std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Defines the handle mode When a function is kNotFound in the catalog lookup.
  enum class FunctionNotFoundHandleMode { kReturnNotFound, kReturnError };

 private:
  // Looks up a function in the catalog, returning error status if not found.
  // If the Catalog lookup returns kNotFound then this method will either
  // return a not found or invalid argument error depending on the value of
  // <handle_mode>. Also returns the <error_mode> based on whether or
  // not the function had a "SAFE." prefix.
  absl::Status LookupFunctionFromCatalog(
      const ASTNode* ast_location,
      const std::vector<std::string>& function_name_path,
      FunctionNotFoundHandleMode handle_mode, const Function** function,
      ResolvedFunctionCallBase::ErrorMode* error_mode) const;

  // Common implementation for resolving operator expressions and non-standard
  // functions such as NOT, EXTRACT and CASE.  Looks up the
  // <function_name> from the catalog.  This is a wrapper function around
  // ResolveFunctionCallImpl().
  // NOTE: If the input is ASTFunctionCall, consider calling ResolveFunctionCall
  // instead, which also verifies the aggregate properties.
  absl::Status ResolveFunctionCallByNameWithoutAggregatePropertyCheck(
      const ASTNode* ast_location, const std::string& function_name,
      const absl::Span<const ASTExpression* const> arguments,
      const std::map<int, SpecialArgumentType>& argument_option_map,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Similar to the previous method. First calls
  // ResolveFunctionCallByNameWithoutAggregatePropertyCheck(), but if it fails
  // with INVALID_ARGUMENT, updates the literals to be explicitly typed
  // (using AddCastOrConvertLiteral) and tries again by calling
  // ResolveFunctionCallWithResolvedArguments().
  absl::Status ResolveFunctionCallWithLiteralRetry(
      const ASTNode* ast_location, const std::string& function_name,
      const absl::Span<const ASTExpression* const> arguments,
      const std::map<int, SpecialArgumentType>& argument_option_map,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Helper function used by ResolveFunctionCallWithLiteralRetry().
  // Loops through <resolved_expr_list> adding an explicit CAST() on every
  // ResolvedLiteral.
  // The ResolvedExpr* in <resolved_expr_list> may be replaced with new ones.
  absl::Status UpdateLiteralsToExplicit(
      const absl::Span<const ASTExpression* const> ast_arguments,
      std::vector<std::unique_ptr<const ResolvedExpr>>* resolved_expr_list);

  // Resolves function by calling ResolveFunctionCallArguments() followed by
  // ResolveFunctionCallWithResolvedArguments()
  absl::Status ResolveFunctionCallImpl(
      const ASTNode* ast_location,
      const Function* function,
      ResolvedFunctionCallBase::ErrorMode error_mode,
      const absl::Span<const ASTExpression* const> arguments,
      const std::map<int, SpecialArgumentType>& argument_option_map,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedScan> with_group_rows_subquery,
      std::vector<std::unique_ptr<const ResolvedColumnRef>>
          with_group_rows_correlation_references,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  // Returns the function name, arguments and options. It handles the special
  // cases for COUNT(*) and DATE functions.
  // If the function is an anonymized aggregate function, then updates
  // <query_resolution_info> to indicate the presence of anonymization.
  absl::Status GetFunctionNameAndArguments(
      const ASTFunctionCall* function_call,
      std::vector<std::string>* function_name_path,
      std::vector<const ASTExpression*>* function_arguments,
      std::map<int, SpecialArgumentType>* argument_option_map,
      QueryResolutionInfo* query_resolution_info);

  // Resolve the value part of a hint or option key/value pair.
  // This includes checking against <allowed> to ensure the options are
  // valid (typically used with AnalyzerOptions::allowed_hints_and_options).
  // The value must be an identifier, literal or query parameter.
  // <is_hint> indicates if this is a hint or an option.
  // <ast_qualifier> must be NULL if !is_hint.
  absl::Status ResolveHintOrOptionAndAppend(
      const ASTExpression* ast_value, const ASTIdentifier* ast_qualifier,
      const ASTIdentifier* ast_name, bool is_hint,
      const AllowedHintsAndOptions& allowed,
      std::vector<std::unique_ptr<const ResolvedOption>>* option_list);

  // Resolve <ast_hint> and add entries into <hints>.
  absl::Status ResolveHintAndAppend(
      const ASTHint* ast_hint,
      std::vector<std::unique_ptr<const ResolvedOption>>* hints);

  // Resolve <ast_hint> and add resolved hints onto <resolved_node>.
  // Works for ResolvedScan or ResolvedStatement (or any node with a hint_list).
  template <class NODE_TYPE>
  absl::Status ResolveHintsForNode(const ASTHint* ast_hints,
                                   NODE_TYPE* resolved_node);

  // Resolve <options_list> and add the options onto <resolved_options>
  // as ResolvedHints.
  absl::Status ResolveOptionsList(
      const ASTOptionsList* options_list,
      std::vector<std::unique_ptr<const ResolvedOption>>* resolved_options);

  // Resolve <table_and_column_info> and add the entry into
  // <resolved_table_and_column_info_list>.
  absl::Status ResolveTableAndColumnInfoAndAppend(
      const ASTTableAndColumnInfo* table_and_column_info,
      std::vector<std::unique_ptr<const ResolvedTableAndColumnInfo>>*
          resolved_table_and_column_info_list);

  // Resolve <table_and_column_info_list> and add the ResolveTableAndColumnInfo
  // entries into <resolved_table_and_column_info_list> as
  // ResolvedTableAndColumnInfoList.
  absl::Status ResolveTableAndColumnInfoList(
      const ASTTableAndColumnInfoList* table_and_column_info_list,
      std::vector<std::unique_ptr<const ResolvedTableAndColumnInfo>>*
          resolved_table_and_column_info_list);

  // Resolve <options_list> and add the options onto <resolved_options>.
  // Requires valid anonymization option names and types - delta, epsilon,
  // kappa, k_threshold.  Validates option expression types and coerces
  // them to target types if necessary.
  absl::Status ResolveAnonymizationOptionsList(
      const ASTOptionsList* options_list,
      std::vector<std::unique_ptr<const ResolvedOption>>* resolved_options);

  // Verify that the expression is an integer parameter or literal, returning
  // error status if not.
  absl::Status ValidateIntegerParameterOrLiteral(
      const char* clause_name, const ASTNode* ast_location,
      const ResolvedExpr& expr) const;

  // Validates the argument to LIMIT, OFFSET, ASSERT_ROWS_MODIFIED, or the
  // table sample clause.  The argument must be an integer parameter or
  // literal (possibly wrapped in an int64_t cast).  If the expr type is not
  // int64_t then <expr> is updated to be cast to int64_t.
  absl::Status ValidateParameterOrLiteralAndCoerceToInt64IfNeeded(
      const char* clause_name, const ASTNode* ast_location,
      std::unique_ptr<const ResolvedExpr>* expr) const;

  // <referencing_table> can be NULL.  If the table does not exist in the
  // Catalog, we  try to resolve the ALTER statement anyway.
  absl::Status ResolveAddConstraintAction(
      const Table* referencing_table, const ASTAlterStatementBase* alter_stmt,
      const ASTAddConstraintAction* alter_action,
      std::unique_ptr<const ResolvedAddConstraintAction>*
          resolved_alter_action);

  // <referencing_table> can be NULL. If the table does not exist in the catalog
  // and the alter statement uses IF EXISTS, we try to resolve the foriegn key
  // anyway.
  absl::Status ResolveAddForeignKey(
      const Table* referencing_table, const ASTAlterStatementBase* alter_stmt,
      const ASTAddConstraintAction* alter_action,
      std::unique_ptr<const ResolvedAddConstraintAction>*
          resolved_alter_action);

  // <target_table> can be NULL. If the table does not exist in the catalog and
  // the alter statement uses IF EXISTS, we try to resolve the primary key
  // anyway.
  absl::Status ResolveAddPrimaryKey(
      const Table* target_table, const ASTAlterStatementBase* alter_stmt,
      const ASTAddConstraintAction* alter_action,
      std::unique_ptr<const ResolvedAddConstraintAction>*
          resolved_alter_action);

  // Resolve the ASTType <type> as a Type <resolved_type>. If
  // <resolved_type_params> is not a nullptr, resolve any type parameters in
  // <type>.
  //
  // If <resolved_type_params> is a nullptr it means that type parameters are
  // disallowed by the caller of ResolveType() and should error out with
  // <type_parameter_context> in the error message if type parameters exist.
  // <type_parameter_context> must be specified if <resolved_type_params> is a
  // nullptr.
  absl::Status ResolveType(
      const ASTType* type,
      const absl::optional<absl::string_view> type_parameter_context,
      const Type** resolved_type, TypeParameters* resolved_type_params);

  absl::Status ResolveSimpleType(
      const ASTSimpleType* type,
      const absl::optional<absl::string_view> type_parameter_context,
      const Type** resolved_type, TypeParameters* resolved_type_params);

  absl::Status ResolveArrayType(
      const ASTArrayType* array_type,
      const absl::optional<absl::string_view> type_parameter_context,
      const ArrayType** resolved_type, TypeParameters* resolved_type_params);

  absl::Status ResolveStructType(
      const ASTStructType* struct_type,
      const absl::optional<absl::string_view> type_parameter_context,
      const StructType** resolved_type, TypeParameters* resolved_type_params);

  // Resolve type parameters to the resolved TypeParameters class, which stores
  // type parameters as a TypeParametersProto. If there are no type parameters,
  // then an empty TypeParameters class is returned. The type parameters can be
  // found in <type_parameters>. <resolved_type> must corresponds to the Type
  // returned when the ASTType parent of <type_parameters> is resolved.
  //
  // If the type is a STRUCT or ARRAY type, <child_parameter_list> should hold
  // the type parameters of the STRUCT fields or ARRAY elements. Otherwise,
  // <child_parameter_list> is empty.
  absl::StatusOr<TypeParameters> ResolveTypeParameters(
      const ASTTypeParameterList* type_parameters, const Type& resolved_type,
      const std::vector<TypeParameters>& child_parameter_list);

  // Resolve the simple type literals for each input type parameter. Valid
  // literal types are BOOL, BYTES, FLOAT, INT, STRING, and MAX.
  absl::StatusOr<std::vector<TypeParameterValue>> ResolveParameterLiterals(
      const ASTTypeParameterList& type_parameters);

  // Resolves operation collation for a function call from its argument list.
  // The collation will be stored in <function_call>.collation_list.
  // <error_location> is used for error messages.
  absl::Status MaybeResolveCollationForFunctionCallBase(
      const ASTNode* error_location, ResolvedFunctionCallBase* function_call);

  // Resolves operation collation for a subquery expression. Operation collation
  // is calculated only when the subquery type is IN; otherwise, this call
  // returns OK.
  // The collation will be stored in <subquery_expr>.in_collation.
  // <error_location> is used for error messages.
  absl::Status MaybeResolveCollationForSubqueryExpr(
      const ASTNode* error_location, ResolvedSubqueryExpr* subquery_expr);

  void FetchCorrelatedSubqueryParameters(
      const CorrelatedColumnsSet& correlated_columns_set,
      std::vector<std::unique_ptr<const ResolvedColumnRef>>* parameters);

  const absl::TimeZone default_time_zone() const {
    return analyzer_options_.default_time_zone();
  }

  bool in_strict_mode() const {
    return language().name_resolution_mode() == NAME_RESOLUTION_STRICT;
  }

  ProductMode product_mode() const { return language().product_mode(); }

  // Check our assumptions about value tables.
  // These errors shouldn't show up to users. They only happen if an engine
  // gives us a bad Table in the Catalog.
  absl::Status CheckValidValueTable(const ASTPathExpression* path_expr,
                                    const Table* table) const;
  absl::Status CheckValidValueTableFromTVF(const ASTTVF* path_expr,
                                           const std::string& full_tvf_name,
                                           const TVFRelation& schema) const;

  // Collapse the expression trees (present inside <node_ptr>) into literals if
  // possible, thus mutating the <node_ptr> subsequently.
  // This will not change any semantics of the tree and is mostly done to allow
  // typed struct literals as hints.
  void TryCollapsingExpressionsAsLiterals(
      const ASTNode* ast_location,
      std::unique_ptr<const ResolvedNode>* node_ptr);

  // Given a ResolvedUpdateStmt or ResolvedMergeStmt statement, this will call
  // RecordColumnAccess with READ access for scenarios where the AST does not
  // directly indicate a READ, but for which a READ is implied by the operation.
  // For example, all nested DML on arrays imply a READ because they allow
  // the caller to count the number of rows on the array. For example, the
  // following SQL will give an error if any rows exist, which should require
  // READ.
  //   UPDATE Table SET
  //   (DELETE ArrayCol WHERE CAST(ERROR("Rows found!") AS BOOL));
  // Similarly, access to fields of a proto/struct requires the engine to read
  // the old proto value before modifying it and writing it back. We can
  // consider relaxing this if needed in the future.
  // Array offsets also are implied READS even when used in the LHS because
  // the lack of a runtime exception tells the caller the array is at least the
  // size of the offset.
  absl::Status RecordImpliedAccess(const ResolvedStatement* statement);

  // Records access to a column (or vector of columns). Access is bitwise OR'd
  // with any existing access. If analyzer_options_.prune_unused_columns is
  // true, columns without any recorded access will be removed from the
  // table_scan().
  void RecordColumnAccess(const ResolvedColumn& column,
                          ResolvedStatement::ObjectAccess access_flags =
                              ResolvedStatement::READ);
  void RecordColumnAccess(const std::vector<ResolvedColumn>& columns,
                          ResolvedStatement::ObjectAccess access_flags =
                              ResolvedStatement::READ);

  // For all ResolvedScan nodes under <node>, prune the column_lists to remove
  // any columns not included in referenced_columns_.  This removes any columns
  // from the Resolved AST that were never referenced in the query.
  // NOTE: This mutates the column_list on Scan nodes in <tree>.
  // Must be called before SetColumnAccessList.
  absl::Status PruneColumnLists(const ResolvedNode* node) const;

  // Fills in <column_access_list> on <statement> to indicate, for each
  // ResolvedColumn in statement's <table_scan> whether it was read and/or
  // written. Only applies on ResolvedUpdateStmt and ResolvedMergeStmt.
  // Must be called after PruneColumnList.
  absl::Status SetColumnAccessList(ResolvedStatement* statement);

  // If the given expression is an untyped parameter, replaces it with an
  // equivalent parameter with type <type>. The return value indicates whether
  // the expression was replaced.
  absl::StatusOr<bool> MaybeAssignTypeToUndeclaredParameter(
      std::unique_ptr<const ResolvedExpr>* expr, const Type* type);

  // Checks that the type of a previously encountered parameter referenced at
  // <location> agrees with <type> and records it in undeclared_parameters_.
  // Erases the corresponding entry in untyped_undeclared_parameters_.
  absl::Status AssignTypeToUndeclaredParameter(
      const ParseLocationPoint& location, const Type* type);

  // Attempts to find a table in the catalog. Sets <table> to nullptr if not
  // found.
  absl::Status FindTable(const ASTPathExpression* name, const Table** table);

  // Attempts to find a column in <table> by <name>. Sets <index> to -1 if not
  // found; otherwise, sets it to the first column found, starting at index 0.
  // Sets <duplicate> to true if two or more were found.
  static void FindColumnIndex(const Table* table, const std::string& name,
                              int* index, bool* duplicate);

  // Returns true if two values of the given types can be tested for equality
  // either directly or by coercing the values to a common supertype.
  absl::StatusOr<bool> SupportsEquality(const Type* type1, const Type* type2);

  // Returns the column alias from <expr_resolution_info> if <ast_expr> matches
  // the top level expression in <expr_resolution_info>. Returns an empty
  // IdString if the <expr_resolution_info> has no top level expression,
  // <ast_expr> does not match, or the column alias is an internal alias.
  static IdString GetColumnAliasForTopLevelExpression(
      ExprResolutionInfo* expr_resolution_info, const ASTExpression* ast_expr);

  // Returns an error for an unrecognized identifier.  Errors take the form
  // "Unrecognized name: foo", with a "Did you mean <bar>?" suggestion added
  // if the path expression is sufficiently close to a symbol in <name_scope>
  // or <catalog_>.
  absl::Status GetUnrecognizedNameError(const ASTPathExpression* ast_path_expr,
                                        const NameScope* name_scope);

  // Returns an internal catalog used just for looking up system variables.
  // The results of this function are cached in system_variables_catalog_, so
  // only the first call actually populates the catalog.
  Catalog* GetSystemVariablesCatalog();

  // Checks if the signature in the TVF matches input arguments. This method
  // doesn't support signature overloading and assumes only one signature
  // supported by the TVF.
  // The <arg_locations> and <resolved_tvf_args> are function outputs, and
  // reflect and match 1:1 to the concrete function call arguments in the
  // <result_signature>.
  // Returning integer is the index of the matching signature, in this case,
  // it should always be 0 because this method is using the first signature to
  // match input arguments; if it doesn't match, this method return a non-OK
  // status.
  absl::StatusOr<int> MatchTVFSignature(
      const ASTTVF* ast_tvf, const TableValuedFunction* tvf_catalog_entry,
      const NameScope* external_scope, const NameScope* local_scope,
      const FunctionResolver& function_resolver,
      std::unique_ptr<FunctionSignature>* result_signature,
      std::vector<const ASTNode*>* arg_locations,
      std::vector<ResolvedTVFArg>* resolved_tvf_args,
      SignatureMatchResult* signature_match_result);

  // Generates an error status about a TVF call not matching a signature.
  // It is made to avoid redundant code in MatchTVFSignature.
  absl::Status GenerateTVFNotMatchError(
    const ASTTVF* ast_tvf,
    const SignatureMatchResult& signature_match_result,
    const TableValuedFunction& tvf_catalog_entry,
    const std::string& tvf_name,
    const std::vector<InputArgumentType>& input_arg_types,
    int signature_idx);

  // Struct to control the features to be resolved by
  // ResolveCreateTableStmtBaseProperties.
  struct ResolveCreateTableStmtBasePropertiesArgs {
    const bool table_element_list_enabled;
  };

  // Struct to store the properties of ASTCreateTableStmtBase.
  struct ResolveCreateTableStatementBaseProperties {
    std::vector<std::string> table_name;
    const Table* like_table = nullptr;
    std::unique_ptr<const ResolvedScan> clone_from;
    std::unique_ptr<const ResolvedScan> copy_from;
    ResolvedCreateStatement::CreateScope create_scope;
    ResolvedCreateStatement::CreateMode create_mode;
    std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
    std::vector<std::unique_ptr<const ResolvedColumnDefinition>>
        column_definition_list;
    std::vector<ResolvedColumn> pseudo_column_list;
    std::unique_ptr<ResolvedPrimaryKey> primary_key;
    std::vector<std::unique_ptr<const ResolvedForeignKey>> foreign_key_list;
    std::vector<std::unique_ptr<const ResolvedCheckConstraint>>
        check_constraint_list;
    std::unique_ptr<const ResolvedExpr> collation;
    std::vector<std::unique_ptr<const ResolvedExpr>> partition_by_list;
    std::vector<std::unique_ptr<const ResolvedExpr>> cluster_by_list;
    std::unique_ptr<const ResolvedWithPartitionColumns> with_partition_columns;
    std::unique_ptr<const ResolvedConnection> connection;
    bool is_value_table;
    std::unique_ptr<const ResolvedScan> query_scan;
    std::vector<std::unique_ptr<const ResolvedOutputColumn>> output_column_list;

    // Input columns that are visible if the statement has no explicit column
    // definitions.
    ResolvedColumnList default_visible_columns;

    // Gets either the explicit columns if present, or the
    // default_visible_columns.
    absl::Status GetVisibleColumnNames(NameList* column_names) const;

    // Add names from the "WITH PARTITION COLUMNS" clause to 'column_names'
    // if they aren't already there.
    absl::Status WithPartitionColumnNames(NameList* column_names) const;
  };

  // Resolves the shared properties of the statements inheriting from
  // ASTCreateTableStmtBase (ASTCreateTableStatement,
  // ASTCreateExternalTableStatement). The optional features are resolved on the
  // basis of flag values in resolved_properties_control_args.
  absl::Status ResolveCreateTableStmtBaseProperties(
      const ASTCreateTableStmtBase* ast_statement,
      absl::string_view statement_type,
      const ASTPathExpression* like_table_name, const ASTQuery* query,
      const ASTCollate* collate, const ASTPartitionBy* partition_by,
      const ASTClusterBy* cluster_by,
      const ASTWithPartitionColumnsClause* with_partition_columns_clause,
      const ASTWithConnectionClause* with_connection_clause,
      const ResolveCreateTableStmtBasePropertiesArgs&
          resolved_properties_control_args,
      ResolveCreateTableStatementBaseProperties* statement_base_properties);

  // Resolve WithPartitionColumnsClause and also update column_indexes with all
  // the resolved columns from WithPartitionColumnsClause.
  absl::Status ResolveWithPartitionColumns(
      const ASTWithPartitionColumnsClause* with_partition_columns_clause,
      const IdString table_name_id_string, ColumnIndexMap* column_indexes,
      std::unique_ptr<const ResolvedWithPartitionColumns>*
          resolved_with_partition_columns);

  // Computes the pivot-value portion of the name of a pivot column without an
  // explicit alias. <pivot_value> represents the literal IN-clause value used
  // to generate the name. If successful, the generated column name is appended
  // to <*column_name>.
  //
  // Pivot column names are not guaranteed uniqueness; if the same pivot value
  // is used multiple times, the same column name will result.
  //
  // Returns a failed status if we do not support a default name for
  // <pivot_value>. <ast_location> determines the parse location to use for
  // error messages. It is not used unless there's an error.
  absl::Status AppendPivotColumnName(const Value& pivot_value,
                                     const ASTNode* ast_location,
                                     std::string* column_name);

  // Implements AppendPivotColumnName() for the specific case where
  // <pivot_value> is a non-null value of one of the following types:
  //   BOOL/INT32/INT64/UINT32/UINT64/NUMERIC/BIGNUMERIC.
  //
  // Generates the column name by casting the value to STRING (which, for the
  //   above types is guaranteed to succeed), then performing
  //   the following transformations to ensure that the result is a valid
  //   ZetaSQL identifier.
  //  - If the first character is a digit *and* <column_name> is currently
  //    empty (e.g. if no pivot expression alias exists), prepends the generated
  //    name with "_".
  //  - Replaces "-" with "minus_".
  //  - Replaces "." with "_point_".
  absl::Status AppendPivotColumnNameViaStringCast(const Value& pivot_value,
                                                  std::string* column_name);

  absl::StatusOr<ResolvedColumn> CreatePivotColumn(
      const ASTPivotExpression* ast_pivot_expr,
      const ResolvedExpr* resolved_pivot_expr, bool is_only_pivot_expr,
      const ASTPivotValue* ast_pivot_value,
      const ResolvedExpr* resolved_pivot_value);

  absl::Status ResolvePivotExpressions(
      const ASTPivotExpressionList* ast_pivot_expr_list, const NameScope* scope,
      std::vector<std::unique_ptr<const ResolvedExpr>>* pivot_expr_columns,
      QueryResolutionInfo& query_resolution_info);

  absl::Status ResolveForExprInPivotClause(
      const ASTExpression* for_expr, const NameScope* scope,
      std::unique_ptr<const ResolvedExpr>* resolved_for_expr);

  absl::Status ResolveInClauseInPivotClause(
      const ASTPivotValueList* pivot_values, const NameScope* scope,
      const Type* for_expr_type,
      std::vector<std::unique_ptr<const ResolvedExpr>>* resolved_in_exprs);

  // Resolves a PIVOT clause denoted by <ast_pivot_clause>.
  //
  // Expressions inside the pivot clause are resolved using <input_name_list>
  // with <previous_scope> as a fallback scope for names not in the list.
  //
  // On success, sets <output> and <output_name_list> to a scan and name list
  // describing the PIVOT output.
  absl::Status ResolvePivotClause(
      std::unique_ptr<const ResolvedScan> input_scan,
      std::shared_ptr<const NameList> input_name_list,
      const NameScope* previous_scope, bool input_is_subquery,
      const ASTPivotClause* ast_pivot_clause,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  absl::Status ResolveUnpivotOutputValueColumns(
      const ASTPathExpressionList* ast_unpivot_expr_list,
      std::vector<ResolvedColumn>* unpivot_value_columns,
      const std::vector<const Type*>& value_column_types,
      const NameScope* scope);

  absl::Status ResolveUnpivotInClause(
      const ASTUnpivotInItemList* ast_unpivot_expr_list,
      std::vector<std::unique_ptr<const ResolvedUnpivotArg>>* resolved_in_items,
      const std::vector<ResolvedColumn>* input_scan_columns,
      absl::flat_hash_set<ResolvedColumn>* in_clause_input_columns,
      std::vector<const Type*>* val_column_type, const Type** label_type,
      std::vector<std::unique_ptr<const ResolvedLiteral>>* resolved_label_list,
      const ASTUnpivotClause* ast_unpivot_clause, const NameScope* scope);

  // Gets either the explicitly provided label or otherwise auto-generated
  // string label by concatenating columns names from column groups in the IN
  // clause of UNPIVOT.
  absl::StatusOr<Value> GetLabelForUnpivotInColumnList(
      const ASTUnpivotInItem* in_column_list);

  // Resolves an UNPIVOT clause.
  //  - <input_scan> represents the input to unpivot. On success, ownership is
  //      transferred to 'output'.
  //  - <input_name_list> represents a name list for columns in the input scan.
  //      This defines the list of valid columns when resolving unpivot
  //      expressions and FOR expressions.
  //  - <ast_unpivot_clause> represents the parse tree of the entire unpivot
  //  clause.
  //  - On output, '*output' contains a scan representing the result of the
  //      UNPIVOT clause and '*output_name_list' contains a name list which can
  //      be used by external clauses to refer to columns in the UNPIVOT output.
  absl::Status ResolveUnpivotClause(
      std::unique_ptr<const ResolvedScan> input_scan,
      std::shared_ptr<const NameList> input_name_list,
      const NameScope* previous_scope,
      const ASTUnpivotClause* ast_unpivot_clause,
      std::unique_ptr<const ResolvedScan>* output,
      std::shared_ptr<const NameList>* output_name_list);

  absl::Status ResolveAuxLoadDataStatement(
      const ASTAuxLoadDataStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output);

  // Recursively translate the CollationAnnotation in <type_annotation_map> into
  // a ResolvedColumnAnnotations object.
  absl::StatusOr<std::unique_ptr<ResolvedColumnAnnotations>>
  MakeResolvedColumnAnnotationsWithCollation(
      const AnnotationMap* type_annotation_map);

  // Creates a name scope with all column names with access errors. When default
  // value expression references a column in the name scope, it throws error.
  absl::StatusOr<std::unique_ptr<NameScope>>
  CreateNameScopeWithAccessErrorForDefaultExpr(
      IdString table_name_id_string, std::vector<IdString>& all_column_names);

  friend class AnalyticFunctionResolver;
  friend class FunctionResolver;
  friend class FunctionResolverTest;
  friend class ResolverTest;
};

// Encapsulates metadata about function arguments when resolving a
// `CREATE ... FUNCTION` statement or when resolving the body of an invoked
// function template.
class FunctionArgumentInfo {
 public:
  // Details about a specific argument.
  struct ArgumentDetails {
    IdString name;
    FunctionArgumentType arg_type;
    // <arg_kind> is used only for scalar arguments.
    std::optional<ResolvedArgumentDef::ArgumentKind> arg_kind;
  };

  // Returns true if there is an arg <name>.
  bool HasArg(const IdString& name) const;

  // Returns a pointer to the argument details if a relational argument <name>
  // is found in this metadata. Otherwise, returns nullptr.
  const ArgumentDetails* FindTableArg(IdString name) const;

  // Returns a pointer to the argument details if a scalar argument <name> is
  // found in this metadata. Otherwise, returns nullptr.
  const ArgumentDetails* FindScalarArg(IdString name) const;

  // Returns a pointer to the argument details if an argument <name> is found in
  // this metadata. Otherwise, returns nullptr.
  const ArgumentDetails* FindArg(IdString name) const;

  // Returns true if any of the arguments contained is a template argument.
  bool contains_templated_arguments() const {
    return contains_templated_arguments_;
  }

  // Returns a list of argument names as strings in the order they were added.
  std::vector<std::string> ArgumentNames() const;

  // Returns a list of argument types in the order they were added. This is used
  // when constructing a FunctionSignature.
  FunctionArgumentTypeList SignatureArguments() const;

  // Add details of a scalar argument.
  absl::Status AddScalarArg(IdString name,
                            ResolvedArgumentDef::ArgumentKind arg_kind,
                            FunctionArgumentType arg_type);

  // Add details for a relation argument.
  absl::Status AddRelationArg(IdString name, FunctionArgumentType arg_type);

 private:
  // std::unique_ptr is used to ensure stability of any pointers to details
  // returned even when more arguments are added to the details_ list.
  // details_ is stored in argument order to enable constructing function
  // signature and name lists.
  std::vector<std::unique_ptr<ArgumentDetails>> details_;
  // This map functions as an index of details to make lookup-by-name cheap and
  // idiomatic.
  IdStringHashMapCase<int64_t> details_index_by_name_;
  bool contains_templated_arguments_ = false;

  absl::Status AddArgCommon(ArgumentDetails details);
};

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_RESOLVER_H_
