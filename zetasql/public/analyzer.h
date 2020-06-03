//
// Copyright 2019 ZetaSQL Authors
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

#ifndef ZETASQL_PUBLIC_ANALYZER_H_
#define ZETASQL_PUBLIC_ANALYZER_H_

#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/base/atomic_sequence_num.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/proto/options.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/node_hash_set.h"
#include "zetasql/base/case.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

class ASTExpression;
class ASTScript;
class ASTStatement;
class ParseResumeLocation;
class ParserOptions;
class ParserOutput;
class ResolvedExpr;
class ResolvedLiteral;
class ResolvedOption;
class ResolvedStatement;

// Performs a case-insensitive less-than vector<string> comparison, element
// by element, using the C/POSIX locale for element comparisons. This function
// object is useful as a template parameter for STL set/map of
// std::vector<string>s, if uniqueness of the vector keys is case-insensitive.
struct StringVectorCaseLess {
  bool operator()(const std::vector<std::string>& v1,
                  const std::vector<std::string>& v2) const;
};

typedef std::map<std::string, const Type*> QueryParametersMap;

// Key = name path of system variable.  Value = type of variable.
// Name elements in the key do not include the "@@" prefix.
// For example, if @@foo.bar has type INT32, the corresponding map entry is:
//    key = {"foo", "bar"}
//    value = type_factory->get_int32()
typedef std::map<std::vector<std::string>, const Type*, StringVectorCaseLess>
    SystemVariablesMap;

// Associates each system variable with its current value.
using SystemVariableValuesMap =
    std::map<std::vector<std::string>, Value, StringVectorCaseLess>;

// This class specifies a set of allowed hints and options, and their expected
// types.
//
// Each hint or option has an expected Type, which can be NULL.  If the
// expected type is NULL, then any type is allowed.
// If a type is specified, the resolved value for the hint will always have
// the expected type, and the analyzer will give an error if coercion is
// not possible.
//
// Hint, option and qualifier names are all case insensitive.
// The resolved AST will contain the original case as written by the user.
//
// The <disallow_unknown_options> and <disallow_unknown_hints_with_qualifiers>
// fields can be set to indicate that errors should be given on unknown
// options or hints (with specific qualifiers).  Unknown hints with other
// qualifiers do not cause errors.
struct AllowedHintsAndOptions {
  AllowedHintsAndOptions() {}
  AllowedHintsAndOptions(const AllowedHintsAndOptions&) = default;
  AllowedHintsAndOptions& operator=(const AllowedHintsAndOptions&) = default;

  // This is recommended constructor to use for normal settings.
  // All supported hints and options should be added with the Add methods.
  // Unknown options will be errors.
  // Unknown hints without qualifiers, or with <qualifier>, will be errors.
  // Unkonwn hints with other qualifiers will be allowed (because these are
  // typically interpreted as hints intended for other engines).
  explicit AllowedHintsAndOptions(const std::string& qualifier) {
    disallow_unknown_options = true;
    disallow_unknown_hints_with_qualifiers.insert("");
    disallow_unknown_hints_with_qualifiers.insert(qualifier);
  }

  // Add an option.  <type> may be NULL to indicate that all Types are allowed.
  void AddOption(const std::string& name, const Type* type);

  // Add a hint.
  // <qualifier> may be empty to add this hint only unqualified, but hints
  //    for some engine should normally allow the engine name as a qualifier.
  // <type> may be NULL to indicate that all Types are allowed.
  // If <allow_unqualified> is true, this hint is allowed both unqualified
  //   and qualified with <qualifier>.
  void AddHint(const std::string& qualifier, const std::string& name,
               const Type* type, bool allow_unqualified = true);

  // Deserialize AllowedHintsAndOptions from proto. Types will be deserialized
  // using the given TypeFactory and Descriptors from the given DescriptorPools.
  // The TypeFactory and the DescriptorPools must both outlive the result
  // AllowedHintsAndOptions.
  static absl::Status Deserialize(
      const AllowedHintsAndOptionsProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory,
      AllowedHintsAndOptions* result);

  // Serialize this AllowedHIntsAndOptions into protobuf. The provided map
  // is used to store serialized FileDescriptorSets, which can be deserialized
  // into separate DescriptorPools in order to reconstruct the Type. The map
  // may be non-empty and may be used across calls to this method in order to
  // serialize multiple types. The map may NOT be null.
  absl::Status Serialize(
      FileDescriptorSetMap* file_descriptor_set_map,
      AllowedHintsAndOptionsProto* proto) const;

  // If true, give an error for an unknown option.
  bool disallow_unknown_options = false;

  // For each qualifier in this set, give errors for unknown hints with that
  // qualifier.  If "" is in the set, give errors for unknown unqualified hints.
  std::set<std::string, zetasql_base::StringCaseLess> disallow_unknown_hints_with_qualifiers;

  // Maps containing declared hints and options, keyed on lower case strings.
  //
  // For hints, the key is (qualifier, hint).  Unqualified hints are declared
  // using an empty qualifier. The same hint is typically added twice, once
  // qualified and once unqualified.
  //
  // If the map value (a Type*) is nullptr, that declares that the hint exists
  // but that it does not have an enforced Type, so any Type is allowed.
  // (We could allow callbacks or some other mechanism to specify more rules
  // about what types or values are allowed.)
  absl::flat_hash_map<std::pair<std::string, std::string>, const Type*>
      hints_lower;
  absl::flat_hash_map<std::string, const Type*> options_lower;

 private:
  absl::Status AddHintImpl(const std::string& qualifier,
                           const std::string& name, const Type* type,
                           bool allow_unqualified = true);
  absl::Status AddOptionImpl(const std::string& name, const Type* type);
};

// AnalyzerOptions contains options that affect analyzer behavior. The language
// options that control the language accepted are accessible via the
// language() member.
class AnalyzerOptions {
 public:
  typedef std::function<absl::Status(const std::string&, const Type**)>
      LookupExpressionColumnCallback;

  // Callback to retrieve pseudo-columns for the target of a DDL statement.
  // <options> is the contents of the OPTIONS clause attached to the statement,
  // if any. The callback populates <pseudo_columns> with the names and types of
  // the pseudo-columns for the table. The names must be valid identifiers and
  // must be distinct.
  using DdlPseudoColumnsCallback = std::function<absl::Status(
      const std::vector<std::string>& table_name,
      const std::vector<const ResolvedOption*>& options,
      std::vector<std::pair<std::string, const Type*>>* pseudo_columns)>;

  AnalyzerOptions();
  explicit AnalyzerOptions(const LanguageOptions& language_options);
  AnalyzerOptions(const AnalyzerOptions& options) = default;
  AnalyzerOptions(AnalyzerOptions&& options) = default;
  AnalyzerOptions& operator=(const AnalyzerOptions& options) = default;
  AnalyzerOptions& operator=(AnalyzerOptions&& options) = default;
  ~AnalyzerOptions();

  // Deserialize AnalyzerOptions from proto. Types will be deserialized using
  // the given TypeFactory and Descriptors from the given DescriptorPools.
  // The TypeFactory and the DescriptorPools must both outlive the result
  // AnalyzerOptions.
  static absl::Status Deserialize(
      const AnalyzerOptionsProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory,
      AnalyzerOptions* result);

  // Serialize the options into protobuf. The provided map is used to store
  // serialized FileDescriptorSets, which can be deserialized into separate
  // DescriptorPools in order to reconstruct the Type. The map may be
  // non-empty and may be used across calls to this method in order to
  // serialize multiple types. The map may NOT be null.
  absl::Status Serialize(
      FileDescriptorSetMap* map, AnalyzerOptionsProto* proto) const;

  // Options for the language. Please use the shorter versions below; these
  // versions are deprecated and will be removed eventually.
  // TODO: Migrate clients and remove these.
  const LanguageOptions& language_options() const { return language_options_; }
  LanguageOptions* mutable_language_options() { return &language_options_; }

  void set_language_options(const LanguageOptions& options) {
    language_options_ = options;
  }

  // Shorter equivalents for language().
  const LanguageOptions& language() const { return language_options_; }
  LanguageOptions* mutable_language() { return &language_options_; }

  void set_language(const LanguageOptions& options) {
    language_options_ = options;
  }

  // Options for Find*() name lookups into the Catalog.
  const Catalog::FindOptions& find_options() const { return find_options_; }
  Catalog::FindOptions* mutable_find_options() { return &find_options_; }
  void set_find_options(const Catalog::FindOptions& options) {
    find_options_ = options;
  }

  // Adds a named query parameter.
  // Parameter name lookups are case insensitive. Paramater names in the output
  // ResolvedParameter nodes will always be in lowercase.
  //
  // ZetaSQL only uses the parameter Type and not the Value. Query analysis is
  // not dependent on the value, and query engines may substitute a value after
  // analysis.
  //
  // For example, for the query
  //   SELECT * FROM table WHERE CustomerId = @customer_id
  // the parameter can be added using
  //   analyzer_options.AddQueryParameter("customer_id", types::Int64Type());
  //
  // Note that an error will be produced if type is not supported according to
  // the current language options.
  absl::Status AddQueryParameter(const std::string& name, const Type* type);

  const QueryParametersMap& query_parameters() const {
    return query_parameters_;
  }

  // Clears <query_parameters_>.
  void clear_query_parameters() {
    query_parameters_.clear();
  }

  // Adds a positional query parameter.
  //
  // ZetaSQL only uses the parameter Type and not the Value. Query analysis is
  // not dependent on the value, and query engines may substitute a value after
  // analysis.
  //
  // For example, for the query
  //   SELECT * FROM table WHERE CustomerId = ?
  // the parameter can be added using
  //   analyzer_options.AddPositionalQueryParameter(types::Int64Type());
  //
  // Note that an error will be produced if type is not supported according to
  // the current language options. At least as many positional parameters must
  // be provided as there are ? in the query. When allow_undeclared_parameters
  // is true, no positional parameters may be provided.
  absl::Status AddPositionalQueryParameter(const Type* type);

  // Defined positional parameters. Only used in positional parameter mode.
  // Index 0 corresponds with the query parameter at position 1 and so on.
  const std::vector<const Type*>& positional_query_parameters() const {
    return positional_query_parameters_;
  }

  // Clears <positional_query_parameters_>.
  void clear_positional_query_parameters() {
    positional_query_parameters_.clear();
  }

  // Add columns that are visible when resolving standalone expressions.
  // These are used only in AnalyzeExpression, and have no effect on other
  // analyzer entrypoints.
  //
  // AddExpressionColumn is used to add one or more columns resolvable by name.
  //
  // SetInScopeExpressionColumn is used to add at most one expression column
  // that can be resolved by name (if <name> is non-empty), and is also
  // implicitly in scope so that fields on the value can be used directly,
  // without qualifiers.
  // Expression column names take precedence over in-scope field names.
  //
  // SetLookupExpressionColumnCallback is used to add a callback function to
  // resolve expression columns. The columns referenced in the expressions but
  // not added in the above functions will be resolved using the callback
  // function. The column name passed in the callback function is always in the
  // lower case.
  //
  // Column name lookups are case insensitive.  Columns names in the output
  // ResolvedExpressionColumn nodes will always be in lowercase.
  //
  // For example, to support the expression
  //   enabled = true AND cost > 0.0
  // those columns can be added using
  //   analyzer_options.AddExpressionColumn("enabled", types::BoolType());
  //   analyzer_options.AddExpressionColumn("cost", types::DoubleType());
  //
  // To evaluate an expression in the scope of a particular proto, like
  //   has_cost AND cost > 0 AND value.cost != 10
  // we can add that proto type as an in-scope expression column.
  //   TypeFactory type_factory;
  //   const ProtoType* proto_type;
  //   ZETASQL_CHECK_OK(type_factory.MakeProtoType(MyProto::descriptor(), &proto_type);
  //   analyzer_options.SetInScopeExpressionColumn("value", proto_type);
  // The proto in the example has a `cost` field.  We can also access the
  // whole proto as `value` because we provided that name.
  //
  // Note that an error will be produced if type is not supported according to
  // the current language options.
  absl::Status AddExpressionColumn(const std::string& name, const Type* type);
  absl::Status SetInScopeExpressionColumn(const std::string& name,
                                          const Type* type);
  void SetLookupExpressionColumnCallback(
      const LookupExpressionColumnCallback& lookup_expression_column_callback) {
    lookup_expression_column_callback_ = lookup_expression_column_callback;
  }

  LookupExpressionColumnCallback lookup_expression_column_callback() const {
    return lookup_expression_column_callback_;
  }

  // Get the named expression columns added.
  // This will include the in-scope expression column if one was set.
  // This doesn't include the columns resolved using the
  // 'lookup_expression_column_callback_' function.
  const QueryParametersMap& expression_columns() const {
    return expression_columns_;
  }

  bool has_in_scope_expression_column() const {
    return in_scope_expression_column_type() != nullptr;
  }
  // Get the name and Type of the in-scope expression column.
  // These return "" and NULL if there is no in-scope expression column.
  const std::string& in_scope_expression_column_name() const {
    return in_scope_expression_column_.first;
  }
  const Type* in_scope_expression_column_type() const {
    return in_scope_expression_column_.second;
  }

  // Provides the set of pseudo-columns that will be visible on tables created
  // with DDL statements. These columns can be referenced in the PARTITION BY
  // and CLUSTER BY clauses on CREATE TABLE.
  // A fixed set of pseudo-columns can be provided using SetDdlPseudoColumns, or
  // the set can be computed by a callback per table using
  // SetDdlPseudoColumnsCallback.
  // Note that the callback version does not support serialization in
  // AnalyzerOptionsProto.
  void SetDdlPseudoColumnsCallback(
      DdlPseudoColumnsCallback ddl_pseudo_columns_callback);
  void SetDdlPseudoColumns(
      const std::vector<std::pair<std::string, const Type*>>&
          ddl_pseudo_columns);
  // Returns the callback to access pseudo-columns for the target of a DDL
  // statement.
  const DdlPseudoColumnsCallback& ddl_pseudo_columns_callback() const {
    return ddl_pseudo_columns_callback_;
  }

  void set_column_id_sequence_number(zetasql_base::SequenceNumber* sequence) {
    column_id_sequence_number_ = sequence;
  }
  zetasql_base::SequenceNumber* column_id_sequence_number() const {
    return column_id_sequence_number_;
  }

  // Sets an IdStringPool for storing strings used in parsing and analysis.
  // If it is not set, then analysis will create a new IdStringPool for every
  // query that is analyzed. WARNING: If this is set, calling Analyze functions
  // concurrently with the same AnalyzerOptions is not allowed.
  void set_id_string_pool(const std::shared_ptr<IdStringPool>& id_string_pool) {
    id_string_pool_ = id_string_pool;
  }
  std::shared_ptr<IdStringPool> id_string_pool() const {
    return id_string_pool_;
  }

  // Sets an zetasql_base::UnsafeArena for storing objects created during parsing and
  // analysis. If it is not set, then analysis will create a new zetasql_base::UnsafeArena for
  // every query that is analyzed. WARNING: If this is set, calling Analyze
  // functions concurrently with the same AnalyzerOptions is not allowed.
  void set_arena(std::shared_ptr<zetasql_base::UnsafeArena> arena) {
    arena_ = std::move(arena);
  }
  std::shared_ptr<zetasql_base::UnsafeArena> arena() const { return arena_; }

  // Creates default-sized id_string_pool() and arena().
  // WARNING: After calling this, calling Analyze functions concurrently with
  // the same AnalyzerOptions is no longer allowed.
  void CreateDefaultArenasIfNotSet();

  // Returns true if arena() and id_string_pool() are both non-NULL.
  bool AllArenasAreInitialized() const {
    return arena_ != nullptr && id_string_pool_ != nullptr;
  }

  static constexpr ErrorMessageMode ERROR_MESSAGE_WITH_PAYLOAD =
      zetasql::ERROR_MESSAGE_WITH_PAYLOAD;
  static constexpr ErrorMessageMode ERROR_MESSAGE_ONE_LINE =
      zetasql::ERROR_MESSAGE_ONE_LINE;
  static constexpr ErrorMessageMode ERROR_MESSAGE_MULTI_LINE_WITH_CARET =
      zetasql::ERROR_MESSAGE_MULTI_LINE_WITH_CARET;

  void set_error_message_mode(ErrorMessageMode mode) {
    error_message_mode_ = mode;
  }
  ErrorMessageMode error_message_mode() const { return error_message_mode_; }

  // TODO Should this be a LanguageOption instead?
  void set_default_time_zone(absl::TimeZone timezone) {
    default_timezone_ = timezone;
  }
  const absl::TimeZone default_time_zone() const { return default_timezone_; }

  void set_statement_context(StatementContext context) {
    statement_context_ = context;
  }
  StatementContext statement_context() const {
    return statement_context_;
  }

  // Set this to true to record parse locations in resolved AST nodes, which
  // can be obtained via ResolvedNode::GetParseLocationRangeOrNULL().
  // Currently, only some types of nodes have parse locations. Parse locations
  // may be added for more nodes over time.
  void set_record_parse_locations(bool value) {
    record_parse_locations_ = value;
  }
  bool record_parse_locations() const {
    return record_parse_locations_;
  }

  void set_create_new_column_for_each_projected_output(bool value) {
    create_new_column_for_each_projected_output_ = value;
  }
  bool create_new_column_for_each_projected_output() const {
    return create_new_column_for_each_projected_output_;
  }

  // Controls whether undeclared parameters are allowed. Undeclared parameters
  // don't appear in query_parameters(). Their type will be assigned by the
  // analyzer in the output AST and returned in
  // AnalyzerOutput::undeclared_parameters() or
  // AnalyzerOutput::undeclared_positional_parameters() depending on the
  // parameter mode. When allow_undeclared_parameters is true and the parameter
  // mode is positional, no positional parameters may be provided in
  // AnalyzerOptions.
  void set_allow_undeclared_parameters(bool value) {
    allow_undeclared_parameters_ = value;
  }
  bool allow_undeclared_parameters() const {
    return allow_undeclared_parameters_;
  }

  // Controls whether positional parameters are allowed. The analyzer supports
  // either named parameters or positional parameters but not both in the same
  // query.
  void set_parameter_mode(ParameterMode mode) {
    parameter_mode_ = mode;
  }
  ParameterMode parameter_mode() const {
    return parameter_mode_;
  }

  void set_prune_unused_columns(bool value) { prune_unused_columns_ = value; }
  bool prune_unused_columns() const { return prune_unused_columns_; }

  void set_allowed_hints_and_options(const AllowedHintsAndOptions& allowed) {
    allowed_hints_and_options_ = allowed;
  }
  const AllowedHintsAndOptions& allowed_hints_and_options() const {
    return allowed_hints_and_options_;
  }

  // Controls whether to preserve aliases of aggregate columns and analytic
  // function columns. This option has no effect on query semantics and just
  // changes what names are used inside ResolvedColumns.
  //
  // If true, the analyzer uses column aliases as names of aggregate columns and
  // analytic function columns if they exist, and falls back to using internal
  // names such as "$agg1" otherwise. If false, the analyzer uses internal names
  // unconditionally.
  //
  // TODO: Make this the default and remove this option.
  void set_preserve_column_aliases(bool value) {
    preserve_column_aliases_ = value;
  }
  bool preserve_column_aliases() const { return preserve_column_aliases_; }

  // Returns the ParserOptions to use for these AnalyzerOptions, including the
  // same id_string_pool() and arena() values.
  ParserOptions GetParserOptions() const;

  const SystemVariablesMap& system_variables() const {
    return system_variables_;
  }
  void clear_system_variables() {
    system_variables_.clear();
  }
  absl::Status AddSystemVariable(const std::vector<std::string>& name_path,
                                 const Type* type);

  // DEPRECATED: WILL BE REMOVED SOON
  // If <types> is non empty, the result of analyzed SQL will be coerced to the
  // given types using assignment semantics, requiring that the passed in SQL:
  // * Is a query statement
  // * Has a matching number of columns as the number of given types
  // * Has columns whose types are coercible to the given types based on
  //   positional order
  // * Does not use untyped parameters as column output
  // Otherwise an analysis error will be returned.
  // Note that if a query produces a value table, the provided types must
  // only contain a single struct or proto type.
  // TODO: Remove this last condition
  void set_target_column_types(absl::Span<const Type* const> types) {
    target_column_types_ = std::vector<const Type*>(types.begin(), types.end());
  }
  absl::Span<const Type* const> get_target_column_types() const {
    return target_column_types_;
  }

 private:
  // ======================================================================
  // NOTE: Please update options.proto and AnalyzerOptions.java accordingly
  // when adding new fields here.
  // ======================================================================

  // These options determine the language that is accepted.
  LanguageOptions language_options_;

  // These options are used for name lookups into the catalog, i.e., for
  // Catalog::Find*() calls.
  Catalog::FindOptions find_options_;

  // Maps of defined parameters and expression columns (including in-scope).
  // The keys are lowercased.  Only used in named parameter mode.
  // This doesn't include the columns resolved using the
  // 'lookup_expression_column_callback_' function.
  QueryParametersMap query_parameters_;
  QueryParametersMap expression_columns_;

  // Maps system variables to their types.
  SystemVariablesMap system_variables_;

  // Callback function to resolve columns in standalone expressions.
  LookupExpressionColumnCallback lookup_expression_column_callback_ = nullptr;

  // Defined positional parameters. Only used in positional parameter mode.
  // Index 0 corresponds with the query parameter at position 1 and so on.
  std::vector<const Type*> positional_query_parameters_;

  // If we have an in-scope expression column, its name and Type are stored
  // here (and also in expression_columns_).  The name may be empty.
  std::pair<std::string, const Type*> in_scope_expression_column_;

  // Callback to retrieve pseudo-columns visible in top-level PARTITION BY and
  // CLUSTER BY clauses of DDL statements analyzed using AnalyzeStatement, or
  // else an explicit list of columns. If ddl_pseudo_columns_ is non-empty, the
  // callback is a wrapper that returns ddl_pseudo_columns_ as output regardless
  // of the input.
  DdlPseudoColumnsCallback ddl_pseudo_columns_callback_;
  std::vector<std::pair<std::string, const Type*>> ddl_pseudo_columns_;

  // If set, use this to allocate column_ids for the resolved AST.
  // This can be used to analyze multiple queries and ensure that
  // all resolved ASTs have non-overlapping column_ids.
  zetasql_base::SequenceNumber* column_id_sequence_number_ = nullptr;  // Not owned.

  // Allocate parts of the parse tree and resolved AST in this arena.
  // The arena will also be referenced in AnalyzerOutput to keep it alive.
  std::shared_ptr<zetasql_base::UnsafeArena> arena_;

  // Allocate all IdStrings in the resolved AST in this pool.
  // The pool will also be referenced in AnalyzerOutput to keep it alive.
  std::shared_ptr<IdStringPool> id_string_pool_;

  ErrorMessageMode error_message_mode_ = ERROR_MESSAGE_ONE_LINE;

  // Some timestamp-related functions take an optional timezone argument, and
  // allow a default timezone to be used if the argument is not provided.
  // The <default_timezone_> may also be used when coercing string literals
  // to timestamp literals during analysis.  Defaults to America/Los_Angeles.
  absl::TimeZone default_timezone_;

  // This identifies the ZetaSQL resolution context - whether we are in
  // a normal statement context or whether we are resolving statements
  // in a module.  See (broken link) for details.
  StatementContext statement_context_ = CONTEXT_DEFAULT;

  // If set to true, record parse locations in ResolvedNodes.
  bool record_parse_locations_ = false;

  // If set to true, creates a new column for each output produced by each
  // ResolvedProjectScan. This means that each entry in the column_list will
  // always have a corresponding entry in the expr_list.
  //
  // Here is an example:
  //
  //  SELECT * FROM (SELECT a AS b FROM (SELECT 1 AS a));
  //
  //  option      scan      column_list        expr_list
  // -------- ------------- ----------- -----------------------
  //          SELECT 1 AS a    [a#1]      [a#1 := Literal(1)]
  //   false  SELECT a AS b    [a#1]              []
  //          SELECT *         [a#1]              []
  // -------- ------------- ----------- -----------------------
  //          SELECT 1 AS a    [a#1]      [a#1 := Literal(1)]
  //   true   SELECT a AS b    [b#2]    [b#2 := ColumnRef(a#1)]
  //          SELECT *         [b#3]    [b#3 := ColumnRef(b#2)]
  // -------- ------------- ----------- -----------------------
  //
  // Setting this option to true results in a larger resolved AST, but has the
  // benefit that it provides a place to store additional information about how
  // the query was parsed. In combination with record_parse_locations, it
  // allows keeping track of all places in the query where columns are
  // referenced, which is useful to highlight long chains of dependencies
  // through complex queries composed of deeply nested subqueries.
  bool create_new_column_for_each_projected_output_ = false;

  bool allow_undeclared_parameters_ = false;

  ParameterMode parameter_mode_ = PARAMETER_NAMED;

  // If true, columns that were never referenced in the query will be pruned
  // from column_lists of all ResolvedScans.  This allows using the column_list
  // on ResolvedTableScan for column-level ACL checking.
  // If false, ResolvedTableScans will include all columns on the table,
  // regardless of what the user selected.
  //
  // TODO I want to make this the default once engines are updated,
  // and then remove this option.
  bool prune_unused_columns_ = false;

  // This specifies the set of allowed hints and options, their expected
  // types, and whether to give errors on unrecognized names.
  // See the class definition for details.
  AllowedHintsAndOptions allowed_hints_and_options_;

  // Controls whether to preserve aliases of aggregate columns and analytic
  // function columns. See set_preserve_column_aliases() for details.
  bool preserve_column_aliases_ = true;

  // Target output column types for a query.
  std::vector<const Type*> target_column_types_;

  // Copyable
};

class AnalyzerOutputProperties {
 public:
  AnalyzerOutputProperties() {}
 private:

  // Copyable
};

class AnalyzerOutput {
 public:
  AnalyzerOutput(
      std::shared_ptr<IdStringPool> id_string_pool,
      std::shared_ptr<zetasql_base::UnsafeArena> arena,
      std::unique_ptr<const ResolvedStatement> statement,
      const AnalyzerOutputProperties& analyzer_output_properties,
      std::unique_ptr<ParserOutput> parser_output,
      const std::vector<absl::Status>& deprecation_warnings,
      const QueryParametersMap& undeclared_parameters,
      const std::vector<const Type*>& undeclared_positional_parameters);
  AnalyzerOutput(
      std::shared_ptr<IdStringPool> id_string_pool,
      std::shared_ptr<zetasql_base::UnsafeArena> arena,
      std::unique_ptr<const ResolvedExpr> expr,
      const AnalyzerOutputProperties& analyzer_output_properties,
      std::unique_ptr<ParserOutput> parser_output,
      const std::vector<absl::Status>& deprecation_warnings,
      const QueryParametersMap& undeclared_parameters,
      const std::vector<const Type*>& undeclared_positional_parameters);
  AnalyzerOutput(const AnalyzerOutput&) = delete;
  AnalyzerOutput& operator=(const AnalyzerOutput&) = delete;
  ~AnalyzerOutput();

  // Present for output from AnalyzeStatement.
  // IdStrings in this resolved AST are allocated in the IdStringPool attached
  // to this AnalyzerOutput, and copies of those IdStrings will be valid only
  // if the IdStringPool is still alive.
  const ResolvedStatement* resolved_statement() const {
    return statement_.get();
  }

  // Present for output from AnalyzeExpression.
  // IdStrings in this resolved AST are allocated in the IdStringPool attached
  // to this AnalyzerOutput, and copies of those IdStrings will be valid only
  // if the IdStringPool is still alive.
  const ResolvedExpr* resolved_expr() const { return expr_.get(); }

  // These are warnings for use of deprecated features.
  // The statuses will have code INVALID_ARGUMENT and will include a location,
  // when possible. They will also have DeprecationWarning protos attached to
  // them.
  // If there are multiple warnings with the same error message and
  // DeprecationWarning::Kind, they will be deduplicated..
  const std::vector<absl::Status>& deprecation_warnings() const {
    return deprecation_warnings_;
  }
  void set_deprecation_warnings(const std::vector<absl::Status>& warnings) {
    deprecation_warnings_ = warnings;
  }

  // Returns the undeclared query parameters found in the query and their
  // inferred types. If none are present, returns an empty set.
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

  // Returns the IdStringPool that stores IdStrings allocated for the
  // resolved AST.  This was propagated from AnalyzerOptions.
  std::shared_ptr<IdStringPool> id_string_pool() const {
    return id_string_pool_;
  }

  // Returns the arena() that was propagated from AnalyzerOptions. This contains
  // some or all of the resolved AST and parse tree.
  std::shared_ptr<zetasql_base::UnsafeArena> arena() const { return arena_; }

  const AnalyzerOutputProperties& analyzer_output_properties() const {
    return analyzer_output_properties_;
  }

 private:
  // This IdStringPool and arena must be kept alive for the Resolved trees below
  // to be valid.
  std::shared_ptr<IdStringPool> id_string_pool_;
  std::shared_ptr<zetasql_base::UnsafeArena> arena_;

  std::unique_ptr<const ResolvedStatement> statement_;
  std::unique_ptr<const ResolvedExpr> expr_;

  AnalyzerOutputProperties analyzer_output_properties_;

  // AnalyzerOutput can (but is not guaranteed to) take ownership of the parser
  // output so deleting the parser AST can be deferred.  Deleting the parser
  // AST is expensive.  This allows engines to defer AnalyzerOutput cleanup
  // until after critical-path work is done.  May be NULL.
  std::unique_ptr<ParserOutput> parser_output_;

  std::vector<absl::Status> deprecation_warnings_;

  QueryParametersMap undeclared_parameters_;
  std::vector<const Type*> undeclared_positional_parameters_;
};

// Analyze a ZetaSQL statement.
//
// This can return errors that point at a location in the input. How this
// location is reported is given by <options.error_message_mode()>.
absl::Status AnalyzeStatement(absl::string_view sql,
                              const AnalyzerOptions& options_in,
                              Catalog* catalog, TypeFactory* type_factory,
                              std::unique_ptr<const AnalyzerOutput>* output);

// Analyze one statement from a string that may contain multiple statements.
// This can be called in a loop with the same <resume_location> to parse
// all statements from a string.
//
// On successful return,
// <*at_end_of_input> is true if parsing reached the end of the string.
// <*output> contains the next statement found.
//
// Statements are separated by semicolons.  A final semicolon is not required
// on the last statement.  If only whitespace and comments follow the
// semicolon, <*at_end_of_input> will be set.
//
// This can return errors that point at a location in the input. How this
// location is reported is given by <options.error_message_mode()>.
//
// After an error, <resume_location> may not be updated and analyzing further
// statements is not supported.
absl::Status AnalyzeNextStatement(
    ParseResumeLocation* resume_location,
    const AnalyzerOptions& options_in,
    Catalog* catalog,
    TypeFactory* type_factory,
    std::unique_ptr<const AnalyzerOutput>* output,
    bool* at_end_of_input);

// Same as AnalyzeStatement(), but analyze from the parsed AST contained in a
// ParserOutput instead of raw SQL string. For projects which are allowed to use
// the parser directly, using this may save double parsing. If the
// AnalyzerOptions does not specify arena() or id_string_pool(), this will reuse
// the arenas from the ParserOutput for analysis.
//
// On success, the <*statement_parser_output> is moved to be owned by <*output>.
// On failure, the ownership of <*statement_parser_output> remains unchanged.
// The statement contained within remains valid.
absl::Status AnalyzeStatementFromParserOutputOwnedOnSuccess(
    std::unique_ptr<ParserOutput>* statement_parser_output,
    const AnalyzerOptions& options, absl::string_view sql, Catalog* catalog,
    TypeFactory* type_factory, std::unique_ptr<const AnalyzerOutput>* output);
// Similar to the previous function, but does *not* change ownership of
// <statement_parser_output>.
absl::Status AnalyzeStatementFromParserOutputUnowned(
    std::unique_ptr<ParserOutput>* statement_parser_output,
    const AnalyzerOptions& options, absl::string_view sql, Catalog* catalog,
    TypeFactory* type_factory, std::unique_ptr<const AnalyzerOutput>* output);

// Analyze a ZetaSQL expression.  The expression may include query
// parameters, subqueries, and any other valid expression syntax.
//
// The Catalog provides functions and named data types as usual.  If it
// includes Tables, those tables will be queryable in subqueries inside the
// expression.
//
// Column names added to <options> with AddExpressionColumn will be usable
// in the expression, and will show up as ResolvedExpressionColumn nodes in
// the resolved output.
//
// Can return errors that point at a location in the input. This location can be
// reported in multiple ways depending on <options.error_message_mode()>.
absl::Status AnalyzeExpression(absl::string_view sql,
                               const AnalyzerOptions& options, Catalog* catalog,
                               TypeFactory* type_factory,
                               std::unique_ptr<const AnalyzerOutput>* output);

// Similar to the above, but coerces the expression to <target_type>.
// The conversion is performed using assignment semantics.
// For details, see Coercer::AssignableTo() in
// .../public/coercer.h.  If the conversion is not possible, an
// error is issued, with a location attached corresonding to the start of the
// expression.
//
// If <target_type> is nullptr, behaves the same as AnalyzeExpression().
// TODO: Deprecate this method and use AnalyzerOptions
// target_column_types_ to trigger expression coercion to the target type.
absl::Status AnalyzeExpressionForAssignmentToType(
    absl::string_view sql, const AnalyzerOptions& options, Catalog* catalog,
    TypeFactory* type_factory, const Type* target_type,
    std::unique_ptr<const AnalyzerOutput>* output);

// TODO: Take a ParserOutput instead of ASTExpression; also make the
// constructor of ParserOutput take an unowned ASTExpression.
// Resolves a standalone AST expression.
absl::Status AnalyzeExpressionFromParserAST(
    const ASTExpression& ast_expression, const AnalyzerOptions& options_in,
    absl::string_view sql, TypeFactory* type_factory, Catalog* catalog,
    std::unique_ptr<const AnalyzerOutput>* output);

// Similar to the above, but coerces the expression to <target_type>.
// The conversion is performed using assignment semantics.
// For details, see Coercer::AssignableTo() in
// .../public/coercer.h.  If the conversion is not possible, an
// error is issued, with a location attached corresonding to the start of the
// expression.
//
// If <target_type> is nullptr, behaves the same as
// AnalyzeExpressionFromParserAST.
// TODO: Take a ParserOutput instead of ASTExpression (similar to
// AnalyzeExpressionFromParserAST()).
// TODO: Deprecate this method and use AnalyzerOptions
// target_column_types_ to trigger expression coercion to the target type
// (similar to AnalyzeExpressionForAssignmentToType()).
absl::Status AnalyzeExpressionFromParserASTForAssignmentToType(
    const ASTExpression& ast_expression, const AnalyzerOptions& options_in,
    absl::string_view sql, TypeFactory* type_factory, Catalog* catalog,
    const Type* target_type, std::unique_ptr<const AnalyzerOutput>* output);

// Parse and analyze a ZetaSQL type name.
// The type may reference type names from <catalog>.
// Returns a type in <output_type> on success.
//
// This can return errors that point at a location in the input. How this
// location is reported is given by <options.error_message_mode()>.
absl::Status AnalyzeType(const std::string& type_name,
                         const AnalyzerOptions& options_in, Catalog* catalog,
                         TypeFactory* type_factory, const Type** output_type);

// A set of table names found is returned in <*table_names>, where each
// table name is an identifier path stored as a vector<string>.
// The identifiers will be in their as-written case.
// There can be duplicates that differ only in case.
typedef std::set<std::vector<std::string>> TableNamesSet;

// Perform lightweight analysis of a SQL statement and extract the set
// of referenced table names.
//
// This analysis is done without any Catalog, so it has no knowledge of
// which tables or functions actually exist, and knows nothing about data types.
// This just extracts the identifier paths that look syntactically like they
// should be table names.
//
// This will fail on parse errors and on some analysis errors.
// If this passes, it does not mean the query is valid.
// This can return errors that point at a location in the input. How this
// location is reported is given by <options.error_message_mode()>.
//
// Parameter table_names must not be null.
absl::Status ExtractTableNamesFromStatement(absl::string_view sql,
                                            const AnalyzerOptions& options_in,
                                            TableNamesSet* table_names);

// Same as ExtractTableNamesFromStatement(), but extracts table names from one
// SQL statement from a string. The string may contain multiple statements, so
// this can be called in a loop with the same <resume_location> to parse all
// statements from a string.
//
// On successful return,
// <*at_end_of_input> is true if parsing reached the end of the string.
// <*table_names> contains table names referenced in the next statement.
//
// Statements are separated by semicolons. A final semicolon is not required
// on the last statement. If only whitespace and comments follow the
// semicolon, <*at_end_of_input> will be set.
//
// After an error, <resume_location> may not be updated and analyzing further
// statements is not supported.
absl::Status ExtractTableNamesFromNextStatement(
    ParseResumeLocation* resume_location, const AnalyzerOptions& options_in,
    TableNamesSet* table_names, bool* at_end_of_input);

// Same as ExtractTableNamesFromStatement(), but extracts table names from the
// parsed AST instead of a raw SQL string. For projects which are allowed to use
// the parser directly, using this may save double parsing.
//
// On successful return,
// <*table_names> contains table names referenced in the AST statement.
absl::Status ExtractTableNamesFromASTStatement(
    const ASTStatement& ast_statement, const AnalyzerOptions& options_in,
    absl::string_view sql, TableNamesSet* table_names);

// Extract the set of referenced table names from a script.
//
// This analysis is done without a Catalog, so it has no knowledge of
// which tables or functions actually exist, and knows nothing about data types.
// This just extracts the identifier paths that look syntactically like they
// should be table names.
//
// This will fail on parse errors.
//
// If this passes, it does not mean the query is valid.
// This can return errors that point at a location in the input. How this
// location is reported is given by <options.error_message_mode()>.
//
// Parameter table_names must not be null.
absl::Status ExtractTableNamesFromScript(absl::string_view sql,
                                         const AnalyzerOptions& options_in,
                                         TableNamesSet* table_names);

// Same as ExtractTableNamesFromScript(), but extracts table names from
// the parsed AST script. For projects which are allowed to use the parser
// directly, using this may save double parsing.
//
// On successful return,
// <*table_names> contains table names referenced in the AST statement.
absl::Status ExtractTableNamesFromASTScript(const ASTScript& ast_script,
                                            const AnalyzerOptions& options_in,
                                            absl::string_view sql,
                                            TableNamesSet* table_names);

// Resolved "FOR SYSTEM_TIME AS OF" expression.
struct TableResolutionTimeExpr {
  const ASTExpression* ast_expr;
  // Only id_string_pool, arena, and resolved_expr are populated.
  std::unique_ptr<const AnalyzerOutput> analyzer_output_with_expr;
};

// Resolution time info of a table in a query.
struct TableResolutionTimeInfo {
  // A list of resolved "FOR SYSTEM_TIME AS OF" expressions for the table.
  std::vector<TableResolutionTimeExpr> exprs;
  // True means the table is also referenced without "FOR SYSTEM_TIME AS OF".
  bool has_default_resolution_time = false;
};

// A mapping of identifier paths to resolved "FOR SYSTEM_TIME AS OF"
// expressions. The paths are in their original case, and duplicates
// that differ only in case may show up.
typedef std::map<std::vector<std::string>, TableResolutionTimeInfo>
    TableResolutionTimeInfoMap;

// Similarly to ExtractTableNamesFromStatement, perform lightweight analysis
// of a SQL statement and extract the set of referenced table names.
// In addition, for every referenced table, extracts all resolved temporal
// expressions as specified by "FOR SYSTEM_TIME AS OF" clauses. For table
// references w/o an explicit "FOR SYSTEM_TIME AS OF" temporal expression,
// a placeholder value (nullptr) is returned.
//
// Note that the set of temporal reference expressions for a table may contain
// multiple temporal expressions, either equivalent (semantically or possibly
// even syntactically) or not.
//
// Examples:
//
// [Input]  SELECT 1 FROM KeyValue
// [Output] KeyValue => {
//   exprs = []
//   has_default_resolution_time = true
// }
//
// [Input]  SELECT 1 FROM KeyValue FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP
// [Output] KeyValue => {
//   exprs = [FunctionCall(ZetaSQL:current_timestamp() -> TIMESTAMP)]
//   has_default_resolution_time = false
// }
//
// [Input]  SELECT 0 FROM KeyValue FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP
//          GROUP BY (SELECT 1 FROM KeyValue FOR SYSTEM_TIME AS OF
//                        TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY))
// [Output] KeyValue => {
//   exprs = [
//     FunctionCall(ZetaSQL:current_timestamp() -> TIMESTAMP),
//
//     FunctionCall(ZetaSQL:timestamp_sub(
//         TIMESTAMP,
//         INT64,
//         ENUM<zetasql.functions.DateTimestampPart>) -> TIMESTAMP)
//     +-FunctionCall(ZetaSQL:current_timestamp() -> TIMESTAMP)
//     +-Literal(type=INT64, value=1)
//     +-Literal(type=ENUM<zetasql.functions.DateTimestampPart>, value=DAY)
//   ]
//
//   has_default_resolution_time = false
// }
//
// [Input]  SELECT *
//          FROM KeyValue kv1,
//               KeyValue kv2 FOR SYSTEM_TIME AS OF
//                   TIMESTAMP_ADD(CURRENT_TIMESTAMP, INTERVAL 0 DAY)
// [Output] KeyValue => {
//   exprs = [
//     FunctionCall(ZetaSQL:timestamp_add(
//         TIMESTAMP,
//         INT64,
//         ENUM<zetasql.functions.DateTimestampPart>) -> TIMESTAMP)
//     +-FunctionCall(ZetaSQL:current_timestamp() -> TIMESTAMP)
//     +-Literal(type=INT64, value=1)
//     +-Literal(type=ENUM<zetasql.functions.DateTimestampPart>, value=DAY)
//   ]
//
//   has_default_resolution_time = true
// }
//
// <table_resolution_time_info_map> must not be null.
//
// If <catalog> is non-null then <type_factory> must also be non-null, and
// the temporal reference expression will be analyzed with the analyzer
// output stored in <table_resolution_time_info_map>; <catalog> and
// <type_factory> must outlive <*table_resolution_time_info_map>.
//
// <parser_output> must not be null. TableResolutionTimeExpr.ast_expr in
// <*table_resolution_time_info_map> will point to the elements in
// <*parser_output>.
//
// This doesn't impose any restriction on the contents of the temporal
// expressions other than that they resolve to timestamp type.
absl::Status ExtractTableResolutionTimeFromStatement(
    absl::string_view sql, const AnalyzerOptions& options_in,
    TypeFactory* type_factory, Catalog* catalog,
    TableResolutionTimeInfoMap* table_resolution_time_info_map,
    std::unique_ptr<ParserOutput>* parser_output);

// Same as ExtractTableResolutionTimeFromStatement(), but extracts table
// resolution time from the parsed AST instead of a raw SQL string.
// For projects which are allowed to use the parser directly, using this
// may save double parsing.
//
// On successful return,
// <*table_resolution_time_info_map> contains resolution time for tables
// appearing in the AST statement. TableResolutionTimeExpr.ast_expr in
// <*table_resolution_time_info_map> will point to the elements in
// <ast_statement>.
absl::Status ExtractTableResolutionTimeFromASTStatement(
    const ASTStatement& ast_statement, const AnalyzerOptions& options_in,
    absl::string_view sql, TypeFactory* type_factory, Catalog* catalog,
    TableResolutionTimeInfoMap* table_resolution_time_info_map);

// Maps a ResolvedLiteral to a new parameter name (excluding @).
typedef std::map<const ResolvedLiteral*, std::string> LiteralReplacementMap;
typedef std::map<std::string, zetasql::Value> GeneratedParameterMap;

// Replaces literals in <sql> by new query parameters and returns the new query
// in <result_sql>. <analyzer_output> must have been produced using the option
// 'record_parse_locations', otherwise no replacement is done. The mapping from
// literals to parameter names is returned in <literal_parameter_map>. Generated
// parameter names are returned in <generated_parameters>. They are guaranteed
// to be unique and not collide with those already present in <analyzer_output>.
//
// This can return errors that point at a location in the input. How this
// location is reported is given by <options.error_message_mode()>.
absl::Status ReplaceLiteralsByParameters(
    const std::string& sql,
    const absl::node_hash_set<std::string>& option_names_to_ignore,
    const AnalyzerOptions& analyzer_options,
    const AnalyzerOutput* analyzer_output, LiteralReplacementMap* literal_map,
    GeneratedParameterMap* generated_parameters, std::string* result_sql);

// Same as above only all options will be ignored.
absl::Status ReplaceLiteralsByParameters(
    const std::string& sql, const AnalyzerOptions& analyzer_options,
    const AnalyzerOutput* analyzer_output, LiteralReplacementMap* literal_map,
    GeneratedParameterMap* generated_parameters, std::string* result_sql);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_ANALYZER_H_
