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

#ifndef ZETASQL_PUBLIC_ANALYZER_OPTIONS_H_
#define ZETASQL_PUBLIC_ANALYZER_OPTIONS_H_

#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/type.h"
#include "zetasql/base/case.h"
#include "absl/base/attributes.h"
#include "absl/container/btree_set.h"

namespace zetasql {

class ResolvedOption;

// Performs a case-insensitive less-than vector<string> comparison, element
// by element, using the C/POSIX locale for element comparisons. This function
// object is useful as a template parameter for STL set/map of
// std::vector<string>s, if uniqueness of the vector keys is case-insensitive.
struct StringVectorCaseLess {
  bool operator()(const std::vector<std::string>& v1,
                  const std::vector<std::string>& v2) const;
};

// Map associating each query parameter with its type. Keys are lowercase to
// achieve case-insensitive matching.
typedef std::map<std::string, const Type*> QueryParametersMap;

// Key = name path of system variable.  Value = type of variable.
// Name elements in the key do not include the "@@" prefix.
// For example, if @@foo.bar has type INT32, the corresponding map entry is:
//    key = {"foo", "bar"}
//    value = type_factory->get_int32()
typedef std::map<std::vector<std::string>, const Type*, StringVectorCaseLess>
    SystemVariablesMap;

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
      TypeFactory* factory, AllowedHintsAndOptions* result);

  // Serialize this AllowedHIntsAndOptions into protobuf. The provided map
  // is used to store serialized FileDescriptorSets, which can be deserialized
  // into separate DescriptorPools in order to reconstruct the Type. The map
  // may be non-empty and may be used across calls to this method in order to
  // serialize multiple types. The map may NOT be null.
  absl::Status Serialize(FileDescriptorSetMap* file_descriptor_set_map,
                         AllowedHintsAndOptionsProto* proto) const;

  // If true, give an error for an unknown option.
  bool disallow_unknown_options = false;

  // For each qualifier in this set, give errors for unknown hints with that
  // qualifier.  If "" is in the set, give errors for unknown unqualified hints.
  std::set<std::string, zetasql_base::CaseLess>
      disallow_unknown_hints_with_qualifiers;

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
  // Represents a set of ASTRewrites.
  using ASTRewriteSet = absl::btree_set<ResolvedASTRewrite>;

  using LookupExpressionColumnCallback =
      std::function<absl::Status(const std::string&, const Type**)>;

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
      TypeFactory* factory, AnalyzerOptions* result);

  // Serialize the options into protobuf. The provided map is used to store
  // serialized FileDescriptorSets, which can be deserialized into separate
  // DescriptorPools in order to reconstruct the Type. The map may be
  // non-empty and may be used across calls to this method in order to
  // serialize multiple types. The map may NOT be null.
  absl::Status Serialize(FileDescriptorSetMap* map,
                         AnalyzerOptionsProto* proto) const;

  // Options for the language.
  const LanguageOptions& language() const { return language_options_; }
  LanguageOptions* mutable_language() { return &language_options_; }
  void set_language(const LanguageOptions& options) {
    language_options_ = options;
  }

  // Allows updating the set of enabled AST rewrites.
  // By default rewrites in DefaultResolvedASTRewrites() are enabled.
  // These are documented with the ResolvedASTRewrite enum.
  void set_enabled_rewrites(absl::btree_set<ResolvedASTRewrite> rewrites) {
    enabled_rewrites_ = std::move(rewrites);
  }
  const absl::btree_set<ResolvedASTRewrite>& enabled_rewrites() const {
    return enabled_rewrites_;
  }
  // Enables or disables a particular rewrite.
  void enable_rewrite(ResolvedASTRewrite rewrite, bool enable = true);
  // Returns if a given AST rewrite is enabled.
  ABSL_MUST_USE_RESULT bool rewrite_enabled(ResolvedASTRewrite rewrite) const {
    return enabled_rewrites_.contains(rewrite);
  }
  // Returns the set of rewrites that are enabled by default.
  static absl::btree_set<ResolvedASTRewrite> DefaultRewrites();

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
  void clear_query_parameters() { query_parameters_.clear(); }

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
  StatementContext statement_context() const { return statement_context_; }

  void set_parse_location_record_type(
      ParseLocationRecordType parse_location_record_type) {
    parse_location_record_type_ = parse_location_record_type;
  }
  const ParseLocationRecordType& parse_location_record_type() const {
    return parse_location_record_type_;
  }
  // Set this to true to record parse locations in resolved AST nodes,
  ABSL_DEPRECATED("Inline me!")
  void set_record_parse_locations(bool value) {
    parse_location_record_type_ = (value ? PARSE_LOCATION_RECORD_CODE_SEARCH
                                         : PARSE_LOCATION_RECORD_NONE);
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
  void set_parameter_mode(ParameterMode mode) { parameter_mode_ = mode; }
  ParameterMode parameter_mode() const { return parameter_mode_; }

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
  void clear_system_variables() { system_variables_.clear(); }
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
  // NOTE: Ensure that variables captured by this lamda either outlive this
  // AnalyzerOptions and any copy of it OR are captured by value instead.
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

  // Option that controls whether and how parse locations are recorded in
  // ResolvedNodes.
  ParseLocationRecordType parse_location_record_type_ =
      PARSE_LOCATION_RECORD_NONE;

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

  // The set of ASTRewrites that are enabled.
  // Note that we store these as a btree_set to make the order in which the
  // rewrites are applied consistent, and thus prevent instability in the
  // analyzer test column ids.
  absl::btree_set<ResolvedASTRewrite> enabled_rewrites_ = DefaultRewrites();

  // Copyable
};

// Returns <options> if it already has all arenas initialized, or otherwise
// populates <copy> as a copy for <options>, creates arenas in <copy> and
// returns it. This avoids unnecessary duplication of AnalyzerOptions, which
// might be expensive.
const AnalyzerOptions& GetOptionsWithArenas(
    const AnalyzerOptions* options, std::unique_ptr<AnalyzerOptions>* copy);

// Verifies that the provided AnalyzerOptions have a valid combination of
// settings.
absl::Status ValidateAnalyzerOptions(const AnalyzerOptions& options);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_ANALYZER_OPTIONS_H_
