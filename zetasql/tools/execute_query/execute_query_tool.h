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

#ifndef ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_TOOL_H_
#define ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_TOOL_H_

#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "google/protobuf/descriptor_database.h"
#include "zetasql/common/options_utils.h"
#include "zetasql/parser/macros/macro_expander.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/multi_catalog.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"

namespace zetasql {

// Configuration data on how `ExecuteQuery` should behave.
class ExecuteQueryConfig {
 public:
  ExecuteQueryConfig();

  ExecuteQueryConfig(const ExecuteQueryConfig&) = delete;
  ExecuteQueryConfig& operator=(const ExecuteQueryConfig&) = delete;

  enum class ToolMode {
    // Parse the query, and print a debug string of the parsed AST.
    kParse,

    // Parse the query, print an 'unparse' of the input using the parsers
    // Unparse API. Should be semantically equivalent.
    kUnparse,

    // Resolve the query, and print the ResolveAST
    kResolve,

    // Analyze the query, then run 'sqlbuilder' to produce a semantically
    // equivalent sql query (...mostly).
    kUnAnalyze,

    // Prepare the query, and print a representation of the query from the
    // reference implementation.
    kExplain,

    // Execute the query and pretty print the result.
    kExecute
  };

  enum class SqlMode {
    // Treat sql as a query, the output is a table.
    kQuery,

    // Treat sql as an expression, the output is a single value.
    kExpression
  };

  void clear_tool_modes() { tool_modes_.clear(); }
  void add_tool_mode(ToolMode tool_mode) { tool_modes_.insert(tool_mode); }
  bool has_tool_mode(ToolMode tool_mode) const {
    return tool_modes_.contains(tool_mode);
  }
  const absl::flat_hash_set<ToolMode>& tool_modes() const {
    return tool_modes_;
  }
  void set_tool_modes(const absl::flat_hash_set<ToolMode>& tool_modes) {
    tool_modes_ = tool_modes;
  }

  // Returns the tool mode if the mode string matches one of the tool modes.
  static std::optional<ToolMode> parse_tool_mode(absl::string_view mode) {
    static const auto* tool_mode_map =
        new absl::flat_hash_map<absl::string_view, ToolMode>({
            {"parse", ToolMode::kParse},
            {"parser", ToolMode::kParse},
            {"unparse", ToolMode::kUnparse},
            {"unparser", ToolMode::kUnparse},
            {"resolve", ToolMode::kResolve},
            {"resolver", ToolMode::kResolve},
            {"analyze", ToolMode::kResolve},
            {"analyzer", ToolMode::kResolve},
            {"sql_builder", ToolMode::kUnAnalyze},
            {"sqlbuilder", ToolMode::kUnAnalyze},
            {"unanalyze", ToolMode::kUnAnalyze},
            {"unanalyzer", ToolMode::kUnAnalyze},
            {"unresolve", ToolMode::kUnAnalyze},
            {"unresolver", ToolMode::kUnAnalyze},
            {"explain", ToolMode::kExplain},
            {"execute", ToolMode::kExecute},
        });

    std::string mode_lower{absl::AsciiStrToLower(mode)};
    if (tool_mode_map->contains(mode_lower)) {
      return tool_mode_map->at(mode_lower);
    }
    return std::nullopt;
  }

  // Returns the name of the tool mode.
  static absl::string_view tool_mode_name(ToolMode tool_mode) {
    static const auto* tool_mode_names =
        new absl::flat_hash_map<ToolMode, absl::string_view>({
            {ToolMode::kParse, "parse"},
            {ToolMode::kUnparse, "unparse"},
            {ToolMode::kResolve, "analyze"},
            {ToolMode::kUnAnalyze, "unanalyze"},
            {ToolMode::kExplain, "explain"},
            {ToolMode::kExecute, "execute"},
        });

    ABSL_CHECK(tool_mode_names->contains(tool_mode))
        << "Unknown tool mode: " << static_cast<int>(tool_mode);
    return tool_mode_names->at(tool_mode);
  }

  void set_sql_mode(SqlMode sql_mode) { sql_mode_ = sql_mode; }
  SqlMode sql_mode() const { return sql_mode_; }

  // Defaults matches AnalyzerOptions default.
  const AnalyzerOptions& analyzer_options() const { return analyzer_options_; }
  AnalyzerOptions& mutable_analyzer_options() { return analyzer_options_; }

  const EvaluatorOptions& evaluator_options() const {
    return evaluator_options_;
  }
  EvaluatorOptions& mutable_evaluator_options() { return evaluator_options_; }

  const ParameterValueMap& query_parameter_values() {
    return query_parameter_values_;
  }
  ParameterValueMap& mutable_query_parameter_values() {
    return query_parameter_values_;
  }

  // This is the Catalog to use for lookups.  It's a MultiCatalog containing
  // the wrapper_catalog, base_catalog and builtins_catalog.
  Catalog* catalog() { return catalog_.get(); }

  // Set the base catalog, which is used to find tables, custom functions, etc.
  // It doesn't need to include builtin functions since those are provided by
  // the builtins_catalog.
  // nullptr is allowed if there is no base catalog.
  void SetBaseCatalog(Catalog* catalog);

  Catalog* base_catalog() { return base_catalog_; }
  SimpleCatalog* builtins_catalog() { return &builtins_catalog_; }
  SimpleCatalog* wrapper_catalog() { return &wrapper_catalog_; }

  // A TypeFactory that can be used for creating tables for this request.
  TypeFactory* type_factory() { return &type_factory_; }

  using ExamineResolvedASTCallback =
      std::function<absl::Status(const ResolvedNode* node)>;

  // If provided, this callback will be invoked before evaluating the query
  // (or whatever action is specified by tool mode).  If an error is returned
  // it will be propagated back as an error in `ExecuteQuery`.
  const ExamineResolvedASTCallback& examine_resolved_ast_callback() const {
    return examine_resolved_ast_callback_;
  }

  void set_examine_resolved_ast_callback(ExamineResolvedASTCallback callback) {
    examine_resolved_ast_callback_ = std::move(callback);
  }

  absl::Status SetCatalogFromString(const std::string& value);

  // Set the google::protobuf::DescriptorPool to use when resolving types.
  // The DescriptorPool can only be set once and cannot be changed.
  void SetDescriptorPool(const google::protobuf::DescriptorPool* pool);
  void SetOwnedDescriptorPool(
      std::unique_ptr<const google::protobuf::DescriptorPool> pool);
  void SetOwnedDescriptorDatabase(
      std::unique_ptr<google::protobuf::DescriptorDatabase> db);

  const google::protobuf::DescriptorPool* descriptor_pool() const {
    return descriptor_pool_;
  }

  const parser::macros::MacroCatalog& macro_catalog() const {
    return macro_catalog_;
  }
  parser::macros::MacroCatalog& mutable_macro_catalog() {
    return macro_catalog_;
  }
  const std::list<std::string>& macro_sources() const { return macro_sources_; }
  std::list<std::string>& mutable_macro_sources() { return macro_sources_; }

  void AddArtifacts(std::unique_ptr<const ParserOutput> parser_output,
                    std::unique_ptr<const AnalyzerOutput> analyzer_output) {
    parser_artifacts_.push_back(std::move(parser_output));
    analyzer_artifacts_.push_back(std::move(analyzer_output));
  }

 private:
  ExamineResolvedASTCallback examine_resolved_ast_callback_ = nullptr;
  // if no tool modes are added then Execute is the default mode.
  absl::flat_hash_set<ToolMode> tool_modes_ = {ToolMode::kExecute};
  SqlMode sql_mode_ = SqlMode::kQuery;
  AnalyzerOptions analyzer_options_;

  // The effective Catalog is a MultiCatalog with
  //   wrapper_catalog  - any tables or types added based on flags
  //   base_catalog     - the Catalog of tables, etc from SelectableCatalogs.
  //   builtins_catalog - the Catalog providing built-in functions, set up
  //                      based on LanguageOptions inferred from config.
  SimpleCatalog builtins_catalog_;
  Catalog* base_catalog_ = nullptr;  // Not owned, may be nullptr.
  SimpleCatalog wrapper_catalog_;
  std::unique_ptr<MultiCatalog> catalog_;

  TypeFactory type_factory_;

  EvaluatorOptions evaluator_options_;
  ParameterValueMap query_parameter_values_;
  const google::protobuf::DescriptorPool* descriptor_pool_ = nullptr;
  std::unique_ptr<const google::protobuf::DescriptorPool> owned_descriptor_pool_;
  std::unique_ptr<google::protobuf::DescriptorDatabase> descriptor_db_;
  parser::macros::MacroCatalog macro_catalog_;
  // std::list, not a vector, because we need stability. The entries in
  // `macro_catalog_` have string_views into these sources.
  std::list<std::string> macro_sources_;
  // These are used to keep parsing and analysis artifacts alive.
  std::vector<std::unique_ptr<const ParserOutput>> parser_artifacts_;
  std::vector<std::unique_ptr<const AnalyzerOutput>> analyzer_artifacts_;
};

absl::Status SetToolModeFromFlags(ExecuteQueryConfig& config);

absl::Status SetSqlModeFromFlags(ExecuteQueryConfig& config);

absl::Status SetDescriptorPoolFromFlags(ExecuteQueryConfig& config);

absl::StatusOr<std::unique_ptr<SimpleTable>> MakeTableFromCsvFile(
    absl::string_view table_name, absl::string_view path);

absl::StatusOr<std::unique_ptr<SimpleTable>> MakeTableFromBinaryProtoFile(
    absl::string_view table_name, absl::string_view path,
    const ProtoType* column_proto_type);

absl::StatusOr<std::unique_ptr<SimpleTable>> MakeTableFromTextProtoFile(
    absl::string_view table_name, absl::string_view path,
    const ProtoType* column_proto_type);

absl::Status AddTablesFromFlags(ExecuteQueryConfig& config);

absl::StatusOr<std::unique_ptr<ExecuteQueryWriter>> MakeWriterFromFlags(
    const ExecuteQueryConfig& config, std::ostream& output);

// Note: Currently this only sets product_mode
// TODO: expand this to support setting other language features
//                  via flag.
absl::Status SetLanguageOptionsFromFlags(ExecuteQueryConfig& config);

absl::Status SetAnalyzerOptionsFromFlags(ExecuteQueryConfig& config);

absl::Status SetEvaluatorOptionsFromFlags(ExecuteQueryConfig& config);

// Set query parameters in analyzer options as well as for use in the evaluator.
absl::Status SetQueryParametersFromFlags(ExecuteQueryConfig& config);

// Initialize an ExecuteQueryConfig with default values and values from flags.
absl::Status InitializeExecuteQueryConfig(ExecuteQueryConfig& config);

// Execute the query according to `config`. `config` is logically const, but due
// to ZetaSQL calling conventions related to Catalog objects, must be
// non-const.
absl::Status ExecuteQuery(absl::string_view sql, ExecuteQueryConfig& config,
                          ExecuteQueryWriter& writer);

}  // namespace zetasql

// Exposed for tests only
ABSL_DECLARE_FLAG(std::vector<std::string>, mode);
ABSL_DECLARE_FLAG(zetasql::internal::EnabledAstRewrites,
                  enabled_ast_rewrites);
ABSL_DECLARE_FLAG(std::string, product_mode);
ABSL_DECLARE_FLAG(std::string, catalog);
ABSL_DECLARE_FLAG(bool, strict_name_resolution_mode);
ABSL_DECLARE_FLAG(bool, fold_literal_cast);
ABSL_DECLARE_FLAG(std::string, sql_mode);
ABSL_DECLARE_FLAG(std::string, table_spec);
ABSL_DECLARE_FLAG(std::string, descriptor_pool);
ABSL_DECLARE_FLAG(std::string, output_mode);
ABSL_DECLARE_FLAG(std::string, parameters);
ABSL_DECLARE_FLAG(int64_t, evaluator_max_value_byte_size);
ABSL_DECLARE_FLAG(int64_t, evaluator_max_intermediate_byte_size);

#endif  // ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_TOOL_H_
