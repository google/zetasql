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
#include <memory>
#include <string>
#include <utility>

#include "google/protobuf/descriptor_database.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
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

    // Resolve the query, and print the ResolveAST
    kResolve,

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

  void set_tool_mode(ToolMode tool_mode) { tool_mode_ = tool_mode; }
  ToolMode tool_mode() const { return tool_mode_; }

  void set_sql_mode(SqlMode sql_mode) { sql_mode_ = sql_mode; }
  SqlMode sql_mode() const { return sql_mode_; }

  // Defaults matches AnalyzerOptions default.
  const AnalyzerOptions& analyzer_options() const { return analyzer_options_; }
  AnalyzerOptions& mutable_analyzer_options() { return analyzer_options_; }

  const EvaluatorOptions& evaluator_options() const {
    return evaluator_options_;
  }
  EvaluatorOptions& mutable_evaluator_options() { return evaluator_options_; }

  // Defaults matches SimpleCatalog("").
  SimpleCatalog& mutable_catalog() { return catalog_; }
  const SimpleCatalog& catalog() const { return catalog_; }

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

 private:
  ExamineResolvedASTCallback examine_resolved_ast_callback_ = nullptr;
  ToolMode tool_mode_ = ToolMode::kExecute;
  SqlMode sql_mode_ = SqlMode::kQuery;
  AnalyzerOptions analyzer_options_;
  SimpleCatalog catalog_;
  EvaluatorOptions evaluator_options_;
  const google::protobuf::DescriptorPool* descriptor_pool_ = nullptr;
  std::unique_ptr<const google::protobuf::DescriptorPool> owned_descriptor_pool_;
  std::unique_ptr<google::protobuf::DescriptorDatabase> descriptor_db_;
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

absl::Status SetEvaluatorOptionsFromFlags(ExecuteQueryConfig& config);

// Execute the query according to `config`. `config` is logically const, but due
// to ZetaSQL calling conventions related to Catalog objects, must be
// non-const.
absl::Status ExecuteQuery(absl::string_view sql, ExecuteQueryConfig& config,
                          ExecuteQueryWriter& writer);

}  // namespace zetasql

// Exposed for tests only
ABSL_DECLARE_FLAG(std::string, mode);
ABSL_DECLARE_FLAG(std::string, sql_mode);
ABSL_DECLARE_FLAG(std::string, table_spec);
ABSL_DECLARE_FLAG(std::string, descriptor_pool);
ABSL_DECLARE_FLAG(std::string, output_mode);
ABSL_DECLARE_FLAG(int64_t, evaluator_max_value_byte_size);
ABSL_DECLARE_FLAG(int64_t, evaluator_max_intermediate_byte_size);

#endif  // ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_TOOL_H_
