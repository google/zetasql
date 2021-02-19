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

#include "zetasql/tools/execute_query/execute_query_tool.h"

#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

ABSL_FLAG(std::string, mode, "execute",
          "The tool mode to use. Valid values are:"
          "\n     'parse'   parse the parser AST"
          "\n     'resolve' print the resolved AST"
          "\n     'explain' print the query plan"
          "\n     'execute' actually run the query and print the result. (not"
          "                 all functionality is supported).");

ABSL_FLAG(std::string, table_spec, "",
          "The table spec to use for building the ZetaSQL Catalog. This is a "
          "comma-delimited list of strings of the form <table_name>=<spec>, "
          "where <spec> is of the form:"
          "\n    csv:<path> - csv file that is represented by a table whose "
          "string-typed column names are determined from the header row).");

ABSL_FLAG(
    std::string, descriptor_pool,
    "generated",
    "The descriptor pool to use while resolving the query. This can be:"
    "\n    'generated' - the generated pool of protos compiled into "
    "this binary"
    "\n    'none'      - no protos are included (but syntax is still "
    "supported");
// TODO: Support specifying proto files to parse.

namespace zetasql {

namespace {
using ToolMode = ExecuteQueryConfig::ToolMode;
}  // namespace

absl::Status SetToolModeFromFlags(ExecuteQueryConfig& config) {
  const std::string mode = absl::GetFlag(FLAGS_mode);
  if (mode == "parse") {
    config.set_tool_mode(ToolMode::kParse);
    return absl::OkStatus();
  } else if (mode == "resolve") {
    config.set_tool_mode(ToolMode::kResolve);
    return absl::OkStatus();
  } else if (mode == "explain") {
    config.set_tool_mode(ToolMode::kExplain);
    return absl::OkStatus();
  } else if (mode == "execute") {
    config.set_tool_mode(ToolMode::kExecute);
    return absl::OkStatus();
  } else if (mode.empty()) {
    return zetasql_base::InvalidArgumentErrorBuilder() << "Must specify --mode";
  } else {
    return zetasql_base::InvalidArgumentErrorBuilder() << "Invalid --mode: " << mode;
  }
}

absl::Status SetDescriptorPoolFromFlags(ExecuteQueryConfig& config) {
  const std::string pool = absl::GetFlag(FLAGS_descriptor_pool);

  if (pool == "none") {
    // Do nothing
    return absl::OkStatus();
  } else if (pool == "generated") {
    config.mutable_catalog().SetDescriptorPool(
        google::protobuf::DescriptorPool::generated_pool());
    return absl::OkStatus();
  } else {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "--descriptor_pool flag must be one of: none, generated"
    );
  }
}

static zetasql_base::StatusOr<const ProtoType*> GetProtoType(
    ExecuteQueryConfig& config, absl::string_view proto_name) {
  const zetasql::Type* type = nullptr;
  if (!config.mutable_catalog().GetType(std::string(proto_name), &type).ok() ||
      type == nullptr) {
    return zetasql_base::NotFoundErrorBuilder()
           << "Unknown protocol buffer message: '" << proto_name << "'";
  }
  ZETASQL_RET_CHECK(type->IsProto());
  return type->AsProto();
}

static zetasql_base::StatusOr<std::unique_ptr<const Table>> MakeTableFromTableSpec(
    absl::string_view table_spec, ExecuteQueryConfig& config) {
  std::vector<absl::string_view> table_spec_parts =
      absl::StrSplit(table_spec, '=');
  if (table_spec_parts.size() != 2) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid table specification: " << table_spec;
  }
  absl::string_view table_name = table_spec_parts[0];
  absl::string_view spec = table_spec_parts[1];

  std::vector<std::string> spec_parts = absl::StrSplit(spec, ':');
  if (spec_parts.empty()) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid empty specification for table " << table_name;
  }
  absl::string_view format = spec_parts[0];
  if (format == "csv") {
    if (spec_parts.size() != 2) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Invalid specification for csv table " << table_name << ": "
             << table_spec;
    }
    absl::string_view path = spec_parts[1];
    return MakeTableFromCsvFile(table_name, path);
  } else {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Unknown format " << format << " for table " << table_name;
  }
}

absl::Status AddTablesFromFlags(ExecuteQueryConfig& config) {
  std::vector<std::string> table_specs =
      absl::StrSplit(absl::GetFlag(FLAGS_table_spec), ',', absl::SkipEmpty());

  for (const std::string& table_spec : table_specs) {
    ZETASQL_ASSIGN_OR_RETURN(auto table, MakeTableFromTableSpec(table_spec, config));
    config.mutable_catalog().AddOwnedTable(std::move(table));
  }
  return absl::OkStatus();
}

ExecuteQueryConfig::ExecuteQueryConfig() : catalog_("") {}

absl::Status ExecuteQuery(absl::string_view sql, ExecuteQueryConfig& config,
                          ExecuteQueryWriter& writer) {
  if (config.tool_mode() == ToolMode::kParse) {
    std::unique_ptr<ParserOutput> parser_output;
    ParserOptions parser_options;

    parser_options.set_language_options(&config.analyzer_options().language());
    ZETASQL_RETURN_IF_ERROR(ParseStatement(sql, parser_options, &parser_output));

    const ASTNode* root = parser_output->statement();
    ZETASQL_RET_CHECK_NE(root, nullptr);

    // Note, ASTNode is not public, and therefore cannot be part of the public
    // interface, thus, we can only return the string.
    return writer.parsed(root->DebugString());
  }

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  ZETASQL_RETURN_IF_ERROR(AnalyzeStatement(
      sql, config.analyzer_options(), &config.mutable_catalog(),
      config.mutable_catalog().type_factory(), &analyzer_output));

  const ResolvedStatement* resolved_statement =
      analyzer_output->resolved_statement();

  ZETASQL_RET_CHECK_NE(resolved_statement, nullptr);
  if (config.examine_resolved_ast_callback() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(config.examine_resolved_ast_callback()(resolved_statement));
  }

  if (config.tool_mode() == ToolMode::kResolve) {
    return writer.resolved(*resolved_statement);
  }

  ZETASQL_RET_CHECK_EQ(resolved_statement->node_kind(), RESOLVED_QUERY_STMT);

  PreparedQuery query{resolved_statement->GetAs<ResolvedQueryStmt>(),
                      EvaluatorOptions()};

  ZETASQL_RETURN_IF_ERROR(
      query.Prepare(config.analyzer_options(), &config.mutable_catalog()));

  switch (config.tool_mode()) {
    case ToolMode::kExplain: {
      ZETASQL_ASSIGN_OR_RETURN(const std::string explain, query.ExplainAfterPrepare());

      return writer.explained(*resolved_statement, explain);
    }
    case ToolMode::kExecute: {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());

      return writer.executed(*resolved_statement, std::move(iter));
    }
    default:
      return absl::InternalError(absl::StrCat(
          "unknown tool mode: ", static_cast<int>(config.tool_mode())));
  }
}

// Runs the tool.
absl::Status ExecuteQuery(absl::string_view sql, ExecuteQueryConfig& config,
                          std::ostream& out_stream) {
  ExecuteQueryStreamWriter writer{out_stream};
  return ExecuteQuery(sql, config, writer);
}

}  // namespace zetasql
