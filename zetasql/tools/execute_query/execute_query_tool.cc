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
#include "google/protobuf/descriptor_database.h"
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
#include "zetasql/tools/execute_query/execute_query_proto_writer.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
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
          "\n    binproto:<proto>:<path> - binary proto file that is "
          "represented by a value table"
          "\n    textproto:<proto>:<path> - text proto file that is "
          "represented by a value table"
          "\n    csv:<path> - csv file that is represented by a table whose "
          "string-typed column names are determined from the header row.");

ABSL_FLAG(
    std::string, descriptor_pool,
    "generated",
    "The descriptor pool to use while resolving the query. This can be:"
    "\n    'generated' - the generated pool of protos compiled into "
    "this binary"
    "\n    'none'      - no protos are included (but syntax is still "
    "supported");
// TODO: Support specifying proto files to parse.

ABSL_FLAG(std::string, output_mode, "box",
          "Format to use for query results. Available choices:"
          "\nbox - Tabular format for human consumption"
          "\njson - JSON serialization"
          "\ntextproto - Protocol buffer text format");

ABSL_FLAG(std::string, sql_mode, "query",
          "How to interpret the input sql. Available choices:"
          "\nquery"
          "\nexpression");

ABSL_FLAG(
    int64_t, evaluator_max_value_byte_size, -1 /* sentinel for unset*/,
    R"(Limit on the maximum number of in-memory bytes used by an individual Value
  that is constructed during evaluation. This bound applies to all Value
  types, including variable-sized types like STRING, BYTES, ARRAY, and
  STRUCT. Exceeding this limit results in an error. See the implementation of
  Value::physical_byte_size for more details.)");

ABSL_FLAG(
    int64_t, evaluator_max_intermediate_byte_size, -1 /* sentinel for unset*/,
    R"(The limit on the maximum number of in-memory bytes that can be used for
  storing accumulated rows (e.g., during an ORDER BY query). Exceeding this
  limit results in an error.)");

namespace zetasql {

namespace {
using ToolMode = ExecuteQueryConfig::ToolMode;
using SqlMode = ExecuteQueryConfig::SqlMode;
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
  } else {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid --mode: '" << mode << "'";
  }
}

absl::Status SetSqlModeFromFlags(ExecuteQueryConfig& config) {
  const std::string sql_mode = absl::GetFlag(FLAGS_sql_mode);
  if (sql_mode == "query") {
    config.set_sql_mode(SqlMode::kQuery);
    return absl::OkStatus();
  } else if (sql_mode == "expression") {
    config.set_sql_mode(SqlMode::kExpression);
    return absl::OkStatus();
  } else {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid --sql_mode: '" << sql_mode << "'";
  }
}

absl::Status SetDescriptorPoolFromFlags(ExecuteQueryConfig& config) {
  const std::string pool = absl::GetFlag(FLAGS_descriptor_pool);

  if (pool == "none") {
    // Do nothing
    return absl::OkStatus();
  } else if (pool == "generated") {
    config.SetDescriptorPool(google::protobuf::DescriptorPool::generated_pool());
    return absl::OkStatus();
  } else {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "--descriptor_pool flag must be one of: none, generated"
    );
  }
}

static absl::StatusOr<const ProtoType*> GetProtoType(
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

static absl::StatusOr<std::unique_ptr<const Table>> MakeTableFromTableSpec(
    absl::string_view table_spec, ExecuteQueryConfig& config) {
  std::vector<absl::string_view> table_spec_parts =
           absl::StrSplit(table_spec, absl::MaxSplits('=', 1));
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
  } else if (format == "binproto") {
    if (spec_parts.size() != 3) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Invalid specification for table " << table_name << ": "
             << table_spec;
    }
    absl::string_view proto_name = spec_parts[1];
    absl::string_view path = spec_parts[2];

    ZETASQL_ASSIGN_OR_RETURN(const ProtoType* record_type,
                     GetProtoType(config, proto_name));
    return MakeTableFromBinaryProtoFile(table_name, path, record_type);
  } else if (format == "textproto") {
    if (spec_parts.size() != 3) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Invalid specification for table " << table_name << ": "
             << table_spec;
    }
    absl::string_view proto_name = spec_parts[1];
    absl::string_view path = spec_parts[2];

    ZETASQL_ASSIGN_OR_RETURN(const ProtoType* record_type,
                     GetProtoType(config, proto_name));
    return MakeTableFromTextProtoFile(table_name, path, record_type);
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

absl::StatusOr<std::unique_ptr<ExecuteQueryWriter>> MakeWriterFromFlags(
    const ExecuteQueryConfig& config, std::ostream& output) {
  const std::string mode = absl::GetFlag(FLAGS_output_mode);

  if (mode.empty()) {
    return zetasql_base::InvalidArgumentErrorBuilder() << "Must specify --output_mode";
  }

  if (mode == "box") {
    return std::make_unique<ExecuteQueryStreamWriter>(output);
  }

  std::function<absl::Status(const google::protobuf::Message& msg, std::ostream&)>
      proto_writer_func;

  if (mode == "json") {
    proto_writer_func = &ExecuteQueryWriteJson;
  } else if (mode == "textproto") {
    proto_writer_func = &ExecuteQueryWriteTextproto;
  } else {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Unknown output mode " << mode;
  }

  ZETASQL_RET_CHECK(proto_writer_func != nullptr);

  const google::protobuf::DescriptorPool* pool = config.descriptor_pool();

  ZETASQL_RET_CHECK_NE(pool, nullptr);

  return std::make_unique<ExecuteQueryStreamProtobufWriter>(
      pool, [proto_writer_func, &output](const google::protobuf::Message& msg) {
        return proto_writer_func(msg, output);
      });
}

absl::Status SetEvaluatorOptionsFromFlags(ExecuteQueryConfig& config) {
  if (int64_t val = absl::GetFlag(FLAGS_evaluator_max_value_byte_size);
      val != -1) {
    config.mutable_evaluator_options().max_value_byte_size = val;
  }
  if (int64_t val = absl::GetFlag(FLAGS_evaluator_max_intermediate_byte_size);
      val != -1) {
    config.mutable_evaluator_options().max_intermediate_byte_size = val;
  }
  return absl::OkStatus();
}

ExecuteQueryConfig::ExecuteQueryConfig() : catalog_("") {}

void ExecuteQueryConfig::SetDescriptorPool(const google::protobuf::DescriptorPool* pool) {
  ZETASQL_CHECK(descriptor_pool_ == nullptr) << __func__ << " can only be called once";
  owned_descriptor_pool_.reset();
  descriptor_pool_ = pool;
  catalog_.SetDescriptorPool(pool);
}

void ExecuteQueryConfig::SetOwnedDescriptorPool(
    std::unique_ptr<const google::protobuf::DescriptorPool> pool) {
  ZETASQL_CHECK(descriptor_pool_ == nullptr) << __func__ << " can only be called once";
  owned_descriptor_pool_ = std::move(pool);
  descriptor_pool_ = owned_descriptor_pool_.get();
  catalog_.SetDescriptorPool(descriptor_pool_);
}

void ExecuteQueryConfig::SetOwnedDescriptorDatabase(
    std::unique_ptr<google::protobuf::DescriptorDatabase> db) {
  ZETASQL_CHECK(descriptor_db_ == nullptr) << __func__ << " can only be called once";

  // The descriptor database given to the pool needs to be owned locally.
  descriptor_db_ = std::move(db);

  SetOwnedDescriptorPool(
      std::make_unique<const google::protobuf::DescriptorPool>(descriptor_db_.get()));
}

absl::Status ExecuteQuery(absl::string_view sql, ExecuteQueryConfig& config,
                          ExecuteQueryWriter& writer) {
  if (config.tool_mode() == ToolMode::kParse) {
    std::unique_ptr<ParserOutput> parser_output;
    ParserOptions parser_options;

    const ASTNode* root = nullptr;
    if (config.sql_mode() == SqlMode::kQuery) {
      parser_options.set_language_options(
          &config.analyzer_options().language());
      ZETASQL_RETURN_IF_ERROR(ParseStatement(sql, parser_options, &parser_output));

      root = parser_output->statement();
    } else if (config.sql_mode() == SqlMode::kExpression) {
      parser_options.set_language_options(
          &config.analyzer_options().language());
      ZETASQL_RETURN_IF_ERROR(ParseExpression(sql, parser_options, &parser_output));
      root = parser_output->expression();
    } else {
      return absl::InternalError(absl::StrCat(
          "unknown sql_mode", static_cast<int>(config.sql_mode())));
    }
    ZETASQL_RET_CHECK_NE(root, nullptr);

    // Note, ASTNode is not public, and therefore cannot be part of the public
    // interface, thus, we can only return the string.
    return writer.parsed(root->DebugString());
  }

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  const ResolvedNode* resolved_node = nullptr;
  if (config.sql_mode() == SqlMode::kQuery) {
    ZETASQL_RETURN_IF_ERROR(AnalyzeStatement(
        sql, config.analyzer_options(), &config.mutable_catalog(),
        config.mutable_catalog().type_factory(), &analyzer_output));
    resolved_node = analyzer_output->resolved_statement();
  } else if (config.sql_mode() == SqlMode::kExpression) {
    ZETASQL_RETURN_IF_ERROR(AnalyzeExpression(
        sql, config.analyzer_options(), &config.mutable_catalog(),
        config.mutable_catalog().type_factory(), &analyzer_output));
    resolved_node = analyzer_output->resolved_expr();
  }

  ZETASQL_RET_CHECK_NE(resolved_node, nullptr);
  if (const ExecuteQueryConfig::ExamineResolvedASTCallback callback =
          config.examine_resolved_ast_callback();
      callback) {
    ZETASQL_RETURN_IF_ERROR(callback(resolved_node));
  }

  if (config.tool_mode() == ToolMode::kResolve) {
    return writer.resolved(*resolved_node);
  }

  if (config.sql_mode() == SqlMode::kQuery) {
    ZETASQL_RET_CHECK_EQ(resolved_node->node_kind(), RESOLVED_QUERY_STMT);

    PreparedQuery query{resolved_node->GetAs<ResolvedQueryStmt>(),
                        config.evaluator_options()};

    ZETASQL_RETURN_IF_ERROR(
        query.Prepare(config.analyzer_options(), &config.mutable_catalog()));

    switch (config.tool_mode()) {
      case ToolMode::kExplain: {
        ZETASQL_ASSIGN_OR_RETURN(const std::string explain,
                         query.ExplainAfterPrepare());

        return writer.explained(*resolved_node, explain);
      }
      case ToolMode::kExecute: {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<EvaluatorTableIterator> iter,
                         query.ExecuteAfterPrepare());

        return writer.executed(*resolved_node, std::move(iter));
      }
      default:
        return absl::InternalError(absl::StrCat(
            "unknown tool mode: ", static_cast<int>(config.tool_mode())));
    }
  } else if (config.sql_mode() == SqlMode::kExpression) {
    ZETASQL_RET_CHECK(resolved_node->IsExpression());

    PreparedExpression expression{resolved_node->GetAs<ResolvedExpr>(),
                                  config.evaluator_options()};

    ZETASQL_RETURN_IF_ERROR(expression.Prepare(config.analyzer_options(),
                                       &config.mutable_catalog()));

    switch (config.tool_mode()) {
      case ToolMode::kExplain: {
        ZETASQL_ASSIGN_OR_RETURN(const std::string explain,
                         expression.ExplainAfterPrepare());

        return writer.explained(*resolved_node, explain);
      }
      case ToolMode::kExecute: {
        ZETASQL_ASSIGN_OR_RETURN(Value value, expression.ExecuteAfterPrepare());

        return writer.ExecutedExpression(*resolved_node, value);
      }
      default:
        return absl::InternalError(absl::StrCat(
            "unknown tool mode: ", static_cast<int>(config.tool_mode())));
    }
  } else {
    return absl::InternalError(
        absl::StrCat("unknown sql_mode", static_cast<int>(config.sql_mode())));
  }
}

}  // namespace zetasql
