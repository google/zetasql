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

#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/common/options_utils.h"
#include "zetasql/parser/macros/macro_expander.h"
#include "zetasql/parser/macros/standalone_macro_expansion.h"
#include "zetasql/parser/macros/standalone_macro_expansion_impl.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/simple_catalog_util.h"
#include "zetasql/public/sql_formatter.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/sql_builder.h"
#include "zetasql/tools/execute_query/execute_query_proto_writer.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "zetasql/base/check.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor_database.h"
#include "google/protobuf/message.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

ABSL_FLAG(std::string, product_mode, "internal",
          "The product_mode to use in language options. Note, language_features"
          " is an orthongal way to configure language options."
          "\nValid values are:"
          "\n     'internal': supports protos, DOUBLE, signed ints, etc. "
          "\n     'external': mode used in Cloud engines");

ABSL_FLAG(std::vector<std::string>, mode, {"execute"},
          "The comma-separated tool modes to use. Valid values are:"
          "\n     'parse'   parse the parser AST"
          "\n     'unparse'  parse, then dump as sql"
          "\n     'analyze'  print the resolved AST"
          "\n     'unanalyze'  analyze, then dump as sql"
          "\n     'explain'  print the evaluator query plan"
          "\n     'execute'  actually run the query and print the result. (not"
          "                  all functionality is supported).");

ABSL_FLAG(zetasql::internal::EnabledAstRewrites, enabled_ast_rewrites,
          zetasql::internal::EnabledAstRewrites{
              .enabled_ast_rewrites = zetasql::internal::GetAllRewrites()},
          "The AST Rewrites to enable in the analyzer, format is:"
          "\n   <BASE>[,+<ADDED_OPTION>][,-<REMOVED_OPTION>]..."
          "\n Where BASE is one of:"
          "\n   'NONE'    : the empty set"
          "\n   'ALL'     :   all possible rewrites, including those in"
          " development"
          "\n   'DEFAULTS': all ResolvedASTRewrite's with 'default_enabled' set"
          "\n"
          "\n enum values must be listed with 'REWRITE_' stripped"
          "\n Example:"
          "\n    --enabled_ast_rewrites='DEFAULT,-FLATTEN,+ANONYMIZATION'"
          "\n Will enable all the default options plus ANONYMIZATION, but"
          " excluding flatten");

ABSL_FLAG(std::optional<zetasql::internal::EnabledLanguageFeatures>,
          enabled_language_features, std::nullopt,
          zetasql::internal::EnabledLanguageFeatures::kFlagDescription);

ABSL_FLAG(std::string, parameters, {},
          zetasql::internal::kQueryParameterMapHelpstring);

ABSL_FLAG(bool, strict_name_resolution_mode, false,
          "Sets LanguageOptions::strict_resolution_mode.");

ABSL_FLAG(bool, evaluator_scramble_undefined_orderings, false,
          "When true, shuffle the order of rows in intermediate reults that "
          "are unordered.");

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
using ExpansionOutput = parser::macros::ExpansionOutput;
}  // namespace

absl::Status SetToolModeFromFlags(ExecuteQueryConfig& config) {
  const std::vector<std::string> mode = absl::GetFlag(FLAGS_mode);
  const absl::flat_hash_set<absl::string_view> modes(mode.begin(), mode.end());

  config.clear_tool_modes();
  if (modes.empty()) {
    config.add_tool_mode(ToolMode::kExecute);
    return absl::OkStatus();
  }

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

  for (const auto& mode : modes) {
    if (tool_mode_map->contains(absl::AsciiStrToLower(mode))) {
      config.add_tool_mode(tool_mode_map->at(mode));
    } else {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Invalid --mode: '" << mode << "'";
    }
  }

  return absl::OkStatus();
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

static absl::Status SetRewritersFromFlags(ExecuteQueryConfig& config) {
  config.mutable_analyzer_options().set_enabled_rewrites(
      absl::GetFlag(FLAGS_enabled_ast_rewrites).enabled_ast_rewrites);
  return absl::OkStatus();
}

static absl::Status SetLanguageFeaturesFromFlags(ExecuteQueryConfig& config) {
  std::optional<internal::EnabledLanguageFeatures> features =
      absl::GetFlag(FLAGS_enabled_language_features);
  if (features.has_value()) {
    config.mutable_analyzer_options()
        .mutable_language()
        ->SetEnabledLanguageFeatures(
            {features->enabled_language_features.begin(),
             features->enabled_language_features.end()});
  }
  return absl::OkStatus();
}

static absl::Status SetProductModeFromFlags(ExecuteQueryConfig& config) {
  std::string product_mode =
      absl::AsciiStrToLower(absl::GetFlag(FLAGS_product_mode));
  if (product_mode == "internal") {
    config.mutable_analyzer_options().mutable_language()->set_product_mode(
        PRODUCT_INTERNAL);
    return absl::OkStatus();
  } else if (product_mode == "external") {
    config.mutable_analyzer_options().mutable_language()->set_product_mode(
        PRODUCT_EXTERNAL);
    return absl::OkStatus();
  }
  return zetasql_base::InvalidArgumentErrorBuilder()
         << "Invalid --product_mode:'" << product_mode << "'";
}

static absl::Status SetNameResolutionModeFromFlags(ExecuteQueryConfig& config) {
  config.mutable_analyzer_options()
      .mutable_language()
      ->set_name_resolution_mode(
          absl::GetFlag(FLAGS_strict_name_resolution_mode)
              ? NAME_RESOLUTION_STRICT
              : NAME_RESOLUTION_DEFAULT);

  return absl::OkStatus();
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

absl::Status SetLanguageOptionsFromFlags(ExecuteQueryConfig& config) {
  ZETASQL_RETURN_IF_ERROR(SetProductModeFromFlags(config));
  ZETASQL_RETURN_IF_ERROR(SetNameResolutionModeFromFlags(config));
  return SetLanguageFeaturesFromFlags(config);
}

absl::Status SetAnalyzerOptionsFromFlags(ExecuteQueryConfig& config) {
  return SetRewritersFromFlags(config);
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
  config.mutable_evaluator_options().scramble_undefined_orderings =
      absl::GetFlag(FLAGS_evaluator_scramble_undefined_orderings);
  return absl::OkStatus();
}

absl::Status SetQueryParametersFromFlags(ExecuteQueryConfig& config) {
  ParameterValueMap parameters;
  std::string err;
  if (!internal::ParseQueryParameterFlag(
          absl::GetFlag(FLAGS_parameters), config.analyzer_options(),
          &config.mutable_catalog(), &parameters, &err)) {
    return absl::InvalidArgumentError(err);
  }
  for (const auto& [name, value] : parameters) {
    ZETASQL_RETURN_IF_ERROR(config.mutable_analyzer_options().AddQueryParameter(
        name, value.type()));
  }

  config.mutable_query_parameter_values() = std::move(parameters);
  return absl::OkStatus();
}

ExecuteQueryConfig::ExecuteQueryConfig() : catalog_("") {}

void ExecuteQueryConfig::SetDescriptorPool(const google::protobuf::DescriptorPool* pool) {
  ABSL_CHECK(descriptor_pool_ == nullptr) << __func__ << " can only be called once";
  owned_descriptor_pool_.reset();
  descriptor_pool_ = pool;
  catalog_.SetDescriptorPool(pool);
}

void ExecuteQueryConfig::SetOwnedDescriptorPool(
    std::unique_ptr<const google::protobuf::DescriptorPool> pool) {
  ABSL_CHECK(descriptor_pool_ == nullptr) << __func__ << " can only be called once";
  owned_descriptor_pool_ = std::move(pool);
  descriptor_pool_ = owned_descriptor_pool_.get();
  catalog_.SetDescriptorPool(descriptor_pool_);
}

void ExecuteQueryConfig::SetOwnedDescriptorDatabase(
    std::unique_ptr<google::protobuf::DescriptorDatabase> db) {
  ABSL_CHECK(descriptor_db_ == nullptr) << __func__ << " can only be called once";

  // The descriptor database given to the pool needs to be owned locally.
  descriptor_db_ = std::move(db);

  SetOwnedDescriptorPool(
      std::make_unique<const google::protobuf::DescriptorPool>(descriptor_db_.get()));
}

static absl::StatusOr<bool> RegisterMacro(absl::string_view sql,
                                          ExecuteQueryConfig& config,
                                          ExecuteQueryWriter& writer) {
  // TODO: Hack: we did not implement
  // ResolvedDefineMacroStatement yet, so for now we short-circuit into
  // updating the macro catalog.
  std::unique_ptr<ParserOutput> parser_output;
  absl::Status parse_stmt = ParseStatement(
      sql, ParserOptions(config.analyzer_options().language()), &parser_output);
  if (parse_stmt.ok() &&
      parser_output->statement()->Is<ASTDefineMacroStatement>()) {
    config.mutable_macro_sources().push_back(std::string(sql));
    auto define_macro_statement =
        parser_output->statement()->GetAsOrNull<ASTDefineMacroStatement>();
    ZETASQL_RETURN_IF_ERROR(config.mutable_macro_catalog().RegisterMacro(
        {.source_text = config.macro_sources().back(),
         .location = define_macro_statement->GetParseLocationRange(),
         .name_location =
             define_macro_statement->name()->GetParseLocationRange(),
         .body_location =
             define_macro_statement->body()->GetParseLocationRange()}));

    std::string macro_name = define_macro_statement->name()->GetAsString();
    ZETASQL_RETURN_IF_ERROR(
        writer.unparsed(absl::StrFormat("Macro registered: %s", macro_name)));
    return true;
  }

  return false;
}

static absl::StatusOr<std::string> ExpandMacros(
    absl::string_view sql, const ExecuteQueryConfig& config,
    ExecuteQueryWriter& writer) {
  ZETASQL_ASSIGN_OR_RETURN(ExpansionOutput expansion_output,
                   parser::macros::ExpandMacros(
                       "<filename>", sql, config.macro_catalog(),
                       config.analyzer_options().language(),
                       config.analyzer_options().error_message_options()));
  std::string expanded_sql =
      parser::macros::TokensToString(expansion_output.expanded_tokens);
  for (const absl::Status& warning : expansion_output.warnings) {
    ZETASQL_RETURN_IF_ERROR(
        writer.unparsed(absl::StrCat("Warning: ", warning.message())));
  }
  ZETASQL_RETURN_IF_ERROR(writer.unparsed("Expanded SQL:"));
  ZETASQL_RETURN_IF_ERROR(writer.unparsed(expanded_sql));
  return expanded_sql;
}

static absl::StatusOr<const ASTNode*> ParseSql(
    absl::string_view sql, const ExecuteQueryConfig& config,
    std::unique_ptr<ParserOutput>* parser_output) {
  ParserOptions parser_options;

  const ASTNode* root = nullptr;
  switch (config.sql_mode()) {
    case SqlMode::kQuery: {
      parser_options.set_language_options(config.analyzer_options().language());
      ZETASQL_RETURN_IF_ERROR(ParseStatement(sql, parser_options, parser_output));
      root = (*parser_output)->statement();
      break;
    }
    case SqlMode::kExpression: {
      parser_options.set_language_options(config.analyzer_options().language());
      ZETASQL_RETURN_IF_ERROR(ParseExpression(sql, parser_options, parser_output));
      root = (*parser_output)->expression();
      break;
    }
  }
  ZETASQL_RET_CHECK_NE(root, nullptr);
  return root;
}

static absl::Status WriteParsedAndOrUnparsedAst(
    const ASTNode* root, const ExecuteQueryConfig& config,
    ExecuteQueryWriter& writer) {
  if (config.has_tool_mode(ToolMode::kParse)) {
    // Note, ASTNode is not public, and therefore cannot be part of the
    // public interface, thus, we can only return the string.
    ZETASQL_RETURN_IF_ERROR(writer.parsed(root->DebugString()));
  }
  if (config.has_tool_mode(ToolMode::kUnparse)) {
    ZETASQL_RETURN_IF_ERROR(writer.unparsed(Unparse(root)));
  }

  return absl::OkStatus();
}

static bool IsAnalysisRequired(const ExecuteQueryConfig& config) {
  return config.has_tool_mode(ToolMode::kResolve) ||
         config.has_tool_mode(ToolMode::kUnAnalyze) ||
         config.has_tool_mode(ToolMode::kExplain) ||
         config.has_tool_mode(ToolMode::kExecute);
}

static absl::StatusOr<const ResolvedNode*> AnalyzeSql(
    absl::string_view sql, const ASTNode* ast, ExecuteQueryConfig& config,
    ExecuteQueryWriter& writer,
    std::unique_ptr<const AnalyzerOutput>* analyzer_output) {
  const ResolvedNode* resolved_node = nullptr;
  switch (config.sql_mode()) {
    case SqlMode::kQuery: {
      ZETASQL_RETURN_IF_ERROR(AnalyzeStatementFromParserAST(
          *ast->GetAsOrDie<ASTStatement>(), config.analyzer_options(), sql,
          &config.mutable_catalog(), config.mutable_catalog().type_factory(),
          analyzer_output));
      resolved_node = (*analyzer_output)->resolved_statement();
      break;
    }
    case SqlMode::kExpression: {
      ZETASQL_RETURN_IF_ERROR(AnalyzeExpressionFromParserAST(
          *ast->GetAsOrDie<ASTExpression>(), config.analyzer_options(), sql,
          config.mutable_catalog().type_factory(), &config.mutable_catalog(),
          analyzer_output));
      resolved_node = (*analyzer_output)->resolved_expr();
      break;
    }
  }

  if (config.has_tool_mode(ToolMode::kResolve)) {
    ZETASQL_RET_CHECK_NE(resolved_node, nullptr);
    ZETASQL_RETURN_IF_ERROR(writer.resolved(*resolved_node));
  }

  return resolved_node;
}

static absl::Status UnanalyzeQuery(const ResolvedNode* resolved_node,
                                   ExecuteQueryConfig& config,
                                   ExecuteQueryWriter& writer) {
  SQLBuilder::SQLBuilderOptions sql_builder_options;
  sql_builder_options.language_options = config.analyzer_options().language();
  sql_builder_options.catalog = &config.mutable_catalog();
  SQLBuilder builder(sql_builder_options);
  ZETASQL_RETURN_IF_ERROR(builder.Process(*resolved_node));

  std::string formatted_sql;
  ZETASQL_RETURN_IF_ERROR(FormatSql(builder.sql(), &formatted_sql));
  formatted_sql.append(1, '\n');
  return writer.unanalyze(formatted_sql);
}

static absl::Status RegisterFunction(absl::string_view sql,
                                     ExecuteQueryConfig& config,
                                     ExecuteQueryWriter& writer) {
  std::unique_ptr<const AnalyzerOutput> function_artifacts;
  ZETASQL_RET_CHECK_OK(AddFunctionFromCreateFunction(
      sql, config.analyzer_options(),
      /*allow_persistent_function=*/true, /*function_options=*/std::nullopt,
      function_artifacts, config.mutable_catalog()));
  config.AddFunctionArtifacts(std::move(function_artifacts));
  return writer.unparsed("Function registered.");
}

static absl::Status ExplainAndOrExecuteSql(const ResolvedNode* resolved_node,
                                           ExecuteQueryConfig& config,
                                           ExecuteQueryWriter& writer) {
  switch (config.sql_mode()) {
    case SqlMode::kQuery: {
      ZETASQL_RET_CHECK_EQ(resolved_node->node_kind(), RESOLVED_QUERY_STMT);

      PreparedQuery query{resolved_node->GetAs<ResolvedQueryStmt>(),
                          config.evaluator_options()};
      ZETASQL_RETURN_IF_ERROR(
          query.Prepare(config.analyzer_options(), &config.mutable_catalog()));

      if (config.has_tool_mode(ToolMode::kExplain)) {
        ZETASQL_ASSIGN_OR_RETURN(std::string explain, query.ExplainAfterPrepare());
        explain.append(1, '\n');
        ZETASQL_RETURN_IF_ERROR(writer.explained(*resolved_node, explain));
      }

      if (config.has_tool_mode(ToolMode::kExecute)) {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<EvaluatorTableIterator> iter,
                         query.ExecuteAfterPrepare(
                             {.parameters = config.query_parameter_values()}));
        ZETASQL_RETURN_IF_ERROR(writer.executed(*resolved_node, std::move(iter)));
      }

      return absl::OkStatus();
    }
    case SqlMode::kExpression: {
      ZETASQL_RET_CHECK(resolved_node->IsExpression());

      PreparedExpression expression{resolved_node->GetAs<ResolvedExpr>(),
                                    config.evaluator_options()};
      ZETASQL_RETURN_IF_ERROR(expression.Prepare(config.analyzer_options(),
                                         &config.mutable_catalog()));

      if (config.has_tool_mode(ToolMode::kExplain)) {
        ZETASQL_ASSIGN_OR_RETURN(const std::string explain,
                         expression.ExplainAfterPrepare());
        ZETASQL_RETURN_IF_ERROR(writer.explained(*resolved_node, explain));
      }

      if (config.has_tool_mode(ToolMode::kExecute)) {
        PreparedExpressionBase::ExpressionOptions expression_options;
        expression_options.parameters = config.query_parameter_values();
        ZETASQL_ASSIGN_OR_RETURN(Value value, expression.ExecuteAfterPrepare(
                                          std::move(expression_options)));
        ZETASQL_RETURN_IF_ERROR(writer.ExecutedExpression(*resolved_node, value));
      }

      return absl::OkStatus();
    }
  }
}

absl::Status ExecuteQuery(absl::string_view sql, ExecuteQueryConfig& config,
                          ExecuteQueryWriter& writer) {
  bool enable_macros =
      config.sql_mode() == SqlMode::kQuery &&
      config.analyzer_options().language().LanguageFeatureEnabled(
          FEATURE_V_1_4_SQL_MACROS);

  std::string expanded_sql;  // Defined outside of if() to stay in scope.
  if (enable_macros) {
    // macro registration
    if (config.has_tool_mode(ToolMode::kExecute)) {
      ZETASQL_ASSIGN_OR_RETURN(bool macro_registered,
                       RegisterMacro(sql, config, writer));
      if (macro_registered) {
        return absl::OkStatus();
      }
    }

    // macro expansion
    ZETASQL_ASSIGN_OR_RETURN(expanded_sql, ExpandMacros(sql, config, writer));
    sql = expanded_sql;
  }

  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_ASSIGN_OR_RETURN(const ASTNode* ast, ParseSql(sql, config, &parser_output));

  if (config.has_tool_mode(ToolMode::kParse) ||
      config.has_tool_mode(ToolMode::kUnparse)) {
    ZETASQL_RETURN_IF_ERROR(WriteParsedAndOrUnparsedAst(ast, config, writer));
  }

  if (!IsAnalysisRequired(config)) {
    return absl::OkStatus();
  }

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  ZETASQL_ASSIGN_OR_RETURN(const ResolvedNode* resolved_node,
                   AnalyzeSql(sql, ast, config, writer, &analyzer_output));
  ZETASQL_RET_CHECK_NE(resolved_node, nullptr);

  const ExecuteQueryConfig::ExamineResolvedASTCallback callback =
      config.examine_resolved_ast_callback();
  if (callback) {
    ZETASQL_RETURN_IF_ERROR(callback(resolved_node));
  }

  if (resolved_node->node_kind() == RESOLVED_CREATE_FUNCTION_STMT) {
    ZETASQL_RETURN_IF_ERROR(RegisterFunction(sql, config, writer));
  }

  if (config.has_tool_mode(ToolMode::kUnAnalyze)) {
    ZETASQL_RETURN_IF_ERROR(UnanalyzeQuery(resolved_node, config, writer));
  }

  if ((resolved_node->node_kind() == RESOLVED_QUERY_STMT ||
       resolved_node->IsExpression()) &&
      (config.has_tool_mode(ToolMode::kExplain) ||
       config.has_tool_mode(ToolMode::kExecute))) {
    ZETASQL_RETURN_IF_ERROR(ExplainAndOrExecuteSql(resolved_node, config, writer));
  }

  return absl::OkStatus();
}

}  // namespace zetasql
