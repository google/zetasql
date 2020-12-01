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

#include "zetasql/public/analyzer.h"

#include <iostream>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/analyzer/function_resolver.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/public/table_name_resolver.h"
#include "zetasql/base/arena.h"
#include "zetasql/common/errors.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_helpers.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/validator.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

ABSL_FLAG(bool, zetasql_validate_resolved_ast, true,
          "Run validator on resolved AST before returning it.");

// This provides a way to extract and look at the zetasql resolved AST
// from within some other test or tool.  It prints to cout rather than logging
// because the output is often too big to log without truncating.
ABSL_FLAG(bool, zetasql_print_resolved_ast, false,
          "Print resolved AST to stdout after resolving (for debugging)");

namespace zetasql {

namespace {

// Verifies that the provided AnalyzerOptions have a valid combination of
// settings.
absl::Status ValidateAnalyzerOptions(const AnalyzerOptions& options) {
  switch (options.parameter_mode()) {
    case PARAMETER_NAMED:
      ZETASQL_RET_CHECK(options.positional_query_parameters().empty())
          << "Positional parameters cannot be provided in named parameter "
             "mode";
      break;
    case PARAMETER_POSITIONAL:
      ZETASQL_RET_CHECK(options.query_parameters().empty())
          << "Named parameters cannot be provided in positional parameter "
             "mode";
      // AddPositionalQueryParameter guards against the case where
      // allow_undeclared_parameters is true and a positional parameter is
      // added, but not the reverse order.
      ZETASQL_RET_CHECK(!options.allow_undeclared_parameters() ||
                options.positional_query_parameters().empty())
          << "When undeclared parameters are allowed, no positional query "
             "parameters can be provided";
      break;
    case PARAMETER_NONE:
      ZETASQL_RET_CHECK(options.query_parameters().empty() &&
                options.positional_query_parameters().empty())
          << "Parameters are disabled and cannot be provided";
      break;
  }

  return absl::OkStatus();
}

// Sets <has_default_resolution_time> to true for every table name in
// 'table_names' if it does not have "FOR SYSTEM_TIME AS OF" expression.
void EnsureResolutionTimeInfoForEveryTable(
    const TableNamesSet& table_names,
    TableResolutionTimeInfoMap* table_resolution_time_info_map) {
  for (const auto& table_name : table_names) {
    TableResolutionTimeInfo& expressions =
        (*table_resolution_time_info_map)[table_name];
    if (expressions.exprs.empty()) {
      expressions.has_default_resolution_time = true;
    }
  }
}

// Returns <options> if it already has all arenas initialized, or otherwise
// populates <copy> as a copy for <options>, creates arenas in <copy> and
// returns it. This avoids unnecessary duplication of AnalyzerOptions, which
// might be expensive.
const AnalyzerOptions& GetOptionsWithArenas(
    const AnalyzerOptions* options, std::unique_ptr<AnalyzerOptions>* copy) {
  if (options->AllArenasAreInitialized()) {
    return *options;
  }
  *copy = absl::make_unique<AnalyzerOptions>(*options);
  (*copy)->CreateDefaultArenasIfNotSet();
  return **copy;
}

}  // namespace

AnalyzerOutput::AnalyzerOutput(
    std::shared_ptr<IdStringPool> id_string_pool,
    std::shared_ptr<zetasql_base::UnsafeArena> arena,
    std::unique_ptr<const ResolvedStatement> statement,
    const AnalyzerOutputProperties& analyzer_output_properties,
    std::unique_ptr<ParserOutput> parser_output,
    const std::vector<absl::Status>& deprecation_warnings,
    const QueryParametersMap& undeclared_parameters,
    const std::vector<const Type*>& undeclared_positional_parameters,
    int max_column_id)
    : id_string_pool_(std::move(id_string_pool)),
      arena_(std::move(arena)),
      statement_(std::move(statement)),
      analyzer_output_properties_(analyzer_output_properties),
      parser_output_(std::move(parser_output)),
      deprecation_warnings_(deprecation_warnings),
      undeclared_parameters_(undeclared_parameters),
      undeclared_positional_parameters_(undeclared_positional_parameters),
      max_column_id_(max_column_id) {}

AnalyzerOutput::AnalyzerOutput(
    std::shared_ptr<IdStringPool> id_string_pool,
    std::shared_ptr<zetasql_base::UnsafeArena> arena,
    std::unique_ptr<const ResolvedExpr> expr,
    const AnalyzerOutputProperties& analyzer_output_properties,
    std::unique_ptr<ParserOutput> parser_output,
    const std::vector<absl::Status>& deprecation_warnings,
    const QueryParametersMap& undeclared_parameters,
    const std::vector<const Type*>& undeclared_positional_parameters,
    int max_column_id)
    : id_string_pool_(std::move(id_string_pool)),
      arena_(std::move(arena)),
      expr_(std::move(expr)),
      analyzer_output_properties_(analyzer_output_properties),
      parser_output_(std::move(parser_output)),
      deprecation_warnings_(deprecation_warnings),
      undeclared_parameters_(undeclared_parameters),
      undeclared_positional_parameters_(undeclared_positional_parameters),
      max_column_id_(max_column_id) {}

AnalyzerOutput::~AnalyzerOutput() {
}

// Common post-parsing work for AnalyzeStatement() series.
static absl::Status FinishAnalyzeStatementImpl(
    absl::string_view sql, const ASTStatement& ast_statement,
    Resolver* resolver, const AnalyzerOptions& options, Catalog* catalog,
    TypeFactory* type_factory,
    std::unique_ptr<const ResolvedStatement>* resolved_statement) {
  ZETASQL_VLOG(5) << "Parsed AST:\n" << ast_statement.DebugString();

  ZETASQL_RETURN_IF_ERROR(
      resolver->ResolveStatement(sql, &ast_statement, resolved_statement));

  ZETASQL_VLOG(3) << "Resolved AST:\n" << (*resolved_statement)->DebugString();

  if (absl::GetFlag(FLAGS_zetasql_validate_resolved_ast)) {
    Validator validator(options.language());
    ZETASQL_RETURN_IF_ERROR(
        validator.ValidateResolvedStatement(resolved_statement->get()));
  }

  if (absl::GetFlag(FLAGS_zetasql_print_resolved_ast)) {
    std::cout << "Resolved AST from thread "
              << std::this_thread::get_id()
              << ":" << std::endl
              << (*resolved_statement)->DebugString() << std::endl;
  }

  if (options.language().error_on_deprecated_syntax() &&
      !resolver->deprecation_warnings().empty()) {
    return resolver->deprecation_warnings().front();
  }

  // Make sure we're starting from a clean state for CheckFieldsAccessed.
  (*resolved_statement)->ClearFieldsAccessed();

  return absl::OkStatus();
}

static absl::Status UnsupportedStatementErrorOrStatus(
    const absl::Status& status, const ParseResumeLocation& resume_location,
    const AnalyzerOptions& options) {
  ZETASQL_RET_CHECK(!status.ok()) << "Expected an error status";
  const ResolvedNodeKind kind = GetNextStatementKind(resume_location);
  if (kind != RESOLVED_LITERAL
      && !options.language().SupportsStatementKind(kind)) {
    ParseLocationPoint location_point = ParseLocationPoint::FromByteOffset(
        resume_location.filename(), resume_location.byte_position());
    return MakeSqlErrorAtPoint(location_point) << "Statement not supported: "
        << ResolvedNodeKindToString(kind);
  }
  return status;
}

static absl::Status AnalyzeStatementImpl(
    absl::string_view sql, const AnalyzerOptions& options, Catalog* catalog,
    TypeFactory* type_factory, std::unique_ptr<const AnalyzerOutput>* output) {
  output->reset();

  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));

  ZETASQL_VLOG(1) << "Parsing statement:\n" << sql;
  std::unique_ptr<ParserOutput> parser_output;
  const absl::Status status = ParseStatement(
      sql, options.GetParserOptions(), &parser_output);
  if (!status.ok()) {
    return UnsupportedStatementErrorOrStatus(
        status, ParseResumeLocation::FromStringView(sql), options);
  }

  return AnalyzeStatementFromParserOutputOwnedOnSuccess(
      &parser_output, options, sql, catalog, type_factory, output);
}

absl::Status AnalyzeStatement(absl::string_view sql,
                              const AnalyzerOptions& options_in,
                              Catalog* catalog, TypeFactory* type_factory,
                              std::unique_ptr<const AnalyzerOutput>* output) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const absl::Status status =
      AnalyzeStatementImpl(sql, options, catalog, type_factory, output);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

static absl::Status AnalyzeNextStatementImpl(
    ParseResumeLocation* resume_location,
    const AnalyzerOptions& options,
    Catalog* catalog,
    TypeFactory* type_factory,
    std::unique_ptr<const AnalyzerOutput>* output,
    bool* at_end_of_input) {
  output->reset();

  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));

  if (resume_location->byte_position() == 0) {
    ZETASQL_VLOG(1) << "Parsing first statement from:\n" << resume_location->input();
  } else {
    ZETASQL_VLOG(2) << "Parsing next statement at position "
            << resume_location->byte_position();
  }

  std::unique_ptr<ParserOutput> parser_output;
  const absl::Status status = ParseNextStatement(
      resume_location, options.GetParserOptions(), &parser_output,
      at_end_of_input);
  if (!status.ok()) {
    return UnsupportedStatementErrorOrStatus(status, *resume_location, options);
  }
  ZETASQL_RET_CHECK(parser_output != nullptr);

  return AnalyzeStatementFromParserOutputOwnedOnSuccess(
      &parser_output, options, resume_location->input(), catalog,
      type_factory, output);
}

absl::Status AnalyzeNextStatement(
    ParseResumeLocation* resume_location,
    const AnalyzerOptions& options_in,
    Catalog* catalog,
    TypeFactory* type_factory,
    std::unique_ptr<const AnalyzerOutput>* output,
    bool* at_end_of_input) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const absl::Status status =
      AnalyzeNextStatementImpl(resume_location, options, catalog,
                               type_factory, output, at_end_of_input);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), resume_location->input(), status);
}

static absl::Status AnalyzeStatementHelper(
    const ASTStatement& ast_statement, const AnalyzerOptions& options,
    absl::string_view sql, Catalog* catalog, TypeFactory* type_factory,
    std::unique_ptr<ParserOutput>* statement_parser_output,
    bool take_parser_output_ownership_on_success,
    std::unique_ptr<const AnalyzerOutput>* output) {
  output->reset();
  ZETASQL_RET_CHECK(options.AllArenasAreInitialized());
  std::unique_ptr<const ResolvedStatement> resolved_statement;
  Resolver resolver(catalog, type_factory, &options);
  const absl::Status status =
      FinishAnalyzeStatementImpl(sql, ast_statement, &resolver, options,
                                 catalog, type_factory, &resolved_statement);
  if (!status.ok()) {
    return ConvertInternalErrorLocationAndAdjustErrorString(
        options.error_message_mode(), sql, status);
  }

  std::unique_ptr<ParserOutput> owned_parser_output;
  if (take_parser_output_ownership_on_success) {
    owned_parser_output = std::move(*statement_parser_output);
  }

  auto original_output = absl::make_unique<AnalyzerOutput>(
      options.id_string_pool(), options.arena(), std::move(resolved_statement),
      resolver.analyzer_output_properties(), std::move(owned_parser_output),
      ConvertInternalErrorLocationsAndAdjustErrorStrings(
          options.error_message_mode(), sql, resolver.deprecation_warnings()),
      resolver.undeclared_parameters(),
      resolver.undeclared_positional_parameters(), resolver.max_column_id());
  ZETASQL_RETURN_IF_ERROR(
      RewriteResolvedAst(options, catalog, type_factory, *original_output));
  *output = std::move(original_output);
  return absl::OkStatus();
}

static absl::Status AnalyzeStatementFromParserOutputImpl(
    std::unique_ptr<ParserOutput>* statement_parser_output,
    bool take_ownership_on_success, const AnalyzerOptions& options,
    absl::string_view sql, Catalog* catalog, TypeFactory* type_factory,
    std::unique_ptr<const AnalyzerOutput>* output) {
  AnalyzerOptions local_options = options;

  // If the arena and IdStringPool are not set in <options>, use the
  // arena and IdStringPool from the parser output by default.
  if (local_options.arena() == nullptr) {
    ZETASQL_RET_CHECK((*statement_parser_output)->arena() != nullptr);
    local_options.set_arena((*statement_parser_output)->arena());
  }
  if (local_options.id_string_pool() == nullptr) {
    ZETASQL_RET_CHECK((*statement_parser_output)->id_string_pool() != nullptr);
    local_options.set_id_string_pool(
        (*statement_parser_output)->id_string_pool());
  }

  const ASTStatement* ast_statement = (*statement_parser_output)->statement();
  return AnalyzeStatementHelper(
      *ast_statement, local_options, sql, catalog, type_factory,
      statement_parser_output, take_ownership_on_success, output);
}

absl::Status AnalyzeStatementFromParserOutputOwnedOnSuccess(
    std::unique_ptr<ParserOutput>* statement_parser_output,
    const AnalyzerOptions& options, absl::string_view sql, Catalog* catalog,
    TypeFactory* type_factory, std::unique_ptr<const AnalyzerOutput>* output) {
  return AnalyzeStatementFromParserOutputImpl(
      statement_parser_output, /*take_ownership_on_success=*/true, options,
      sql, catalog, type_factory, output);
}

absl::Status AnalyzeStatementFromParserOutputUnowned(
    std::unique_ptr<ParserOutput>* statement_parser_output,
    const AnalyzerOptions& options, absl::string_view sql, Catalog* catalog,
    TypeFactory* type_factory, std::unique_ptr<const AnalyzerOutput>* output) {
  return AnalyzeStatementFromParserOutputImpl(
      statement_parser_output, /*take_ownership_on_success=*/false, options,
      sql, catalog, type_factory, output);
}

absl::Status AnalyzeStatementFromParserAST(
    const ASTStatement& statement, const AnalyzerOptions& options,
    absl::string_view sql, Catalog* catalog, TypeFactory* type_factory,
    std::unique_ptr<const AnalyzerOutput>* output) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options_with_arenas =
      GetOptionsWithArenas(&options, &copy);
  return AnalyzeStatementHelper(
      statement, options_with_arenas, sql, catalog, type_factory,
      /*statement_parser_output=*/nullptr,
      /*take_parser_output_ownership_on_success=*/false, output);
}

// Coerces <resolved_expr> to <target_type>, using assignment semantics
// For details, see Coercer::AssignableTo() in
// .../public/coercer.h
//
// Upon success, a resolved tree that implements the conversion is stored in
// <resolved_expr>, replacing the tree that was previously there.
static absl::Status ConvertExprToTargetType(
    const ASTExpression& ast_expression, absl::string_view sql,
    const AnalyzerOptions& analyzer_options, Catalog* catalog,
    TypeFactory* type_factory, const Type* target_type,
    std::unique_ptr<const ResolvedExpr>* resolved_expr) {
  Resolver resolver(catalog, type_factory, &analyzer_options);
  return ConvertInternalErrorLocationToExternal(
      resolver.CoerceExprToType(&ast_expression, target_type,
                                     /*assignment_semantics=*/true,
                                     /*clause_name=*/nullptr, resolved_expr),
      sql);
}

static absl::Status AnalyzeExpressionFromParserASTImpl(
    const ASTExpression& ast_expression,
    std::unique_ptr<ParserOutput> parser_output, absl::string_view sql,
    const AnalyzerOptions& options, Catalog* catalog, TypeFactory* type_factory,
    const Type* target_type, std::unique_ptr<const AnalyzerOutput>* output) {
  std::unique_ptr<const ResolvedExpr> resolved_expr;
  Resolver resolver(catalog, type_factory, &options);
  ZETASQL_RETURN_IF_ERROR(resolver.ResolveStandaloneExpr(
      sql, &ast_expression, &resolved_expr));
  ZETASQL_VLOG(3) << "Resolved AST:\n" << resolved_expr->DebugString();

  if (target_type != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ConvertExprToTargetType(ast_expression, sql, options,
                                            catalog, type_factory, target_type,
                                            &resolved_expr));
  }

  if (absl::GetFlag(FLAGS_zetasql_validate_resolved_ast)) {
    Validator validator(options.language());
    ZETASQL_RETURN_IF_ERROR(
        validator.ValidateStandaloneResolvedExpr(resolved_expr.get()));
  }

  if (absl::GetFlag(FLAGS_zetasql_print_resolved_ast)) {
    std::cout << "Resolved AST from thread "
              << std::this_thread::get_id()
              << ":" << std::endl
              << resolved_expr->DebugString() << std::endl;
  }

  if (options.language().error_on_deprecated_syntax() &&
      !resolver.deprecation_warnings().empty()) {
    return resolver.deprecation_warnings().front();
  }

  // Make sure we're starting from a clean state for CheckFieldsAccessed.
  resolved_expr->ClearFieldsAccessed();

  auto original_output = absl::make_unique<AnalyzerOutput>(
      options.id_string_pool(), options.arena(), std::move(resolved_expr),
      resolver.analyzer_output_properties(),
      std::move(parser_output),
      ConvertInternalErrorLocationsAndAdjustErrorStrings(
          options.error_message_mode(), sql, resolver.deprecation_warnings()),
      resolver.undeclared_parameters(),
      resolver.undeclared_positional_parameters(),
      resolver.max_column_id());
  ZETASQL_RETURN_IF_ERROR(
      RewriteResolvedAst(options, catalog, type_factory, *original_output));
  *output = std::move(original_output);
  return absl::OkStatus();
}

static absl::Status AnalyzeExpressionImpl(
    absl::string_view sql, const AnalyzerOptions& options_in, Catalog* catalog,
    TypeFactory* type_factory, const Type* target_type,
    std::unique_ptr<const AnalyzerOutput>* output) {
  output->reset();

  ZETASQL_VLOG(1) << "Parsing expression:\n" << sql;
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));

  std::unique_ptr<ParserOutput> parser_output;
  ParserOptions parser_options = options.GetParserOptions();
  ZETASQL_RETURN_IF_ERROR(ParseExpression(sql, parser_options, &parser_output));
  const ASTExpression* expression = parser_output->expression();
  ZETASQL_VLOG(5) << "Parsed AST:\n" << expression->DebugString();

  return AnalyzeExpressionFromParserASTImpl(
      *expression, std::move(parser_output), sql, options, catalog,
      type_factory, target_type, output);
}

absl::Status AnalyzeExpression(absl::string_view sql,
                               const AnalyzerOptions& options, Catalog* catalog,
                               TypeFactory* type_factory,
                               std::unique_ptr<const AnalyzerOutput>* output) {
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql,
      AnalyzeExpressionImpl(sql, options, catalog, type_factory, nullptr,
                            output));
}

absl::Status AnalyzeExpressionForAssignmentToType(
    absl::string_view sql, const AnalyzerOptions& options, Catalog* catalog,
    TypeFactory* type_factory, const Type* target_type,
    std::unique_ptr<const AnalyzerOutput>* output) {
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql,
      AnalyzeExpressionImpl(sql, options, catalog, type_factory, target_type,
                            output));
}

absl::Status AnalyzeExpressionFromParserAST(
    const ASTExpression& ast_expression, const AnalyzerOptions& options_in,
    absl::string_view sql, TypeFactory* type_factory, Catalog* catalog,
    std::unique_ptr<const AnalyzerOutput>* output) {
  return AnalyzeExpressionFromParserASTForAssignmentToType(
      ast_expression, options_in, sql, type_factory, catalog,
      /*target_type=*/nullptr, output);
}

absl::Status AnalyzeExpressionFromParserASTForAssignmentToType(
    const ASTExpression& ast_expression, const AnalyzerOptions& options_in,
    absl::string_view sql, TypeFactory* type_factory, Catalog* catalog,
    const Type* target_type, std::unique_ptr<const AnalyzerOutput>* output) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const absl::Status status = AnalyzeExpressionFromParserASTImpl(
      ast_expression, /*parser_output=*/nullptr, sql, options, catalog,
      type_factory, target_type, output);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

static absl::Status AnalyzeTypeImpl(const std::string& type_name,
                                    const AnalyzerOptions& options,
                                    Catalog* catalog, TypeFactory* type_factory,
                                    const Type** output_type) {
  *output_type = nullptr;

  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));
  ZETASQL_VLOG(1) << "Resolving type: " << type_name;

  Resolver resolver(catalog, type_factory, &options);
  ZETASQL_RETURN_IF_ERROR(resolver.ResolveTypeName(type_name, output_type));

  ZETASQL_VLOG(3) << "Resolved type: " << (*output_type)->DebugString();
  return absl::OkStatus();
}

absl::Status AnalyzeType(const std::string& type_name,
                         const AnalyzerOptions& options_in, Catalog* catalog,
                         TypeFactory* type_factory, const Type** output_type) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const absl::Status status =
      AnalyzeTypeImpl(type_name, options, catalog, type_factory, output_type);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), type_name, status);
}

static absl::Status ExtractTableNamesFromStatementImpl(
    absl::string_view sql, const AnalyzerOptions& options,
    TableNamesSet* table_names) {
  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));
  ZETASQL_VLOG(3) << "Extracting table names from statement:\n" << sql;
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_RETURN_IF_ERROR(
      ParseStatement(sql, options.GetParserOptions(), &parser_output));
  ZETASQL_VLOG(5) << "Parsed AST:\n" << parser_output->statement()->DebugString();

  return table_name_resolver::FindTables(sql, *parser_output->statement(),
                                         options, table_names);
}

static absl::Status ExtractTableResolutionTimeFromStatementImpl(
    absl::string_view sql, const AnalyzerOptions& options,
    TypeFactory* type_factory, Catalog* catalog,
    TableResolutionTimeInfoMap* table_resolution_time_info_map,
    std::unique_ptr<ParserOutput>* parser_output) {
  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));
  ZETASQL_VLOG(3) << "Extracting table resolution time from statement:\n" << sql;
  ZETASQL_RETURN_IF_ERROR(
      ParseStatement(sql, options.GetParserOptions(), parser_output));
  ZETASQL_VLOG(5) << "Parsed AST:\n" << (*parser_output)->statement()->DebugString();

  TableNamesSet table_names;
  ZETASQL_RETURN_IF_ERROR(table_name_resolver::FindTableNamesAndResolutionTime(
      sql, *(*parser_output)->statement(), options, type_factory, catalog,
      &table_names, table_resolution_time_info_map));
  // Note that "FOR SYSTEM_TIME AS OF ..." expressions are only valid inside
  // table path expressions. However, we want to have an entry in the output
  // map for every source table,
  EnsureResolutionTimeInfoForEveryTable(table_names,
                                        table_resolution_time_info_map);
  return absl::OkStatus();
}

absl::Status ExtractTableNamesFromStatement(absl::string_view sql,
                                            const AnalyzerOptions& options_in,
                                            TableNamesSet* table_names) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const absl::Status status =
      ExtractTableNamesFromStatementImpl(sql, options, table_names);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

absl::Status ExtractTableResolutionTimeFromStatement(
    absl::string_view sql, const AnalyzerOptions& options_in,
    TypeFactory* type_factory, Catalog* catalog,
    TableResolutionTimeInfoMap* table_resolution_time_info_map,
    std::unique_ptr<ParserOutput>* parser_output) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const absl::Status status = ExtractTableResolutionTimeFromStatementImpl(
      sql, options, type_factory, catalog, table_resolution_time_info_map,
      parser_output);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

static absl::Status ExtractTableNamesFromNextStatementImpl(
    ParseResumeLocation* resume_location, const AnalyzerOptions& options,
    TableNamesSet* table_names, bool* at_end_of_input) {
  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));

  ZETASQL_VLOG(2) << "Extracting table names from next statement at position "
          << resume_location->byte_position();
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_RETURN_IF_ERROR(ParseNextStatement(resume_location,
                                     options.GetParserOptions(), &parser_output,
                                     at_end_of_input));
  ZETASQL_VLOG(5) << "Parsed AST:\n" << parser_output->statement()->DebugString();

  return table_name_resolver::FindTables(resume_location->input(),
                                         *parser_output->statement(),
                                         options, table_names);
}

absl::Status ExtractTableNamesFromNextStatement(
    ParseResumeLocation* resume_location, const AnalyzerOptions& options_in,
    TableNamesSet* table_names, bool* at_end_of_input) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const absl::Status status = ExtractTableNamesFromNextStatementImpl(
      resume_location, options, table_names, at_end_of_input);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), resume_location->input(), status);
}

absl::Status ExtractTableNamesFromASTStatement(
    const ASTStatement& ast_statement, const AnalyzerOptions& options_in,
    absl::string_view sql, TableNamesSet* table_names) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const absl::Status status = table_name_resolver::FindTables(
      sql, ast_statement, options, table_names);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

static absl::Status ExtractTableResolutionTimeFromASTStatementImpl(
    absl::string_view sql, const AnalyzerOptions& options,
    const ASTStatement& ast_statement, TypeFactory* type_factory,
    Catalog* catalog,
    TableResolutionTimeInfoMap* table_resolution_time_info_map) {
  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));
  ZETASQL_VLOG(3) << "Extracting table resolution time from parsed AST statement:\n"
          << ast_statement.DebugString();

  TableNamesSet table_names;
  ZETASQL_RETURN_IF_ERROR(table_name_resolver::FindTableNamesAndResolutionTime(
      sql, ast_statement, options, type_factory, catalog, &table_names,
      table_resolution_time_info_map));
  // Note that "FOR SYSTEM_TIME AS OF ..." expressions are only valid inside
  // table path expressions. However, we want to have an entry in the output
  // map for every source table,
  EnsureResolutionTimeInfoForEveryTable(table_names,
                                        table_resolution_time_info_map);
  return absl::OkStatus();
}

absl::Status ExtractTableResolutionTimeFromASTStatement(
    const ASTStatement& ast_statement, const AnalyzerOptions& options_in,
    absl::string_view sql, TypeFactory* type_factory, Catalog* catalog,
    TableResolutionTimeInfoMap* table_resolution_time_info_map) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const absl::Status status =
      ExtractTableResolutionTimeFromASTStatementImpl(
          sql, options, ast_statement, type_factory, catalog,
          table_resolution_time_info_map);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

absl::Status ExtractTableNamesFromScript(absl::string_view sql,
                                         const AnalyzerOptions& options_in,
                                         TableNamesSet* table_names) {
  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options_in));
  ZETASQL_VLOG(3) << "Extracting table names from script:\n" << sql;
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_RETURN_IF_ERROR(ParseScript(sql, options.GetParserOptions(),
                              options.error_message_mode(), &parser_output));
  ZETASQL_VLOG(5) << "Parsed AST:\n" << parser_output->script()->DebugString();

  absl::Status status = table_name_resolver::FindTableNamesInScript(
      sql, *(parser_output->script()), options, table_names);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

absl::Status ExtractTableNamesFromASTScript(const ASTScript& ast_script,
                                            const AnalyzerOptions& options_in,
                                            absl::string_view sql,
                                            TableNamesSet* table_names) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const absl::Status status = table_name_resolver::FindTableNamesInScript(
      sql, ast_script, options, table_names);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

}  // namespace zetasql
