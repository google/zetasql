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

#include "zetasql/analyzer/analyzer_impl.h"

#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include <thread>

#include "zetasql/base/logging.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/analyzer/rewrite_resolved_ast.h"
#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/common/errors.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/validator.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
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

absl::Status AnalyzeExpressionImpl(
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

  return InternalAnalyzeExpressionFromParserAST(
      *expression, std::move(parser_output), sql, options, catalog,
      type_factory, target_type, output);
}

}  // namespace

absl::Status InternalAnalyzeExpression(
    absl::string_view sql, const AnalyzerOptions& options, Catalog* catalog,
    TypeFactory* type_factory, const Type* target_type,
    std::unique_ptr<const AnalyzerOutput>* output) {
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql,
      AnalyzeExpressionImpl(sql, options, catalog, type_factory, target_type,
                            output));
}

absl::Status ConvertExprToTargetType(
    const ASTExpression& ast_expression, absl::string_view sql,
    const AnalyzerOptions& analyzer_options, Catalog* catalog,
    TypeFactory* type_factory, const Type* target_type,
    std::unique_ptr<const ResolvedExpr>* resolved_expr) {
  Resolver resolver(catalog, type_factory, &analyzer_options);
  return ConvertInternalErrorLocationToExternal(
      resolver.CoerceExprToType(&ast_expression, target_type,
                                Resolver::kImplicitAssignment, resolved_expr),
      sql);
}

absl::Status InternalAnalyzeExpressionFromParserAST(
    const ASTExpression& ast_expression,
    std::unique_ptr<ParserOutput> parser_output, absl::string_view sql,
    const AnalyzerOptions& options, Catalog* catalog, TypeFactory* type_factory,
    const Type* target_type, std::unique_ptr<const AnalyzerOutput>* output) {
  std::unique_ptr<const ResolvedExpr> resolved_expr;
  Resolver resolver(catalog, type_factory, &options);
  ZETASQL_RETURN_IF_ERROR(
      resolver.ResolveStandaloneExpr(sql, &ast_expression, &resolved_expr));
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
      resolver.analyzer_output_properties(), std::move(parser_output),
      ConvertInternalErrorLocationsAndAdjustErrorStrings(
          options.error_message_mode(), sql, resolver.deprecation_warnings()),
      resolver.undeclared_parameters(),
      resolver.undeclared_positional_parameters(), resolver.max_column_id());
  ZETASQL_RETURN_IF_ERROR(InternalRewriteResolvedAst(options, sql, catalog,
                                             type_factory, *original_output));
  *output = std::move(original_output);
  return absl::OkStatus();
}
}  // namespace zetasql
