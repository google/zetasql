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

#include "zetasql/analyzer/rewrite_resolved_ast.h"

#include <memory>
#include <string>
#include <utility>

#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/base/logging.h"
#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/validator.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {


namespace {

// Returns a ResolvedNode from AnalyzerOutput. This function assumes one
// of resolved_statement() and resolved_expr() is non-null and returns that.
const ResolvedNode* NodeFromAnalyzerOutput(const AnalyzerOutput& output) {
  if (output.resolved_statement() != nullptr) {
    return output.resolved_statement();
  }
  return output.resolved_expr();
}

// Returns an AnalyzerOptions suitable for passing to rewriters. Most of the
// settings are copied from <analyzer_options>, which is the options used to
// analyze the outer statement. Some settings are overridden as required by
// the rewriter implementation.
AnalyzerOptions AnalyzerOptionsForRewrite(
    const AnalyzerOptions& analyzer_options,
    const AnalyzerOutput& analyzer_output,
    zetasql_base::SequenceNumber& fallback_sequence_number) {
  AnalyzerOptions options_for_rewrite = analyzer_options;

  // Require that rewrite substitution fragments are written in strict name
  // resolution mode so that column names are qualified. In theory, we could
  // relax this to DEFAULT at the cost of some robustness of the rewriting
  // rules. We cannot remove this line and allow the engine's selection to be
  // passed through. In that case, a rewriting rule written without column name
  // qualification might pass tests and work on most query engines but produce
  // incoherant error messages on engines that operate in strict resolution
  // mode.
  options_for_rewrite.mutable_language()->set_name_resolution_mode(
      NameResolutionMode::NAME_RESOLUTION_STRICT);

  // Rewriter fragment substitution uses named query parameters as an
  // implementation detail. We override settings that are required to enable
  // named query parameters.
  options_for_rewrite.set_allow_undeclared_parameters(false);
  options_for_rewrite.set_parameter_mode(ParameterMode::PARAMETER_NAMED);
  options_for_rewrite.set_statement_context(StatementContext::CONTEXT_DEFAULT);

  // Arenas are set to match those in <analyzer_output>, overriding any arenas
  // previously used by the AnalyzerOptions.
  options_for_rewrite.set_arena(analyzer_output.arena());
  options_for_rewrite.set_id_string_pool(analyzer_output.id_string_pool());

  // If <analyzer_options> does not have a column_id_sequence_number(), sets the
  // sequence number to <fallback_sequence_number>. Also,
  // <fallback_sequence_number> is advanced until it is greater than
  // <analyzer_output.max_column_id()>. In this case, the
  // <fallback_sequence_number> must outlive the returned options.
  if (analyzer_options.column_id_sequence_number() == nullptr) {
    // Advance the sequence number so that the column ids generated are unique
    // with respect to the AnalyzerOutput so far.
    while (fallback_sequence_number.GetNext() <
           analyzer_output.max_column_id()) {
    }
    options_for_rewrite.set_column_id_sequence_number(
        &fallback_sequence_number);
  }
  return options_for_rewrite;
}

}  // namespace

// Helper to allow mutating AnalyzerOutput.
class AnalyzerOutputMutator {
 public:
  // 'column_factory' and 'output' must outlive AnalyzerOutputMutator.
  explicit AnalyzerOutputMutator(AnalyzerOutput* output) : output_(*output) {}

  // Updates the output with the new ResolvedNode (and new max column id).
  absl::Status Update(std::unique_ptr<const ResolvedNode> node,
                      zetasql_base::SequenceNumber& column_id_seq_num) {
    output_.max_column_id_ = static_cast<int>(column_id_seq_num.GetNext() - 1);
    if (output_.statement_ != nullptr) {
      ZETASQL_RET_CHECK(node->IsStatement());
      output_.statement_.reset(node.release()->GetAs<ResolvedStatement>());
    } else {
      ZETASQL_RET_CHECK(node->IsExpression());
      output_.expr_.reset(node.release()->GetAs<ResolvedExpr>());
    }
    return absl::OkStatus();
  }

  AnalyzerOutputProperties& mutable_output_properties() {
    return output_.analyzer_output_properties_;
  }

 private:
  AnalyzerOutput& output_;
};

namespace {
absl::Status InternalRewriteResolvedAstNoConvertErrorLocation(
    const AnalyzerOptions& analyzer_options,
    absl::Span<const Rewriter* const> rewriters, Catalog* catalog,
    TypeFactory* type_factory, AnalyzerOutput& analyzer_output) {
  zetasql_base::SequenceNumber fallback_sequence_number;
  AnalyzerOptions options_for_rewrite = AnalyzerOptionsForRewrite(
      analyzer_options, analyzer_output, fallback_sequence_number);
  bool rewrite_activated = false;
  AnalyzerOutputMutator output_mutator(&analyzer_output);

  ZETASQL_VLOG(3) << "Enabled rewriters: "
          << absl::StrJoin(analyzer_options.enabled_rewrites(), " ",
                           [](std::string* s, ResolvedASTRewrite r) {
                             absl::StrAppend(s, ResolvedASTRewrite_Name(r));
                           });

  std::unique_ptr<const ResolvedNode> last_rewrite_result;
  const ResolvedNode* rewrite_input = NodeFromAnalyzerOutput(analyzer_output);

  for (const Rewriter* rewriter : rewriters) {
    if (rewriter->ShouldRewrite(analyzer_options, analyzer_output)) {
      ZETASQL_VLOG(2) << "Running rewriter " << rewriter->Name();
      ZETASQL_ASSIGN_OR_RETURN(
          last_rewrite_result,
          rewriter->Rewrite(options_for_rewrite, rewriters, *rewrite_input,
                            *catalog, *type_factory,
                            output_mutator.mutable_output_properties()));
      rewrite_input = last_rewrite_result.get();
      rewrite_activated = true;
    } else {
      ZETASQL_VLOG(3) << "Skipped rewriter " << rewriter->Name();
    }
  }

  if (rewrite_activated) {
    ZETASQL_RETURN_IF_ERROR(output_mutator.Update(
        std::move(last_rewrite_result),
        *options_for_rewrite.column_id_sequence_number()));

    // Make sure the generated ResolvedAST is valid.
    Validator validator(analyzer_options.language());
    if (analyzer_output.resolved_statement() != nullptr) {
      ZETASQL_RETURN_IF_ERROR(validator.ValidateResolvedStatement(
          analyzer_output.resolved_statement()));
    } else {
      ZETASQL_RET_CHECK(analyzer_output.resolved_expr() != nullptr);
      ZETASQL_RETURN_IF_ERROR(validator.ValidateStandaloneResolvedExpr(
          analyzer_output.resolved_expr()));
    }
  }
  return absl::OkStatus();
}

}  // namespace

// For now each rewrite that activates requires copying the AST. As we add more
// we'll likely want to improve the rewrite capactiy of the resolved AST so we
// can do this efficiently without needing unnecessary copies / allocations.
absl::Status RewriteResolvedAst(const AnalyzerOptions& analyzer_options,
                                absl::Span<const Rewriter* const> rewriters,
                                absl::string_view sql, Catalog* catalog,
                                TypeFactory* type_factory,
                                AnalyzerOutput& analyzer_output) {
  if (analyzer_output.resolved_statement() == nullptr &&
      analyzer_output.resolved_expr() == nullptr) {
    return absl::OkStatus();
  }
  return ConvertInternalErrorLocationAndAdjustErrorString(
      analyzer_options.error_message_mode(), sql,
      InternalRewriteResolvedAstNoConvertErrorLocation(
          analyzer_options, rewriters, catalog, type_factory, analyzer_output));
}
}  // namespace zetasql
