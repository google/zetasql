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

#include <memory>

#include "zetasql/analyzer/rewriters/flatten_rewriter.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "zetasql/resolved_ast/validator.h"
#include "absl/status/status.h"

namespace zetasql {

// Helper to allow mutating AnalyzerOutput.
class AnalyzerOutputMutator {
 public:
  // 'column_factory' and 'output' must outlive AnalyzerOutputMutator.
  AnalyzerOutputMutator(const ColumnFactory* column_factory,
                        AnalyzerOutput* output)
      : column_factory_(*column_factory),
        output_(*output) {}

  // Updates the output with the new ResolvedStatement (and new max column id).
  void Update(std::unique_ptr<const ResolvedStatement> resolved_statement) {
    output_.max_column_id_ = column_factory_.max_column_id();
    output_.statement_ = std::move(resolved_statement);
  }

  AnalyzerOutputProperties& mutable_output_properties() {
    return output_.analyzer_output_properties_;
  }

 private:
  const ColumnFactory& column_factory_;
  AnalyzerOutput& output_;
};

// For now each rewrite that activates requires copying the AST. As we add more
// we'll likely want to improve the rewrite capactiy of the resolved AST so we
// can do this efficiently without needing unnecessary copies / allocations.
absl::Status RewriteResolvedAst(
    const AnalyzerOptions& analyzer_options, Catalog* catalog,
    TypeFactory* type_factory,
    AnalyzerOutput& analyzer_output) {
  if (analyzer_output.resolved_statement() == nullptr) {
    return absl::OkStatus();
  }

  ColumnFactory column_factory(analyzer_output.max_column_id(),
                               analyzer_options.column_id_sequence_number());
  bool rewrite_activated = false;
  AnalyzerOutputMutator output_mutator(&column_factory, &analyzer_output);

  if (analyzer_output.analyzer_output_properties().has_flatten &&
      analyzer_options.rewrite_enabled(REWRITE_FLATTEN)) {
    rewrite_activated = true;
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedStatement> result,
        RewriteResolvedFlatten(*analyzer_output.resolved_statement(),
                               column_factory));
    output_mutator.Update(std::move(result));
  }

  if (rewrite_activated) {
    // Make sure the generated ResolvedAST is valid.
    Validator validator;
    ZETASQL_RETURN_IF_ERROR(validator.ValidateResolvedStatement(
        analyzer_output.resolved_statement()));
  }
  return absl::OkStatus();
}

}  // namespace zetasql
