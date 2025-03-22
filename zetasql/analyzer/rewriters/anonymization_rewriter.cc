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

#include "zetasql/analyzer/rewriters/anonymization_rewriter.h"

#include <memory>
#include <string>

#include "zetasql/analyzer/rewriters/anonymization_helper.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/rewriter_interface.h"
#include "zetasql/resolved_ast/column_factory.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/status/statusor.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

class AnonymizationRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.AllArenasAreInitialized());
    ColumnFactory column_factory(0, options.id_string_pool().get(),
                                 options.column_id_sequence_number());
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedNode> node,
        RewriteHelper(input, options, column_factory, catalog, type_factory));
    return node;
  }

  std::string Name() const override { return "AnonymizationRewriter"; }

  // This rewriter is provided an unfiltered Catalog by the ZetaSQL analyzer,
  // and so filtering must be done by the rewriter itself to ensure that
  // non-builtin Catalog objects are not referenced.
  // TODO: b/388929260 - Determine if filtering is actually required, and either
  // apply it or justify here why it's not required.
  bool ProvideUnfilteredCatalogToBuiltinRewriter() const override {
    return true;
  }
};

absl::StatusOr<RewriteForAnonymizationOutput> RewriteForAnonymization(
    const ResolvedNode& query, Catalog* catalog, TypeFactory* type_factory,
    const AnalyzerOptions& analyzer_options, ColumnFactory& column_factory) {
  RewriteForAnonymizationOutput result;
  ZETASQL_ASSIGN_OR_RETURN(result.node,
                   RewriteHelper(query, analyzer_options, column_factory,
                                 *catalog, *type_factory));
  return result;
}

const Rewriter* GetAnonymizationRewriter() {
  static const Rewriter* kRewriter = new AnonymizationRewriter;
  return kRewriter;
}

}  // namespace zetasql
