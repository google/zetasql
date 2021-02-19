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
#include <utility>
#include <vector>

#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "absl/status/status.h"

namespace zetasql {
namespace {

class UnpivotRewriterVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  UnpivotRewriterVisitor(Catalog* catalog, TypeFactory* type_factory)
      : catalog_(catalog), type_factory_(type_factory) {}

  UnpivotRewriterVisitor(const UnpivotRewriterVisitor&) = delete;
  UnpivotRewriterVisitor& operator=(const UnpivotRewriterVisitor&) = delete;

  absl::Status VisitResolvedUnpivotScan(
      const ResolvedUnpivotScan* node) override;

  const AnalyzerOptions* analyzer_options_;
  Catalog* const catalog_;
  TypeFactory* type_factory_;
};

absl::Status UnpivotRewriterVisitor::VisitResolvedUnpivotScan(
    const ResolvedUnpivotScan* node) {
  // TODO: Implement the unpivot rewrite. Have added an empty node
  // for now since not pushing a node here throws errors in presubmit.
  PushNodeToStack(MakeResolvedProjectScan({}, {}, {}));
  return absl::OkStatus();
}

}  // namespace

class UnpivotRewriter : public Rewriter {
 public:
  bool ShouldRewrite(const AnalyzerOptions& analyzer_options,
                     const AnalyzerOutput& analyzer_output) const override {
    return analyzer_options.rewrite_enabled(REWRITE_UNPIVOT) &&
           analyzer_output.analyzer_output_properties().has_unpivot;
  }

  zetasql_base::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options,
      absl::Span<const Rewriter* const> rewriters, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    UnpivotRewriterVisitor visitor(&catalog, &type_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&visitor));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedNode> result,
                     visitor.ConsumeRootNode<ResolvedStatement>());
    return result;
  }
  std::string Name() const override { return "UnpivotRewriter"; }
};

const Rewriter* GetUnpivotRewriter() {
  static const auto* const kRewriter = new UnpivotRewriter;
  return kRewriter;
}

}  // namespace zetasql
