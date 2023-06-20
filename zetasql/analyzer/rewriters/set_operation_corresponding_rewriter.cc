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

#include "zetasql/analyzer/rewriters/set_operation_corresponding_rewriter.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_builder.h"
#include "zetasql/resolved_ast/resolved_ast_rewrite_visitor.h"
#include "zetasql/resolved_ast/rewrite_utils.h"

namespace zetasql {
namespace {

bool NeedsProjectScanForByPosition(const ResolvedSetOperationItem& item) {
  return item.scan()->column_list() != item.output_column_list();
}

class SetOperationCorrespondingRewriteVisitor
    : public ResolvedASTRewriteVisitor {
 private:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedSetOperationScan(
      std::unique_ptr<const ResolvedSetOperationScan> node) override {
    if (node->column_match_mode() == ResolvedSetOperationScan::BY_POSITION) {
      return node;
    }
    ResolvedSetOperationScanBuilder builder = ToBuilder(std::move(node));
    std::vector<std::unique_ptr<const ResolvedSetOperationItem>> items =
        builder.release_input_item_list();
    for (std::unique_ptr<const ResolvedSetOperationItem>& item : items) {
      if (!NeedsProjectScanForByPosition(*item)) {
        continue;
      }
      ResolvedSetOperationItemBuilder item_builder = ToBuilder(std::move(item));
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedProjectScan> project_scan,
                       ResolvedProjectScanBuilder()
                           .set_input_scan(item_builder.release_scan())
                           .set_column_list(item_builder.output_column_list())
                           .Build());
      item_builder.set_scan(std::move(project_scan));
      ZETASQL_ASSIGN_OR_RETURN(item, std::move(item_builder).Build());
    }

    builder.set_column_match_mode(ResolvedSetOperationScan::BY_POSITION);
    builder.set_column_propagation_mode(ResolvedSetOperationScan::STRICT);

    return std::move(builder).set_input_item_list(std::move(items)).Build();
  }
};

}  // namespace

class SetOperationCorrespondingRewriter : public Rewriter {
 public:
  std::string Name() const override {
    return "SetOperationCorrespondingRewriter";
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, std::unique_ptr<const ResolvedNode> input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.id_string_pool() != nullptr);
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    SetOperationCorrespondingRewriteVisitor rewriter;
    return rewriter.VisitAll(std::move(input));
  };
};

const Rewriter* GetSetOperationCorrespondingRewriter() {
  static const auto* const kRewriter = new SetOperationCorrespondingRewriter;
  return kRewriter;
}

}  // namespace zetasql
