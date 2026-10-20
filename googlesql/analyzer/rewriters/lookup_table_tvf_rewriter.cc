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

#include "googlesql/analyzer/rewriters/lookup_table_tvf_rewriter.h"

#include <memory>
#include <string>
#include <utility>

#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/analyzer_output_properties.h"
#include "googlesql/public/builtin_function.pb.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/id_string.h"
#include "googlesql/public/rewriter_interface.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_ast_builder.h"
#include "googlesql/resolved_ast/resolved_ast_rewrite_visitor.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "googlesql/resolved_ast/resolved_node.h"
#include "googlesql/resolved_ast/rewrite_utils.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {
namespace {

// Rewrites a ResolvedTVFScan for the built-in LookupTable TVF into a
// ResolvedTableScan using the catalog Table resolved in the TVFSignature.
class LookupTableTvfRewriteVisitor : public ResolvedASTRewriteVisitor {
 public:
  LookupTableTvfRewriteVisitor(IdStringPool& id_string_pool,
                               ColumnReplacementMap& column_map)
      : id_string_pool_(id_string_pool), column_map_(column_map) {}

 private:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> PostVisitResolvedTVFScan(
      std::unique_ptr<const ResolvedTVFScan> node) override {
    if (node->tvf() == nullptr || !node->tvf()->IsGoogleSQLBuiltin() ||
        node->function_call_signature() == nullptr ||
        node->function_call_signature()->context_id() != FN_TVF_LOOKUP_TABLE) {
      return node;
    }
    GOOGLESQL_RET_CHECK_NE(node->signature(), nullptr);
    const Table* table = node->signature()->result_table_schema();
    GOOGLESQL_RET_CHECK_NE(table, nullptr);

    // Columns produced by the ResolvedTVFScan have `table_name` set to the TVF
    // name ("LookupTable"). Map each column to a replacement with the same
    // column_id and `table_name` updated to the catalog table's name so that
    // all references in the Resolved AST reflect the underlying table.
    IdString table_name = id_string_pool_.Make(table->Name());
    for (const ResolvedColumn& col : node->column_list()) {
      column_map_[col] = ResolvedColumn(col.column_id(), table_name,
                                        col.name_id(), col.annotated_type());
    }

    ResolvedTVFScanBuilder tvf_scan_builder = ToBuilder(std::move(node));
    return ResolvedTableScanBuilder()
        .set_table(table)
        .set_column_list(tvf_scan_builder.release_column_list())
        .set_column_index_list(tvf_scan_builder.release_column_index_list())
        .set_alias(tvf_scan_builder.alias())
        .set_hint_list(tvf_scan_builder.release_hint_list())
        .Build();
  }

  IdStringPool& id_string_pool_;
  ColumnReplacementMap& column_map_;
};

}  // namespace

class LookupTableTvfRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, std::unique_ptr<const ResolvedNode> input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    GOOGLESQL_RET_CHECK_NE(options.id_string_pool(), nullptr);
    ColumnReplacementMap column_map;
    LookupTableTvfRewriteVisitor visitor(*options.id_string_pool(), column_map);
    GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedNode> result,
                     visitor.VisitAll(std::move(input)));
    if (!column_map.empty()) {
      return RemapSpecifiedColumns(std::move(result), column_map);
    }
    return result;
  }

  std::string Name() const override { return "LookupTableTvfRewriter"; }
};

const Rewriter* GetLookupTableTvfRewriter() {
  static const auto* const kRewriter = new LookupTableTvfRewriter;
  return kRewriter;
}

}  // namespace googlesql
