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

#include "zetasql/analyzer/rewriters/sql_view_inliner.h"

#include <memory>
#include <string>
#include <utility>

#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/sql_view.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

// A visitor that replaces calls to SQL view scans with the resolved query.
class SqlViewInlineVistor : public ResolvedASTDeepCopyVisitor {
 public:
  explicit SqlViewInlineVistor(ColumnFactory* column_factory)
      : column_factory_(column_factory) {}

 private:
  ColumnFactory* column_factory_;

  absl::StatusOr<bool> IsScanInlinable(const ResolvedTableScan* scan) {
    const Table* table = scan->table();
    if (table == nullptr || !table->Is<SQLView>()) {
      return false;
    }
    const SQLView* view = table->GetAs<SQLView>();
    if (!view->enable_view_inline()) {
      return false;
    }
    if (view->sql_security() != SQLView::kSecurityInvoker) {
      // The ResolvedAST has no mechanism to annotate the authentication
      // difference between nodes from inside the view vs outside the view.
      return absl::InvalidArgumentError(absl::StrCat(
          "View inlining is only supported for SQL_SECURITY_INVOKER views",
          ". View ", view->Name(), " has ",
          ResolvedCreateStatementEnums::SqlSecurity_Name(view->sql_security()),
          ", and the catalog should not report it as inlineable."));
    }
    if (scan->hint_list_size() > 0) {
      // View inlining leaves no place to hang table scan hints. It's not clear
      // that inlining a view scan with hints is even the right thing to do.
      return absl::UnimplementedError(
          "Hints are not supported on invocations of inlined views.");
    }
    return true;
  }

  absl::Status VisitResolvedTableScan(const ResolvedTableScan* node) override {
    ZETASQL_ASSIGN_OR_RETURN(bool is_inlinable, IsScanInlinable(node));
    if (is_inlinable) {
      return InlineSqlView(node, node->table()->GetAs<SQLView>());
    }
    return CopyVisitResolvedTableScan(node);
  }

  absl::Status InlineSqlView(const ResolvedTableScan* scan,
                             const SQLView* view) {
    ZETASQL_RET_CHECK_NE(column_factory_, nullptr);

    // The view definition was pre-compiled (or will be lazily compiled) and
    // owned by the catalog. The ResolvedColumns in the view definition scan
    // are not allocated by 'column_factory_' and are thus declared in a
    // differenct id space. When we copy the view definition, we need to map
    // those columns to columns that are allocated by column_factory_. There
    // are two sorts of columns that we need to concern ourselves with.
    //
    // 1) Columns that are output by the view and consumed in the scan. This is
    //    possibly a subset of the view definitions projected columns. For these
    //    columns we want to remap the column ids used in the view definition to
    //    the existing column ids that were projected by the TableScan.
    // 2) Columns that are defined within the view but are either not projected
    //    by the view definition scan or aren't consumed by the TableScan. These
    //    columns are handled inside ColumnRemappingResolvedASTDeepCopyVisitor
    //    and will get new column ids allocated by 'column_factory_'.

    const ResolvedScan* const view_def = view->view_query();
    ZETASQL_RET_CHECK(view_def != nullptr);

    // Initialize a map from the column ids in the view definition to the column
    // ids in the invoking query to remap the columns that were consumed by the
    // TableScan.
    ColumnReplacementMap column_map;
    for (int i = 0; i < scan->column_index_list_size(); ++i) {
      int column_idx = scan->column_index_list(i);
      ZETASQL_RET_CHECK_GT(view_def->column_list_size(), column_idx);
      column_map[view_def->column_list(column_idx)] = scan->column_list(i);
    }
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedScan> view_query,
                     CopyResolvedASTAndRemapColumns(*view_def, *column_factory_,
                                                    column_map));
    PushNodeToStack(std::move(view_query));
    return absl::OkStatus();
  }
};

class SqlViewScanInliner : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ColumnFactory column_factory(0, options.id_string_pool().get(),
                                 options.column_id_sequence_number());
    SqlViewInlineVistor rewriter(&column_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "SqlViewScanInliner"; }
};

}  // namespace

const Rewriter* GetSqlViewInliner() {
  static const auto* const kRewriter = new SqlViewScanInliner;
  return kRewriter;
}

}  // namespace zetasql
