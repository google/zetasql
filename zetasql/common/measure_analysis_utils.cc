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

#include "zetasql/common/measure_analysis_utils.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/common/internal_analyzer_options.h"
#include "zetasql/common/measure_utils.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/column_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/base/case.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

// Validates the measure expression. A measure expression is an aggregate
// expression, and is hence composed of 2 logic components:
//
// 1. A list of top-level aggregate function calls. A top-level aggregate
//    function call is an aggregate function call that is not nested within a
//    subquery.
//
// 2. A scalar expression evaluated over the list of top-level aggregate
//    function calls.
//
// For example, consider the measure expression:
//
//  `SUM(X) + AVG(MIN(Y) GROUP BY Z) + (SELECT SUM(1) FROM UNNEST([1]))`.
//
// The expression has 2 top-level aggregate function calls: `SUM` and `AVG`. The
// scalar expression is composed of the two '+' function / operators over the
// top-level aggregate function calls and the subquery.
//
// The following properties must hold for a measure expression to be valid:
//
// - The expression must have at least 1 top-level aggregate function call.
// - Aggregate function calls must not have a HAVING MIN/MAX clause, or an ORDER
//   BY clause.
// - ExpressionColumns can only appear within a subtree of a top-level aggregate
//   function call.
// - User-defined entities may not be referenced (e.g. TVFs, UDFs, UDAs)
class MeasureExpressionValidator : public ResolvedASTVisitor {
 public:
  explicit MeasureExpressionValidator(absl::string_view measure_expr_str)
      : measure_expr_str_(measure_expr_str) {}

  absl::Status Validate(const ResolvedExpr& measure_expr) {
    // Copy the measure expression
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> measure_expr_copy,
                     ResolvedASTDeepCopyVisitor::Copy(&measure_expr));

    // Rewrite the copied measure expression to extract top-level aggregates.
    // The rewrite requires a ColumnFactory, so set up the helper classes here.
    auto shared_arena = std::make_shared<zetasql_base::UnsafeArena>(/*block_size=*/4096);
    IdStringPool id_string_pool(shared_arena);
    // TODO: b/350555383 - Don't hardcode the max seen column id. It's unlikely
    // that we'll see more than 100K columns in a measure expression, but we
    // should still get the number by walking the tree.
    ColumnFactory column_factory(/*max_seen_col_id=*/100000, id_string_pool,
                                 std::make_unique<zetasql_base::SequenceNumber>());
    // Extract top-level aggregates.
    std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
        extracted_aggregates;
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedExpr> measure_scalar_expr,
        ExtractTopLevelAggregates(std::move(measure_expr_copy),
                                  extracted_aggregates, column_factory));

    if (extracted_aggregates.empty()) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Measure expression must contain at least one aggregate "
                "function call that is not nested within a subquery: "
             << measure_expr_str_;
    }

    // The scalar expression must not contain any expression columns.
    std::vector<const ResolvedNode*> expression_columns;
    measure_scalar_expr->GetDescendantsWithKinds({RESOLVED_EXPRESSION_COLUMN},
                                                 &expression_columns);
    if (!expression_columns.empty()) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Expression columns in a measure expression can only be "
                "referenced within an aggregate function call: "
             << measure_expr_str_;
    }

    // Neither the top-level aggregates nor the scalar expression may contain
    // HAVING MIN/MAX clauses, ORDER BY clauses, or user-defined entities.
    ZETASQL_RETURN_IF_ERROR(measure_scalar_expr->Accept(this));
    for (const auto& extracted_aggregate : extracted_aggregates) {
      ZETASQL_RETURN_IF_ERROR(extracted_aggregate->Accept(this));
    }
    return absl::OkStatus();
  }

 protected:
  absl::Status VisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall* node) override {
    if (node->having_modifier() != nullptr) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Measure expression must not contain an aggregate function "
                "with a HAVING MIN/MAX clause: "
             << measure_expr_str_;
    }
    if (!node->order_by_item_list().empty()) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Measure expression must not contain an aggregate function "
                "with an ORDER BY clause: "
             << measure_expr_str_;
    }
    if (!node->function()->IsZetaSQLBuiltin()) {
        return zetasql_base::InvalidArgumentErrorBuilder()
             << "Measure expression must not reference user defined entities; "
                "found: "
             << node->function()->Name();
    }
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    if (!node->function()->IsZetaSQLBuiltin()) {
        return zetasql_base::InvalidArgumentErrorBuilder()
             << "Measure expression must not reference user defined entities; "
                "found: "
             << node->function()->Name();
    }
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedTVFScan(const ResolvedTVFScan* node) override {
    // Currently there are no builtin TVFs.
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Measure expression must not reference user defined entities; "
              "found: "
           << node->tvf()->Name();
  }

 private:
  absl::string_view measure_expr_str_;
};

absl::Status ValidateMeasureExpression(const LanguageOptions& language_options,
                                       absl::string_view measure_expr,
                                       const ResolvedExpr& resolved_expr) {
  MeasureExpressionValidator validator(measure_expr);
  return validator.Validate(resolved_expr);
}

// Creates a measure column from a measure expression.
// The `language_options` is used to validate the `resolved_measure_expr`.
absl::StatusOr<std::unique_ptr<SimpleColumn>> CreateMeasureColumn(
    absl::string_view table_name, absl::string_view measure_name,
    absl::string_view measure_expr, const ResolvedExpr& resolved_measure_expr,
    const LanguageOptions& language_options, TypeFactory& type_factory,
    bool is_pseudo_column = false) {
  ZETASQL_ASSIGN_OR_RETURN(const Type* measure_type,
                   type_factory.MakeMeasureType(resolved_measure_expr.type()));
  return std::make_unique<SimpleColumn>(
      table_name, measure_name, measure_type,
      /*attributes=*/
      SimpleColumn::Attributes{
          .is_pseudo_column = is_pseudo_column,
          .is_writable_column = false,
          .column_expression = std::make_optional<Column::ExpressionAttributes>(
              {Column::ExpressionAttributes::ExpressionKind::MEASURE_EXPRESSION,
               std::string(measure_expr), &resolved_measure_expr}),
      });
}

absl::Status EnsureNoDuplicateColumnNames(const Table& table) {
  absl::flat_hash_set<std::string, zetasql_base::StringViewCaseHash,
                      zetasql_base::StringViewCaseEqual>
      column_names;
  for (int i = 0; i < table.NumColumns(); i++) {
    const Column* column = table.GetColumn(i);
    if (!column_names.insert(column->Name()).second) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Measures cannot be defined on tables with duplicate column "
                "names. Table: "
             << table.Name() << ". Duplicate column name: " << column->Name();
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<const ResolvedExpr*> AnalyzeMeasureExpressionInternal(
    absl::string_view measure_expr, const Table& table, Catalog& catalog,
    TypeFactory& type_factory, AnalyzerOptions analyzer_options,
    std::unique_ptr<const AnalyzerOutput>& analyzer_output) {
  ZETASQL_RETURN_IF_ERROR(EnsureNoDuplicateColumnNames(table));
  ZETASQL_RET_CHECK(analyzer_options.expression_columns().empty());
  ZETASQL_RET_CHECK(
      !InternalAnalyzerOptions::GetLookupExpressionCallback(analyzer_options));
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_ENABLE_MEASURES);
  analyzer_options.set_allow_aggregate_standalone_expression(true);

  // Add all non-measure columns from `table` as expression columns that can
  // be looked up when resolving the measure expression.
  for (int i = 0; i < table.NumColumns(); i++) {
    const Column* column = table.GetColumn(i);
    if (column->GetType()->IsMeasureType()) {
      continue;
    }
    ZETASQL_RETURN_IF_ERROR(analyzer_options.AddExpressionColumn(column->Name(),
                                                         column->GetType()));
  }

  // Deliberately use `local_analyzer_output` instead of `analyzer_output` to
  // ensure that the caller cannot use the output unless the measure validation
  // logic succeeds.
  std::unique_ptr<const AnalyzerOutput> local_analyzer_output;
  ZETASQL_RETURN_IF_ERROR(AnalyzeExpression(measure_expr, analyzer_options, &catalog,
                                    &type_factory, &local_analyzer_output));

  // Validate the resolved measure expression.
  const ResolvedExpr* resolved_expr = local_analyzer_output->resolved_expr();
  ZETASQL_RET_CHECK(resolved_expr != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateMeasureExpression(analyzer_options.language(),
                                            measure_expr, *resolved_expr));
  analyzer_output = std::move(local_analyzer_output);
  return resolved_expr;
}

absl::StatusOr<std::vector<std::unique_ptr<const AnalyzerOutput>>>
AddMeasureColumnsToTable(SimpleTable& table,
                         std::vector<MeasureColumnDef> measures,
                         TypeFactory& type_factory, Catalog& catalog,
                         AnalyzerOptions analyzer_options) {
  std::vector<std::unique_ptr<const AnalyzerOutput>> analyzer_outputs;
  for (const MeasureColumnDef& measure_column : measures) {
    std::unique_ptr<const AnalyzerOutput> analyzer_output;
    ZETASQL_ASSIGN_OR_RETURN(const ResolvedExpr* resolved_measure_expr,
                     AnalyzeMeasureExpressionInternal(
                         measure_column.expression, table, catalog,
                         type_factory, analyzer_options, analyzer_output));
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<SimpleColumn> new_column,
        CreateMeasureColumn(table.Name(), measure_column.name,
                            measure_column.expression, *resolved_measure_expr,
                            analyzer_options.language(), type_factory,
                            measure_column.is_pseudo_column));
    ZETASQL_RETURN_IF_ERROR(table.AddColumn(new_column.release(), /*is_owned=*/true));
    analyzer_outputs.push_back(std::move(analyzer_output));
  }
  return analyzer_outputs;
}

}  // namespace zetasql
