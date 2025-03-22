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

#include "zetasql/public/measure_expression.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/validator.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

// Validates the measure expression.
// A valid measure expression must be an aggregate expression with least 1
// aggregate function call, and column references cannot occur outside an
// aggregate function call.
class MeasureExpressionValidator : public ResolvedASTVisitor {
 public:
  explicit MeasureExpressionValidator(absl::string_view measure_expr)
      : measure_expr_(measure_expr) {}

  absl::Status Validate(const ResolvedExpr& resolved_expr) {
    ZETASQL_RETURN_IF_ERROR(resolved_expr.Accept(this));
    if (!has_aggregate_function_call_) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Measure expression must be an aggregate function call or a "
                "scalar function of aggregate function calls: "
             << measure_expr_;
    }
    return absl::OkStatus();
  }

 protected:
  absl::Status VisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall* node) override {
    has_aggregate_function_call_ = true;
    return absl::OkStatus();
  }

  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    for (int i = 0; i < node->argument_list_size(); ++i) {
      // The argument must be a valid measure expression or a constant
      // literal.
      ZETASQL_RETURN_IF_ERROR(node->argument_list(i)->Accept(this));
    }
    return absl::OkStatus();
  }

  absl::Status VisitResolvedLiteral(const ResolvedLiteral* node) override {
    return absl::OkStatus();
  }

  absl::Status VisitResolvedCast(const ResolvedCast* node) override {
    return node->expr()->Accept(this);
  }

  absl::Status DefaultVisit(const ResolvedNode* node) override {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Measure expression must be an aggregate function call or a "
              "scalar function of aggregate function calls: "
           << measure_expr_ << " " << node->node_kind_string();
  }

 private:
  absl::string_view measure_expr_;
  bool has_aggregate_function_call_ = false;
};

absl::Status ValidateMeasureExpression(const LanguageOptions& language_options,
                                       absl::string_view measure_expr,
                                       const ResolvedExpr& resolved_expr) {
  MeasureExpressionValidator validator(measure_expr);
  return validator.Validate(resolved_expr);
}

// Analyzes the expression of a measure in a table and return the resolved
// expression.
// The returned resolved expression is used to create a measure type catalog
// column.
// The `measure_expression` is an aggregate expression that defines the measure,
// see (broken link) for details.
// The `analyzer_options`, the options to analyze the expression. it must have:
// ** enable `FEATURE_V_1_4_ENABLE_MEASURES`
// ** set `allow_aggregate_standalone_expression` to true.
// ** callbacks to resolve the columns in the expression.
// The `catalog` is used to resolve the function calls in the measure
// expression.
// The `type_factory` is used to create the measure type.
// The `analyzer_output`: the output analyzer result, owned by the caller.
absl::StatusOr<const ResolvedExpr*> AnalyzeMeasureExpression(
    absl::string_view measure_expr, const AnalyzerOptions& analyzer_options,
    Catalog& catalog, TypeFactory& type_factory,
    std::unique_ptr<const AnalyzerOutput>& analyzer_output) {
  ZETASQL_RET_CHECK(analyzer_options.language().LanguageFeatureEnabled(
      FEATURE_V_1_4_ENABLE_MEASURES));
  ZETASQL_RET_CHECK(analyzer_options.allow_aggregate_standalone_expression());
  ZETASQL_RETURN_IF_ERROR(AnalyzeExpression(measure_expr, analyzer_options, &catalog,
                                    &type_factory, &analyzer_output));
  const ResolvedExpr* resolved_expr = analyzer_output->resolved_expr();
  ZETASQL_RET_CHECK(resolved_expr != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateMeasureExpression(analyzer_options.language(),
                                            measure_expr, *resolved_expr));

  return resolved_expr;
}

// Creates a measure column from a measure expression.
// The `language_options` is used to validate the `resolved_measure_expr`.
absl::StatusOr<std::unique_ptr<SimpleColumn>> CreateMeasureColumn(
    absl::string_view table_name, absl::string_view measure_name,
    absl::string_view measure_expr, const ResolvedExpr& resolved_measure_expr,
    const LanguageOptions& language_options, TypeFactory& type_factory) {
  ZETASQL_RETURN_IF_ERROR(ValidateMeasureExpression(language_options, measure_expr,
                                            resolved_measure_expr));
  ZETASQL_ASSIGN_OR_RETURN(const Type* measure_type,
                   type_factory.MakeMeasureType(resolved_measure_expr.type()));
  return std::make_unique<SimpleColumn>(
      table_name, measure_name, measure_type,
      /*attributes=*/
      SimpleColumn::Attributes{
          .is_writable_column = false,
          .column_expression = std::make_optional<Column::ExpressionAttributes>(
              {Column::ExpressionAttributes::ExpressionKind::MEASURE_EXPRESSION,
               std::string(measure_expr), &resolved_measure_expr}),
      });
}

}  // namespace

absl::StatusOr<std::vector<std::unique_ptr<const AnalyzerOutput>>>
AddMeasureColumnsToTable(SimpleTable& table,
                         std::vector<MeasureColumnDef> measures,
                         TypeFactory& type_factory, Catalog& catalog,
                         AnalyzerOptions& analyzer_options) {
  ZETASQL_RET_CHECK(!table.AllowDuplicateColumnNames());
  std::vector<std::unique_ptr<const AnalyzerOutput>> analyzer_outputs;
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_V_1_4_ENABLE_MEASURES);
  analyzer_options.set_allow_aggregate_standalone_expression(true);
  analyzer_options.SetLookupExpressionColumnCallback(
      [&table](const std::string& column_name,
               const Type** type) -> absl::Status {
        const Column* column = table.FindColumnByName(column_name);
        if (column == nullptr) {
          return absl::NotFoundError(
              absl::StrCat("Column not found: ", column_name));
        } else if (column->GetType()->IsMeasureType()) {
          return absl::InvalidArgumentError(absl::StrCat(
              "Measure cannot reference another measure-type column: ",
              column_name));
        }
        *type = column->GetType();
        return absl::OkStatus();
      });
  for (const MeasureColumnDef& measure_column : measures) {
    std::unique_ptr<const AnalyzerOutput> analyzer_output;
    ZETASQL_ASSIGN_OR_RETURN(const ResolvedExpr* resolved_measure_expr,
                     zetasql::AnalyzeMeasureExpression(
                         measure_column.expression, analyzer_options, catalog,
                         type_factory, analyzer_output));
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<SimpleColumn> new_column,
        zetasql::CreateMeasureColumn(
            table.Name(), measure_column.name, measure_column.expression,
            *resolved_measure_expr, analyzer_options.language(), type_factory));
    ZETASQL_RETURN_IF_ERROR(table.AddColumn(new_column.release(), /*is_owned=*/true));
    analyzer_outputs.push_back(std::move(analyzer_output));
  }
  return analyzer_outputs;
}

}  // namespace zetasql
