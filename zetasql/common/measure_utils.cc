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


#include "zetasql/common/measure_utils.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/analyzer/name_scope.h"
#include "zetasql/parser/ast_node.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/column_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_rewrite_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

class MeasureAggregateExtractor : public ResolvedASTRewriteVisitor {
 public:
  static absl::StatusOr<std::unique_ptr<const ResolvedExpr>>
  ExtractTopLevelAggregates(
      std::unique_ptr<const ResolvedExpr> measure_expr,
      std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>&
          extracted_aggregates,
      ColumnFactory& column_factory) {
    ZETASQL_RET_CHECK(extracted_aggregates.empty());
    MeasureAggregateExtractor extractor(column_factory);
    ZETASQL_ASSIGN_OR_RETURN(measure_expr,
                     extractor.VisitAll<ResolvedExpr>(std::move(measure_expr)));
    extracted_aggregates = extractor.GetExtractedAggregates();
    return measure_expr;
  }

  std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
  GetExtractedAggregates() {
    return std::move(extracted_aggregates_);
  }

 protected:
  absl::Status PreVisitResolvedSubqueryExpr(
      const ResolvedSubqueryExpr& subquery) override {
    seen_subquery_exprs_.insert(&subquery);
    return absl::OkStatus();
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedSubqueryExpr(
      std::unique_ptr<const ResolvedSubqueryExpr> subquery_expr) override {
    ZETASQL_RET_CHECK_EQ(seen_subquery_exprs_.erase(subquery_expr.get()), 1);
    return subquery_expr;
  }

  absl::Status PreVisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall& agg) override {
    seen_aggregate_function_calls_.insert(&agg);
    return absl::OkStatus();
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedAggregateFunctionCall(
      std::unique_ptr<const ResolvedAggregateFunctionCall> agg) override {
    ZETASQL_RET_CHECK_EQ(seen_aggregate_function_calls_.erase(agg.get()), 1);
    if (!seen_aggregate_function_calls_.empty() ||
        !seen_subquery_exprs_.empty()) {
      return agg;
    }

    // This is a top-level aggregate function call not nested within a subquery.

    // Create a `ResolvedComputedColumn` for the aggregate function call and add
    // it to `extracted_aggregates_`.
    ResolvedColumn column = column_factory_.MakeCol(
        "$aggregate",
        absl::StrCat("constituent_aggregate_", ++columns_allocated_),
        agg->type());
    extracted_aggregates_.push_back(
        MakeResolvedComputedColumn(column, std::move(agg)));

    // Then, return a `ResolvedColumnRef` to the resolved column.
    return MakeResolvedColumnRef(column.type(), column,
                                 /*is_correlated=*/false);
  }

 private:
  explicit MeasureAggregateExtractor(ColumnFactory& column_factory)
      : column_factory_(column_factory) {}
  MeasureAggregateExtractor(const MeasureAggregateExtractor&) = delete;
  MeasureAggregateExtractor& operator=(const MeasureAggregateExtractor&) =
      delete;

  int columns_allocated_ = 0;
  absl::flat_hash_set<const ResolvedAggregateFunctionCall*>
      seen_aggregate_function_calls_;
  ColumnFactory& column_factory_;
  std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
      extracted_aggregates_;
  absl::flat_hash_set<const ResolvedSubqueryExpr*> seen_subquery_exprs_;
};

}  // namespace

bool IsOrContainsMeasure(const Type* type) {
  if (type->IsMeasureType()) {
    return true;
  }
  if (type->IsArray() && IsOrContainsMeasure(type->AsArray()->element_type())) {
    return true;
  }
  if (type->IsStruct()) {
    for (const StructType::StructField& field : type->AsStruct()->fields()) {
      if (IsOrContainsMeasure(field.type)) {
        return true;
      }
    }
  }
  if (type->IsMap()) {
    if (IsOrContainsMeasure(type->AsMap()->key_type())) {
      return true;
    }
    if (IsOrContainsMeasure(type->AsMap()->value_type())) {
      return true;
    }
  }
  return false;
}

absl::Status EnsureNoMeasuresInNameList(const NameListPtr name_list,
                                        const ASTNode* location,
                                        absl::string_view operation_name,
                                        ProductMode product_mode) {
  // Check regular columns.
  for (int i = 0; i < name_list->columns().size(); ++i) {
    const NamedColumn& named_column = name_list->columns()[i];
    const Type* col_type = named_column.column().type();
    if (IsOrContainsMeasure(col_type)) {
      std::string measure_column_name =
          IsInternalAlias(named_column.name())
              ? absl::StrCat("at position ", i + 1)
              : named_column.name().ToString();
      return MakeSqlErrorAt(location)
             << "Column " << measure_column_name << " of type "
             << col_type->ShortTypeName(product_mode)
             << " cannot propagate through " << operation_name
             << (col_type->IsMeasureType()
                     ? ""
                     : " because it contains a MEASURE type");
      ;
    }
  }
  // Check pseudo columns.
  std::vector<NamedColumn> pseudo_columns = name_list->GetNamedPseudoColumns();
  for (int i = 0; i < pseudo_columns.size(); ++i) {
    const Type* col_type = pseudo_columns[i].column().type();
    if (IsOrContainsMeasure(col_type)) {
      return MakeSqlErrorAt(location)
             << "Pseudo-column " << pseudo_columns[i].name() << " of type "
             << col_type->ShortTypeName(product_mode)
             << " cannot propagate through " << operation_name
             << (col_type->IsMeasureType()
                     ? ""
                     : " because it contains a MEASURE type");
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<const ResolvedExpr>> ExtractTopLevelAggregates(
    std::unique_ptr<const ResolvedExpr> measure_expr,
    std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>&
        extracted_aggregates,
    ColumnFactory& column_factory) {
  return MeasureAggregateExtractor::ExtractTopLevelAggregates(
      std::move(measure_expr), extracted_aggregates, column_factory);
}

}  // namespace zetasql
