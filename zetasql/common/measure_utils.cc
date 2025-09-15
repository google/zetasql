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

#include <algorithm>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/parser/ast_node.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/column_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_rewrite_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
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

class MeasureAggregateExtractor : public ResolvedASTDeepCopyVisitor {
 public:
  static absl::StatusOr<std::unique_ptr<const ResolvedExpr>>
  ExtractTopLevelAggregates(
      std::unique_ptr<const ResolvedExpr> measure_expr,
      std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>&
          extracted_aggregates,
      ColumnFactory& column_factory) {
    ZETASQL_RET_CHECK(extracted_aggregates.empty());
    MeasureAggregateExtractor extractor(column_factory);
    ZETASQL_RETURN_IF_ERROR(measure_expr->Accept(&extractor));
    extracted_aggregates = extractor.GetExtractedAggregates();
    return extractor.ConsumeRootNode<ResolvedExpr>();
  }

  std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
  GetExtractedAggregates() {
    return std::move(extracted_aggregates_);
  }

 protected:
  absl::Status VisitResolvedSubqueryExpr(
      const ResolvedSubqueryExpr* subquery) override {
    // First, process the `in_expr` field. The `in_expr` may contain top-level
    // aggregates, so we need to extract them.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> rewritten_in_expr,
                     ProcessNode(subquery->in_expr()));
    // Other child nodes cannot contain top-level aggregates, so don't process
    // them. Copy the subquery using the deep copy visitor, replace the
    // `in_expr` and push it to the top of the stack.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedSubqueryExpr> copied_subquery,
        ResolvedASTDeepCopyVisitor::Copy(subquery));
    auto subquery_builder = ToBuilder(std::move(copied_subquery));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedSubqueryExpr> rewritten_subquery,
                     std::move(subquery_builder)
                         .set_in_expr(std::move(rewritten_in_expr))
                         .BuildMutable());
    PushNodeToStack(std::move(rewritten_subquery));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedInlineLambda(
      const ResolvedInlineLambda* lambda) override {
    // A lambda cannot contain top-level aggregates, so we don't need to
    // process it. Copy the lambda and push it to the top of the stack.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedInlineLambda> lambda_copy,
                     ResolvedASTDeepCopyVisitor::Copy(lambda));
    auto lambda_builder = ToBuilder(std::move(lambda_copy));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedInlineLambda> lambda_mutable,
                     std::move(lambda_builder).BuildMutable());
    PushNodeToStack(std::move(lambda_mutable));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall* agg) override {
    // This is a top-level aggregate function call not nested within a subquery.
    // Create a `ResolvedComputedColumn` for the aggregate function call and add
    // it to `extracted_aggregates_`.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedAggregateFunctionCall> agg_copy,
        ResolvedASTDeepCopyVisitor::Copy(agg));
    ResolvedColumn column = column_factory_.MakeCol(
        "$aggregate",
        absl::StrCat("constituent_aggregate_", ++columns_allocated_),
        agg_copy->type());
    extracted_aggregates_.push_back(
        MakeResolvedComputedColumn(column, std::move(agg_copy)));
    // Then, return a `ResolvedColumnRef` to the resolved column.
    PushNodeToStack(
        MakeResolvedColumnRef(column.type(), column, /*is_correlated=*/false));
    return absl::OkStatus();
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

// Validates the measure expression. A measure expression is (typically) an
// aggregate expression, and is hence composed of 2 logic components:
//
// 1. A (possibly empty) list of top-level aggregate function calls. A top-level
//    aggregate function call is an aggregate function call that is not nested
//    within a subquery.
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
// - Aggregate function calls must not have a HAVING MIN/MAX clause, or an ORDER
//   BY clause.
// - ExpressionColumns can only appear within a subtree of a top-level aggregate
//   function call.
// - UDFs may be referenced, but not UDAs or TVFs.
class MeasureExpressionValidator : public ResolvedASTVisitor {
 public:
  explicit MeasureExpressionValidator(absl::string_view measure_expr_str)
      : measure_expr_str_(measure_expr_str) {}

  absl::Status Validate(const ResolvedExpr& measure_expr) {
    // Copy the measure expression
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> measure_expr_copy,
                     ResolvedASTDeepCopyVisitor::Copy(&measure_expr));

    // Compute the max column id seen in the measure expression for use in the
    // ColumnFactory below.
    ZETASQL_ASSIGN_OR_RETURN(int max_column_id,
                     GetMaxColumnId(measure_expr_copy.get()));
    // Rewrite the copied measure expression to extract top-level aggregates.
    // The rewrite requires a ColumnFactory, so set up the helper classes here.
    auto shared_arena = std::make_shared<zetasql_base::UnsafeArena>(/*block_size=*/4096);
    IdStringPool id_string_pool(shared_arena);
    ColumnFactory column_factory(max_column_id, id_string_pool,
                                 std::make_unique<zetasql_base::SequenceNumber>());

    // Extract top-level aggregates.
    // Note that it is possible (but unlikely) for the measure expression to
    // have no top-level aggregates (e.g. '1+1' is a valid measure expression).
    std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
        extracted_aggregates;
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedExpr> measure_scalar_expr,
        ExtractTopLevelAggregates(std::move(measure_expr_copy),
                                  extracted_aggregates, column_factory));

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
    if (node->where_expr() != nullptr) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Measure expression must not contain an aggregate function "
                "with a WHERE filter clause: "
             << measure_expr_str_;
    }
    if (node->having_expr() != nullptr) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Measure expression must not contain an aggregate function "
                "with a HAVING filter clause: "
             << measure_expr_str_;
    }
    if (!node->function()->IsZetaSQLBuiltin()) {
        return zetasql_base::InvalidArgumentErrorBuilder()
             << "Measure expression must not reference UDA; "
                "found: "
             << node->function()->Name();
    }
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedTVFScan(const ResolvedTVFScan* node) override {
    // Currently there are no builtin TVFs.
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Measure expression must not reference TVF; "
              "found: "
           << node->tvf()->Name();
  }

 private:
  absl::string_view measure_expr_str_;
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

absl::Status ValidateScanCanEmitMeasureColumns(const ResolvedScan* scan) {
  // These are the only scan kinds that can emit measure columns. We use an
  // allow list instead of a deny list to ensure any new scan kinds don't
  // accidentally allow measure column emission.
  absl::flat_hash_set<ResolvedNodeKind> allow_listed_scan_kinds = {
      RESOLVED_TABLE_SCAN,
      RESOLVED_JOIN_SCAN,
      RESOLVED_ARRAY_SCAN,
      RESOLVED_FILTER_SCAN,
      RESOLVED_ORDER_BY_SCAN,
      RESOLVED_LIMIT_OFFSET_SCAN,
      RESOLVED_WITH_REF_SCAN,
      RESOLVED_PROJECT_SCAN,
      RESOLVED_WITH_SCAN,
      RESOLVED_STATIC_DESCRIBE_SCAN,
      RESOLVED_LOG_SCAN,
      RESOLVED_PIPE_IF_SCAN,
      RESOLVED_SUBPIPELINE_INPUT_SCAN,
      RESOLVED_ANALYTIC_SCAN,
      RESOLVED_SAMPLE_SCAN,
  };
  auto it =
      std::find_if(scan->column_list().cbegin(), scan->column_list().cend(),
                   [](const ResolvedColumn& column) {
                     return IsOrContainsMeasure(column.type());
                   });
  // If the scan does not emit any measure columns, return ok.
  if (it == scan->column_list().cend()) {
    return absl::OkStatus();
  }
  int measure_column_idx =
      static_cast<int>(std::distance(scan->column_list().begin(), it));
  ZETASQL_RET_CHECK_GE(measure_column_idx, 0);
  // Scan emits measure columns; check if it is allow listed.
  if (!std::any_of(allow_listed_scan_kinds.begin(),
                   allow_listed_scan_kinds.end(),
                   [scan](const ResolvedNodeKind& kind) {
                     return kind == scan->node_kind();
                   })) {
    return zetasql_base::InternalErrorBuilder()
           << scan->node_kind_string() << " cannot emit column "
           << it->DebugString() << " of type " << it->type()->DebugString();
  }
  // Scan is allow listed and emits measure columns. This is generally valid,
  // but we need an additional check for JOIN scans. Only INNER JOINs can
  // currently emit measure columns; outer JOIN support relies on aggregate
  // filtering which is still in-development.
  if (scan->node_kind() == RESOLVED_JOIN_SCAN) {
    const ResolvedJoinScan* join_scan = scan->GetAs<ResolvedJoinScan>();
    ZETASQL_RET_CHECK(join_scan != nullptr);
    if (join_scan->join_type() != ResolvedJoinScan::INNER) {
      return zetasql_base::InternalErrorBuilder()
             << scan->node_kind_string() << " cannot emit column "
             << it->DebugString() << " of type " << it->type()->DebugString();
    }
  }
  return absl::OkStatus();
}

absl::Status ValidateMeasureExpression(absl::string_view measure_expr,
                                       const ResolvedExpr& resolved_expr) {
  MeasureExpressionValidator validator(measure_expr);
  return validator.Validate(resolved_expr);
}

}  // namespace zetasql
