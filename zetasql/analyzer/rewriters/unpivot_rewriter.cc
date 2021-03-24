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
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/status/status.h"

namespace zetasql {
namespace {

// A visitor that rewrites ResolvedUnpivotScan to a ArrayScan with structs.
// If table1 contains 4 columns, inputcol_1, inputcol_2, inputcol_3, inputcol_4,
// then the query:
// SELECT *
// FROM table1
//   UNPIVOT (value_col FOR label_col IN ( input_col1 AS "label1",
//                                       input_col2 as "label2" ) )
//
// Gets rewritten to:
// SELECT input_col3, input_col4, value_col, label_col
// FROM table1
//      CROSS JOIN UNNEST (
//            [ struct ( input_col1 AS value_col, 'label1' AS label_col ) ,
//              struct ( input_col2 AS value_col, 'label2' AS label_col ) ] ) ;
class UnpivotRewriterVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  UnpivotRewriterVisitor(const AnalyzerOptions* analyzer_options,
                         Catalog* catalog, TypeFactory* type_factory)
      : analyzer_options_(analyzer_options),
        catalog_(catalog),
        type_factory_(type_factory) {}

  UnpivotRewriterVisitor(const UnpivotRewriterVisitor&) = delete;
  UnpivotRewriterVisitor& operator=(const UnpivotRewriterVisitor&) = delete;

 private:
  absl::Status VisitResolvedUnpivotScan(
      const ResolvedUnpivotScan* node) override;

  // Creates an ArrayScan from struct elements which outputs a row for each
  // struct element into the new element_column of the ArrayScan.
  // The vector of struct elements is created with the elements from IN clause,
  // where the struct_type (also passed as the param) is created from the new
  // unpivot value and label columns.
  // Example, UNPIVOT((a, b) for c in ((w , x) , (y , z)))
  // struct_type : STRUCT <a type, b type, c type>
  // vector of struct elements :
  // { <w , x, label_list[0]>, <y , z , label_list[1]> }
  zetasql_base::StatusOr<std::unique_ptr<ResolvedArrayScan>>
  CreateArrayScanWithStructElements(const ResolvedUnpivotScan* node,
                                    const StructType** struct_type);

  const AnalyzerOptions* analyzer_options_;
  Catalog* const catalog_;
  TypeFactory* type_factory_;
  // id used to assign a sequence number to element_column of ArrayScan of
  // unpivot rewrite tree. This helps to identify the column in the case of
  // multiple unpivots in the query (while debugging).
  int unpivot_array_sequence_id_ = 0;
};

absl::Status UnpivotRewriterVisitor::VisitResolvedUnpivotScan(
    const ResolvedUnpivotScan* node) {
  const StructType* struct_type;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedArrayScan> struct_elements_array_scan,
      CreateArrayScanWithStructElements(node, &struct_type));

  // Create a ResolvedProjectScan that gets the individual struct fields from
  // the element_column of the ArrayScan and puts their values into the new
  // unpivot value and name columns, in that order.
  std::vector<std::unique_ptr<ResolvedComputedColumn>> expr_list;
  ResolvedColumn unnest_column = struct_elements_array_scan->element_column();
  for (int i = 0; i <= node->value_column_list_size(); ++i) {
    std::unique_ptr<ResolvedExpr> column_expr = MakeResolvedGetStructField(
        struct_type->field(i).type,
        MakeResolvedColumnRef(unnest_column.type(), unnest_column,
                              /*is_correlated=*/false),
        i);
    expr_list.push_back(MakeResolvedComputedColumn(
        i < node->value_column_list_size() ? node->value_column_list(i)
                                           : node->label_column(),
        std::move(column_expr)));
  }

  PushNodeToStack(
      MakeResolvedProjectScan(node->column_list(), std::move(expr_list),
                              std::move(struct_elements_array_scan)));

  return absl::OkStatus();
}

zetasql_base::StatusOr<std::unique_ptr<ResolvedArrayScan>>
UnpivotRewriterVisitor::CreateArrayScanWithStructElements(
    const ResolvedUnpivotScan* node, const StructType** struct_type) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedScan> input_scan,
                   ProcessNode(node->input_scan()));

  // Struct type is created using names and datatypes from the unpivot value
  // columns and label column, in that order.
  std::vector<StructField> struct_fields;
  for (const ResolvedColumn& col : node->value_column_list()) {
    struct_fields.emplace_back(col.name(), col.type());
  }
  struct_fields.emplace_back(node->label_column().name(),
                             node->label_column().type());
  ZETASQL_RETURN_IF_ERROR(type_factory_->MakeStructType(struct_fields, struct_type));

  // Columns groups from the unpivot IN clause are used to create struct
  // expressions. A new struct is created of the struct_type using these struct
  // expressions.
  std::vector<std::unique_ptr<const ResolvedExpr>> struct_elements_list;
  for (int column_index = 0; column_index < node->unpivot_arg_list_size();
       ++column_index) {
    const std::unique_ptr<const ResolvedUnpivotArg>& in_col_list =
        node->unpivot_arg_list().at(column_index);
    std::vector<std::unique_ptr<const ResolvedExpr>> struct_expressions_list;
    for (const std::unique_ptr<const ResolvedColumnRef>& in_col :
         in_col_list->column_list()) {
      struct_expressions_list.push_back(MakeResolvedColumnRef(
          in_col->type(), in_col->column(), /*is_correlated=*/false));
    }
    struct_expressions_list.push_back(MakeResolvedLiteral(
        node->label_column().type(), node->label_list(column_index)->value(),
        /*has_explicit_type=*/true));
    struct_elements_list.push_back(MakeResolvedMakeStruct(
        *struct_type, std::move(struct_expressions_list)));
  }

  const ArrayType* struct_array_type;
  ZETASQL_RETURN_IF_ERROR(
      type_factory_->MakeArrayType(*struct_type, &struct_array_type));

  // The make_array_function takes struct elements and constructs an array for
  // them.
  const Function* make_array_func;
  ZETASQL_RET_CHECK_OK(catalog_->FindFunction({"$make_array"}, &make_array_func))
      << "UNPIVOT is not supported since the engine does not support "
         "make_array function";
  ZETASQL_RET_CHECK_NE(make_array_func, nullptr);
  FunctionArgumentType function_arg(
      *struct_type, FunctionArgumentType::REPEATED,
      static_cast<int>(struct_elements_list.size()));
  ZETASQL_RET_CHECK_EQ(make_array_func->signatures().size(), 1);
  FunctionSignature signature(struct_array_type, {function_arg},
                              make_array_func->GetSignature(0)->context_id());
  signature.SetConcreteResultType(struct_array_type);
  std::unique_ptr<const ResolvedExpr> resolved_function_call =
      MakeResolvedFunctionCall(struct_array_type, make_array_func, signature,
                               std::move(struct_elements_list),
                               ResolvedFunctionCallBase::DEFAULT_ERROR_MODE);

  // Construct element column for array scan that'll hold the newly generated
  // value of struct type.
  const IdString& array_id_string = analyzer_options_->id_string_pool()->Make(
      absl::StrCat("unpivot_array_", unpivot_array_sequence_id_++));
  const IdString& unnest_id_string =
      analyzer_options_->id_string_pool()->Make("unpivot_unnest");
  const ResolvedColumn unnest_column(
      static_cast<int>(
          analyzer_options_->column_id_sequence_number()->GetNext()),
      array_id_string, unnest_id_string, *struct_type);

  std::vector<ResolvedColumn> output_column_list = input_scan->column_list();
  output_column_list.push_back(unnest_column);
  return MakeResolvedArrayScan(output_column_list, std::move(input_scan),
                               std::move(resolved_function_call), unnest_column,
                               /*array_offset_column=*/nullptr,
                               /*join_expr=*/nullptr, /*is_outer=*/false);
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
    UnpivotRewriterVisitor visitor(&options, &catalog, &type_factory);
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
