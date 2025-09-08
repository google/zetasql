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

#include "zetasql/analyzer/annotation_propagator.h"

#include <memory>
#include <utility>
#include <vector>

#include "zetasql/parser/ast_node.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/annotation/collation.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

AnnotationPropagator::AnnotationPropagator(
    const AnalyzerOptions& analyzer_options, TypeFactory& type_factory)
    : analyzer_options_(analyzer_options), type_factory_(type_factory) {
  InitializeAnnotationSpecs(analyzer_options_);
}

void AnnotationPropagator::InitializeAnnotationSpecs(
    const AnalyzerOptions& analyzer_options) {
  if (!analyzer_options.language().LanguageFeatureEnabled(
          FEATURE_ANNOTATION_FRAMEWORK)) {
    return;
  }

  if (analyzer_options.language().LanguageFeatureEnabled(
          FEATURE_COLLATION_SUPPORT)) {
    owned_annotation_specs_.push_back(std::make_unique<CollationAnnotation>());
  }

  // Copy ZetaSQL annotation specs to combined_annotation_specs_
  for (const auto& annotation_spec : owned_annotation_specs_) {
    annotation_specs_.push_back(annotation_spec.get());
  }

  // Copy Engine Specific annotation specs to combined_annotation_specs_
  for (const auto& annotation_spec : analyzer_options.get_annotation_specs()) {
    annotation_specs_.push_back(annotation_spec);
  }
}

static absl::Status CheckAndPropagateAnnotationsImpl(
    const ResolvedNode* resolved_node,
    const std::vector<AnnotationSpec*>* annotation_specs,
    AnnotationMap* annotation_map) {
  for (AnnotationSpec* annotation_spec : *annotation_specs) {
    switch (resolved_node->node_kind()) {
      case RESOLVED_COLUMN_REF: {
        auto* column_ref = resolved_node->GetAs<ResolvedColumnRef>();
        ZETASQL_RETURN_IF_ERROR(annotation_spec->CheckAndPropagateForColumnRef(
            *column_ref, annotation_map));
      } break;
      case RESOLVED_GET_STRUCT_FIELD: {
        auto* get_struct_field = resolved_node->GetAs<ResolvedGetStructField>();
        ZETASQL_RETURN_IF_ERROR(annotation_spec->CheckAndPropagateForGetStructField(
            *get_struct_field, annotation_map));
      } break;
      case RESOLVED_MAKE_STRUCT: {
        ZETASQL_RET_CHECK(annotation_map->IsStructMap());
        auto* make_struct = resolved_node->GetAs<ResolvedMakeStruct>();
        ZETASQL_RETURN_IF_ERROR(annotation_spec->CheckAndPropagateForMakeStruct(
            *make_struct, annotation_map->AsStructMap()));
      } break;
      case RESOLVED_FUNCTION_CALL: {
        auto* function_call = resolved_node->GetAs<ResolvedFunctionCall>();
        ZETASQL_RETURN_IF_ERROR(annotation_spec->CheckAndPropagateForFunctionCallBase(
            *function_call, annotation_map));
      } break;
      case RESOLVED_AGGREGATE_FUNCTION_CALL: {
        auto* function_call =
            resolved_node->GetAs<ResolvedAggregateFunctionCall>();
        ZETASQL_RETURN_IF_ERROR(annotation_spec->CheckAndPropagateForFunctionCallBase(
            *function_call, annotation_map));
      } break;
      case RESOLVED_ANALYTIC_FUNCTION_CALL: {
        auto* function_call =
            resolved_node->GetAs<ResolvedAnalyticFunctionCall>();
        ZETASQL_RETURN_IF_ERROR(annotation_spec->CheckAndPropagateForFunctionCallBase(
            *function_call, annotation_map));
      } break;
      case RESOLVED_CAST: {
        auto* cast = resolved_node->GetAs<ResolvedCast>();
        ZETASQL_RETURN_IF_ERROR(
            annotation_spec->CheckAndPropagateForCast(*cast, annotation_map));
      } break;
      case RESOLVED_SUBQUERY_EXPR: {
        auto* subquery_expr = resolved_node->GetAs<ResolvedSubqueryExpr>();
        ZETASQL_RETURN_IF_ERROR(annotation_spec->CheckAndPropagateForSubqueryExpr(
            *subquery_expr, annotation_map));
      } break;
      default:
        break;
    }
  }
  return absl::OkStatus();
}

absl::Status AnnotationPropagator::CheckAndPropagateAnnotations(
    const ASTNode* error_node, ResolvedNode* resolved_node) {
  if (!analyzer_options_.language().LanguageFeatureEnabled(
          FEATURE_ANNOTATION_FRAMEWORK)) {
    return absl::OkStatus();
  }
  if (resolved_node->IsExpression()) {
    auto* expr = resolved_node->GetAs<ResolvedExpr>();
    // TODO: support annotation for Proto and ExtendedType.
    if (expr->type()->IsProto() || expr->type()->IsExtendedType()) {
      return absl::OkStatus();
    }
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(expr->type());
    absl::Status status = CheckAndPropagateAnnotationsImpl(
        resolved_node, &annotation_specs_, annotation_map.get());
    if (!status.ok() && error_node != nullptr) {
      return MakeSqlErrorAt(error_node) << status.message();
    }
    // It is possible that annotation_map is empty after all the propagation,
    // set type_annotation_map to nullptr in this case.
    if (annotation_map->Empty()) {
      expr->set_type_annotation_map(nullptr);
    } else {
      ZETASQL_ASSIGN_OR_RETURN(const AnnotationMap* type_factory_owned_map,
                       type_factory_.TakeOwnership(std::move(annotation_map)));
      expr->set_type_annotation_map(type_factory_owned_map);
    }
  } else if (resolved_node->Is<ResolvedSetOperationScan>()) {
    auto* set_operation_scan = resolved_node->GetAs<ResolvedSetOperationScan>();
    // Owns the pointers.
    std::vector<std::unique_ptr<AnnotationMap>> annotation_maps;
    std::vector<AnnotationMap*> annotation_map_ptrs;
    for (const ResolvedColumn& column : set_operation_scan->column_list()) {
      if (column.type()->IsProto() || column.type()->IsExtendedType()) {
        annotation_maps.push_back(nullptr);
        annotation_map_ptrs.push_back(nullptr);
      } else {
        annotation_maps.push_back(AnnotationMap::Create(column.type()));
        annotation_map_ptrs.push_back(annotation_maps.back().get());
      }
    }

    // Check and propagate the annotations for set operations for every
    // AnnotationSpec supported by the resolver.
    for (AnnotationSpec* annotation_spec : annotation_specs_) {
      absl::Status status =
          annotation_spec->CheckAndPropagateForSetOperationScan(
              *set_operation_scan, annotation_map_ptrs);
      if (!status.ok() && error_node != nullptr) {
        return MakeSqlErrorAt(error_node) << status.message();
      }
    }

    std::vector<ResolvedColumn> annotated_column_list;
    annotated_column_list.reserve(set_operation_scan->column_list_size());
    for (int i = 0; i < set_operation_scan->column_list_size(); i++) {
      const ResolvedColumn& original_column =
          set_operation_scan->column_list(i);
      if (annotation_maps[i] == nullptr || annotation_maps[i]->Empty()) {
        annotated_column_list.push_back(original_column);
        continue;
      }

      ZETASQL_ASSIGN_OR_RETURN(
          const AnnotationMap* column_annotation_map,
          type_factory_.TakeOwnership(std::move(annotation_maps[i])));

      annotated_column_list.push_back(ResolvedColumn(
          original_column.column_id(), original_column.table_name_id(),
          original_column.name_id(),
          AnnotatedType(original_column.type(), column_annotation_map)));
    }
    set_operation_scan->set_column_list(std::move(annotated_column_list));
  } else if (resolved_node->Is<ResolvedRecursiveScan>()) {
    auto* recursive_scan = resolved_node->GetAs<ResolvedRecursiveScan>();
    ZETASQL_RETURN_IF_ERROR(CheckAndPropagateAnnotationsForRecursiveScan(recursive_scan,
                                                                 error_node));
  }
  return absl::OkStatus();
}

absl::Status AnnotationPropagator::CheckAndPropagateAnnotationsForRecursiveScan(
    ResolvedRecursiveScan* recursive_scan, const ASTNode* error_node) {
  if (!analyzer_options_.language().LanguageFeatureEnabled(
          FEATURE_COLLATION_IN_WITH_RECURSIVE)) {
    // Disable collation propagation through ResolvedRecursiveScan.
    for (const ResolvedSetOperationItem* item :
         {recursive_scan->non_recursive_term(),
          recursive_scan->recursive_term()}) {
      // The recursive_term can be null since we try to propagate annotations
      // earlier after non_recursive_term is resolved.
      if (item == nullptr) {
        continue;
      }
      for (const ResolvedColumn& output_column : item->output_column_list()) {
        if (CollationAnnotation::ExistsIn(
                output_column.type_annotation_map())) {
          return MakeSqlErrorAt(error_node)
                 << "Collation is not supported in recursive queries";
        }
      }
    }
  }
  // Owns the pointers.
  std::vector<std::unique_ptr<AnnotationMap>> annotation_maps;
  std::vector<AnnotationMap*> annotation_map_ptrs;
  for (const ResolvedColumn& column : recursive_scan->column_list()) {
    // Insert place holder for proto and extended type. We don't propagate
    // annotations for them.
    if (column.type()->IsProto() || column.type()->IsExtendedType()) {
      annotation_maps.push_back(nullptr);
      annotation_map_ptrs.push_back(nullptr);
    } else {
      annotation_maps.push_back(AnnotationMap::Create(column.type()));
      annotation_map_ptrs.push_back(annotation_maps.back().get());
    }
  }

  // Check and propagate the annotations for recursive scan for every
  // AnnotationSpec supported by the resolver.
  for (AnnotationSpec* annotation_spec : annotation_specs_) {
    absl::Status status = annotation_spec->CheckAndPropagateForRecursiveScan(
        *recursive_scan, annotation_map_ptrs);
    if (!status.ok() && error_node != nullptr) {
      return MakeSqlErrorAt(error_node) << status.message();
    }
  }

  std::vector<ResolvedColumn> annotated_column_list;
  annotated_column_list.reserve(recursive_scan->column_list_size());
  for (int i = 0; i < recursive_scan->column_list_size(); i++) {
    const ResolvedColumn& original_column = recursive_scan->column_list(i);
    if (annotation_maps[i] == nullptr || annotation_maps[i]->Empty()) {
      annotated_column_list.push_back(original_column);
      continue;
    }

    ZETASQL_ASSIGN_OR_RETURN(
        const AnnotationMap* column_annotation_map,
        type_factory_.TakeOwnership(std::move(annotation_maps[i])));

    annotated_column_list.push_back(ResolvedColumn(
        original_column.column_id(), original_column.table_name_id(),
        original_column.name_id(),
        AnnotatedType(original_column.type(), column_annotation_map)));
  }
  recursive_scan->set_column_list(std::move(annotated_column_list));
  return absl::OkStatus();
}

}  // namespace zetasql
