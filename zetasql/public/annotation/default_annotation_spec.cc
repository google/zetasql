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

#include "zetasql/public/annotation/default_annotation_spec.h"

#include <string>
#include <vector>

#include "zetasql/public/types/annotation.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"

namespace zetasql {

absl::Status DefaultAnnotationSpec::CheckAndPropagateForFunctionCallBase(
    const ResolvedFunctionCallBase& function_call,
    AnnotationMap* result_annotation_map) {
  return absl::OkStatus();
}

absl::Status DefaultAnnotationSpec::CheckAndPropagateForColumnRef(
    const ResolvedColumnRef& column_ref, AnnotationMap* result_annotation_map) {
  if (result_annotation_map == nullptr) return absl::OkStatus();
  return MergeAnnotations(column_ref.column().type_annotation_map(),
                          *result_annotation_map);
}

absl::Status DefaultAnnotationSpec::CheckAndPropagateForGetStructField(
    const ResolvedGetStructField& get_struct_field,
    AnnotationMap* result_annotation_map) {
  if (result_annotation_map == nullptr) return absl::OkStatus();
  const AnnotationMap* struct_annotation_map =
      get_struct_field.expr()->type_annotation_map();
  if (struct_annotation_map != nullptr) {
    ZETASQL_RET_CHECK(struct_annotation_map->IsStructMap());
    int field_idx = get_struct_field.field_idx();
    ZETASQL_RET_CHECK_LT(field_idx, struct_annotation_map->AsStructMap()->num_fields());
    ZETASQL_RETURN_IF_ERROR(
        MergeAnnotations(struct_annotation_map->AsStructMap()->field(field_idx),
                         *result_annotation_map));
  }
  return absl::OkStatus();
}

absl::Status DefaultAnnotationSpec::CheckAndPropagateForMakeStruct(
    const ResolvedMakeStruct& make_struct,
    StructAnnotationMap* result_annotation_map) {
  if (result_annotation_map == nullptr) return absl::OkStatus();
  ZETASQL_RET_CHECK_EQ(result_annotation_map->num_fields(),
               make_struct.field_list_size());
  for (int i = 0; i < make_struct.field_list_size(); i++) {
    ZETASQL_RETURN_IF_ERROR(
        MergeAnnotations(make_struct.field_list(i)->type_annotation_map(),
                         *result_annotation_map->mutable_field(i)));
  }
  return absl::OkStatus();
}

absl::Status DefaultAnnotationSpec::CheckAndPropagateForSubqueryExpr(
    const ResolvedSubqueryExpr& subquery_expr,
    AnnotationMap* result_annotation_map) {
  if (result_annotation_map == nullptr) return absl::OkStatus();
  const ResolvedScan* subquery_scan = subquery_expr.subquery();
  ZETASQL_RET_CHECK_NE(subquery_scan, nullptr);
  if (subquery_expr.subquery_type() == ResolvedSubqueryExpr::ARRAY) {
    ZETASQL_RET_CHECK_EQ(subquery_scan->column_list_size(), 1);
    ZETASQL_RET_CHECK(result_annotation_map->IsArrayMap());
    ZETASQL_RET_CHECK(subquery_scan->column_list(0).type()->Equivalent(
        subquery_expr.type()->AsArray()->element_type()));
    result_annotation_map =
        result_annotation_map->AsArrayMap()->mutable_element();
  } else if (subquery_expr.subquery_type() == ResolvedSubqueryExpr::SCALAR) {
    ZETASQL_RET_CHECK_EQ(subquery_scan->column_list_size(), 1);
    ZETASQL_RET_CHECK(
        subquery_scan->column_list(0).type()->Equivalent(subquery_expr.type()));
  } else {
    // No way to propagate annotations by default for existence/in/like
    // subqueries.
    return absl::OkStatus();
  }
  auto* annotation = subquery_scan->column_list(0).type_annotation_map();
  return MergeAnnotations(annotation, *result_annotation_map);
}

absl::Status DefaultAnnotationSpec::CheckAndPropagateForSetOperationScan(
    const ResolvedSetOperationScan& set_operation_scan,
    const std::vector<AnnotationMap*>& result_annotation_maps) {
  const int column_list_size = set_operation_scan.column_list_size();
  ZETASQL_RET_CHECK_EQ(column_list_size, result_annotation_maps.size());
  for (int item_index = 0;
       item_index < set_operation_scan.input_item_list_size(); ++item_index) {
    const auto* item = set_operation_scan.input_item_list(item_index);
    ZETASQL_RET_CHECK_EQ(item->output_column_list_size(), column_list_size);
    for (int i = 0; i < column_list_size; i++) {
      if (result_annotation_maps[i] == nullptr) continue;
      ZETASQL_RETURN_IF_ERROR(
          MergeAnnotations(item->output_column_list(i).type_annotation_map(),
                           *result_annotation_maps[i]))
          << "in column " << i + 1 << ", item " << item_index + 1
          << " of set operation scan";
    }
  }
  return absl::OkStatus();
}

absl::Status DefaultAnnotationSpec::ScalarMergeIfCompatible(
    const AnnotationMap* in, AnnotationMap& out) const {
  const SimpleValue* value_before = out.GetAnnotation(Id());
  const SimpleValue* value_in =
      in != nullptr ? in->GetAnnotation(Id()) : nullptr;

  if (value_before == nullptr) {
    // If <out> doesn't already have a value for Id(), copy the one that's in
    // <in>, if <in> does contain a value.
    if (value_in == nullptr) return absl::OkStatus();
    out.SetAnnotation(Id(), *value_in);
    return absl::OkStatus();
  }

  if (value_in == nullptr || !value_in->Equals(*value_before)) {
    // If both <out> and <in> have a value and they are not the same, it's an
    // error.
    return MakeSqlError() << Name()
                          << " conflict: " << AnnotationDebugString(in)
                          << " vs. " << AnnotationDebugString(&out);
  }
  return absl::OkStatus();
}

absl::Status DefaultAnnotationSpec::MergeAnnotations(
    const AnnotationMap* left, AnnotationMap& right) const {
  ZETASQL_RETURN_IF_ERROR(ScalarMergeIfCompatible(left, right));
  if (left == nullptr) return absl::OkStatus();
  if (left->IsArrayMap()) {
    ZETASQL_RET_CHECK(right.IsArrayMap()) << right.DebugString(Id());
    ZETASQL_RETURN_IF_ERROR(MergeAnnotations(left->AsArrayMap()->element(),
                                     *right.AsArrayMap()->mutable_element()));
  } else if (left->IsStructMap()) {
    ZETASQL_RET_CHECK(right.IsStructMap()) << right.DebugString(Id());
    ZETASQL_RET_CHECK_EQ(left->AsStructMap()->num_fields(),
                 right.AsStructMap()->num_fields());
    for (int i = 0; i < left->AsStructMap()->num_fields(); i++) {
      ZETASQL_RETURN_IF_ERROR(MergeAnnotations(left->AsStructMap()->field(i),
                                       *right.AsStructMap()->mutable_field(i)));
    }
  }
  return absl::OkStatus();
}

std::string DefaultAnnotationSpec::Name() const {
  if (Id() <= static_cast<int>(AnnotationKind::kMaxBuiltinAnnotationKind)) {
    return GetAnnotationKindName(static_cast<AnnotationKind>(Id()));
  } else {
    return absl::StrCat("Annotation[", std::to_string(Id()), "]");
  }
}
}  // namespace zetasql
