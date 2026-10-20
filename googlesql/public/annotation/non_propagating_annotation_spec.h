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

#ifndef GOOGLESQL_PUBLIC_ANNOTATION_NON_PROPAGATING_ANNOTATION_SPEC_H_
#define GOOGLESQL_PUBLIC_ANNOTATION_NON_PROPAGATING_ANNOTATION_SPEC_H_

#include <vector>

#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/simple_value.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"

namespace googlesql {

// Base class for annotations that do not propagate through query expressions.
class NonPropagatingAnnotationSpec : public AnnotationSpec {
 public:
  ~NonPropagatingAnnotationSpec() override = default;

  absl::Status CheckAndPropagateForFunctionCallBase(
      const ResolvedFunctionCallBase& function_call,
      AnnotationMap* result_annotation_map) override {
    return absl::OkStatus();
  }

  absl::Status CheckAndPropagateForParameter(
      const ResolvedParameter& parameter,
      AnnotationMap* result_annotation_map) override {
    CopyAnnotation(parameter.type_annotation_map(), result_annotation_map);
    return absl::OkStatus();
  }

  absl::Status CheckAndPropagateForGetStructField(
      const ResolvedGetStructField& get_struct_field,
      AnnotationMap* result_annotation_map) override {
    return absl::OkStatus();
  }

  absl::Status CheckAndPropagateForGetRowField(
      const ResolvedGetRowField& get_row_field,
      AnnotationMap* result_annotation_map) override {
    return absl::OkStatus();
  }

  absl::Status CheckAndPropagateForMakeStruct(
      const ResolvedMakeStruct& make_struct,
      StructAnnotationMap* result_annotation_map) override {
    return absl::OkStatus();
  }

  absl::Status CheckAndPropagateForMakeMap(
      const ResolvedMakeMap& make_map,
      StructAnnotationMap* result_annotation_map) override {
    return absl::OkStatus();
  }

  absl::Status CheckAndPropagateForSubqueryExpr(
      const ResolvedSubqueryExpr& subquery_expr,
      AnnotationMap* result_annotation_map) override {
    return absl::OkStatus();
  }

  absl::Status CheckAndPropagateForSetOperationScan(
      const ResolvedSetOperationScan& set_operation_scan,
      const std::vector<AnnotationMap*>& result_annotation_maps) override {
    return absl::OkStatus();
  }

  absl::Status CheckAndPropagateForRecursiveScan(
      const ResolvedRecursiveScan& recursive_scan,
      const std::vector<AnnotationMap*>& result_annotation_maps) override {
    return absl::OkStatus();
  }

  absl::Status CheckAndPropagateForCast(
      const ResolvedCast& cast, AnnotationMap* result_annotation_map) override {
    return absl::OkStatus();
  }

 private:
  void CopyAnnotation(const AnnotationMap* in, AnnotationMap* out) const {
    if (in == nullptr || out == nullptr) {
      return;
    }
    if (const SimpleValue* val = in->GetAnnotation(Id()); val != nullptr) {
      out->SetAnnotation(Id(), *val);
    }
    if (in->IsStructMap() && out->IsStructMap()) {
      for (int i = 0; i < in->AsStructMap()->num_fields(); ++i) {
        CopyAnnotation(in->AsStructMap()->field(i),
                       out->AsStructMap()->mutable_field(i));
      }
    }
  }
};

}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_ANNOTATION_NON_PROPAGATING_ANNOTATION_SPEC_H_
