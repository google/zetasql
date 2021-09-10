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

#ifndef ZETASQL_TESTDATA_SAMPLE_ANNOTATION_H_
#define ZETASQL_TESTDATA_SAMPLE_ANNOTATION_H_

#include "zetasql/public/type.h"
#include "zetasql/public/types/annotation.h"
#include "absl/status/status.h"

namespace zetasql {

// Sample annotation for testing purposes.
// This annotation is always propagated from ResolvedAST nodes input to their
// output whenever possible, so that the propagation paths are visible through
// the tests.
class SampleAnnotation : public AnnotationSpec {
 public:
  SampleAnnotation() {}

  static int GetId() {
    return static_cast<int>(AnnotationKind::kSampleAnnotation);
  }

  int Id() const override { return GetId(); }

  absl::Status CheckAndPropagateForFunctionCallBase(
      const ResolvedFunctionCallBase& function_call,
      AnnotationMap* result_annotation_map) override {
    return absl::OkStatus();
  };

  absl::Status CheckAndPropagateForColumnRef(
      const ResolvedColumnRef& column_ref,
      AnnotationMap* result_annotation_map) override;

  absl::Status CheckAndPropagateForGetStructField(
      const ResolvedGetStructField& get_struct_field,
      AnnotationMap* result_annotation_map) override;

  absl::Status CheckAndPropagateForMakeStruct(
      const ResolvedMakeStruct& make_struct,
      StructAnnotationMap* result_annotation_map) override {
    return absl::OkStatus();
  };

  absl::Status CheckAndPropagateForSubqueryExpr(
      const ResolvedSubqueryExpr& subquery_expr,
      AnnotationMap* result_annotation_map) override {
    return absl::OkStatus();
  }
};

}  // namespace zetasql

#endif  // ZETASQL_TESTDATA_SAMPLE_ANNOTATION_H_
