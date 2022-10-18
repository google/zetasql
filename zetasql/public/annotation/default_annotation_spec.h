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

#ifndef ZETASQL_PUBLIC_ANNOTATION_DEFAULT_ANNOTATION_SPEC_H_
#define ZETASQL_PUBLIC_ANNOTATION_DEFAULT_ANNOTATION_SPEC_H_

#include <string>
#include <vector>

#include "zetasql/public/types/annotation.h"
#include "zetasql/resolved_ast/resolved_ast.h"

namespace zetasql {

// This class provides default implementations for most CheckAndPropagate
// methods. Most AnnotationSpecs should inherit from this and customize behavior
// as needed.
class DefaultAnnotationSpec : public AnnotationSpec {
 public:
  ~DefaultAnnotationSpec() override {}

  // Returns the name of the annotation.
  //
  // Default implementation:
  // - If this is a built-in implementation, returns the annotation kind name.
  // - Otherwise, returns Annotation[<Id()>].
  virtual std::string Name() const;

  // Does nothing and returns OK.
  absl::Status CheckAndPropagateForFunctionCallBase(
      const ResolvedFunctionCallBase& function_call,
      AnnotationMap* result_annotation_map) override;

  // These functions copy the annotation from their source to the result
  // annotation map. If the annotation is already set, they raise an error.
  // <result_annotation_map> may be nullptr for any of these functions, in which
  // case they simply return.
  // If <result_annotation_map> is non-null, it's an error if its structure
  // doesn't match the implied structure of the ResolvedNode. For example, in
  // the case of CheckAndPropagateForMakeStruct, <result_annotation_map> must
  // be a StructAnnotationMap.
  absl::Status CheckAndPropagateForColumnRef(
      const ResolvedColumnRef& column_ref,
      AnnotationMap* result_annotation_map) override;
  absl::Status CheckAndPropagateForGetStructField(
      const ResolvedGetStructField& get_struct_field,
      AnnotationMap* result_annotation_map) override;
  absl::Status CheckAndPropagateForMakeStruct(
      const ResolvedMakeStruct& make_struct,
      StructAnnotationMap* result_annotation_map) override;

  // If the subquery is an array subquery, copies the element type of its
  // annotation map to <result_annotation_map>. Otherwise, copies the annotation
  // map of the subquery's column to <result_annotation_map>.
  absl::Status CheckAndPropagateForSubqueryExpr(
      const ResolvedSubqueryExpr& subquery_expr,
      AnnotationMap* result_annotation_map) override;

  // Merges the AnnotationMaps for each column in the scan by successively
  // calling ScalarMergeIfCompatible on their components.
  absl::Status CheckAndPropagateForSetOperationScan(
      const ResolvedSetOperationScan& set_operation_scan,
      const std::vector<AnnotationMap*>& result_annotation_maps) override;

  // Merges two scalar annotations, placing the result in 'out'.
  // This function should not call AsStructMap or AsArrayMap on either 'in' or
  // 'out'. The work of traversing struct map or array map structures is
  // handled by MergeAnnotations, which calls this.
  //
  // Default behavior:
  // - Copies <in>'s setting for this annotation to <out>. If <in> is null or
  //   lacks an annotation for Id(), does nothing.
  // - Reports an error if both <out> and <in> have the annotation and the
  //   annotation value is different.
  virtual absl::Status ScalarMergeIfCompatible(const AnnotationMap* in,
                                               AnnotationMap& out) const;

 protected:
  // Recursively calls ScalarMergeIfCompatible on the <left> and <right>.
  absl::Status MergeAnnotations(const AnnotationMap* left,
                                AnnotationMap& right) const;

  std::string AnnotationDebugString(const AnnotationMap* a) const {
    return a == nullptr ? "<null>" : a->DebugString(Id());
  }
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_ANNOTATION_DEFAULT_ANNOTATION_SPEC_H_
