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

#ifndef GOOGLESQL_PUBLIC_ANNOTATION_VECTOR_DIMS_H_
#define GOOGLESQL_PUBLIC_ANNOTATION_VECTOR_DIMS_H_

#include <cstdint>
#include <optional>

#include "googlesql/public/annotation/default_annotation_spec.h"
#include "googlesql/public/parse_location.h"
#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/type.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace googlesql {

// VectorDims propagates the vector dims parameter across operations and casts.
// The VECTOR type requires dims to be a strictly positive integer. This class
// defines the propagation behavior of this dims annotation for ResolvedAst
// nodes.
class VectorDimsAnnotation : public DefaultAnnotationSpec {
 public:
  static constexpr int64_t kUnconstrained = -1;

  VectorDimsAnnotation() = default;
  ~VectorDimsAnnotation() override = default;

  static int GetId() { return static_cast<int>(AnnotationKind::kVectorDims); }

  int Id() const override { return GetId(); }

  // Extracts the effective vector dims from <annotation_map>.
  // Returns std::nullopt if:
  // - <annotation_map> is nullptr,
  // - VectorDimsAnnotation is not present in <annotation_map>, or
  // - VectorDimsAnnotation has value <= 0 (e.g. kUnconstrained = -1).
  // Returns the vector dims if it is a valid, fixed value (> 0).
  static absl::StatusOr<std::optional<int64_t>> GetEffectiveDims(
      const AnnotationMap* annotation_map);

  // Returns false when <map> is nullptr or VectorDimsAnnotation is not
  // present in <map> or any of its nested AnnotationMaps.
  static bool ExistsIn(const AnnotationMap* map) {
    return map != nullptr && map->Has<VectorDimsAnnotation>();
  }

  // Propagates vector dims annotations from the query parameter. Vector
  // parameters (or nested vector slots) are assigned kUnconstrained (-1), since
  // the dims of a bound parameter value are not known at analysis time.
  absl::Status CheckAndPropagateForParameter(
      const ResolvedParameter& parameter,
      AnnotationMap* result_annotation_map) override;

  // Determines whether the vector dims should be propagated to the function's
  // result. Handles specific vector functions such as ENCODE_VECTOR as well
  // as general function signatures (e.g. ARRAY_CONCAT, IF, CASE).
  absl::Status CheckAndPropagateForFunctionCallBase(
      const ResolvedFunctionCallBase& function_call,
      AnnotationMap* result_annotation_map) override;

  // Assigns an annotation to the output of a cast if the target type is a
  // VECTOR and specifies a dims parameter, e.g. CAST(.. AS VECTOR(3)).
  absl::Status CheckAndPropagateForCast(
      const ResolvedCast& cast, AnnotationMap* result_annotation_map) override;

  // This is called to assign the annotation vector dims of the output type
  // from TypeParameters. This function operates recursively within composite
  // types.
  static absl::Status PropagateFromTypeParameters(
      const Type* target_type, const TypeParameters& target_type_params,
      const AnnotationMap* input_map, AnnotationMap& result_annotation_map,
      bool return_null_on_error, const ParseLocationRange* error_location);

  // Merges scalar vector dims annotations from `in` into `out`.
  //
  // - If `in` or `out` is unannotated (nullptr, e.g. untyped NULL or empty
  //   container literal), the other's annotation is retained (identity).
  // - If matching (`in == out`), `out` retains the dims.
  // - If conflicting (`in != out`), `out` transitions to `kUnconstrained`
  //   (-1).
  // - If either is `kUnconstrained` (-1), the result transitions to
  //   `kUnconstrained` (absorbing).
  absl::Status ScalarMergeIfCompatible(const AnnotationMap* in,
                                       AnnotationMap& out) const override;
};

}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_ANNOTATION_VECTOR_DIMS_H_
