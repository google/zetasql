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

#ifndef GOOGLESQL_PUBLIC_ANNOTATION_VECTOR_ENCODING_H_
#define GOOGLESQL_PUBLIC_ANNOTATION_VECTOR_ENCODING_H_

#include <optional>
#include <string>

#include "googlesql/public/annotation/default_annotation_spec.h"
#include "googlesql/public/parse_location.h"
#include "googlesql/public/proto/vector_encoding_id.pb.h"
#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_parameters.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace googlesql {

// VectorEncoding propagates the vector encoding parameter across operations
// and casts. This class defines the propagation behavior of this encoding
// annotation for ResolvedAst nodes.
class VectorEncodingAnnotation : public DefaultAnnotationSpec {
 public:
  static constexpr absl::string_view kUnconstrained = "UNCONSTRAINED";

  VectorEncodingAnnotation() = default;
  ~VectorEncodingAnnotation() override = default;

  static int GetId() {
    return static_cast<int>(AnnotationKind::kVectorEncoding);
  }
  int Id() const override { return GetId(); }

  // Extracts the effective vector encoding from <annotation_map>.
  // Returns std::nullopt if:
  // - <annotation_map> is nullptr,
  // - VectorEncodingAnnotation is not present in <annotation_map>, or
  // - VectorEncodingAnnotation has value kUnconstrained ("UNCONSTRAINED").
  // Returns the vector encoding if it is a valid, fixed encoding.
  static absl::StatusOr<std::optional<std::string>> GetEffectiveEncoding(
      const AnnotationMap* annotation_map);

  // Returns false when <map> is nullptr or VectorEncodingAnnotation is not
  // present in <map> or any of its nested AnnotationMaps.
  static bool ExistsIn(const AnnotationMap* map) {
    return map != nullptr && map->Has<VectorEncodingAnnotation>();
  }

  // Assigns an annotation to the output of a cast if the target type is a
  // VECTOR and specifies an encoding parameter, e.g.
  // CAST(.. AS VECTOR(4, 'FLOAT32')).
  absl::Status CheckAndPropagateForCast(
      const ResolvedCast& cast, AnnotationMap* result_annotation_map) override;

  // Propagates vector encoding annotations from the query parameter. Vector
  // parameters (or nested vector slots) are assigned kUnconstrained
  // ("UNCONSTRAINED"), since the encoding of a bound parameter value is not
  // known at analysis time.
  absl::Status CheckAndPropagateForParameter(
      const ResolvedParameter& parameter,
      AnnotationMap* result_annotation_map) override;

  // This is called to assign the annotation vector encoding of the output type
  // from TypeParameters. This function operates recursively within composite
  // types.
  static absl::Status PropagateFromTypeParameters(
      const Type* target_type, const TypeParameters& target_type_params,
      const AnnotationMap* input_map, AnnotationMap& result_annotation_map,
      bool return_null_on_error, const ParseLocationRange* error_location);

  // Determines whether the vector encoding should be propagated to the
  // function's result. Currently handles specific vector functions such as
  // ENCODE_VECTOR where the output vector encoding is derived from the encoding
  // argument if specified (if omitted, the implicit default is "FLOAT32").
  absl::Status CheckAndPropagateForFunctionCallBase(
      const ResolvedFunctionCallBase& function_call,
      AnnotationMap* result_annotation_map) override;

  // Merges scalar vector encoding annotations from `in` into `out`.
  //
  // - If `in` or `out` is unannotated (nullptr, e.g. untyped NULL or empty
  //   container literal), the other's annotation is retained (identity).
  // - If matching (`in == out`), `out` retains the encoding.
  // - If conflicting (`in != out`), `out` transitions to `kUnconstrained`
  //   ("UNCONSTRAINED").
  // - If either is `kUnconstrained`, the result transitions to
  //   `kUnconstrained` (absorbing).
  absl::Status ScalarMergeIfCompatible(const AnnotationMap* in,
                                       AnnotationMap& out) const override;
};

}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_ANNOTATION_VECTOR_ENCODING_H_
