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

#ifndef ZETASQL_PUBLIC_ANNOTATION_TIMESTAMP_PRECISION_H_
#define ZETASQL_PUBLIC_ANNOTATION_TIMESTAMP_PRECISION_H_

#include <cstdint>
#include <optional>

#include "zetasql/public/annotation/default_annotation_spec.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/annotation.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace zetasql {

class TimestampPrecisionAnnotation : public DefaultAnnotationSpec {
 public:
  explicit TimestampPrecisionAnnotation(int default_precision)
      : default_precision_(default_precision) {}

  static int GetId() {
    return static_cast<int>(AnnotationKind::kTimestampPrecision);
  }

  int Id() const override { return GetId(); }

  // Timestamp precisions merge by taking the maximum of the two.
  absl::Status ScalarMergeIfCompatible(const AnnotationMap* in,
                                       AnnotationMap& out) const override;

  absl::Status CheckAndPropagateForColumnRef(
      const ResolvedColumnRef& column_ref,
      AnnotationMap* result_annotation_map) override;

  // Unlike the default behavior, cast may generate a new TIMESTAMP precision
  // annotation when it specifies a precision, e.g. CAST(.. AS TIMESTAMP(3)).
  // Override the default behavior which generates no annotations and drops
  // annotations on the input.
  absl::Status CheckAndPropagateForCast(
      const ResolvedCast& cast, AnnotationMap* result_annotation_map) override;

  // Grabs timestamp precision from annotation map. Returns empty if there is
  // no TimestampPrecisionAnnotation.
  static absl::StatusOr<std::optional<int64_t>> GrabPrecision(
      const AnnotationMap* in);

 private:
  // This is called from CheckAndPropagateForCast to assign the annotation
  // precision of the output type. The input type & annotation are needed for
  // when the target contains TIMESTAMP without a particular precision, in which
  // case the input one is propagated if present, otherwise the default
  // precision is assigned. This function operates recursively within composite
  // types.
  absl::Status PropagateFromTypeParameters(
      AnnotatedType input_annotated_type, const Type* target_type,
      const TypeParameters& target_type_params,
      AnnotationMap& result_annotation_map) const;

  // The default precision for the system (controlled by LanguageOptions).
  // Used when some inputs lack an annotation, as we would want the output to
  // use the higher precision between the explicit and the default one. e.g.
  //   TIMESTAMP_DIFF(TIMESTAMP(3), TIMESTAMP)
  //   TIMESTAMP_DIFF(TIMESTAMP(12), TIMESTAMP)
  int default_precision_;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_ANNOTATION_TIMESTAMP_PRECISION_H_
