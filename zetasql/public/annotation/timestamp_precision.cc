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

#include "zetasql/public/annotation/timestamp_precision.h"

#include <algorithm>
#include <cstdint>
#include <optional>

#include "zetasql/public/function.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/type.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

absl::Status TimestampPrecisionAnnotation::ScalarMergeIfCompatible(
    const AnnotationMap* in, AnnotationMap& out) const {
  ZETASQL_ASSIGN_OR_RETURN(std::optional<int64_t> in_precision, GrabPrecision(in));
  if (!in_precision.has_value()) {
    // Nothing to merge.
    return absl::OkStatus();
  }
  ZETASQL_ASSIGN_OR_RETURN(std::optional<int64_t> out_precision, GrabPrecision(&out));
  if (!out_precision.has_value()) {
    out.SetAnnotation<TimestampPrecisionAnnotation>(
        SimpleValue::Int64(*in_precision));
    return absl::OkStatus();
  }

  if (in_precision >= out_precision) {
    out.SetAnnotation<TimestampPrecisionAnnotation>(
        SimpleValue::Int64(std::max(*in_precision, *out_precision)));
  }
  return absl::OkStatus();
}

static bool IsTimestampRelated(const Type* type) {
  switch (type->kind()) {
    case TYPE_TIMESTAMP:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_TIME:
    case TYPE_INTERVAL:
      return true;
    default:
      return false;
  }
}

absl::Status TimestampPrecisionAnnotation::PropagateFromTypeParameters(
    AnnotatedType input_annotated_type, const Type* target_type,
    const TypeParameters& target_type_params,
    AnnotationMap& result_annotation_map) const {
  if (target_type->IsSimpleType()) {
    ZETASQL_RET_CHECK_EQ(target_type_params.num_children(), 0);
    if (!target_type->IsTimestamp()) {
      // For now, only TIMESTAMP type will propagate the precision from CAST.
      // In the future, other TIMESTAMP-related types will apply that logic.
      return absl::OkStatus();
    }
    int64_t target_precision;
    if (target_type_params.IsTimestampTypeParameters()) {
      // The CAST explicitly specifies the precision, so that's the target.
      target_precision =
          target_type_params.timestamp_type_parameters().precision();
    } else {
      // Propagate precision from the input. Use the default if the input
      // doesn't have a precision (e.g. casting from an unrelated type like
      // STRING).
      ZETASQL_ASSIGN_OR_RETURN(std::optional<int64_t> in_precision,
                       GrabPrecision(input_annotated_type.annotation_map));
      target_precision = in_precision.value_or(default_precision_);
    }

    result_annotation_map.SetAnnotation<TimestampPrecisionAnnotation>(
        SimpleValue::Int64(target_precision));
    return absl::OkStatus();
  }

  TypeParameters empty_type_param;
  switch (target_type->kind()) {
    case TYPE_ARRAY: {
      ZETASQL_RET_CHECK(result_annotation_map.IsArrayMap());
      ZETASQL_RET_CHECK(input_annotated_type.type->IsArray());
      const AnnotationMap* input_element_annotation_map = nullptr;
      if (input_annotated_type.annotation_map != nullptr) {
        ZETASQL_RET_CHECK(input_annotated_type.annotation_map->IsArrayMap());
        input_element_annotation_map =
            input_annotated_type.annotation_map->AsArrayMap()->element();
      }
      return PropagateFromTypeParameters(
          AnnotatedType(input_annotated_type.type->AsArray()->element_type(),
                        input_element_annotation_map),
          target_type->AsArray()->element_type(),
          target_type_params.num_children() > 0 ? target_type_params.child(0)
                                                : empty_type_param,
          *result_annotation_map.AsArrayMap()->mutable_element());
    }
    case TYPE_STRUCT: {
      ZETASQL_RET_CHECK(result_annotation_map.IsStructMap());
      if (target_type_params.num_children() > 0) {
        // Must be either empty, or match the number of fields.
        ZETASQL_RET_CHECK_EQ(target_type_params.num_children(),
                     result_annotation_map.AsStructMap()->num_fields());
      }
      // RET_CHECKs on the input are simply asserting that we're propagating
      // from a CAST that the resolver already decided is valid.
      ZETASQL_RET_CHECK(input_annotated_type.type->IsStruct());
      for (int i = 0; i < target_type_params.num_children(); ++i) {
        const AnnotationMap* input_field_annotation_map = nullptr;
        if (input_annotated_type.annotation_map != nullptr) {
          ZETASQL_RET_CHECK(input_annotated_type.annotation_map->IsStructMap());
          input_field_annotation_map =
              input_annotated_type.annotation_map->AsStructMap()->field(i);
        }
        ZETASQL_RETURN_IF_ERROR(PropagateFromTypeParameters(
            AnnotatedType(input_annotated_type.type->AsStruct()->field(i).type,
                          input_field_annotation_map),
            target_type->AsStruct()->field(i).type,
            target_type_params.num_children() > 0 ? target_type_params.child(i)
                                                  : empty_type_param,
            *result_annotation_map.AsStructMap()->mutable_field(i)));
      }
      return absl::OkStatus();
    }
    default:
      // Very soon we will add RANGE, MAP, GRAPH_ELEMENT, etc here.
      // We want a general interface on Type to retrieve component types,
      // rather than having to enumerate every subclass, especially when we
      // start allowing UDTs that are component types.
      return absl::OkStatus();
  }

  ZETASQL_RET_CHECK_FAIL() << "Should return from the switch statement above.";
}

absl::Status TimestampPrecisionAnnotation::CheckAndPropagateForCast(
    const ResolvedCast& cast, AnnotationMap* result_annotation_map) {
  // It looks like this is never null.
  ZETASQL_RET_CHECK(result_annotation_map != nullptr);

  return PropagateFromTypeParameters(cast.expr()->annotated_type(), cast.type(),
                                     cast.type_modifiers().type_parameters(),
                                     *result_annotation_map);
}

absl::Status TimestampPrecisionAnnotation::CheckAndPropagateForColumnRef(
    const ResolvedColumnRef& column_ref, AnnotationMap* result_annotation_map) {
  // It looks like this is never null.
  ZETASQL_RET_CHECK(result_annotation_map != nullptr);

  ZETASQL_RETURN_IF_ERROR(MergeAnnotations(column_ref.column().type_annotation_map(),
                                   *result_annotation_map));
  // If still no timestamp precision annotation, but this column's type is
  // relevant to timestamp-precision, set it to the default.
  // This is important for when this column is an argument for a function_call.
  ZETASQL_ASSIGN_OR_RETURN(std::optional<int64_t> result_precision,
                   GrabPrecision(result_annotation_map));
  if (!result_precision.has_value() &&
      IsTimestampRelated(column_ref.column().type())) {
    result_annotation_map->SetAnnotation<TimestampPrecisionAnnotation>(
        SimpleValue::Int64(default_precision_));
  }
  return absl::OkStatus();
}

absl::StatusOr<std::optional<int64_t>>
TimestampPrecisionAnnotation::GrabPrecision(const AnnotationMap* in) {
  if (in == nullptr) {
    return std::nullopt;
  }
  const SimpleValue* in_precision =
      in->GetAnnotation(TimestampPrecisionAnnotation::GetId());
  if (in_precision == nullptr) {
    return std::nullopt;
  }
  ZETASQL_RET_CHECK(in_precision->IsValid());
  ZETASQL_RET_CHECK(in_precision->has_int64_value());
  return in_precision->int64_value();
}

}  // namespace zetasql
