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

#include "zetasql/public/types/annotation.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/annotation.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

static std::string GetAnnotationKindName(AnnotationKind kind) {
  switch (kind) {
    case AnnotationKind::kCollation:
      return "Collation";
    case AnnotationKind::kSampleAnnotation:
      return "SampleAnnotation";
    case AnnotationKind::kMaxBuiltinAnnotationKind:
      return "MaxBuiltinAnnotationKind";
  }
}

absl::Status AnnotationMap::Serialize(AnnotationMapProto* proto) const {
  for (const auto& annotation_pair : annotations_) {
    AnnotationProto* annotation_proto = proto->add_annotations();
    annotation_proto->set_id(annotation_pair.first);
    ZETASQL_RETURN_IF_ERROR(
        annotation_pair.second.Serialize(annotation_proto->mutable_value()));
  }
  return absl::OkStatus();
}

// static
absl::StatusOr<std::unique_ptr<AnnotationMap>> AnnotationMap::Deserialize(
    const AnnotationMapProto& proto) {
  ZETASQL_RET_CHECK(!proto.is_null())
      << "is_null could only be true for struct field or array element";
  std::unique_ptr<AnnotationMap> annotation_map;
  // Recursively handle struct fields and array element.
  if (proto.struct_fields_size() > 0) {
    annotation_map = absl::WrapUnique(new StructAnnotationMap());
    for (int i = 0; i < proto.struct_fields_size(); i++) {
      std::unique_ptr<AnnotationMap> struct_field;
      if (!proto.struct_fields(i).is_null()) {
        ZETASQL_ASSIGN_OR_RETURN(struct_field, Deserialize(proto.struct_fields(i)));
      }
      annotation_map->AsStructMap()->fields_.push_back(std::move(struct_field));
    }
  } else if (proto.has_array_element()) {
    annotation_map = absl::WrapUnique(new ArrayAnnotationMap());
    if (!proto.array_element().is_null()) {
      ZETASQL_ASSIGN_OR_RETURN(annotation_map->AsArrayMap()->element_,
                       Deserialize(proto.array_element()));
    }
  } else {
    annotation_map = absl::WrapUnique(new AnnotationMap());
  }
  // Deserialize annotation map.
  for (const auto& annotation_proto : proto.annotations()) {
    ZETASQL_ASSIGN_OR_RETURN(SimpleValue value,
                     SimpleValue::Deserialize(annotation_proto.value()));
    annotation_map->SetAnnotation(static_cast<int>(annotation_proto.id()),
                                  value);
  }
  return annotation_map;
}

// static
bool AnnotationMap::HasCompatibleStructure(const AnnotationMap* lhs,
                                           const AnnotationMap* rhs) {
  if (lhs == nullptr || rhs == nullptr) {
    return true;
  }
  if (lhs->IsStructMap()) {
    if (!rhs->IsStructMap() ||
        lhs->AsStructMap()->num_fields() != rhs->AsStructMap()->num_fields()) {
      return false;
    }
    for (int i = 0; i < lhs->AsStructMap()->num_fields(); i++) {
      if (!HasCompatibleStructure(lhs->AsStructMap()->field(i),
                                  lhs->AsStructMap()->field(i))) {
        return false;
      }
    }
    return true;
  } else if (lhs->IsArrayMap()) {
    return rhs->IsArrayMap() &&
           HasCompatibleStructure(lhs->AsArrayMap()->element(),
                                  rhs->AsArrayMap()->element());
  }
  return !rhs->IsStructMap() && !rhs->IsArrayMap();
}

std::unique_ptr<AnnotationMap> AnnotationMap::Clone() const {
  std::unique_ptr<AnnotationMap> target;
  if (IsStructMap()) {
    target.reset(new StructAnnotationMap());
    target->AsStructMap()->fields_.resize(AsStructMap()->num_fields());
    for (int i = 0; i < AsStructMap()->num_fields(); i++) {
      if (AsStructMap()->field(i) != nullptr) {
        target->AsStructMap()->fields_[i] = AsStructMap()->field(i)->Clone();
      }
    }
  } else if (IsArrayMap()) {
    target.reset(new ArrayAnnotationMap());
    if (AsArrayMap()->element() != nullptr) {
      target->AsArrayMap()->element_ = AsArrayMap()->element()->Clone();
    }
  } else {
    target.reset(new AnnotationMap());
  }
  target->annotations_ = annotations_;
  return target;
}

bool AnnotationMap::HasCompatibleStructure(const Type* type) const {
  if (IsStructMap()) {
    if (!type->IsStruct() ||
        AsStructMap()->num_fields() != type->AsStruct()->num_fields()) {
      return false;
    }
    for (int i = 0; i < AsStructMap()->num_fields(); i++) {
      if (AsStructMap()->field(i) != nullptr &&
          !AsStructMap()->field(i)->HasCompatibleStructure(
              type->AsStruct()->field(i).type)) {
        return false;
      }
    }
    return true;
  } else if (IsArrayMap()) {
    return type->AsArray() && (AsArrayMap()->element() == nullptr ||
                               AsArrayMap()->element()->HasCompatibleStructure(
                                   type->AsArray()->element_type()));
  }
  return !type->IsStruct() && !type->IsArray();
}

bool AnnotationMap::NormalizeInternal() {
  bool empty = annotations_.empty();
  if (IsStructMap()) {
    for (int i = 0; i < AsStructMap()->num_fields(); i++) {
      std::unique_ptr<AnnotationMap>& field_ptr = AsStructMap()->fields_[i];
      if (field_ptr != nullptr) {
        if (field_ptr->NormalizeInternal()) {
          // Set field pointer to nullptr if the AnnotationMap is empty.
          field_ptr.reset(nullptr);
        } else {
          empty = false;
        }
      }
    }
  } else if (IsArrayMap()) {
    std::unique_ptr<AnnotationMap>& element_ptr = AsArrayMap()->element_;
    if (element_ptr != nullptr) {
      if (element_ptr->NormalizeInternal()) {
        // Set element pointer to nullptr if the AnnotationMap is empty.
        element_ptr.reset(nullptr);
      } else {
        empty = false;
      }
    }
  }
  return empty;
}

bool AnnotationMap::IsNormalized() const {
  return IsNormalizedAndNonEmpty(/*check_non_empty=*/false);
}

bool AnnotationMap::IsNormalizedAndNonEmpty(bool check_non_empty) const {
  bool children_non_empty = false;
  if (IsStructMap()) {
    for (int i = 0; i < AsStructMap()->num_fields(); i++) {
      const AnnotationMap* ptr = AsStructMap()->field(i);
      // The normalized form is that a struct field is either null or non-empty.
      if (ptr != nullptr &&
          !ptr->IsNormalizedAndNonEmpty(/*check_non_empty=*/true)) {
        return false;
      }
      children_non_empty = children_non_empty || ptr != nullptr;
    }
  } else if (IsArrayMap()) {
    const AnnotationMap* ptr = AsArrayMap()->element();
    // The normalized form is that an array element is either null or non-empty.
    if (ptr != nullptr &&
        !ptr->IsNormalizedAndNonEmpty(/*check_non_empty=*/true)) {
      return false;
    }
    children_non_empty = ptr != nullptr;
  }
  if (!check_non_empty) {
    return true;
  }
  return children_non_empty || !annotations_.empty();
}

int64_t AnnotationMap::GetEstimatedOwnedMemoryBytesSize() const {
  int64_t total_size = 0;
  for (const auto& annotation : annotations_) {
    total_size += sizeof(annotation.first) +
                  annotation.second.GetEstimatedOwnedMemoryBytesSize();
  }
  if (IsStructMap()) {
    for (int i = 0; i < AsStructMap()->num_fields(); i++) {
      total_size +=
          sizeof(std::unique_ptr<AnnotationMap>) +
          (AsStructMap()->field(i) == nullptr
               ? 0
               : AsStructMap()->field(i)->GetEstimatedOwnedMemoryBytesSize());
    }
  } else if (IsArrayMap()) {
    total_size +=
        sizeof(std::unique_ptr<AnnotationMap>) +
        (AsArrayMap()->element() == nullptr
             ? 0
             : AsArrayMap()->element()->GetEstimatedOwnedMemoryBytesSize());
  }
  return total_size;
}

// static
bool AnnotationMap::SimpleValueEqualsHelper(const SimpleValue* lhs,
                                            const SimpleValue* rhs) {
  return (lhs == nullptr && rhs == nullptr) ||
         (lhs != nullptr && rhs != nullptr && lhs->Equals(*rhs));
}

// static
bool AnnotationMap::EqualsInternal(const AnnotationMap* lhs,
                                   const AnnotationMap* rhs,
                                   std::optional<int> annotation_spec_id) {
  if (lhs == nullptr) {
    return rhs == nullptr || rhs->EmptyInternal(annotation_spec_id);
  }
  if (rhs == nullptr) {
    return lhs->EmptyInternal(annotation_spec_id);
  }
  // lhs and rhs have been guaranteed to be non-null.
  if (annotation_spec_id.has_value()) {
    // If <annotation_spec_id> has value, only compares annotation value for the
    // given AnnotationSpec id.
    if (!SimpleValueEqualsHelper(
            lhs->GetAnnotation(annotation_spec_id.value()),
            rhs->GetAnnotation(annotation_spec_id.value()))) {
      return false;
    }
  } else if (lhs->annotations_ != rhs->annotations_) {
    return false;
  }
  if (lhs->IsStructMap()) {
    if (!rhs->IsStructMap() ||
        lhs->AsStructMap()->num_fields() != rhs->AsStructMap()->num_fields()) {
      return false;
    }
    for (int i = 0; i < lhs->AsStructMap()->num_fields(); i++) {
      if (!EqualsInternal(lhs->AsStructMap()->field(i),
                          rhs->AsStructMap()->field(i), annotation_spec_id)) {
        return false;
      }
    }
    return true;
  } else if (lhs->IsArrayMap()) {
    return rhs->IsArrayMap() &&
           EqualsInternal(lhs->AsArrayMap()->element(),
                          rhs->AsArrayMap()->element(), annotation_spec_id);
  }
  // lhs is neither a struct nor an array.
  return !rhs->IsStructMap() && !rhs->IsArrayMap();
}

bool AnnotationMap::EmptyInternal(std::optional<int> annotation_spec_id) const {
  if (annotation_spec_id.has_value()) {
    if (GetAnnotation(annotation_spec_id.value()) != nullptr) {
      return false;
    }
  } else if (!annotations_.empty()) {
    return false;
  }
  if (IsStructMap()) {
    for (int i = 0; i < AsStructMap()->num_fields(); i++) {
      if (AsStructMap()->field(i) != nullptr &&
          !AsStructMap()->field(i)->EmptyInternal(annotation_spec_id)) {
        return false;
      }
    }
  } else if (IsArrayMap()) {
    if (AsArrayMap()->element() != nullptr &&
        !AsArrayMap()->element()->EmptyInternal(annotation_spec_id)) {
      return false;
    }
  }
  return true;
}

// Static
std::unique_ptr<AnnotationMap> AnnotationMap::Create(const Type* type) {
  if (type->IsStruct()) {
    return absl::WrapUnique(new StructAnnotationMap(type->AsStruct()));
  } else if (type->IsArray()) {
    return absl::WrapUnique(new ArrayAnnotationMap(type->AsArray()));
  } else {
    return absl::WrapUnique(new AnnotationMap());
  }
}

std::string AnnotationMap::DebugStringInternal(
    std::optional<int> annotation_spec_id) const {
  if (annotations_.empty()) {
    return "";
  }

  std::string out;
  if (annotation_spec_id.has_value()) {
    const SimpleValue* annotation = GetAnnotation(annotation_spec_id.value());
    if (annotation != nullptr) {
      out = annotation->DebugString();
    }
  } else {
    out = "{";
    absl::StrAppend(
        &out,
        absl::StrJoin(
            annotations_, ", ", [](std::string* out, const auto& pair) {
              std::string annotation_id;
              if (pair.first <=
                  static_cast<int>(AnnotationKind::kMaxBuiltinAnnotationKind)) {
                annotation_id = GetAnnotationKindName(
                    static_cast<AnnotationKind>(pair.first));
              } else {
                annotation_id = std::to_string(pair.first);
              }
              absl::StrAppend(out, annotation_id, ":",
                              pair.second.DebugString());
            }));
    absl::StrAppend(&out, "}");
  }
  return out;
}

StructAnnotationMap::StructAnnotationMap(const StructType* struct_type) {
  for (const StructField& field : struct_type->fields()) {
    fields_.push_back(AnnotationMap::Create(field.type));
  }
}

absl::Status StructAnnotationMap::Serialize(AnnotationMapProto* proto) const {
  // Serialize parent class AnnotationMap first.
  ZETASQL_RETURN_IF_ERROR(AnnotationMap::Serialize(proto));

  // Serialize annotation for each field.
  for (const auto& field : fields_) {
    auto* proto_field = proto->add_struct_fields();
    if (field == nullptr) {
      proto_field->set_is_null(true);
    } else {
      ZETASQL_RETURN_IF_ERROR(field->Serialize(proto_field));
    }
  }
  return absl::OkStatus();
}

std::string StructAnnotationMap::DebugStringInternal(
    std::optional<int> annotation_spec_id) const {
  std::string out(AnnotationMap::DebugStringInternal(annotation_spec_id));
  absl::StrAppend(&out, "<");
  for (int i = 0; i < num_fields(); i++) {
    std::string field_debug_string =
        field(i) == nullptr ? "_"
                            : field(i)->DebugStringInternal(annotation_spec_id);
    if (field_debug_string.empty() && !annotation_spec_id.has_value()) {
      field_debug_string = "{}";
    }

    absl::StrAppend(&out, field_debug_string);
    if (i != num_fields() - 1) {
      absl::StrAppend(&out, ",");
    }
  }
  absl::StrAppend(&out, ">");
  return out;
}

absl::Status StructAnnotationMap::CloneIntoField(int i,
                                                 const AnnotationMap* from) {
  ZETASQL_RET_CHECK_LT(i, num_fields());
  ZETASQL_RET_CHECK(HasCompatibleStructure(fields_[i].get(), from));
  if (from == nullptr) {
    fields_[i].reset(nullptr);
  } else {
    fields_[i] = from->Clone();
  }
  return absl::OkStatus();
}

absl::Status ArrayAnnotationMap::CloneIntoElement(const AnnotationMap* from) {
  ZETASQL_RET_CHECK(HasCompatibleStructure(element_.get(), from));
  if (from == nullptr) {
    element_.reset(nullptr);
  } else {
    element_ = from->Clone();
  }
  return absl::OkStatus();
}

ArrayAnnotationMap::ArrayAnnotationMap(const ArrayType* array_type) {
  element_ = AnnotationMap::Create(array_type->element_type());
}

std::string ArrayAnnotationMap::DebugStringInternal(
    std::optional<int> annotation_spec_id) const {
  std::string out(AnnotationMap::DebugStringInternal(annotation_spec_id));
  absl::StrAppend(&out, "[");
  std::string element_debug_string =
      element() == nullptr ? "_"
                           : element()->DebugStringInternal(annotation_spec_id);
  if (element_debug_string.empty() && !annotation_spec_id.has_value()) {
    element_debug_string = "{}";
  }
  absl::StrAppend(&out, element_debug_string);
  absl::StrAppend(&out, "]");
  return out;
}

absl::Status ArrayAnnotationMap::Serialize(AnnotationMapProto* proto) const {
  // Serialize parent class AnnotationMap first.
  ZETASQL_RETURN_IF_ERROR(AnnotationMap::Serialize(proto));

  // Serialize array element annotation.
  auto* array_element = proto->mutable_array_element();
  if (element_ == nullptr) {
    array_element->set_is_null(true);
  } else {
    ZETASQL_RETURN_IF_ERROR(element_->Serialize(array_element));
  }
  return absl::OkStatus();
}

}  // namespace zetasql
