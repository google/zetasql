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

#include <memory>
#include <string>

#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/struct_type.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

static std::string GetAnnotationKindName(AnnotationKind kind) {
  switch (kind) {
    case AnnotationKind::COLLATION:
      return "COLLATION";
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
zetasql_base::StatusOr<std::unique_ptr<AnnotationMap>> AnnotationMap::Deserialize(
    const AnnotationMapProto& proto) {
  std::unique_ptr<AnnotationMap> annotation_map;
  // Recursively handle struct fields and array element.
  if (proto.struct_fields_size() > 0) {
    annotation_map = absl::WrapUnique(new StructAnnotationMap());
    for (int i = 0; i < proto.struct_fields_size(); i++) {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<AnnotationMap> struct_field,
                       Deserialize(proto.struct_fields(i)));
      annotation_map->AsStructMap()->fields_.push_back(std::move(struct_field));
    }
  } else if (proto.has_array_element()) {
    annotation_map = absl::WrapUnique(new ArrayAnnotationMap());
    ZETASQL_ASSIGN_OR_RETURN(annotation_map->AsArrayMap()->element_,
                     Deserialize(proto.array_element()));
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

absl::Status AnnotationMap::CopyFrom(const AnnotationMap& that) {
  annotations_ = that.annotations_;

  if (IsStructMap()) {
    ZETASQL_RET_CHECK(that.IsStructMap());
    ZETASQL_RET_CHECK_EQ(AsStructMap()->num_fields(), that.AsStructMap()->num_fields());
    for (int i = 0; i < AsStructMap()->num_fields(); i++) {
      ZETASQL_RETURN_IF_ERROR(AsStructMap()->mutable_field(i)->CopyFrom(
          that.AsStructMap()->field(i)));
    }
  } else if (IsArrayMap()) {
    ZETASQL_RET_CHECK(that.IsArrayMap());
    ZETASQL_RETURN_IF_ERROR(AsArrayMap()->mutable_element()->CopyFrom(
        that.AsArrayMap()->element()));
  }
  return absl::OkStatus();
}

bool AnnotationMap::HasMatchingStructure(const Type* type) const {
  if (IsStructMap()) {
    if (!type->IsStruct() ||
        AsStructMap()->num_fields() != type->AsStruct()->num_fields()) {
      return false;
    }
    for (int i = 0; i < AsStructMap()->num_fields(); i++) {
      if (!AsStructMap()->field(i).HasMatchingStructure(
              type->AsStruct()->field(i).type)) {
        return false;
      }
    }
    return true;
  } else if (IsArrayMap()) {
    return type->AsArray() && AsArrayMap()->element().HasMatchingStructure(
                                  type->AsArray()->element_type());
  }
  return !type->IsStruct() && !type->IsArray();
}

int64_t AnnotationMap::GetEstimatedOwnedMemoryBytesSize() const {
  int64_t total_size = 0;
  for (const auto& annotation : annotations_) {
    total_size += sizeof(annotation.first) +
                  annotation.second.GetEstimatedOwnedMemoryBytesSize();
  }
  if (IsStructMap()) {
    for (int i = 0; i < AsStructMap()->num_fields(); i++) {
      total_size += sizeof(std::unique_ptr<AnnotationMap>) +
                    AsStructMap()->field(i).GetEstimatedOwnedMemoryBytesSize();
    }
  } else if (IsArrayMap()) {
    total_size += sizeof(std::unique_ptr<AnnotationMap>) +
                  AsArrayMap()->element().GetEstimatedOwnedMemoryBytesSize();
  }
  return total_size;
}

bool AnnotationMap::Equals(const AnnotationMap& that) const {
  if (IsStructMap()) {
    if (!that.IsStructMap() ||
        AsStructMap()->num_fields() != that.AsStructMap()->num_fields()) {
      return false;
    }
    for (int i = 0; i < AsStructMap()->num_fields(); i++) {
      if (!AsStructMap()->field(i).Equals(that.AsStructMap()->field(i))) {
        return false;
      }
    }
  } else if (IsArrayMap()) {
    if (!that.AsArrayMap() ||
        !AsArrayMap()->element().Equals(that.AsArrayMap()->element())) {
      return false;
    }
  }
  return true;
}

bool AnnotationMap::Empty() const {
  if (!annotations_.empty()) {
    return false;
  }
  if (IsStructMap()) {
    for (int i = 0; i < AsStructMap()->num_fields(); i++) {
      if (!AsStructMap()->field(i).Empty()) {
        return false;
      }
    }
  } else if (IsArrayMap()) {
    if (!AsArrayMap()->element().Empty()) {
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

std::string AnnotationMap::DebugString() const {
  if (annotations_.empty()) {
    return "";
  }
  std::string out("{");
  absl::StrAppend(
      &out,
      absl::StrJoin(annotations_, ", ", [](std::string* out, const auto& pair) {
        std::string annotation_id;
        if (pair.first <=
            static_cast<int>(AnnotationKind::kMaxBuiltinAnnotationKind)) {
          annotation_id =
              GetAnnotationKindName(static_cast<AnnotationKind>(pair.first));
        } else {
          annotation_id = std::to_string(pair.first);
        }
        absl::StrAppend(out, annotation_id, ":", pair.second.DebugString());
      }));
  absl::StrAppend(&out, "}");
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

  // TODO: only serialize STRUCT field if they are not empty.
  // Serialize annotation for each field.
  for (const auto& field : fields_) {
    ZETASQL_RETURN_IF_ERROR(field->Serialize(proto->add_struct_fields()));
  }
  return absl::OkStatus();
}

std::string StructAnnotationMap::DebugString() const {
  std::string out(AnnotationMap::DebugString());
  absl::StrAppend(&out, "<");
  for (int i = 0; i < num_fields(); i++) {
    std::string field_debug_string = field(i).DebugString();
    absl::StrAppend(&out,
                    field_debug_string.empty() ? "{}" : field_debug_string);
    if (i != num_fields() - 1) {
      absl::StrAppend(&out, ",");
    }
  }
  absl::StrAppend(&out, ">");
  return out;
}

ArrayAnnotationMap::ArrayAnnotationMap(const ArrayType* array_type) {
  element_ = AnnotationMap::Create(array_type->element_type());
}

std::string ArrayAnnotationMap::DebugString() const {
  std::string out(AnnotationMap::DebugString());
  absl::StrAppend(&out, "[");
  absl::StrAppend(&out, element().DebugString());
  absl::StrAppend(&out, "]");
  return out;
}

absl::Status ArrayAnnotationMap::Serialize(AnnotationMapProto* proto) const {
  // Serialize parent class AnnotationMap first.
  ZETASQL_RETURN_IF_ERROR(AnnotationMap::Serialize(proto));

  // Serialize array element annotation.
  ZETASQL_RETURN_IF_ERROR(element_->Serialize(proto->mutable_array_element()));
  return absl::OkStatus();
}

}  // namespace zetasql
