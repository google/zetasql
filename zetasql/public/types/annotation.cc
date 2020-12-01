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
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// static
AnnotationValue AnnotationValue::String(std::string v) {
  return AnnotationValue(TYPE_STRING, std::move(v));
}

// static
AnnotationValue AnnotationValue::Int64(int64_t v) {
  return AnnotationValue(TYPE_INT64, v);
}

AnnotationValue::AnnotationValue(AnnotationValue&& that) {
  // NOLINTNEXTLINE - suppress clang-tidy warning on not TriviallyCopyable.
  memcpy(this, &that, sizeof(AnnotationValue));
  // Invalidate 'that' to disable its destructor.
  that.type_ = TYPE_INVALID;
}

AnnotationValue& AnnotationValue::operator=(const AnnotationValue& that) {
  // Self-copying must not clear the contents of the value.
  if (this == &that) {
    return *this;
  }
  Clear();
  CopyFrom(that);
  return *this;
}

AnnotationValue& AnnotationValue::operator=(AnnotationValue&& that) {
  Clear();
  // NOLINTNEXTLINE - suppress clang-tidy warning on not TriviallyCopyable.
  memcpy(this, &that, sizeof(AnnotationValue));
  // Invalidate 'that' to disable its destructor.
  that.type_ = TYPE_INVALID;
  return *this;
}

void AnnotationValue::Clear() {
  switch (type_) {
    case TYPE_STRING:
      string_ptr_->Unref();
      break;
    case TYPE_INVALID:
    case TYPE_INT64:
      // Nothing to clear.
      break;
    default:
      ZETASQL_CHECK(false) << "All ValueType must be explicitly handled in Clear()";
  }
  type_ = TYPE_INVALID;
}

void AnnotationValue::CopyFrom(const AnnotationValue& that) {
  // Self-copy check is done in the copy constructor. Here we just ZETASQL_DCHECK that.
  ZETASQL_DCHECK_NE(this, &that);
  // NOLINTNEXTLINE - suppress clang-tidy warning on not TriviallyCopyable.
  memcpy(this, &that, sizeof(AnnotationValue));
  if (!IsValid()) {
    return;
  }
  switch (type_) {
    case TYPE_STRING:
      string_ptr_->Ref();
      break;
    case TYPE_INVALID:
    case TYPE_INT64:
      // memcpy() has copied all the data.
      break;
  }
}

int64_t AnnotationValue::int64_value() const {
  ZETASQL_CHECK(has_int64_value()) << "Not an int64_t value";
  return int64_value_;
}

const std::string& AnnotationValue::string_value() const {
  ZETASQL_CHECK(has_string_value()) << "Not a string value";
  return string_ptr_->value();
}

absl::Status AnnotationValue::Serialize(AnnotationProto* proto) const {
  switch (type_) {
    case AnnotationValue::TYPE_INVALID:
      ZETASQL_RET_CHECK_FAIL()
          << "AnnotationValue with TYPE_INVALID cannot be serialized";
      break;
    case AnnotationValue::TYPE_INT64:
      proto->set_int64_value(int64_value());
      break;
    case AnnotationValue::TYPE_STRING:
      proto->set_string_value(string_value());
      break;
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unknown ValueType: " << type_;
  }
  return absl::OkStatus();
}

// static
zetasql_base::StatusOr<AnnotationValue> AnnotationValue::Deserialize(
    const AnnotationProto& proto) {
  AnnotationValue value;
  switch (proto.value_case()) {
    case AnnotationProto::kInt64Value:
      value = AnnotationValue::Int64(proto.int64_value());
      break;
    case AnnotationProto::kStringValue:
      value = AnnotationValue::String(proto.string_value());
      break;
    case AnnotationProto::VALUE_NOT_SET:
      ZETASQL_RET_CHECK_FAIL() << "No value set on AnnotationProto::value";
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unknown annotationProto.value_case():"
                       << proto.value_case();
  }
  return value;
}

bool AnnotationValue::Equals(const AnnotationValue& that) const {
  if (type_ != that.type_) {
    return false;
  }
  switch (type_) {
    case TYPE_INT64:
      return int64_value_ == that.int64_value();
    case TYPE_STRING:
      return string_value() == that.string_value();
    case TYPE_INVALID:
      return true;
  }
}

std::string AnnotationValue::DebugString() const {
  switch (type_) {
    case TYPE_INT64:
      return std::to_string(int64_value());
    case TYPE_STRING:
      return absl::StrCat("\"", string_value(), "\"");
    case TYPE_INVALID:
      return "<INVALID>";
  }
}

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
    ZETASQL_RETURN_IF_ERROR(annotation_pair.second.Serialize(annotation_proto));
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
    ZETASQL_ASSIGN_OR_RETURN(AnnotationValue value,
                     AnnotationValue::Deserialize(annotation_proto));
    annotation_map->SetAnnotation(static_cast<int>(annotation_proto.id()),
                                  value);
  }
  return annotation_map;
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
  bool first = true;
  for (const auto& pair : annotations_) {
    if (first) {
      first = false;
    } else {
      absl::StrAppend(&out, " ,");
    }
    std::string annotation_id;
    if (pair.first <=
        static_cast<int>(AnnotationKind::kMaxBuiltinAnnotationKind)) {
      annotation_id =
          GetAnnotationKindName(static_cast<AnnotationKind>(pair.first));
    } else {
      annotation_id = std::to_string(pair.first);
    }
    absl::StrAppend(&out, annotation_id, ":", pair.second.DebugString());
  }
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
    absl::StrAppend(&out, field(i).DebugString());
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
