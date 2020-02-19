//
// Copyright 2019 ZetaSQL Authors
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

#include "zetasql/public/types/enum_type.h"

#include "zetasql/public/strings.h"
#include "zetasql/public/types/internal_utils.h"

namespace zetasql {

EnumType::EnumType(const TypeFactory* factory,
                   const google::protobuf::EnumDescriptor* enum_descr)
    : Type(factory, TYPE_ENUM), enum_descriptor_(enum_descr) {
  CHECK(enum_descriptor_ != nullptr);
}

EnumType::~EnumType() {
}

const google::protobuf::EnumDescriptor* EnumType::enum_descriptor() const {
  return enum_descriptor_;
}

zetasql_base::Status EnumType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    TypeProto* type_proto,
    absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  type_proto->set_type_kind(kind_);
  EnumTypeProto* enum_type_proto = type_proto->mutable_enum_type();
  enum_type_proto->set_enum_name(enum_descriptor_->full_name());
  enum_type_proto->set_enum_file_name(enum_descriptor_->file()->name());
  // Note that right now we are not supporting TypeProto extensions.  The
  // FileDescriptorSet can be derived from the enum descriptor's FileDescriptor
  // dependencies.
  int set_index;
  ZETASQL_RETURN_IF_ERROR(internal::PopulateDistinctFileDescriptorSets(
      enum_descriptor_->file(), file_descriptor_sets_max_size_bytes,
      file_descriptor_set_map, &set_index));
  if (set_index != 0) {
    enum_type_proto->set_file_descriptor_set_index(set_index);
  }
  return ::zetasql_base::OkStatus();
}

std::string EnumType::TypeName() const {
  return ToIdentifierLiteral(enum_descriptor_->full_name());
}

std::string EnumType::ShortTypeName(ProductMode mode_unused) const {
  // Special case for built-in zetasql enums. Since ShortTypeName is used in
  // the user facing error messages, we need to make these enum names look
  // as special language elements.
  if (enum_descriptor()->full_name() ==
      "zetasql.functions.DateTimestampPart") {
    return "DATE_TIME_PART";
  } else if (enum_descriptor()->full_name() ==
      "zetasql.functions.NormalizeMode") {
    return "NORMALIZE_MODE";
  }
  return enum_descriptor_->full_name();
}

std::string EnumType::TypeName(ProductMode mode_unused) const {
  return TypeName();
}

bool EnumType::FindName(int number, const std::string** name) const {
  *name = nullptr;
  const google::protobuf::EnumValueDescriptor* value_descr =
      enum_descriptor_->FindValueByNumber(number);
  if (value_descr == nullptr) {
    return false;
  }
  *name = &value_descr->name();
  return true;
}

bool EnumType::FindNumber(const std::string& name, int* number) const {
  const google::protobuf::EnumValueDescriptor* value_descr =
      enum_descriptor_->FindValueByName(name);
  if (value_descr == nullptr) {
    *number = std::numeric_limits<int32_t>::min();
    return false;
  }
  *number = value_descr->number();
  return true;
}

bool EnumType::EqualsImpl(const EnumType* const type1,
                          const EnumType* const type2, bool equivalent) {
  if (type1->enum_descriptor() == type2->enum_descriptor()) {
    return true;
  }
  if (equivalent &&
      type1->enum_descriptor()->full_name() ==
      type2->enum_descriptor()->full_name()) {
    return true;
  }
  return false;
}

}  // namespace zetasql
