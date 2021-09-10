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

#include "zetasql/public/types/enum_type.h"

#include <cstdint>
#include <limits>
#include <string>
#include <utility>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/internal_utils.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.pb.h"
#include "zetasql/public/value_content.h"
#include "absl/algorithm/container.h"
#include <cstdint>
#include "absl/container/inlined_vector.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// Gets an enumerator value from the given enumeration value content.
static int32_t GetEnumValue(const ValueContent& value) {
  return value.GetAs<int32_t>();
}

EnumType::EnumType(const TypeFactory* factory,
                   const google::protobuf::EnumDescriptor* enum_descr,
                   const internal::CatalogName* catalog_name)
    : Type(factory, TYPE_ENUM),
      enum_descriptor_(enum_descr),
      catalog_name_(catalog_name) {
  ZETASQL_CHECK(enum_descriptor_ != nullptr);
}

EnumType::~EnumType() {
}

bool EnumType::EqualsForSameKind(const Type* that, bool equivalent) const {
  const EnumType* other = that->AsEnum();
  ZETASQL_DCHECK(other);
  return EnumType::EqualsImpl(this, other, equivalent);
}

void EnumType::DebugStringImpl(bool details, TypeOrStringVector* stack,
                               std::string* debug_string) const {
  if (catalog_name_ != nullptr) {
    absl::StrAppend(debug_string, *catalog_name_->path_string, ".");
  }

  absl::StrAppend(debug_string, "ENUM<", enum_descriptor_->full_name());
  if (details) {
    absl::StrAppend(debug_string,
                    ", file name: ", enum_descriptor_->file()->name(), ", <",
                    enum_descriptor_->DebugString(), ">");
  }
  absl::StrAppend(debug_string, ">");
}

const google::protobuf::EnumDescriptor* EnumType::enum_descriptor() const {
  return enum_descriptor_;
}

absl::Status EnumType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    const BuildFileDescriptorSetMapOptions& options, TypeProto* type_proto,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  type_proto->set_type_kind(kind());
  EnumTypeProto* enum_type_proto = type_proto->mutable_enum_type();
  enum_type_proto->set_enum_name(enum_descriptor_->full_name());
  enum_type_proto->set_enum_file_name(enum_descriptor_->file()->name());
  // Note that right now we are not supporting TypeProto extensions.  The
  // FileDescriptorSet can be derived from the enum descriptor's FileDescriptor
  // dependencies.
  int set_index;
  ZETASQL_RETURN_IF_ERROR(internal::PopulateDistinctFileDescriptorSets(
      options, enum_descriptor_->file(), file_descriptor_set_map, &set_index));
  if (set_index != 0) {
    enum_type_proto->set_file_descriptor_set_index(set_index);
  }

  if (catalog_name_ != nullptr) {
    absl::c_copy(catalog_name_->path,
                 google::protobuf::RepeatedFieldBackInserter(
                     enum_type_proto->mutable_catalog_name_path()));
  }

  return absl::OkStatus();
}

std::string EnumType::TypeName() const {
  std::string catalog_name_path;
  if (catalog_name_ != nullptr) {
    absl::StrAppend(&catalog_name_path, *catalog_name_->path_string, ".");
  }

  return absl::StrCat(catalog_name_path,
                      ToIdentifierLiteral(enum_descriptor_->full_name()));
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

  std::string catalog_name_path;
  if (catalog_name_ != nullptr) {
    absl::StrAppend(&catalog_name_path, *catalog_name_->path_string, ".");
  }

  absl::StrAppend(&catalog_name_path, enum_descriptor_->full_name());

  return catalog_name_path;
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

absl::Span<const std::string> EnumType::CatalogNamePath() const {
  if (catalog_name_ == nullptr) {
    return {};
  } else {
    return catalog_name_->path;
  }
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
  const internal::CatalogName* catalog_name1 = type1->catalog_name_;
  const internal::CatalogName* catalog_name2 = type2->catalog_name_;
  const bool catalogs_are_empty =
      catalog_name1 == nullptr && catalog_name2 == nullptr;
  const bool catalogs_are_equal =
      catalog_name1 != nullptr && catalog_name2 != nullptr &&
      *catalog_name1->path_string == *catalog_name2->path_string;

  if (type1->enum_descriptor() == type2->enum_descriptor() &&
      (catalogs_are_empty || catalogs_are_equal)) {
    return true;
  }

  if (equivalent &&
      type1->enum_descriptor()->full_name() ==
      type2->enum_descriptor()->full_name()) {
    return true;
  }
  return false;
}

bool EnumType::IsSupportedType(const LanguageOptions& language_options) const {
  // Enums are generally unsupported in EXTERNAL mode, except for builtin enums
  // such as the DateTimestampPart enum that is used in many of the date/time
  // related functions.
  if (language_options.product_mode() == ProductMode::PRODUCT_EXTERNAL &&
      !Equivalent(types::DatePartEnumType()) &&
      !Equivalent(types::NormalizeModeEnumType())) {
    return false;
  }
  return true;
}

absl::HashState EnumType::HashTypeParameter(absl::HashState state) const {
  // Enum types are equivalent if they have the same full name, so hash it.
  return absl::HashState::combine(std::move(state),
                                  enum_descriptor()->full_name());
}

absl::HashState EnumType::HashValueContent(const ValueContent& value,
                                           absl::HashState state) const {
  return absl::HashState::combine(std::move(state), GetEnumValue(value));
}

bool EnumType::ValueContentEquals(
    const ValueContent& x, const ValueContent& y,
    const ValueEqualityCheckOptions& options) const {
  return GetEnumValue(x) == GetEnumValue(y);
}

bool EnumType::ValueContentLess(const ValueContent& x, const ValueContent& y,
                                const Type* other_type) const {
  return GetEnumValue(x) < GetEnumValue(y);
}

std::string EnumType::FormatValueContent(
    const ValueContent& value, const FormatValueContentOptions& options) const {
  const std::string* enum_name = nullptr;
  int32_t enum_value = GetEnumValue(value);
  ZETASQL_CHECK(FindName(enum_value, &enum_name))
      << "Value " << enum_value << " not in "
      << enum_descriptor()->DebugString();

  if (options.mode == FormatValueContentOptions::Mode::kDebug) {
    return options.verbose ? absl::StrCat(*enum_name, ":", enum_value)
                           : *enum_name;
  }

  std::string literal = ToStringLiteral(*enum_name);
  return options.as_literal() ? literal
                              : internal::GetCastExpressionString(
                                    literal, this, options.product_mode);
}

absl::Status EnumType::SerializeValueContent(const ValueContent& value,
                                             ValueProto* value_proto) const {
  value_proto->set_enum_value(GetEnumValue(value));
  return absl::OkStatus();
}

absl::Status EnumType::DeserializeValueContent(const ValueProto& value_proto,
                                               ValueContent* value) const {
  if (!value_proto.has_enum_value()) {
    return TypeMismatchError(value_proto);
  }

  if (enum_descriptor()->FindValueByNumber(value_proto.enum_value()) ==
      nullptr) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        absl::StrCat("Invalid value for ", DebugString(), ": ",
                                     value_proto.enum_value()));
  }

  value->set(value_proto.enum_value());

  return absl::OkStatus();
}

}  // namespace zetasql
