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

#include "zetasql/public/types/map_type.h"

#include <algorithm>
#include <string>
#include <utility>


#include "zetasql/base/logging.h"
#include "zetasql/common/thread_stack.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/types/type_modifiers.h"
#include "zetasql/public/types/value_equality_check_options.h"
#include "zetasql/public/types/value_representations.h"
#include "zetasql/public/value.pb.h"
#include "zetasql/public/value_content.h"
#include "absl/hash/hash.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

std::string MapType::ShortTypeName(ProductMode mode,
                                   bool use_external_float32) const {
  return absl::StrCat(
      "MAP<", key_type_->ShortTypeName(mode, use_external_float32), ", ",
      value_type_->ShortTypeName(mode, use_external_float32), ">");
}

std::string MapType::TypeName(ProductMode mode,
                              bool use_external_float32) const {
  return absl::StrCat("MAP<", key_type_->TypeName(mode, use_external_float32),
                      ", ", value_type_->TypeName(mode, use_external_float32),
                      ">");
}

absl::StatusOr<std::string> MapType::TypeNameWithModifiers(
    const TypeModifiers& type_modifiers, ProductMode mode,
    bool use_external_float32) const {
  // TODO: Implement TypeNameWithModifiers.
  return absl::UnimplementedError(
      "MapType::TypeNameWithModifiers is not yet supported.");
}

bool MapType::SupportsOrdering(const LanguageOptions& language_options,
                               std::string* type_description) const {
  return false;
}
bool MapType::SupportsEquality() const { return false; }

bool MapType::IsSupportedType(const LanguageOptions& language_options) const {
  return language_options.LanguageFeatureEnabled(FEATURE_V_1_4_MAP_TYPE) &&
         key_type_->IsSupportedType(language_options) &&
         key_type_->SupportsGrouping(language_options) &&
         value_type_->IsSupportedType(language_options);
}

int MapType::nesting_depth() const {
  return std::max(key_type_->nesting_depth(), value_type_->nesting_depth()) + 1;
}

MapType::MapType(const TypeFactory* factory, const Type* key_type,
                 const Type* value_type)
    : Type(factory, TYPE_MAP), key_type_(key_type), value_type_(value_type) {}
MapType::~MapType() = default;

bool MapType::SupportsGroupingImpl(const LanguageOptions& language_options,
                                   const Type** no_grouping_type) const {
  // Map does not currently support grouping. (broken link).
  *no_grouping_type = this;
  return false;
}

bool MapType::SupportsPartitioningImpl(
    const LanguageOptions& language_options,
    const Type** no_partitioning_type) const {
  // Map does not currently support partitioning. (broken link).
  *no_partitioning_type = this;
  return false;
}

absl::Status MapType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    const BuildFileDescriptorSetMapOptions& options, TypeProto* type_proto,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  type_proto->set_type_kind(kind_);
  ZETASQL_RETURN_IF_ERROR(key_type()->SerializeToProtoAndDistinctFileDescriptorsImpl(
      options, type_proto->mutable_map_type()->mutable_key_type(),
      file_descriptor_set_map));
  return value_type()->SerializeToProtoAndDistinctFileDescriptorsImpl(
      options, type_proto->mutable_map_type()->mutable_value_type(),
      file_descriptor_set_map);
}

bool MapType::EqualsForSameKind(const Type* that, bool equivalent) const {
  const MapType* other = that->AsMap();
  ABSL_DCHECK(other != nullptr)
      << DebugString() << "::EqualsForSameKind cannot compare to non-MAP type "
      << that->DebugString();
  return this->key_type()->EqualsImpl(other->key_type(), equivalent) &&
         this->value_type()->EqualsImpl(other->value_type(), equivalent);
}

void MapType::DebugStringImpl(bool details, TypeOrStringVector* stack,
                              std::string* debug_string) const {
  absl::StrAppend(debug_string, "MAP<");
  stack->push_back(">");
  stack->push_back(value_type());
  stack->push_back(", ");
  stack->push_back(key_type());
}

void MapType::CopyValueContent(const ValueContent& from,
                               ValueContent* to) const {
  from.GetAs<internal::ValueContentMapRef*>()->Ref();
  *to = from;
}

void MapType::ClearValueContent(const ValueContent& value) const {
  value.GetAs<internal::ValueContentMapRef*>()->Unref();
}

absl::HashState MapType::HashTypeParameter(absl::HashState state) const {
  return value_type()->Hash(key_type()->Hash(std::move(state)));
}

absl::HashState MapType::HashValueContent(const ValueContent& value,
                                          absl::HashState state) const {
  // TODO: Implement HashValueContent.
  ABSL_LOG(FATAL) << "HashValueContent is not yet "  // Crash OK
                "supported on MapType.";
}

bool MapType::ValueContentEquals(
    const ValueContent& x, const ValueContent& y,
    const ValueEqualityCheckOptions& options) const {
  // Map does not currently support equality. (broken link).
  return false;
}

absl::Status MapType::SerializeValueContent(const ValueContent& value,
                                            ValueProto* value_proto) const {
  // TODO: Implement SerializeValueContent.
  return absl::UnimplementedError(
      "SerializeValueContent is not yet supported.");
}

absl::Status MapType::DeserializeValueContent(const ValueProto& value_proto,
                                              ValueContent* value) const {
  // TODO: Implement DeserializeValueContent.
  return absl::UnimplementedError(
      "DeserializeValueContent is not yet supported.");
}

bool MapType::ValueContentLess(const ValueContent& x, const ValueContent& y,
                               const Type* other_type) const {
  ABSL_LOG(FATAL) << "Cannot compare " << DebugString() << " to "  // Crash OK
             << other_type->DebugString();
  return false;
}

// TODO: When we add non-debug printing, should refactor to
// be implemented alongside existing ARRAY and STRUCT print logic.
std::string MapType::FormatValueContent(
    const ValueContent& value, const FormatValueContentOptions& options) const {

  // For now, print only the map size if we are not in debug mode.
  // TODO: determine a stable literal syntax for MAP type.
  if (options.mode != Type::FormatValueContentOptions::Mode::kDebug) {
    ABSL_LOG(ERROR) << "Map printing not yet implemented.";
    return "Map printing not yet implemented.";
  }

  std::string result = "{";
  internal::ValueContentMap* value_content_map =
      value.GetAs<internal::ValueContentMapRef*>()->value();
  auto map_entries = value_content_map->value_content_entries();

  absl::StrAppend(
      &result, absl::StrJoin(
                   map_entries, ", ",
                   [options, this](std::string* out, const auto& map_entry) {
                     auto& [key, value] = map_entry;
                     std::string key_str = FormatValueContentContainerElement(
                         key, this->key_type_, options);
                     std::string value_str = FormatValueContentContainerElement(
                         value, this->value_type_, options);
                     absl::StrAppend(out, key_str, ": ", value_str);
                   }));
  absl::StrAppend(&result, "}");
  return result;
}

const Type* GetMapKeyType(const Type* map_type) {
  return static_cast<const MapType*>(map_type)->key_type();
}
const Type* GetMapValueType(const Type* map_type) {
  return static_cast<const MapType*>(map_type)->value_type();
}

}  // namespace zetasql
