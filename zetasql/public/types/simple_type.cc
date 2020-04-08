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

#include "zetasql/public/types/simple_type.h"

#include "zetasql/public/language_options.h"
#include "zetasql/public/types/value_representations.h"
#include "zetasql/public/value_content.h"

namespace zetasql {

namespace {
const std::map<std::string, TypeKind>& SimpleTypeKindMap() {
  static auto result = new std::map<std::string, TypeKind>{
      {"int32", zetasql::TYPE_INT32},
      {"uint32", zetasql::TYPE_UINT32},
      {"int64", zetasql::TYPE_INT64},
      {"uint64", zetasql::TYPE_UINT64},
      {"bool", zetasql::TYPE_BOOL},
      {"boolean", zetasql::TYPE_BOOL},
      {"float", zetasql::TYPE_FLOAT},
      {"float32", zetasql::TYPE_FLOAT},
      {"float64", zetasql::TYPE_DOUBLE},
      {"double", zetasql::TYPE_DOUBLE},
      {"bytes", zetasql::TYPE_BYTES},
      {"string", zetasql::TYPE_STRING},
      {"date", zetasql::TYPE_DATE},
      {"timestamp", zetasql::TYPE_TIMESTAMP},
      {"time", zetasql::TYPE_TIME},
      {"datetime", zetasql::TYPE_DATETIME},
      {"geography", zetasql::TYPE_GEOGRAPHY},
      {"numeric", zetasql::TYPE_NUMERIC},
      {"bignumeric", zetasql::TYPE_BIGNUMERIC},
  };
  return *result;
}

// See (broken link) for approved list of externally visible
// types.
const std::map<std::string, TypeKind>& ExternalModeSimpleTypeKindMap() {
  static auto result = new std::map<std::string, TypeKind>{
      {"int64", zetasql::TYPE_INT64},
      {"bool", zetasql::TYPE_BOOL},
      {"boolean", zetasql::TYPE_BOOL},
      {"float64", zetasql::TYPE_DOUBLE},
      {"bytes", zetasql::TYPE_BYTES},
      {"string", zetasql::TYPE_STRING},
      {"date", zetasql::TYPE_DATE},
      {"timestamp", zetasql::TYPE_TIMESTAMP},
      {"time", zetasql::TYPE_TIME},
      {"datetime", zetasql::TYPE_DATETIME},
      {"geography", zetasql::TYPE_GEOGRAPHY},
      {"numeric", zetasql::TYPE_NUMERIC},
      {"bignumeric", zetasql::TYPE_BIGNUMERIC},
  };
  return *result;
}

const std::set<TypeKind> ExternalModeSimpleTypeKinds() {
  std::set<TypeKind> external_mode_simple_type_kinds;
  for (const auto& external_simple_type : ExternalModeSimpleTypeKindMap()) {
    // Note that kExternalModeSimpleTypeKindMap has duplicate TypeKinds, so
    // we use InsertIfNotPresent() here.
    zetasql_base::InsertIfNotPresent(&external_mode_simple_type_kinds,
                            external_simple_type.second);
  }
  return external_mode_simple_type_kinds;
}

const std::set<TypeKind>& GetExternalModeSimpleTypeKinds() {
  // We populate this set once and use it ever after.
  static const std::set<TypeKind>* kExternalModeSimpleTypeKinds =
      new std::set<TypeKind>(ExternalModeSimpleTypeKinds());
  return *kExternalModeSimpleTypeKinds;
}

}  // namespace

SimpleType::SimpleType(const TypeFactory* factory, TypeKind kind)
    : Type(factory, kind) {
  CHECK(IsSimpleType(kind)) << kind;
}

SimpleType::~SimpleType() {
}

bool SimpleType::IsSupportedType(
    const LanguageOptions& language_options) const {
  const ProductMode mode = language_options.product_mode();
  if (mode == ProductMode::PRODUCT_EXTERNAL) {
    if (!zetasql_base::ContainsKey(GetExternalModeSimpleTypeKinds(), kind())) {
      return false;
    }
  }
  if (IsFeatureV12CivilTimeType() &&
      !language_options.LanguageFeatureEnabled(FEATURE_V_1_2_CIVIL_TIME)) {
    return false;
  }
  if (kind() == TYPE_GEOGRAPHY &&
      !language_options.LanguageFeatureEnabled(FEATURE_GEOGRAPHY)) {
    return false;
  }
  if (kind() == TYPE_NUMERIC &&
      !language_options.LanguageFeatureEnabled(FEATURE_NUMERIC_TYPE)) {
    return false;
  }
  if (kind() == TYPE_BIGNUMERIC &&
      !language_options.LanguageFeatureEnabled(FEATURE_BIGNUMERIC_TYPE)) {
    return false;
  }
  return true;
}

void SimpleType::DebugStringImpl(bool details, TypeOrStringVector* stack,
                                 std::string* debug_string) const {
  absl::StrAppend(debug_string, TypeKindToString(kind(), PRODUCT_INTERNAL));
}

absl::Status SimpleType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    TypeProto* type_proto,
    absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
    FileDescriptorSetMap* file_descriptor_set_map)
    const {
  type_proto->set_type_kind(kind_);
  return absl::OkStatus();
}

std::string SimpleType::TypeName(ProductMode mode) const {
  return TypeKindToString(kind_, mode);
}

bool SimpleType::GetSimpleTypeKindByName(const std::string& type_name,
                                         ProductMode mode, TypeKind* result) {
  const std::map<std::string, TypeKind>& map =
      (mode == PRODUCT_EXTERNAL) ? ExternalModeSimpleTypeKindMap()
                                 : SimpleTypeKindMap();
  auto it = map.find(absl::AsciiStrToLower(type_name));
  if (it == map.end()) {
    return false;
  }
  *result = it->second;
  return true;
}

bool SimpleType::SupportsGroupingImpl(const LanguageOptions& language_options,
                                      const Type** no_grouping_type) const {
  const bool supports_grouping =
      !this->IsGeography() &&
      !(this->IsFloatingPoint() && language_options.LanguageFeatureEnabled(
                                       FEATURE_DISALLOW_GROUP_BY_FLOAT));
  if (no_grouping_type != nullptr) {
    *no_grouping_type = supports_grouping ? nullptr : this;
  }
  return supports_grouping;
}

// TODO: create a template to parameterize the type of simple type
// value's content and use this template to split SimpleType into subclasses to
// avoid TypeKind switch statements.
static bool DoesValueContentUseSimpleReferenceCounted(TypeKind kind) {
  switch (kind) {
    case TYPE_STRING:
    case TYPE_BYTES:
    case TYPE_GEOGRAPHY:
    case TYPE_NUMERIC:
    case TYPE_BIGNUMERIC:
      return true;
    default:
      return false;
  }
}

void SimpleType::InitializeValueContent(ValueContent* value) const {
  switch (kind_) {
    case TYPE_STRING:
    case TYPE_BYTES:
      value->set(new internal::StringRef());
      break;
    case TYPE_GEOGRAPHY:
      value->set(new internal::GeographyRef());
      break;
    case TYPE_NUMERIC:
      value->set(new internal::NumericRef());
      break;
    case TYPE_BIGNUMERIC:
      value->set(new internal::BigNumericRef());
      break;
    default:
      break;
  }
}

void SimpleType::CopyValueContent(const ValueContent& from,
                                  ValueContent* to) const {
  if (DoesValueContentUseSimpleReferenceCounted(kind_)) {
    from.GetAs<zetasql_base::SimpleReferenceCounted*>()->Ref();
  }

  *to = from;
}

void SimpleType::ClearValueContent(const ValueContent& value) const {
  if (DoesValueContentUseSimpleReferenceCounted(kind_)) {
    value.GetAs<zetasql_base::SimpleReferenceCounted*>()->Unref();
  }
}

uint64_t SimpleType::GetValueContentExternallyAllocatedByteSize(
    const ValueContent& value) const {
  switch (kind_) {
    case TYPE_STRING:
    case TYPE_BYTES:
      return value.GetAs<internal::StringRef*>()->physical_byte_size();
    case TYPE_GEOGRAPHY:
      return value.GetAs<internal::GeographyRef*>()->physical_byte_size();
    case TYPE_NUMERIC:
      return sizeof(internal::NumericRef);
    case TYPE_BIGNUMERIC:
      return sizeof(internal::BigNumericRef);
    default:
      return 0;
  }
}

}  // namespace zetasql
