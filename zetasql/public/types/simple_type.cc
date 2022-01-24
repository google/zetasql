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

#include "zetasql/public/types/simple_type.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/timestamp.pb.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/float_margin.h"
#include "zetasql/common/string_util.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/interval_value.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/type_parameters.pb.h"
#include "zetasql/public/types/internal_utils.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/types/value_representations.h"
#include "zetasql/public/value.pb.h"
#include "zetasql/public/value_content.h"
#include <cstdint>
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/time_proto_util.h"

namespace zetasql {

// The types below are used to represent the C++ type of a main value stored in
// ValueContent for a particular ZetaSQL type.
// Note: in addition to their main value content simple types TYPE_TIMESTAMP,
// TYPE_TIME and TYPE_DATETIME also have extended content stored in
// ValueContent::simple_type_extended_content_. The C++ type of this extended
// content is fixed (int32_t).
using DateValueContentType = int32_t;       // Used with TYPE_DATE.
using TimestampValueContentType = int64_t;  // Used with TYPE_TIMESTAMP.
using TimeValueContentType = int32_t;       // Used with TYPE_TIME.
using DateTimeValueContentType = int64_t;   // Used with TYPE_DATETIME.

namespace {

// Constants for NUMERIC / BIGNUMERIC type parameters.
constexpr int kNumericMaxPrecision = 29;
constexpr int kBigNumericMaxPrecision = 38;
constexpr int kNumericMaxScale = 9;
constexpr int kBigNumericMaxScale = 38;

// Specifies the type kind that a type name maps to, and when the type name is
// enabled. Even when the type name is enabled, the type kind might still be
// disabled (controlled by TypeKindInfo). The type name can be used only if both
// are enabled.
struct TypeNameInfo {
  TypeKind type_kind;
  // If false, this type name can be used in both the internal mode and the
  // external mode. If true, this type name can be used in the internal mode
  // only.
  bool internal_product_mode_only = false;
  // If present, then the feature controls whether the type name is enabled.
  // If absent, then the type name does not require any language feature.
  absl::optional<LanguageFeature> alias_feature;
};

const std::map<absl::string_view, TypeNameInfo>& SimpleTypeNameInfoMap() {
  static auto result = new std::map<absl::string_view, TypeNameInfo>{
      {"int32", {TYPE_INT32, true}},
      {"uint32", {TYPE_UINT32, true}},
      {"int64", {TYPE_INT64}},
      {"uint64", {TYPE_UINT64, true}},
      {"bool", {TYPE_BOOL}},
      {"boolean", {TYPE_BOOL}},
      {"float", {TYPE_FLOAT, true}},
      {"float32", {TYPE_FLOAT, true}},
      {"float64", {TYPE_DOUBLE}},
      {"double", {TYPE_DOUBLE, true}},
      {"bytes", {TYPE_BYTES}},
      {"string", {TYPE_STRING}},
      {"date", {TYPE_DATE}},
      {"timestamp", {TYPE_TIMESTAMP}},
      {"time", {TYPE_TIME}},
      {"datetime", {TYPE_DATETIME}},
      {"interval", {TYPE_INTERVAL}},
      {"geography", {TYPE_GEOGRAPHY}},
      {"numeric", {TYPE_NUMERIC}},
      {"decimal", {TYPE_NUMERIC, false, FEATURE_V_1_3_DECIMAL_ALIAS}},
      {"bignumeric", {TYPE_BIGNUMERIC}},
      {"bigdecimal", {TYPE_BIGNUMERIC, false, FEATURE_V_1_3_DECIMAL_ALIAS}},
      {"json", {TYPE_JSON}},
  };
  return *result;
}

// Specifies when the type kind is enabled.
struct TypeKindInfo {
  // If true, this type kind can be used in both the internal mode and the
  // external mode. If false, this type kind can be used in the internal mode
  // only .
  bool internal_product_mode_only = false;
  // If present, then the feature controls whether the type kind is enabled.
  // If absent, then the type kind does not require any language feature.
  absl::optional<LanguageFeature> type_feature;
};

const std::map<TypeKind, TypeKindInfo>& SimpleTypeKindInfoMap() {
  static auto result = new std::map<TypeKind, TypeKindInfo>{
      {TYPE_INT32, {true}},
      {TYPE_UINT32, {true}},
      {TYPE_INT64, {}},
      {TYPE_UINT64, {true}},
      {TYPE_BOOL, {}},
      {TYPE_FLOAT, {true}},
      {TYPE_DOUBLE, {}},
      {TYPE_BYTES, {}},
      {TYPE_STRING, {}},
      {TYPE_DATE, {}},
      {TYPE_TIMESTAMP, {}},
      {TYPE_TIME, {false, FEATURE_V_1_2_CIVIL_TIME}},
      {TYPE_DATETIME, {false, FEATURE_V_1_2_CIVIL_TIME}},
      {TYPE_INTERVAL, {false, FEATURE_INTERVAL_TYPE}},
      {TYPE_GEOGRAPHY, {false, FEATURE_GEOGRAPHY}},
      {TYPE_NUMERIC, {false, FEATURE_NUMERIC_TYPE}},
      {TYPE_BIGNUMERIC, {false, FEATURE_BIGNUMERIC_TYPE}},
      {TYPE_JSON, {false, FEATURE_JSON_TYPE}},
  };
  return *result;
}

// Joined result of TypeNameInfo and TypeKindInfo.
struct TypeInfo {
  TypeKind type_kind;
  bool internal_product_mode_only = false;
  absl::optional<LanguageFeature> type_feature;
  absl::optional<LanguageFeature> alias_feature;
};

// A map joining SimpleTypeNameInfoMap and SimpleTypeKindInfoMap.
// Caller takes ownership of the result.
std::map<absl::string_view, TypeInfo>* BuildSimpleTypeInfoMap() {
  auto* result = new std::map<absl::string_view, TypeInfo>;
  const std::map<TypeKind, TypeKindInfo>& type_kind_info_map =
      SimpleTypeKindInfoMap();
  for (const auto& item : SimpleTypeNameInfoMap()) {
    const TypeNameInfo& type_name_info = item.second;
    TypeKind type_kind = type_name_info.type_kind;
    auto itr = type_kind_info_map.find(type_kind);
    ZETASQL_CHECK(itr != type_kind_info_map.end())
        << TypeKind_Name(type_kind) << " not found in SimpleTypeKindInfoMap()";
    const TypeKindInfo& type_kind_info = itr->second;
    result->emplace(
        item.first,
        TypeInfo{type_kind,
                 type_name_info.internal_product_mode_only ||
                     type_kind_info.internal_product_mode_only,
                 type_kind_info.type_feature, type_name_info.alias_feature});
  }
  return result;
}

DateValueContentType GetDateValue(const ValueContent& value) {
  return value.GetAs<DateValueContentType>();
}

const NumericValue& GetNumericValue(const ValueContent& value) {
  return value.GetAs<internal::NumericRef*>()->value();
}

const BigNumericValue& GetBigNumericValue(const ValueContent& value) {
  return value.GetAs<internal::BigNumericRef*>()->value();
}

const std::string& GetStringValue(const ValueContent& value) {
  return value.GetAs<internal::StringRef*>()->value();
}

const std::string& GetBytesValue(const ValueContent& value) {
  return GetStringValue(value);
}

std::string GetJsonString(const ValueContent& value) {
  return value.GetAs<internal::JSONRef*>()->ToString();
}

const IntervalValue& GetIntervalValue(const ValueContent& value) {
  return value.GetAs<internal::IntervalRef*>()->value();
}

std::string AddTypePrefix(absl::string_view value, const Type* type,
                          ProductMode mode) {
  return absl::StrCat(type->TypeName(mode), " ", ToStringLiteral(value));
}

template <typename ContentT>
bool ContentEquals(const ValueContent& x, const ValueContent& y) {
  return x.GetAs<ContentT>() == y.GetAs<ContentT>();
}

template <typename ReferenceT>
bool ReferencedValueEquals(const ValueContent& x, const ValueContent& y) {
  return x.GetAs<ReferenceT*>()->value() == y.GetAs<ReferenceT*>()->value();
}

template <typename ContentT>
bool ContentLess(const ValueContent& x, const ValueContent& y) {
  return x.GetAs<ContentT>() < y.GetAs<ContentT>();
}

template <typename ReferenceT>
bool ReferencedValueLess(const ValueContent& x, const ValueContent& y) {
  return x.GetAs<ReferenceT*>()->value() < y.GetAs<ReferenceT*>()->value();
}

}  // namespace

SimpleType::SimpleType(const TypeFactory* factory, TypeKind kind)
    : Type(factory, kind) {
  ZETASQL_CHECK(IsSimpleType(kind)) << kind;
}

SimpleType::~SimpleType() {
}

bool SimpleType::IsSupportedType(
    const LanguageOptions& language_options) const {
  const std::map<TypeKind, TypeKindInfo>& type_kind_info_map =
      SimpleTypeKindInfoMap();
  auto itr = type_kind_info_map.find(kind());
  if (itr == type_kind_info_map.end()) {
    return false;
  }
  const TypeKindInfo& info = itr->second;
  if (language_options.product_mode() == PRODUCT_EXTERNAL &&
      info.internal_product_mode_only) {
    return false;
  }
  if (info.type_feature.has_value() &&
      !language_options.LanguageFeatureEnabled(info.type_feature.value())) {
    return false;
  }
  return true;
}

void SimpleType::DebugStringImpl(bool details, TypeOrStringVector* stack,
                                 std::string* debug_string) const {
  absl::StrAppend(debug_string, TypeKindToString(kind(), PRODUCT_INTERNAL));
}

absl::Status SimpleType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    const BuildFileDescriptorSetMapOptions& options, TypeProto* type_proto,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  type_proto->set_type_kind(kind());
  return absl::OkStatus();
}

std::string SimpleType::TypeName(ProductMode mode) const {
  return TypeKindToString(kind(), mode);
}

absl::StatusOr<std::string> SimpleType::TypeNameWithParameters(
    const TypeParameters& type_params, ProductMode mode) const {
  if (type_params.IsStructOrArrayParameters() ||
      type_params.IsExtendedTypeParameters()) {
    return MakeSqlError()
           << "Input type parameter does not correspond to SimpleType";
  }
  std::string type_param_name = "";
  if (type_params.IsNumericTypeParameters()) {
    if (type_params.numeric_type_parameters().has_is_max_precision()) {
      type_param_name = "(MAX, ";
    } else {
      type_param_name = absl::Substitute(
          "($0, ", type_params.numeric_type_parameters().precision());
    }
    absl::StrAppend(
        &type_param_name,
        absl::Substitute("$0)", type_params.numeric_type_parameters().scale()));
  }
  if (type_params.IsStringTypeParameters()) {
    if (type_params.string_type_parameters().has_is_max_length()) {
      type_param_name = "(MAX)";
    } else {
      type_param_name = absl::Substitute(
          "($0)", type_params.string_type_parameters().max_length());
    }
  }
  return absl::StrCat(TypeName(mode), type_param_name);
}

TypeKind SimpleType::GetTypeKindIfSimple(
    absl::string_view type_name, ProductMode mode,
    const LanguageOptions::LanguageFeatureSet* language_features) {
  static const std::map<absl::string_view, TypeInfo>* type_map =
      BuildSimpleTypeInfoMap();
  const TypeInfo* type_info =
      zetasql_base::FindOrNull(*type_map, absl::AsciiStrToLower(type_name));
  if (type_info == nullptr) {
    return TYPE_UNKNOWN;
  }
  if (mode == PRODUCT_EXTERNAL && type_info->internal_product_mode_only) {
    return TYPE_UNKNOWN;
  }
  if (language_features != nullptr) {
    if (type_info->type_feature.has_value() &&
        !language_features->contains(type_info->type_feature.value())) {
      return TYPE_UNKNOWN;
    }
    if (type_info->alias_feature.has_value() &&
        !language_features->contains(type_info->alias_feature.value())) {
      return TYPE_UNKNOWN;
    }
  }
  return type_info->type_kind;
}

bool SimpleType::SupportsGroupingImpl(const LanguageOptions& language_options,
                                      const Type** no_grouping_type) const {
  const bool supports_grouping =
      !this->IsGeography() &&
      !this->IsJson() &&
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
    case TYPE_INTERVAL:
    case TYPE_JSON:
      return true;
    default:
      return false;
  }
}

void SimpleType::CopyValueContent(TypeKind kind, const ValueContent& from,
                                  ValueContent* to) {
  if (DoesValueContentUseSimpleReferenceCounted(kind)) {
    from.GetAs<zetasql_base::SimpleReferenceCounted*>()->Ref();
  }

  *to = from;
}

void SimpleType::CopyValueContent(const ValueContent& from,
                                  ValueContent* to) const {
  CopyValueContent(kind(), from, to);
}

void SimpleType::ClearValueContent(TypeKind kind, const ValueContent& value) {
  if (DoesValueContentUseSimpleReferenceCounted(kind)) {
    value.GetAs<zetasql_base::SimpleReferenceCounted*>()->Unref();
  }
}

void SimpleType::ClearValueContent(const ValueContent& value) const {
  ClearValueContent(kind(), value);
}

uint64_t SimpleType::GetValueContentExternallyAllocatedByteSize(
    const ValueContent& value) const {
  switch (kind()) {
    case TYPE_STRING:
    case TYPE_BYTES:
      return value.GetAs<internal::StringRef*>()->physical_byte_size();
    case TYPE_GEOGRAPHY:
      return value.GetAs<internal::GeographyRef*>()->physical_byte_size();
    case TYPE_NUMERIC:
      return sizeof(internal::NumericRef);
    case TYPE_BIGNUMERIC:
      return sizeof(internal::BigNumericRef);
    case TYPE_JSON:
      return value.GetAs<internal::JSONRef*>()->physical_byte_size();
    default:
      return 0;
  }
}

absl::HashState SimpleType::HashTypeParameter(absl::HashState state) const {
  return state;  // Simple types don't have parameters.
}

absl::HashState SimpleType::HashValueContent(const ValueContent& value,
                                             absl::HashState state) const {
  // These codes are picked arbitrarily.
  static constexpr uint64_t kFloatNanHashCode = 0x739EF9A0B2C15522ull;
  static constexpr uint64_t kDoubleNanHashCode = 0xA00397BC84F93AA7ull;
  static constexpr uint64_t kGeographyHashCode = 0x98389DC9632631AEull;

  switch (kind()) {
    case TYPE_INT32:
      return absl::HashState::combine(std::move(state), value.GetAs<int32_t>());
    case TYPE_INT64:
      return absl::HashState::combine(std::move(state), value.GetAs<int64_t>());
    case TYPE_UINT32:
      return absl::HashState::combine(std::move(state),
                                      value.GetAs<uint32_t>());
    case TYPE_UINT64:
      return absl::HashState::combine(std::move(state),
                                      value.GetAs<uint64_t>());
    case TYPE_BOOL:
      return absl::HashState::combine(std::move(state), value.GetAs<bool>());
    case TYPE_FLOAT: {
      float float_value = value.GetAs<float>();
      if (std::isnan(float_value)) {
        return absl::HashState::combine(std::move(state), kFloatNanHashCode);
      }
      return absl::HashState::combine(std::move(state), float_value);
    }
    case TYPE_DOUBLE: {
      double double_value = value.GetAs<double>();
      if (std::isnan(double_value)) {
        return absl::HashState::combine(std::move(state), kDoubleNanHashCode);
      }
      return absl::HashState::combine(std::move(state), double_value);
    }
    case TYPE_STRING:
    case TYPE_BYTES:
      return absl::HashState::combine(std::move(state), GetStringValue(value));
    case TYPE_DATE:
      return absl::HashState::combine(std::move(state), GetDateValue(value));
    case TYPE_TIMESTAMP:
      return absl::HashState::combine(std::move(state),
                                      value.GetAs<TimestampValueContentType>(),
                                      value.simple_type_extended_content_);
    case TYPE_TIME:
      return absl::HashState::combine(std::move(state),
                                      value.GetAs<TimeValueContentType>(),
                                      value.simple_type_extended_content_);
    case TYPE_DATETIME:
      return absl::HashState::combine(std::move(state),
                                      value.GetAs<DateTimeValueContentType>(),
                                      value.simple_type_extended_content_);
    case TYPE_INTERVAL:
      return absl::HashState::combine(std::move(state),
                                      GetIntervalValue(value));
    case TYPE_NUMERIC:
      return absl::HashState::combine(std::move(state), GetNumericValue(value));
    case TYPE_BIGNUMERIC:
      return absl::HashState::combine(std::move(state),
                                      GetBigNumericValue(value));
    case TYPE_GEOGRAPHY:
      // We have no good hasher for geography (??)
      // so we just rely on a constant for hashing.
      return absl::HashState::combine(std::move(state), kGeographyHashCode);
    case TYPE_JSON:
      return absl::HashState::combine(std::move(state), GetJsonString(value));
    default:
      ZETASQL_LOG(DFATAL) << "Unexpected type kind: " << kind();
      return state;
  }
}

// Returns true if two INTERVAL values are an exact match (where 1 MONTH and
// 30 DAYS are *not* considered equal).
bool AllPartsIntervalMatch(const IntervalValue& x, const IntervalValue& y) {
  return x.get_months() == y.get_months() && x.get_days() == y.get_days() &&
         x.get_micros() == y.get_micros() &&
         x.get_nano_fractions() == y.get_nano_fractions();
}

bool SimpleType::ValueContentEquals(
    const ValueContent& x, const ValueContent& y,
    const ValueEqualityCheckOptions& options) const {
  switch (kind()) {
    case TYPE_INT32:
      return ContentEquals<int32_t>(x, y);
    case TYPE_INT64:
      return ContentEquals<int64_t>(x, y);
    case TYPE_UINT32:
      return ContentEquals<uint32_t>(x, y);
    case TYPE_UINT64:
      return ContentEquals<uint64_t>(x, y);
    case TYPE_BOOL:
      return ContentEquals<bool>(x, y);
    case TYPE_FLOAT:
      return options.float_margin.Equal(x.GetAs<float>(), y.GetAs<float>());
    case TYPE_DOUBLE:
      return options.float_margin.Equal(x.GetAs<double>(), y.GetAs<double>());
    case TYPE_STRING:
    case TYPE_BYTES:
      return ReferencedValueEquals<internal::StringRef>(x, y);
    case TYPE_DATE:
      return ContentEquals<DateValueContentType>(x, y);
    case TYPE_TIMESTAMP:
      return ContentEquals<TimestampValueContentType>(x, y) &&
             x.simple_type_extended_content_ == y.simple_type_extended_content_;
    case TYPE_TIME:
      return ContentEquals<TimeValueContentType>(x, y) &&
             x.simple_type_extended_content_ == y.simple_type_extended_content_;
    case TYPE_DATETIME:
      return ContentEquals<DateTimeValueContentType>(x, y) &&
             x.simple_type_extended_content_ == y.simple_type_extended_content_;
    case TYPE_INTERVAL:
      switch (options.interval_compare_mode) {
        case IntervalCompareMode::kAllPartsEqual:
          return AllPartsIntervalMatch(
              x.GetAs<internal::IntervalRef*>()->value(),
              y.GetAs<internal::IntervalRef*>()->value());
        case IntervalCompareMode::kSqlEquals:
          return ReferencedValueEquals<internal::IntervalRef>(x, y);
      }
    case TYPE_NUMERIC:
      return ReferencedValueEquals<internal::NumericRef>(x, y);
    case TYPE_BIGNUMERIC:
      return ReferencedValueEquals<internal::BigNumericRef>(x, y);
    case TYPE_JSON: {
      return GetJsonString(x) == GetJsonString(y);
    }
    default:
      ZETASQL_LOG(FATAL) << "Unexpected simple type kind: " << kind();
  }
}

bool SimpleType::ValueContentLess(const ValueContent& x, const ValueContent& y,
                                  const Type* other_type) const {
  switch (kind()) {
    case TYPE_INT32:
      return ContentLess<int32_t>(x, y);
    case TYPE_INT64:
      return ContentLess<int64_t>(x, y);
    case TYPE_UINT32:
      return ContentLess<uint32_t>(x, y);
    case TYPE_UINT64:
      return ContentLess<uint64_t>(x, y);
    case TYPE_BOOL:
      return ContentLess<bool>(x, y);
    case TYPE_FLOAT:
      if (std::isnan(x.GetAs<float>()) && !std::isnan(y.GetAs<float>())) {
        return true;
      }
      if (std::isnan(y.GetAs<float>())) {
        return false;
      }
      return ContentLess<float>(x, y);
    case TYPE_DOUBLE:
      if (std::isnan(x.GetAs<double>()) && !std::isnan(y.GetAs<double>())) {
        return true;
      }
      if (std::isnan(y.GetAs<double>())) {
        return false;
      }
      return ContentLess<double>(x, y);
    case TYPE_STRING:
    case TYPE_BYTES:
      return ReferencedValueLess<internal::StringRef>(x, y);
    case TYPE_DATE:
      return ContentLess<DateValueContentType>(x, y);
    case TYPE_TIMESTAMP:
      return ContentLess<TimestampValueContentType>(x, y) ||
             (ContentEquals<TimestampValueContentType>(x, y) &&
              x.simple_type_extended_content_ <
                  y.simple_type_extended_content_);
    case TYPE_TIME:
      return ContentLess<TimeValueContentType>(x, y) ||
             (ContentEquals<TimeValueContentType>(x, y) &&
              x.simple_type_extended_content_ <
                  y.simple_type_extended_content_);
    case TYPE_DATETIME:
      return ContentLess<DateTimeValueContentType>(x, y) ||
             (ContentEquals<DateTimeValueContentType>(x, y) &&
              x.simple_type_extended_content_ <
                  y.simple_type_extended_content_);
    case TYPE_INTERVAL:
      return ReferencedValueLess<internal::IntervalRef>(x, y);
    case TYPE_NUMERIC:
      return ReferencedValueLess<internal::NumericRef>(x, y);
    case TYPE_BIGNUMERIC:
      return ReferencedValueLess<internal::BigNumericRef>(x, y);
    default:
      ZETASQL_LOG(DFATAL) << "Cannot compare " << DebugString() << " to "
                  << DebugString();
      return false;
  }
}

std::string SimpleType::FormatValueContent(
    const ValueContent& value, const FormatValueContentOptions& options) const {
  switch (kind()) {
    case TYPE_BOOL:
      return (value.GetAs<bool>() ? "true" : "false");
    case TYPE_STRING:
      return ToStringLiteral(GetStringValue(value));
    case TYPE_BYTES:
      return ToBytesLiteral(GetBytesValue(value));
    case TYPE_DATE: {
      std::string s;
      // Failure cannot actually happen in this context since date_value()
      // is guaranteed to be valid.
      ZETASQL_CHECK_OK(functions::ConvertDateToString(GetDateValue(value), &s));
      return options.add_simple_type_prefix()
                 ? AddTypePrefix(s, this, options.product_mode)
                 : s;
    }
    case TYPE_TIMESTAMP: {
      std::string s;
      // Failure cannot actually happen in this context since the value
      // is guaranteed to be valid.
      ZETASQL_CHECK_OK(functions::ConvertTimestampToString(GetTimestampValue(value),
                                                   functions::kNanoseconds,
                                                   "+0" /* timezone */, &s));
      return options.add_simple_type_prefix()
                 ? AddTypePrefix(s, this, options.product_mode)
                 : s;
    }
    case TYPE_TIME: {
      std::string s = GetTimeValue(value).DebugString();
      return options.add_simple_type_prefix()
                 ? AddTypePrefix(s, this, options.product_mode)
                 : s;
    }
    case TYPE_DATETIME: {
      std::string s = GetDateTimeValue(value).DebugString();
      return options.add_simple_type_prefix()
                 ? AddTypePrefix(s, this, options.product_mode)
                 : s;
    }
    case TYPE_INT32:
      return options.as_literal()
                 ? absl::StrCat(value.GetAs<int32_t>())
                 : internal::GetCastExpressionString(
                       value.GetAs<int32_t>(), this, options.product_mode);
    case TYPE_UINT32:
      return options.as_literal()
                 ? absl::StrCat(value.GetAs<uint32_t>())
                 : internal::GetCastExpressionString(
                       value.GetAs<uint32_t>(), this, options.product_mode);
    case TYPE_INT64:
      return absl::StrCat(value.GetAs<int64_t>());
    case TYPE_UINT64:
      return options.as_literal()
                 ? absl::StrCat(value.GetAs<uint64_t>())
                 : internal::GetCastExpressionString(
                       value.GetAs<uint64_t>(), this, options.product_mode);
    case TYPE_FLOAT: {
      const float float_value = value.GetAs<float>();

      if (options.mode == FormatValueContentOptions::Mode::kDebug) {
        return RoundTripFloatToString(float_value);
      }

      // Floats and doubles like "inf" and "nan" need to be quoted.
      if (!std::isfinite(float_value)) {
        return internal::GetCastExpressionString(
            ToStringLiteral(RoundTripFloatToString(float_value)), this,
            options.product_mode);
      } else {
        std::string s = RoundTripFloatToString(float_value);
        // Make sure that doubles always print with a . or an 'e' so they
        // don't look like integers.
        if (options.as_literal() &&
            s.find_first_not_of("-0123456789") == std::string::npos) {
          s.append(".0");
        }
        return options.as_literal() ? s
                                    : internal::GetCastExpressionString(
                                          s, this, options.product_mode);
      }
    }
    case TYPE_DOUBLE: {
      const double double_value = value.GetAs<double>();

      if (options.mode == FormatValueContentOptions::Mode::kDebug) {
        // TODO I would like to change this so it returns "1.0" rather
        // than "1", like GetSQL(), but that affects a lot of client code.
        return RoundTripDoubleToString(double_value);
      }

      if (!std::isfinite(double_value)) {
        return internal::GetCastExpressionString(
            ToStringLiteral(RoundTripDoubleToString(double_value)), this,
            options.product_mode);
      } else {
        std::string s = RoundTripDoubleToString(double_value);
        // Make sure that doubles always print with a . or an 'e' so they
        // don't look like integers.
        if (s.find_first_not_of("-0123456789") == std::string::npos) {
          s.append(".0");
        }
        return s;
      }
    }
    case TYPE_NUMERIC: {
      std::string s = GetNumericValue(value).ToString();
      return options.add_simple_type_prefix()
                 ? absl::StrCat("NUMERIC ", ToStringLiteral(s))
                 : s;
    }
    case TYPE_BIGNUMERIC: {
      std::string s = GetBigNumericValue(value).ToString();
      return options.add_simple_type_prefix()
                 ? absl::StrCat("BIGNUMERIC ", ToStringLiteral(s))
                 : s;
    }
    case TYPE_INTERVAL: {
      std::string s = GetIntervalValue(value).ToString();
      return options.mode != FormatValueContentOptions::Mode::kDebug
                 ? absl::StrCat("INTERVAL ", ToStringLiteral(s),
                                " YEAR TO SECOND")
                 : s;
    }
    case TYPE_JSON: {
      std::string s = GetJsonString(value);
      return options.add_simple_type_prefix()
                 ? absl::StrCat("JSON ", ToStringLiteral(s))
                 : s;
    }
    default:
      ZETASQL_LOG(DFATAL) << "Unexpected type kind: " << kind();
      return "<Invalid simple type's value>";
  }
}

absl::Status SimpleType::SerializeValueContent(const ValueContent& value,
                                               ValueProto* value_proto) const {
  switch (kind()) {
    case TYPE_INT32:
      value_proto->set_int32_value(value.GetAs<int32_t>());
      break;
    case TYPE_INT64:
      value_proto->set_int64_value(value.GetAs<int64_t>());
      break;
    case TYPE_UINT32:
      value_proto->set_uint32_value(value.GetAs<uint32_t>());
      break;
    case TYPE_UINT64:
      value_proto->set_uint64_value(value.GetAs<uint64_t>());
      break;
    case TYPE_BOOL:
      value_proto->set_bool_value(value.GetAs<bool>());
      break;
    case TYPE_FLOAT:
      value_proto->set_float_value(value.GetAs<float>());
      break;
    case TYPE_DOUBLE:
      value_proto->set_double_value(value.GetAs<double>());
      break;
    case TYPE_NUMERIC:
      value_proto->set_numeric_value(
          GetNumericValue(value).SerializeAsProtoBytes());
      break;
    case TYPE_BIGNUMERIC:
      value_proto->set_bignumeric_value(
          GetBigNumericValue(value).SerializeAsProtoBytes());
      break;
    case TYPE_JSON:
      value_proto->set_json_value(GetJsonString(value));
      break;
    case TYPE_STRING:
      value_proto->set_string_value(GetStringValue(value));
      break;
    case TYPE_BYTES:
      value_proto->set_bytes_value(GetBytesValue(value));
      break;
    case TYPE_DATE:
      value_proto->set_date_value(GetDateValue(value));
      break;
    case TYPE_TIMESTAMP: {
      ZETASQL_RETURN_IF_ERROR(zetasql_base::EncodeGoogleApiProto(
          GetTimestampValue(value), value_proto->mutable_timestamp_value()));
      break;
    }
    case TYPE_DATETIME: {
      auto* datetime_proto = value_proto->mutable_datetime_value();
      datetime_proto->set_bit_field_datetime_seconds(
          GetDateTimeValue(value).Packed64DatetimeSeconds());
      datetime_proto->set_nanos(GetDateTimeValue(value).Nanoseconds());
      break;
    }
    case TYPE_TIME:
      value_proto->set_time_value(GetTimeValue(value).Packed64TimeNanos());
      break;
    case TYPE_INTERVAL:
      value_proto->set_interval_value(
          GetIntervalValue(value).SerializeAsBytes());
      break;
    default:
      return absl::Status(absl::StatusCode::kInternal,
                          absl::StrCat("Unsupported type ", DebugString()));
  }
  return absl::OkStatus();
}

absl::Status SimpleType::DeserializeValueContent(const ValueProto& value_proto,
                                                 ValueContent* value) const {
  switch (kind()) {
    case TYPE_INT32:
      if (!value_proto.has_int32_value()) {
        return TypeMismatchError(value_proto);
      }
      value->set(value_proto.int32_value());
      break;
    case TYPE_INT64:
      if (!value_proto.has_int64_value()) {
        return TypeMismatchError(value_proto);
      }
      value->set(value_proto.int64_value());
      break;
    case TYPE_UINT32:
      if (!value_proto.has_uint32_value()) {
        return TypeMismatchError(value_proto);
      }
      value->set(value_proto.uint32_value());
      break;
    case TYPE_UINT64:
      if (!value_proto.has_uint64_value()) {
        return TypeMismatchError(value_proto);
      }
      value->set(value_proto.uint64_value());
      break;
    case TYPE_BOOL:
      if (!value_proto.has_bool_value()) {
        return TypeMismatchError(value_proto);
      }
      value->set(value_proto.bool_value());
      break;
    case TYPE_FLOAT:
      if (!value_proto.has_float_value()) {
        return TypeMismatchError(value_proto);
      }
      value->set(value_proto.float_value());
      break;
    case TYPE_DOUBLE:
      if (!value_proto.has_double_value()) {
        return TypeMismatchError(value_proto);
      }
      value->set(value_proto.double_value());
      break;
    case TYPE_NUMERIC: {
      if (!value_proto.has_numeric_value()) {
        return TypeMismatchError(value_proto);
      }
      ZETASQL_ASSIGN_OR_RETURN(
          NumericValue numeric_v,
          NumericValue::DeserializeFromProtoBytes(value_proto.numeric_value()));
      value->set(new internal::NumericRef(numeric_v));
      break;
    }
    case TYPE_BIGNUMERIC: {
      if (!value_proto.has_bignumeric_value()) {
        return TypeMismatchError(value_proto);
      }
      ZETASQL_ASSIGN_OR_RETURN(BigNumericValue bignumeric_v,
                       BigNumericValue::DeserializeFromProtoBytes(
                           value_proto.bignumeric_value()));
      value->set(new internal::BigNumericRef(bignumeric_v));
      break;
    }
    case TYPE_JSON: {
      if (!value_proto.has_json_value()) {
        return TypeMismatchError(value_proto);
      }
      value->set(new internal::JSONRef(std::move(value_proto.json_value())));
      break;
    }
    case TYPE_STRING:
      if (!value_proto.has_string_value()) {
        return TypeMismatchError(value_proto);
      }
      value->set(new internal::StringRef(value_proto.string_value()));
      break;
    case TYPE_BYTES:
      if (!value_proto.has_bytes_value()) {
        return TypeMismatchError(value_proto);
      }
      value->set(new internal::StringRef(value_proto.bytes_value()));
      break;
    case TYPE_DATE:
      if (!value_proto.has_date_value()) {
        return TypeMismatchError(value_proto);
      }
      if (!functions::IsValidDate(value_proto.date_value())) {
        return absl::Status(
            absl::StatusCode::kOutOfRange,
            absl::StrCat("Invalid value for DATE: ", value_proto.date_value()));
      }

      value->set(value_proto.date_value());
      break;
    case TYPE_TIMESTAMP: {
      if (!value_proto.has_timestamp_value()) {
        return TypeMismatchError(value_proto);
      }

      auto time_or =
          zetasql_base::DecodeGoogleApiProto(value_proto.timestamp_value());
      if (!time_or.ok()) {
        return absl::Status(
            absl::StatusCode::kOutOfRange,
            absl::StrCat("Invalid value for TIMESTAMP",
                         value_proto.timestamp_value().DebugString()));
      }

      absl::Time t = time_or.value();
      ZETASQL_RETURN_IF_ERROR(SetTimestampValue(t, value));
      break;
    }
    case TYPE_DATETIME: {
      if (!value_proto.has_datetime_value()) {
        return TypeMismatchError(value_proto);
      }
      DatetimeValue wrapper = DatetimeValue::FromPacked64SecondsAndNanos(
          value_proto.datetime_value().bit_field_datetime_seconds(),
          value_proto.datetime_value().nanos());

      ZETASQL_RETURN_IF_ERROR(SetDateTimeValue(wrapper, value));
      break;
    }
    case TYPE_TIME: {
      if (!value_proto.has_time_value()) {
        return TypeMismatchError(value_proto);
      }
      TimeValue wrapper =
          TimeValue::FromPacked64Nanos(value_proto.time_value());

      ZETASQL_RETURN_IF_ERROR(SetTimeValue(wrapper, value));
      break;
    }
    case TYPE_INTERVAL: {
      if (!value_proto.has_interval_value()) {
        return TypeMismatchError(value_proto);
      }
      ZETASQL_ASSIGN_OR_RETURN(IntervalValue interval_v,
                       IntervalValue::DeserializeFromBytes(
                           value_proto.interval_value()));
      value->set(new internal::IntervalRef(interval_v));
      break;
    }
    default:
      return absl::Status(absl::StatusCode::kInternal,
                          absl::StrCat("Unsupported type ", DebugString()));
  }

  return absl::OkStatus();
}

absl::Time SimpleType::GetTimestampValue(const ValueContent& value) {
  return absl::FromUnixSeconds(value.GetAs<TimestampValueContentType>()) +
         absl::Nanoseconds(value.simple_type_extended_content_);
}

absl::Status SimpleType::SetTimestampValue(absl::Time time,
                                           ValueContent* value) {
  if (!functions::IsValidTime(time)) {
    return absl::Status(
        absl::StatusCode::kOutOfRange,
        absl::StrCat("Invalid value for TIMESTAMP: ", absl::FormatTime(time)));
  }

  const int64_t timestamp_seconds = absl::ToUnixSeconds(time);
  const int32_t subsecond_nanos = static_cast<int32_t>(
      (time - absl::FromUnixSeconds(timestamp_seconds)) / absl::Nanoseconds(1));

  value->set(timestamp_seconds);
  value->simple_type_extended_content_ = subsecond_nanos;

  return absl::OkStatus();
}

TimeValue SimpleType::GetTimeValue(const ValueContent& value) {
  return TimeValue::FromPacked32SecondsAndNanos(
      value.GetAs<TimeValueContentType>(), value.simple_type_extended_content_);
}

absl::Status SimpleType::SetTimeValue(TimeValue time, ValueContent* value) {
  if (!time.IsValid()) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "Invalid value for TIME");
  }

  const TimeValueContentType seconds = time.Packed32TimeSeconds();
  const int32_t nanoseconds = time.Nanoseconds();

  value->set(seconds);
  value->simple_type_extended_content_ = nanoseconds;

  return absl::OkStatus();
}

DatetimeValue SimpleType::GetDateTimeValue(const ValueContent& value) {
  return DatetimeValue::FromPacked64SecondsAndNanos(
      value.GetAs<DateTimeValueContentType>(),
      value.simple_type_extended_content_);
}

absl::Status SimpleType::SetDateTimeValue(DatetimeValue datetime,
                                          ValueContent* value) {
  if (!datetime.IsValid()) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "Invalid value for DATETIME");
  }

  const DateTimeValueContentType seconds = datetime.Packed64DatetimeSeconds();
  const int32_t nanoseconds = datetime.Nanoseconds();

  value->set(seconds);
  value->simple_type_extended_content_ = nanoseconds;

  return absl::OkStatus();
}

absl::StatusOr<TypeParameters> SimpleType::ValidateAndResolveTypeParameters(
    const std::vector<TypeParameterValue>& type_parameter_values,
    ProductMode mode) const {
  if (IsString() || IsBytes()) {
    return ResolveStringBytesTypeParameters(type_parameter_values, mode);
  }
  if (IsNumericType() || IsBigNumericType()) {
    return ResolveNumericBignumericTypeParameters(type_parameter_values, mode);
  }
  return MakeSqlError() << ShortTypeName(mode)
                        << " does not support type parameters";
}

absl::StatusOr<TypeParameters> SimpleType::ResolveStringBytesTypeParameters(
    const std::vector<TypeParameterValue>& type_parameter_values,
    ProductMode mode) const {
  if (type_parameter_values.size() != 1) {
    return MakeSqlError() << ShortTypeName(mode)
                          << " type can only have one parameter. Found "
                          << type_parameter_values.size() << " parameters";
  }

  StringTypeParametersProto type_parameters_proto;
  TypeParameterValue param = type_parameter_values[0];
  if (!param.IsSpecialLiteral() && param.GetValue().has_int64_value()) {
    if (param.GetValue().int64_value() <= 0) {
      return MakeSqlError()
             << ShortTypeName(mode) << " length must be greater than 0";
    }
    type_parameters_proto.set_max_length(param.GetValue().int64_value());
    return TypeParameters::MakeStringTypeParameters(type_parameters_proto);
  }
  if (param.IsSpecialLiteral() &&
      param.GetSpecialLiteral() == TypeParameterValue::kMaxLiteral) {
    type_parameters_proto.set_is_max_length(true);
    return TypeParameters::MakeStringTypeParameters(type_parameters_proto);
  }
  return MakeSqlError()
         << ShortTypeName(mode)
         << " length parameter must be an integer or MAX keyword";
}

absl::StatusOr<TypeParameters>
SimpleType::ResolveNumericBignumericTypeParameters(
    const std::vector<TypeParameterValue>& type_parameter_values,
    ProductMode mode) const {
  if (type_parameter_values.size() > 2) {
    return MakeSqlError() << ShortTypeName(mode)
                          << " type can only have 1 or 2 parameters. Found "
                          << type_parameter_values.size() << " parameters";
  }

  // For both NUMERIC and BIGNUMERIC, scale must be an integer.
  if (type_parameter_values.size() == 2 &&
      !type_parameter_values[1].GetValue().has_int64_value()) {
    return MakeSqlError() << ShortTypeName(mode) << " scale must be an integer";
  }
  int64_t scale = type_parameter_values.size() == 2
                      ? type_parameter_values[1].GetValue().int64_value()
                      : 0;

  // Validate value range for scale.
  NumericTypeParametersProto type_parameters_proto;
  const int max_scale =
      IsNumericType() ? kNumericMaxScale : kBigNumericMaxScale;
  if (scale < 0 || scale > max_scale) {
    return MakeSqlError() << absl::Substitute(
               "In $0(P, S), S must be between 0 and $1", ShortTypeName(mode),
               max_scale);
  }
  type_parameters_proto.set_scale(scale);

  // For NUMERIC, precision can only be an integer.
  // For BIGNUMERIC, precision can be an integer or MAX literal.
  TypeParameterValue precision_param = type_parameter_values[0];
  if (!precision_param.IsSpecialLiteral() &&
      precision_param.GetValue().has_int64_value()) {
    const int max_precision =
        IsNumericType() ? kNumericMaxPrecision : kBigNumericMaxPrecision;
    int64_t precision = type_parameter_values[0].GetValue().int64_value();
    if (precision < std::max(int64_t{1}, scale) ||
        precision > max_precision + scale) {
      if (type_parameter_values.size() == 1) {
        return MakeSqlError()
               << absl::Substitute("In $0(P), P must be between 1 and $1",
                                   ShortTypeName(mode), max_precision);
      }
      return MakeSqlError()
             << absl::Substitute("In $0(P, $1), P must be between $2 and $3",
                                 ShortTypeName(mode), scale,
                                 scale == 0 ? 1 : scale, max_precision + scale);
    }
    type_parameters_proto.set_precision(precision);
    return TypeParameters::MakeNumericTypeParameters(type_parameters_proto);
  }
  if (precision_param.IsSpecialLiteral() &&
      precision_param.GetSpecialLiteral() == TypeParameterValue::kMaxLiteral &&
      IsBigNumericType()) {
    type_parameters_proto.set_is_max_precision(true);
    return TypeParameters::MakeNumericTypeParameters(type_parameters_proto);
  }

  // Error out for invalid precision parameter input.
  if (IsNumericType()) {
    return MakeSqlError() << ShortTypeName(mode)
                          << " precision must be an integer";
  }
  return MakeSqlError() << ShortTypeName(mode)
                        << " precision must be an integer or MAX keyword";
}

absl::Status SimpleType::ValidateResolvedTypeParameters(
    const TypeParameters& type_parameters, ProductMode mode) const {
  if (type_parameters.IsEmpty()) {
    return absl::OkStatus();
  }
  if (IsString() || IsBytes()) {
    ZETASQL_RET_CHECK(type_parameters.IsStringTypeParameters());
    return TypeParameters::ValidateStringTypeParameters(
        type_parameters.string_type_parameters());
  }
  if (IsNumericType() || IsBigNumericType()) {
    ZETASQL_RET_CHECK(type_parameters.IsNumericTypeParameters());
    return ValidateNumericTypeParameters(
        type_parameters.numeric_type_parameters(), mode);
  }
  ZETASQL_RET_CHECK_FAIL() << ShortTypeName(mode)
                   << " does not support type parameters";
}

absl::Status SimpleType::ValidateNumericTypeParameters(
    const NumericTypeParametersProto& numeric_param, ProductMode mode) const {
  // Validate value range for scale.
  int max_scale = IsNumericType() ? kNumericMaxScale : kBigNumericMaxScale;
  int64_t scale = numeric_param.scale();
  ZETASQL_RET_CHECK(scale >= 0 && scale <= max_scale) << absl::Substitute(
      "In $0(P, S), S must be between 0 and $1, actual scale: $2",
      ShortTypeName(mode), max_scale, scale);

  // Validate value range for precision.
  if (numeric_param.has_is_max_precision()) {
    ZETASQL_RET_CHECK(IsBigNumericType());
    ZETASQL_RET_CHECK(numeric_param.is_max_precision())
        << "is_max_precision should either be unset or true";
  } else {
    int64_t precision = numeric_param.precision();
    int max_precision =
        IsNumericType() ? kNumericMaxPrecision : kBigNumericMaxPrecision;
    ZETASQL_RET_CHECK(precision >= std::max(int64_t{1}, scale) &&
              precision <= max_precision + scale)
        << absl::Substitute(
               "In $0(P, $1), P must be between $2 and $3, actual "
               "precision: $4",
               ShortTypeName(mode), scale, scale == 0 ? 1 : scale,
               max_precision + scale, precision);
  }
  return absl::OkStatus();
}

}  // namespace zetasql
