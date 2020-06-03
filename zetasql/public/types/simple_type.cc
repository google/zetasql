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

#include <string>

#include "zetasql/common/string_util.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/types/internal_utils.h"
#include "zetasql/public/types/value_representations.h"
#include "zetasql/public/value_content.h"

namespace zetasql {

// The types below are used to represent the C++ type of a main value stored in
// ValueContent for a particular ZetaSQL type. Note: in addition to their main
// value simple types can also have extended content stored in ValueContent: the
// C++ type of extended content is fixed (int32_t).
using DateValueContentType = int32_t;       // Used with TYPE_DATE.
using TimestampValueContentType = int64_t;  // Used with TYPE_TIMESTAMP.
using TimeValueContentType = int32_t;       // Used with TYPE_TIME.
using DateTimeValueContentType = int64_t;   // Used with TYPE_DATETIME.

namespace {

// Specifies the type kind that a type name maps to, and when the type name is
// enabled. Even when the type name is enabled, the type kind might still be
// disabled (controlled by TypeKindInfo). The type name can be used only if both
// are enabled.
struct TypeNameInfo {
  TypeKind type_kind;
  // If false, this type name can be used in both the internal mode and the
  // external mode. If true, this type name can be used in the internal mode
  // only.
  bool internal_only = false;
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
  bool internal_only = false;
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
  bool internal_only = false;
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
    CHECK(itr != type_kind_info_map.end())
        << TypeKind_Name(type_kind) << " not found in SimpleTypeKindInfoMap()";
    const TypeKindInfo& type_kind_info = itr->second;
    result->emplace(
        item.first,
        TypeInfo{type_kind,
                 type_name_info.internal_only || type_kind_info.internal_only,
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

std::string AddTypePrefix(absl::string_view value, const Type* type,
                          ProductMode mode) {
  return absl::StrCat(type->TypeName(mode), " ", ToStringLiteral(value));
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
  const std::map<TypeKind, TypeKindInfo>& type_kind_info_map =
      SimpleTypeKindInfoMap();
  auto itr = type_kind_info_map.find(kind());
  if (itr == type_kind_info_map.end()) {
    return false;
  }
  const TypeKindInfo& info = itr->second;
  if (language_options.product_mode() == PRODUCT_EXTERNAL &&
      info.internal_only) {
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

TypeKind SimpleType::GetTypeKindIfSimple(
    absl::string_view type_name, ProductMode mode,
    const std::set<LanguageFeature>* language_features) {
  static const std::map<absl::string_view, TypeInfo>* type_map =
      BuildSimpleTypeInfoMap();
  const TypeInfo* type_info =
      zetasql_base::FindOrNull(*type_map, absl::AsciiStrToLower(type_name));
  if (type_info == nullptr) {
    return TYPE_UNKNOWN;
  }
  if (mode == PRODUCT_EXTERNAL && type_info->internal_only) {
    return TYPE_UNKNOWN;
  }
  if (language_features != nullptr) {
    if (type_info->type_feature.has_value() &&
        !zetasql_base::ContainsKey(*language_features,
                          type_info->type_feature.value())) {
      return TYPE_UNKNOWN;
    }
    if (type_info->alias_feature.has_value() &&
        !zetasql_base::ContainsKey(*language_features,
                          type_info->alias_feature.value())) {
      return TYPE_UNKNOWN;
    }
  }
  return type_info->type_kind;
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
  switch (kind()) {
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
  if (DoesValueContentUseSimpleReferenceCounted(kind())) {
    from.GetAs<zetasql_base::SimpleReferenceCounted*>()->Ref();
  }

  *to = from;
}

void SimpleType::ClearValueContent(const ValueContent& value) const {
  if (DoesValueContentUseSimpleReferenceCounted(kind())) {
    value.GetAs<zetasql_base::SimpleReferenceCounted*>()->Unref();
  }
}

internal::StringRef* GetStringRef(const ValueContent& value) {
  return value.GetAs<internal::StringRef*>();
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
      return absl::HashState::combine(std::move(state), value.GetAs<uint32_t>());
    case TYPE_UINT64:
      return absl::HashState::combine(std::move(state), value.GetAs<uint64_t>());
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
    case TYPE_NUMERIC:
      return absl::HashState::combine(std::move(state), GetNumericValue(value));
    case TYPE_BIGNUMERIC:
      return absl::HashState::combine(std::move(state),
                                      GetBigNumericValue(value));
    case TYPE_GEOGRAPHY:
      // We have no good hasher for geography (??)
      // so we just rely on a constant for hashing.
      return absl::HashState::combine(std::move(state), kGeographyHashCode);
    default:
      LOG(DFATAL) << "Unexpected type kind: " << kind();
      return state;
  }
}

template <typename ReferenceT>
bool ReferencedValueEquals(const ValueContent& x, const ValueContent& y) {
  return x.GetAs<ReferenceT*>()->value() == y.GetAs<ReferenceT*>()->value();
}

template <typename ConentT>
bool ContentEquals(const ValueContent& x, const ValueContent& y) {
  return x.GetAs<ConentT>() == y.GetAs<ConentT>();
}

bool SimpleType::ValueContentEqualsImpl(
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
    case TYPE_NUMERIC:
      return ReferencedValueEquals<internal::NumericRef>(x, y);
    case TYPE_BIGNUMERIC:
      return ReferencedValueEquals<internal::BigNumericRef>(x, y);
    case TYPE_JSON:
      LOG(DFATAL) << "Cannot compare JSON values";
      return false;
    default:
      LOG(FATAL) << "Unexpected simple type kind: " << kind();
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
                 : internal::GetCastExpressionString(value.GetAs<int32_t>(), this,
                                                     options.product_mode);
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
    case TYPE_JSON: {
      return "<Invalid value of unimplemented type JSON>";
    }
    default:
      LOG(DFATAL) << "Unexpected type kind: " << kind();
      return "<Invalid simple type's value>";
  }
}

absl::Time SimpleType::GetTimestampValue(const ValueContent& value) {
  return absl::FromUnixSeconds(value.GetAs<TimestampValueContentType>()) +
         absl::Nanoseconds(value.simple_type_extended_content_);
}

TimeValue SimpleType::GetTimeValue(const ValueContent& value) {
  return TimeValue::FromPacked32SecondsAndNanos(
      value.GetAs<TimeValueContentType>(), value.simple_type_extended_content_);
}

DatetimeValue SimpleType::GetDateTimeValue(const ValueContent& value) {
  return DatetimeValue::FromPacked64SecondsAndNanos(
      value.GetAs<DateTimeValueContentType>(),
      value.simple_type_extended_content_);
}

}  // namespace zetasql
