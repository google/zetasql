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

#include "zetasql/public/types/range_type.h"

#include <string>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/collation.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/types/type_modifiers.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/types/value_equality_check_options.h"
#include "zetasql/public/value_content.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/simple_reference_counted.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

std::string RangeType::ShortTypeName(ProductMode mode) const {
  return absl::StrCat("RANGE<", element_type_->ShortTypeName(mode), ">");
}

std::string RangeType::TypeName(ProductMode mode) const {
  return absl::StrCat("RANGE<", element_type_->TypeName(mode), ">");
}

absl::StatusOr<std::string> RangeType::TypeNameWithModifiers(
    const TypeModifiers& type_modifiers, ProductMode mode) const {
  const TypeParameters& type_params = type_modifiers.type_parameters();
  if (!type_params.IsEmpty() && type_params.num_children() != 1) {
    return MakeSqlError()
           << "Input type parameter does not correspond to RangeType";
  }

  const Collation& collation = type_modifiers.collation();
  // TODO: Implement logic to print type name with collation.
  ZETASQL_RET_CHECK(collation.Empty());
  ZETASQL_ASSIGN_OR_RETURN(
      std::string element_type_name,
      element_type_->TypeNameWithModifiers(
          TypeModifiers::MakeTypeModifiers(
              type_params.IsEmpty() ? TypeParameters() : type_params.child(0),
              Collation()),
          mode));
  return absl::StrCat("RANGE<", element_type_name, ">");
}

bool RangeType::IsSupportedType(const LanguageOptions& language_options) const {
  if (!language_options.LanguageFeatureEnabled(FEATURE_RANGE_TYPE)) {
    return false;
  }

  return IsValidElementType(element_type_) &&
         element_type_->IsSupportedType(language_options);
}

RangeType::RangeType(const TypeFactory* factory, const Type* element_type)
    : Type(factory, TYPE_RANGE), element_type_(element_type) {
  // Also blocked in TypeFactory::MakeRangeType.
  ZETASQL_DCHECK(IsValidElementType(element_type_));
}
RangeType::~RangeType() {}

bool RangeType::IsValidElementType(const Type* element_type) {
  // Range element types must be equatable and orderable.
  if (!element_type->SupportsOrdering() || !element_type->SupportsEquality()) {
    return false;
  }
  return IsSupportedElementTypeKind(element_type->kind());
}

bool RangeType::IsSupportedElementTypeKind(const TypeKind element_type_kind) {
  return element_type_kind == TYPE_DATE || element_type_kind == TYPE_DATETIME ||
         element_type_kind == TYPE_TIMESTAMP;
}

bool RangeType::EqualsImpl(const RangeType* const type1,
                           const RangeType* const type2, bool equivalent) {
  return type1->element_type()->EqualsImpl(type2->element_type(), equivalent);
}

absl::Status RangeType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    const BuildFileDescriptorSetMapOptions& options, TypeProto* type_proto,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  type_proto->set_type_kind(kind_);
  return element_type()->SerializeToProtoAndDistinctFileDescriptorsImpl(
      options, type_proto->mutable_range_type()->mutable_element_type(),
      file_descriptor_set_map);
}

bool RangeType::EqualsForSameKind(const Type* that, bool equivalent) const {
  const RangeType* other = that->AsRange();
  ZETASQL_DCHECK(other);
  return EqualsImpl(this, other, equivalent);
}

void RangeType::DebugStringImpl(bool details, TypeOrStringVector* stack,
                                std::string* debug_string) const {
  absl::StrAppend(debug_string, "RANGE<");
  stack->push_back(">");
  stack->push_back(element_type());
}

void RangeType::CopyValueContent(const ValueContent& from,
                                 ValueContent* to) const {
  from.GetAs<zetasql_base::SimpleReferenceCounted*>()->Ref();
  *to = from;
}

void RangeType::ClearValueContent(const ValueContent& value) const {
  value.GetAs<zetasql_base::SimpleReferenceCounted*>()->Unref();
}

absl::HashState RangeType::HashTypeParameter(absl::HashState state) const {
  // Range types are equivalent if their element types are equivalent,
  // so we hash the element type kind.
  return element_type()->Hash(std::move(state));
}

absl::HashState RangeType::HashValueContent(const ValueContent& value,
                                            absl::HashState state) const {
  // TODO: Implement this after implementing range in
  // zetasql::Value.
  ZETASQL_LOG(FATAL) << "Not yet implemented";  // Crash OK
}

bool RangeType::ValueContentEquals(
    const ValueContent& x, const ValueContent& y,
    const ValueEqualityCheckOptions& options) const {
  // TODO: Implement this after implementing range in
  // zetasql::Value.
  ZETASQL_LOG(FATAL) << "Not yet implemented";  // Crash OK
}

bool RangeType::ValueContentLess(const ValueContent& x, const ValueContent& y,
                                 const Type* other_type) const {
  // TODO: Implement this after implementing range in
  // zetasql::Value.
  ZETASQL_LOG(FATAL) << "Not yet implemented";  // Crash OK
}

std::string RangeType::FormatValueContent(
    const ValueContent& value, const FormatValueContentOptions& options) const {
  // TODO: Implement this after implementing range in
  // zetasql::Value.
  ZETASQL_LOG(FATAL) << "Not yet implemented";  // Crash OK
}

absl::Status RangeType::SerializeValueContent(const ValueContent& value,
                                              ValueProto* value_proto) const {
  // TODO: Implement this after implementing range in
  // zetasql::Value.
  return absl::UnimplementedError("Range type is not fully implemented");
}

absl::Status RangeType::DeserializeValueContent(const ValueProto& value_proto,
                                                ValueContent* value) const {
  // TODO: Implement this after implementing range in
  // zetasql::Value.
  return absl::UnimplementedError("Range type is not fully implemented");
}

}  // namespace zetasql
