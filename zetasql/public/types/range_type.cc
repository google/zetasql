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
#include "zetasql/public/strings.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/collation.h"
#include "zetasql/public/types/container_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/types/type_modifiers.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/types/value_equality_check_options.h"
#include "zetasql/public/types/value_representations.h"
#include "zetasql/public/value.pb.h"
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

const Type* RangeType::GetElementType(int i) const { return element_type(); }

RangeType::RangeType(const TypeFactory* factory, const Type* element_type)
    : ContainerType(factory, TYPE_RANGE), element_type_(element_type) {
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
  absl::HashState result = absl::HashState::Create(&state);
  const internal::ValueContentContainer* container =
      value.GetAs<internal::ValueContentContainerRef*>()->value();
  ZETASQL_DCHECK_EQ(container->num_elements(), 2);
  ValueContentContainerElementHasher hasher(element_type());
  const internal::ValueContentContainerElement& start = container->element(0);
  result = absl::HashState::combine(std::move(result), hasher(start));
  const internal::ValueContentContainerElement& end = container->element(1);
  result = absl::HashState::combine(std::move(result), hasher(end));
  return result;
}

std::string RangeType::FormatValueContentContainerElement(
    const internal::ValueContentContainerElement& element,
    const Type::FormatValueContentOptions& options) const {
  std::string result;
  if (element.is_null()) {
    if (options.mode == Type::FormatValueContentOptions::Mode::kSQLLiteral ||
        options.mode == Type::FormatValueContentOptions::Mode::kSQLExpression) {
      return "UNBOUNDED";
    }
    result = "NULL";
  } else {
    Type::FormatValueContentOptions element_format_options = options;
    // Set mode to Debug to get elements formatted without added type prefix
    element_format_options.mode = Type::FormatValueContentOptions::Mode::kDebug;
    result = element_type()->FormatValueContent(element.value_content(),
                                                element_format_options);
  }

  if (options.mode == Type::FormatValueContentOptions::Mode::kDebug &&
      options.verbose) {
    return absl::StrCat(element_type()->CapitalizedName(), "(", result, ")");
  }
  return result;
}

std::string RangeType::FormatValueContent(
    const ValueContent& value,
    const Type::FormatValueContentOptions& options) const {
  const internal::ValueContentContainer* container =
      value.GetAs<internal::ValueContentContainerRef*>()->value();
  const internal::ValueContentContainerElement& start = container->element(0);
  const internal::ValueContentContainerElement& end = container->element(1);

  std::string boundaries =
      absl::StrCat("[", FormatValueContentContainerElement(start, options),
                   ", ", FormatValueContentContainerElement(end, options), ")");
  if (options.mode == Type::FormatValueContentOptions::Mode::kDebug) {
    return boundaries;
  }
  return absl::StrCat(TypeName(options.product_mode), " ",
                      ToStringLiteral(boundaries));
}

bool RangeType::ValueContentEquals(
    const ValueContent& x, const ValueContent& y,
    const ValueEqualityCheckOptions& options) const {
  const internal::ValueContentContainer* x_container =
      x.GetAs<internal::ValueContentContainerRef*>()->value();
  const internal::ValueContentContainer* y_container =
      y.GetAs<internal::ValueContentContainerRef*>()->value();

  const internal::ValueContentContainerElement& x_start =
      x_container->element(0);
  const internal::ValueContentContainerElement& x_end = x_container->element(1);
  const internal::ValueContentContainerElement& y_start =
      y_container->element(0);
  const internal::ValueContentContainerElement& y_end = y_container->element(1);

  ValueContentContainerElementEq eq(options, element_type());

  return eq(x_start, y_start) && eq(x_end, y_end);
}

bool RangeType::ValueContentLess(const ValueContent& x, const ValueContent& y,
                                 const Type* other_type) const {
  const internal::ValueContentContainer* x_container =
      x.GetAs<internal::ValueContentContainerRef*>()->value();
  const internal::ValueContentContainer* y_container =
      y.GetAs<internal::ValueContentContainerRef*>()->value();

  const internal::ValueContentContainerElement& x_start =
      x_container->element(0);
  const internal::ValueContentContainerElement& x_end = x_container->element(1);
  const internal::ValueContentContainerElement& y_start =
      y_container->element(0);
  const internal::ValueContentContainerElement& y_end = y_container->element(1);

  const Type* x_element_type = element_type();
  const Type* y_element_type = other_type->AsRange()->element_type();

  ValueEqualityCheckOptions options;
  ValueContentContainerElementEq eq(options, element_type());

  if (!eq(x_start, y_start)) {
    // [x_start, x_end) > [UNBOUNDED, y_end)
    // Unbounded start orders smallest, so if y has an unbounded start, then x
    // is always bigger than y.
    if (y_start.is_null()) {
      return false;
    }
    // [UNBOUNDED, x_end) < [y_start, y_end)
    // Unbounded start orders smallest, so if x has an unbounded start, then x
    // is always smaller than y.
    return x_start.is_null() ||
           ValueContentContainerElementLess(x_start, y_start, x_element_type,
                                            y_element_type)
               .value_or(false);  // Otherwise, compare the start values.
  } else {
    // Starts are equal, so compare the ends.
    if (x_end.is_null() && y_end.is_null()) {
      // [S, UNBOUNDED) == [S, UNBOUNDED)
      return false;
    } else if (x_end.is_null()) {
      // [S, UNBOUNDED) > [S, y_end)
      // Unbounded end orders largest, so if x has an unbounded end, then for
      // equal start, x is always larger than y.
      return false;
    } else if (y_end.is_null()) {
      // [S, x_end) < [S, UNBOUNDED)
      // Unbounded end orders largest, so if y has an unbounded end, then for
      // equal start, y is always larger than x.
      return true;
    }
    return ValueContentContainerElementLess(x_end, y_end, x_element_type,
                                            y_element_type)
        .value_or(false);
  }
}

absl::Status RangeType::SerializeValueContent(const ValueContent& value,
                                              ValueProto* value_proto) const {
  auto* range_proto = value_proto->mutable_range_value();
  const internal::ValueContentContainer* range_container =
      value.GetAs<internal::ValueContentContainerRef*>()->value();
  const internal::ValueContentContainerElement& start =
      range_container->element(0);
  if (start.is_null()) {
    range_proto->mutable_start()->Clear();
  } else {
    ZETASQL_RETURN_IF_ERROR(element_type()->SerializeValueContent(
        start.value_content(), range_proto->mutable_start()));
  }
  const internal::ValueContentContainerElement& end =
      range_container->element(1);
  if (end.is_null()) {
    range_proto->mutable_end()->Clear();
  } else {
    ZETASQL_RETURN_IF_ERROR(element_type()->SerializeValueContent(
        end.value_content(), range_proto->mutable_end()));
  }
  return absl::OkStatus();
}

absl::Status RangeType::DeserializeValueContent(const ValueProto& value_proto,
                                                ValueContent* value) const {
  return absl::FailedPreconditionError(
      "DeserializeValueContent should never be called for RangeType, since its "
      "value content deserialization is maintained in the Value class");
}

}  // namespace zetasql
