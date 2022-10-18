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

#include "zetasql/public/types/array_type.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_modifiers.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/types/value_representations.h"
#include "zetasql/public/value_content.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/simple_reference_counted.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

struct HashableValueContentContainerElementIgnoringFloat {
  explicit HashableValueContentContainerElementIgnoringFloat(
      const internal::ValueContentContainerElement element, const Type* type)
      : element(element), type(type) {}
  const internal::ValueContentContainerElement element;
  const Type* type;

  template <typename H>
  H Hash(H h) const {
    type->HashValueContent(element.value_content(),
                           absl::HashState::Create(&h));
    return h;
  }

  template <typename H>
  friend H AbslHashValue(
      H h, const HashableValueContentContainerElementIgnoringFloat& v) {
    static constexpr uint64_t kFloatApproximateHashCode = 0x1192AA60660CCFABull;
    static constexpr uint64_t kDoubleApproximateHashCode =
        0x520C31647E82D8E6ull;
    static constexpr uint64_t
        kMessageWithFloatingPointFieldApproximateHashCode =
            0x1F6432686AAF52A4ull;
    if (v.element.is_null()) {
      return H::combine(std::move(h), kNullHashCode);
    }
    switch (v.type->kind()) {
      case TYPE_FLOAT:
        return H::combine(std::move(h), kFloatApproximateHashCode);
      case TYPE_DOUBLE:
        return H::combine(std::move(h), kDoubleApproximateHashCode);
      case TYPE_ARRAY: {
        // We must hash arrays as if unordered to support hash_map and hash_set
        // of values containing arrays with order_kind()=kIgnoresOrder.
        // absl::Hash lacks support for unordered containers, so we create a
        // cheapo solution of just adding the hashcodes.
        absl::Hash<HashableValueContentContainerElementIgnoringFloat>
            element_hasher;
        size_t combined_hash = 1;
        const internal::ValueContentContainer* container =
            v.element.value_content()
                .GetAs<internal::ValueContentContainerRef*>()
                ->value();
        for (int i = 0; i < container->num_elements(); i++) {
          const Type* element_type = v.type->AsArray()->element_type();
          combined_hash +=
              element_hasher(HashableValueContentContainerElementIgnoringFloat(
                  container->element(i), element_type));
        }
        return H::combine(std::move(h), TYPE_ARRAY, combined_hash);
      }
      case TYPE_STRUCT: {
        const internal::ValueContentContainer* container =
            v.element.value_content()
                .GetAs<internal::ValueContentContainerRef*>()
                ->value();
        absl::Hash<HashableValueContentContainerElementIgnoringFloat>
            field_hasher;
        h = H::combine(std::move(h), TYPE_STRUCT);
        for (int i = 0; i < container->num_elements(); i++) {
          const StructType* struct_type = v.type->AsStruct();
          const Type* field_type = struct_type->field(i).type;
          h = H::combine(
              std::move(h),
              field_hasher(HashableValueContentContainerElementIgnoringFloat(
                  container->element(i), field_type)));
        }
        return h;
      }
      case TYPE_PROTO: {
        absl::flat_hash_set<const google::protobuf::Descriptor*> visited;
        const ProtoType* p = v.type->AsProto();
        if (HasFloatingPointFields(p->descriptor(), visited)) {
          return H::combine(std::move(h),
                            kMessageWithFloatingPointFieldApproximateHashCode);
        }
        ABSL_FALLTHROUGH_INTENDED;
      }
      default:
        return v.Hash(std::move(h));
    }
  }

 private:
  static bool HasFloatingPointFields(
      const google::protobuf::Descriptor* d,
      absl::flat_hash_set<const google::protobuf::Descriptor*>& visited) {
    for (int i = 0; i < d->field_count(); ++i) {
      const google::protobuf::FieldDescriptor* f = d->field(i);
      if (f->type() == google::protobuf::FieldDescriptor::TYPE_FLOAT ||
          f->type() == google::protobuf::FieldDescriptor::TYPE_DOUBLE) {
        return true;
      } else if (f->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE &&
                 visited.insert(f->message_type()).second &&
                 HasFloatingPointFields(f->message_type(), visited)) {
        return true;
      }
    }
    return false;
  }
};

// Hasher used by EqualElementMultiSet in tests only.
struct MultisetValueContentContainerElementHasher {
  explicit MultisetValueContentContainerElementHasher(
      FloatMargin float_margin_arg, const Type* type)
      : float_margin(float_margin_arg), type(type) {}

  size_t operator()(const internal::ValueContentContainerElement& x) const {
    if (!float_margin.IsExactEquality()) {
      return absl::Hash<HashableValueContentContainerElementIgnoringFloat>()(
          HashableValueContentContainerElementIgnoringFloat(x, type));
    }
    return absl::Hash<ContainerType::HashableValueContentContainerElement>()(
        ContainerType::HashableValueContentContainerElement{x, type});
  }

 private:
  FloatMargin float_margin;
  const Type* type;
};

ArrayType::ArrayType(const TypeFactory* factory, const Type* element_type)
    : ContainerType(factory, TYPE_ARRAY), element_type_(element_type) {
  ZETASQL_CHECK(!element_type->IsArray());  // Blocked in MakeArrayType.
}

ArrayType::~ArrayType() {}

bool ArrayType::IsSupportedType(const LanguageOptions& language_options) const {
  return element_type()->IsSupportedType(language_options);
}

bool ArrayType::EqualsForSameKind(const Type* that, bool equivalent) const {
  const ArrayType* other = that->AsArray();
  ZETASQL_DCHECK(other);
  return EqualsImpl(this, other, equivalent);
}

void ArrayType::DebugStringImpl(bool details, TypeOrStringVector* stack,
                                std::string* debug_string) const {
  absl::StrAppend(debug_string, "ARRAY<");
  stack->push_back(">");
  stack->push_back(element_type());
}

bool ArrayType::SupportsOrdering(const LanguageOptions& language_options,
                                 std::string* type_description) const {
  if (language_options.LanguageFeatureEnabled(FEATURE_V_1_3_ARRAY_ORDERING) &&
      element_type()->SupportsOrdering(language_options,
                                       /*type_description=*/nullptr)) {
    return true;
  }
  if (type_description != nullptr) {
    if (language_options.LanguageFeatureEnabled(FEATURE_V_1_3_ARRAY_ORDERING)) {
      // If the ARRAY ordering feature is on, then arrays with orderable
      // elements are also orderable.  So return a <type_description> that
      // also indicates the type of the unorderable element.
      *type_description = absl::StrCat(
          TypeKindToString(this->kind(), language_options.product_mode()),
          " containing ",
          TypeKindToString(this->element_type()->kind(),
                           language_options.product_mode()));
    } else {
      // If the ARRAY ordering feature is not enabled then the returned
      // <type_description> is simply ARRAY.
      *type_description = TypeKindToString(this->kind(),
                                           language_options.product_mode());
    }
  }
  return false;
}

bool ArrayType::SupportsEquality() const {
  return element_type()->SupportsEquality();
}

bool ArrayType::SupportsGroupingImpl(const LanguageOptions& language_options,
                                     const Type** no_grouping_type) const {
  if (!language_options.LanguageFeatureEnabled(FEATURE_V_1_2_GROUP_BY_ARRAY)) {
    if (no_grouping_type != nullptr) {
      *no_grouping_type = this;
    }
    return false;
  }

  if (!element_type()->SupportsGroupingImpl(language_options,
                                            no_grouping_type)) {
    return false;
  }
  if (no_grouping_type != nullptr) {
    *no_grouping_type = nullptr;
  }
  return true;
}

bool ArrayType::SupportsPartitioningImpl(
    const LanguageOptions& language_options,
    const Type** no_partitioning_type) const {
  if (!language_options.LanguageFeatureEnabled(FEATURE_V_1_2_GROUP_BY_ARRAY)) {
    if (no_partitioning_type != nullptr) {
      *no_partitioning_type = this;
    }
    return false;
  }

  if (!element_type()->SupportsPartitioningImpl(language_options,
                                                no_partitioning_type)) {
    return false;
  }
  if (no_partitioning_type != nullptr) {
    *no_partitioning_type = nullptr;
  }
  return true;
}

absl::Status ArrayType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    const BuildFileDescriptorSetMapOptions& options, TypeProto* type_proto,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  type_proto->set_type_kind(kind_);
  return element_type()->SerializeToProtoAndDistinctFileDescriptorsImpl(
      options, type_proto->mutable_array_type()->mutable_element_type(),
      file_descriptor_set_map);
}

std::string ArrayType::ShortTypeName(ProductMode mode) const {
  return absl::StrCat("ARRAY<", element_type_->ShortTypeName(mode), ">");
}

std::string ArrayType::TypeName(ProductMode mode) const {
  return absl::StrCat("ARRAY<", element_type_->TypeName(mode), ">");
}

absl::StatusOr<std::string> ArrayType::TypeNameWithModifiers(
    const TypeModifiers& type_modifiers, ProductMode mode) const {
  const TypeParameters& type_params = type_modifiers.type_parameters();
  if (!type_params.IsEmpty() && type_params.num_children() != 1) {
    return MakeSqlError()
           << "Input type parameter does not correspond to ArrayType";
  }

  const Collation& collation = type_modifiers.collation();
  if (!collation.HasCompatibleStructure(this)) {
    return MakeSqlError() << "Input collation " << collation.DebugString()
                          << " is not compatible with type " << DebugString();
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::string element_type_name,
      element_type_->TypeNameWithModifiers(
          TypeModifiers::MakeTypeModifiers(
              type_params.IsEmpty() ? TypeParameters() : type_params.child(0),
              collation.Empty() ? Collation() : collation.child(0)),
          mode));
  return absl::StrCat("ARRAY<", element_type_name, ">");
}

absl::StatusOr<TypeParameters> ArrayType::ValidateAndResolveTypeParameters(
    const std::vector<TypeParameterValue>& type_parameter_values,
    ProductMode mode) const {
  return MakeSqlError() << ShortTypeName(mode)
                        << " type cannot have type parameters by itself, it "
                           "can only have type parameters on its element type";
}

absl::Status ArrayType::ValidateResolvedTypeParameters(
    const TypeParameters& type_parameters, ProductMode mode) const {
  // type_parameters must be empty or has the one child.
  if (type_parameters.IsEmpty()) {
    return absl::OkStatus();
  }
  ZETASQL_RET_CHECK_EQ(type_parameters.num_children(), 1);
  return element_type_->ValidateResolvedTypeParameters(type_parameters.child(0),
                                                       mode);
}

bool ArrayType::EqualsImpl(const ArrayType* const type1,
                           const ArrayType* const type2, bool equivalent) {
  return type1->element_type()->EqualsImpl(type2->element_type(), equivalent);
}

void ArrayType::CopyValueContent(const ValueContent& from,
                                 ValueContent* to) const {
  from.GetAs<zetasql_base::SimpleReferenceCounted*>()->Ref();
  *to = from;
}

void ArrayType::ClearValueContent(const ValueContent& value) const {
  value.GetAs<zetasql_base::SimpleReferenceCounted*>()->Unref();
}

absl::HashState ArrayType::HashTypeParameter(absl::HashState state) const {
  // Array types are equivalent if their element types are equivalent,
  // so we hash the element type kind.
  return element_type()->Hash(std::move(state));
}

absl::HashState ArrayType::HashValueContent(const ValueContent& value,
                                            absl::HashState state) const {
  const internal::ValueContentContainer* container =
      value.GetAs<internal::ValueContentContainerRef*>()->value();
  // We must hash arrays as if unordered to support hash_map and hash_set of
  // values containing arrays with order_kind()=kIgnoresOrder.
  // absl::Hash lacks support for unordered containers, so we create a
  // cheapo solution of just adding the hashcodes.
  size_t combined_hash = 1;
  for (int i = 0; i < container->num_elements(); i++) {
    ValueContentContainerElementHasher hasher(element_type());
    combined_hash += hasher(container->element(i));
  }
  return absl::HashState::combine(std::move(state), combined_hash);
}

// Compares arrays as multisets. Used in tests only. The current algorithm,
// which counts the number of the same elements, may return false negatives if
// !float_margin.IsExactEquality(). Specifically, the method may return 'false'
// on almost-equal bags if those contain elements for which approximate equality
// is non-transitive, e.g., {a, b, c} such that a~b==true, b~c==true,
// a~c==false. See a repro in value_test.cc:AlmostEqualsStructArray.
// TODO: potential fix is to implement Hopcroft-Karp algorithm:
// http://en.wikipedia.org/wiki/Hopcroft%E2%80%93Karp_algorithm
// Its complexity is O(|E|*sqrt(|V|)). Computing E requires |V|^2 comparisons,
// so we get O(|V|^2.5).
bool ArrayType::EqualElementMultiSet(
    const ValueContent& x, const ValueContent& y,
    const ValueEqualityCheckOptions& options) const {
  const internal::ValueContentContainer* x_container =
      x.GetAs<internal::ValueContentContainerRef*>()->value();
  const internal::ValueContentContainer* y_container =
      y.GetAs<internal::ValueContentContainerRef*>()->value();
  std::string* reason = options.reason;
  using CountMap =
      absl::flat_hash_map<internal::ValueContentContainerElement, int,
                          MultisetValueContentContainerElementHasher,
                          ValueContentContainerElementEq>;

  MultisetValueContentContainerElementHasher hasher(options.float_margin,
                                                    element_type());
  ValueContentContainerElementEq eq(options, element_type());
  CountMap x_multiset(x_container->num_elements(), hasher, eq);
  CountMap y_multiset(y_container->num_elements(), hasher, eq);
  ZETASQL_DCHECK_EQ(x_container->num_elements(), y_container->num_elements());
  for (int i = 0; i < x_container->num_elements(); i++) {
    x_multiset[x_container->element(i)]++;
    y_multiset[y_container->element(i)]++;
  }
  const auto& format_options = DebugFormatValueContentOptions();
  for (const auto& p : x_multiset) {
    const internal::ValueContentContainerElement& element = p.first;
    auto it = y_multiset.find(element);
    if (it == y_multiset.end()) {
      if (reason) {
        absl::StrAppend(
            reason,
            absl::Substitute("Multiset element $0 of $1 is missing in $2\n",
                             FormatValueContentContainerElement(
                                 element, element_type(), format_options),
                             FormatValueContent(x, format_options),
                             FormatValueContent(y, format_options)));
      }
      return false;
    }
    if (it->second != p.second) {
      if (reason) {
        absl::StrAppend(
            reason,
            absl::Substitute(
                "Number of occurrences of multiset element $0 is $1 and $2 "
                "respectively in multisets $3 and $4\n",
                FormatValueContentContainerElement(element, element_type(),
                                                   format_options),
                p.second, it->second, FormatValueContent(x, format_options),
                FormatValueContent(y, format_options)));
      }
      return false;
    }
  }
  if (x_multiset.size() == y_multiset.size()) {
    return true;  // All of x is in y and the sizes agree.
  }
  if (reason) {
    // There exists an element in y that's missing from x. Report it.
    for (const auto& p : y_multiset) {
      const internal::ValueContentContainerElement& element = p.first;
      if (x_multiset.find(element) == x_multiset.end()) {
        absl::StrAppend(
            reason,
            absl::Substitute("Multiset element $0 of $1 is missing in $2\n",
                             FormatValueContentContainerElement(
                                 element, element_type(), format_options),
                             FormatValueContent(x, format_options),
                             FormatValueContent(y, format_options)));
      }
    }
    ZETASQL_DCHECK(!reason->empty());
  }
  return false;
}

bool ArrayType::ValueContentEquals(
    const ValueContent& x, const ValueContent& y,
    const ValueEqualityCheckOptions& options) const {
  const internal::ValueContentContainer* x_container =
      x.GetAs<internal::ValueContentContainerRef*>()->value();
  const internal::ValueContentContainer* y_container =
      y.GetAs<internal::ValueContentContainerRef*>()->value();
  if (x_container->num_elements() != y_container->num_elements()) {
    if (options.reason) {
      const auto& format_options = DebugFormatValueContentOptions();
      absl::StrAppend(
          options.reason,
          absl::Substitute(
              "Number of array elements is {$0} and {$1} in respective "
              "arrays {$2} and {$3}\n",
              x_container->num_elements(), y_container->num_elements(),
              FormatValueContent(x, format_options),
              FormatValueContent(y, format_options)));
    }
    return false;
  }

  // By default use options provided in arguments
  ValueEqualityCheckOptions const* element_options = &options;
  std::unique_ptr<ValueEqualityCheckOptions> options_copy = nullptr;
  if (options.deep_order_spec != nullptr) {
    options_copy = std::make_unique<ValueEqualityCheckOptions>(options);
    options_copy->deep_order_spec = &options.deep_order_spec->children[0];
    element_options = options_copy.get();
    if (options.deep_order_spec->ignores_order) {
      return EqualElementMultiSet(x, y, *element_options);
    }
  }

  ValueContentContainerElementEq eq(*element_options, element_type());
  for (int i = 0; i < x_container->num_elements(); i++) {
    if (!eq(x_container->element(i), y_container->element(i))) {
      return false;
    }
  }
  return true;
}

bool ArrayType::ValueContentLess(const ValueContent& x, const ValueContent& y,
                                 const Type* other_type) const {
  const internal::ValueContentContainer* x_container =
      x.GetAs<internal::ValueContentContainerRef*>()->value();
  const internal::ValueContentContainer* y_container =
      y.GetAs<internal::ValueContentContainerRef*>()->value();
  const Type* x_element_type = element_type();
  const Type* y_element_type = other_type->AsArray()->element_type();
  for (int i = 0;
       i < std::min(x_container->num_elements(), y_container->num_elements());
       ++i) {
    const std::optional<bool> is_less = ValueContentContainerElementLess(
        x_container->element(i), y_container->element(i), x_element_type,
        y_element_type);
    if (is_less.has_value()) return *is_less;
  }
  return x_container->num_elements() < y_container->num_elements();
}

absl::Status ArrayType::SerializeValueContent(const ValueContent& value,
                                              ValueProto* value_proto) const {
  return absl::FailedPreconditionError(
      "SerializeValueContent should never be called for ArrayType, since its "
      "value content is maintained in the Value class");
}

absl::Status ArrayType::DeserializeValueContent(const ValueProto& value_proto,
                                                ValueContent* value) const {
  return absl::FailedPreconditionError(
      "DeserializeValueContent should never be called for ArrayType, since its "
      "value content is maintained in the Value class");
}

std::string ArrayType::GetFormatPrefix(
    const ValueContent& value_content,
    const FormatValueContentOptions& options) const {
  std::string prefix;
  switch (options.mode) {
    case Type::FormatValueContentOptions::Mode::kDebug: {
      const internal::ValueContentContainerRef* container_ref =
          value_content.GetAs<internal::ValueContentContainerRef*>();
      if (options.verbose) {
        const internal::ValueContentContainer* container =
            container_ref->value();
        if (container->num_elements() == 0) {
          prefix.append(CapitalizedName());
        } else {
          prefix.append("Array");
        }
      }
      prefix.push_back('[');
      if (!container_ref->preserves_order()) {
        prefix.append("unordered: ");
      }
      break;
    }
    case Type::FormatValueContentOptions::Mode::kSQLLiteral: {
      prefix.push_back('[');
      break;
    }
    case Type::FormatValueContentOptions::Mode::kSQLExpression: {
      prefix.append(TypeName(options.product_mode));
      prefix.push_back('[');
      break;
    }
  }
  return prefix;
}

char ArrayType::GetFormatClosingCharacter(
    const Type::FormatValueContentOptions& options) const {
  return ']';
}

const Type* ArrayType::GetElementType(int index) const {
  return element_type();
}

}  // namespace zetasql
