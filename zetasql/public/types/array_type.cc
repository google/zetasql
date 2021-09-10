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

#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/value_content.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/simple_reference_counted.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

ArrayType::ArrayType(const TypeFactory* factory, const Type* element_type)
    : Type(factory, TYPE_ARRAY),
      element_type_(element_type) {
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

absl::StatusOr<std::string> ArrayType::TypeNameWithParameters(
    const TypeParameters& type_params, ProductMode mode) const {
  if (type_params.IsEmpty()) {
    return TypeName(mode);
  }
  if (type_params.num_children() != 1) {
    return MakeSqlError()
           << "Input type parameter does not correspond to ArrayType";
  }
  ZETASQL_ASSIGN_OR_RETURN(
      std::string element_parameters,
      element_type_->TypeNameWithParameters(type_params.child(0), mode));
  return absl::StrCat("ARRAY<", element_parameters, ">");
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
  ZETASQL_RET_CHECK(type_parameters.IsStructOrArrayParameters());
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
  // TODO: currently ArrayType cannot create a list of Values
  // itself because "types" package doesn't depend on "value" (to avoid
  // dependency cycle). In the future we will create a virtual list factory
  // interface defined outside of "value", but which Value can provide to
  // Array/Struct to use to construct lists.
  ZETASQL_LOG(FATAL) << "HashValueContent should never be called for ArrayType, since "
                "its value content is created in Value class";
}

bool ArrayType::ValueContentEquals(
    const ValueContent& x, const ValueContent& y,
    const ValueEqualityCheckOptions& options) const {
  ZETASQL_LOG(FATAL) << "ValueContentEquals should never be called for ArrayType,"
                "since its value content is compared in Value class";
}

bool ArrayType::ValueContentLess(const ValueContent& x, const ValueContent& y,
                                 const Type* other_type) const {
  ZETASQL_LOG(FATAL) << "ValueContentLess should never be called for ArrayType,"
                "since its value content is compared in Value class";
}

std::string ArrayType::FormatValueContent(
    const ValueContent& value, const FormatValueContentOptions& options) const {
  ZETASQL_LOG(FATAL)
      << "FormatValueContent should never be called for ArrayType, since "
         "its value content is maintained in the Value class";
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

}  // namespace zetasql
