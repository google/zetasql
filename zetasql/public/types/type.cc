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

#include "zetasql/public/types/type.h"

#include <stddef.h>
#include <stdlib.h>

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/simple_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/value.pb.h"
#include "zetasql/public/value_content.h"
#include "absl/base/macros.h"
#include "absl/base/optimization.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {
struct TypeKindInfo {
  const char* const name;

  // This is used to determine the cost of coercing from one type to another,
  // computed as the difference in the two type values.  Note that this is
  // different from <specificity> in that FLOAT and DOUBLE are reversed
  // in cost ordering, reflecting that we prefer to coerce INTs to DOUBLE rather
  // than FLOAT.  Additionally, we prefer coercing from STRING/INT to TIMESTAMP
  // rather than any of the legacy timestamp types (TIMESTAMP_MICROS, etc.).
  // Date/time related types can only be coerced from STRING, we prefer coercing
  // from STRING to the closest of these date/time types before coercing to
  // date/time types that are earlier in the list.
  const int cost;

  // Type specificity is used for Supertype analysis.  To evaluate the
  // Supertype of multiple types, we get the sets of supertypes associated
  // with each type, and the common supertype is the most specific type
  // in their intersection.
  const int specificity;

  const bool simple;
};
}  // namespace

// Order of type names here should match values of TypeKind enum proto.
// None of the built-in type names/aliases should start with "[a-zA-Z]_",
// which is reserved for user-defined objects.
static const TypeKindInfo kTypeKindInfo[]{
    // clang-format off
    // name              cost, specificity,  simple },
    // 0-4
    {"UNKNOWN",             0,           0,   false },
    {"INT32",              12,          12,    true },
    {"INT64",              14,          14,    true },
    {"UINT32",             11,          11,    true },
    {"UINT64",             13,          13,    true },

    // 5-9
    {"BOOL",               10,          10,    true },
    {"FLOAT",              18,          17,    true },
    {"DOUBLE",             17,          18,    true },
    {"STRING",             19,          19,    true },
    {"BYTES",              20,          20,    true },

    // 10-14
    {"DATE",                9,           7,    true },
    {"TIMESTAMP_SECONDS",   7,           3,    true },
    {"TIMESTAMP_MILLIS",    6,           4,    true },
    {"TIMESTAMP_MICROS",    5,           5,    true },
    {"TIMESTAMP_NANOS",     4,           6,    true },

    // 15-19
    {"ENUM",                1,           1,   false },
    {"ARRAY",              23,          23,   false },
    {"STRUCT",             22,          22,   false },
    {"PROTO",              21,          21,   false },
    {"TIMESTAMP",           8,           2,    true },

    // 20-21
    {"TIME",                2,           8,    true },
    {"DATETIME",            3,           9,    true },

    // 22
    {"GEOGRAPHY",          24,          24,    true },

    // 23
    {"NUMERIC",            15,          15,    true },

    // 24
    {"BIGNUMERIC",         16,          16,    true },

    // 25
    {"EXTENDED",           25,          25,   false },

    // 26
    {"JSON",               26,          26,    true },

    // 27
    {"INTERVAL",           27,          27,    true },

    // clang-format on
    // When a new entry is added here, update
    // TypeTest::VerifyCostAndSpecificity.
};

static_assert(ABSL_ARRAYSIZE(kTypeKindInfo) == TypeKind_ARRAYSIZE,
              "kTypeKindInfo wrong size");

// The following condition must hold true or the mapping to kTypeKindInfo
// will not work and the TypeKindToString() function will fail.
// -1 is the __TypeKind__switch_must_have_a_default__ value.
static_assert(TypeKind_MIN == -1 && TypeKind_MAX == TypeKind_ARRAYSIZE -1,
              "TypeKind must go from -1 to ARRAYSIZE -1");

Type::Type(const TypeFactory* factory, TypeKind kind)
    : type_store_(internal::TypeStoreHelper::GetTypeStore(factory)),
      kind_(kind) {}

Type::~Type() {
}

// static
bool Type::IsSimpleType(TypeKind kind) {
  if (ABSL_PREDICT_TRUE(kind > TypeKind_MIN && kind <= TypeKind_MAX)) {
    return kTypeKindInfo[kind].simple;
  }
  return false;
}

bool Type::IsSupportedSimpleTypeKind(TypeKind kind,
                                     const LanguageOptions& language_options) {
  ZETASQL_DCHECK(IsSimpleType(kind));
  const zetasql::Type* type = types::TypeFromSimpleTypeKind(kind);
  return type->IsSupportedType(language_options);
}

TypeKind Type::ResolveBuiltinTypeNameToKindIfSimple(absl::string_view type_name,
                                                    ProductMode mode) {
  return SimpleType::GetTypeKindIfSimple(type_name, mode);
}

TypeKind Type::ResolveBuiltinTypeNameToKindIfSimple(
    absl::string_view type_name, const LanguageOptions& language_options) {
  return SimpleType::GetTypeKindIfSimple(
      type_name, language_options.product_mode(),
      &language_options.GetEnabledLanguageFeatures());
}

std::string Type::TypeKindToString(TypeKind kind, ProductMode mode) {
  // Note that for types not externally supported we still want to produce
  // the internal names for them.  This is because during development
  // we want error messages to indicate what the unsupported type actually
  // is as an aid in debugging.  When used in production in external mode,
  // those internal names should never actually be reachable.
  if (ABSL_PREDICT_TRUE(kind > TypeKind_MIN && kind <= TypeKind_MAX)) {
    if (mode == PRODUCT_EXTERNAL && kind == TYPE_DOUBLE) {
      return "FLOAT64";
    }
    return kTypeKindInfo[kind].name;
  }
  return absl::StrCat("INVALID_TYPE_KIND(", kind, ")");
}

std::string Type::TypeKindListToString(const std::vector<TypeKind>& kinds,
                                       ProductMode mode) {
  std::vector<std::string> kind_strings;
  kind_strings.reserve(kinds.size());
  for (const TypeKind& kind : kinds) {
    kind_strings.push_back(TypeKindToString(kind, mode));
  }
  return absl::StrJoin(kind_strings, ", ");
}

std::string Type::TypeListToString(TypeListView types, ProductMode mode) {
  std::vector<std::string> type_strings;
  type_strings.reserve(types.size());
  for (const Type* type : types) {
    type_strings.push_back(type->ShortTypeName(mode));
  }
  return absl::StrJoin(type_strings, ", ");
}

int Type::KindSpecificity(TypeKind kind) {
  if (ABSL_PREDICT_TRUE(kind > TypeKind_MIN && kind <= TypeKind_MAX)) {
    return kTypeKindInfo[kind].specificity;
  }

  ZETASQL_LOG(FATAL) << "Out of range: " << kind;
}

static int KindCost(TypeKind kind) {
  if (ABSL_PREDICT_TRUE(kind > TypeKind_MIN && kind <= TypeKind_MAX)) {
    return kTypeKindInfo[kind].cost;
  }

  ZETASQL_LOG(FATAL) << "Out of range: " << kind;
}

int Type::GetTypeCoercionCost(TypeKind kind1, TypeKind kind2) {
  return abs(KindCost(kind1) - KindCost(kind2));
}

bool Type::KindSpecificityLess(TypeKind kind1, TypeKind kind2) {
  ZETASQL_DCHECK_NE(kind1, TypeKind::TYPE_EXTENDED);
  ZETASQL_DCHECK_NE(kind2, TypeKind::TYPE_EXTENDED);

  return KindSpecificity(kind1) < KindSpecificity(kind2);
}

bool Type::TypeSpecificityLess(const Type* t1, const Type* t2) {
  return KindSpecificityLess(t1->kind(), t2->kind());
}

absl::Status Type::SerializeToProtoAndFileDescriptors(
    TypeProto* type_proto,
    google::protobuf::FileDescriptorSet* file_descriptor_set,
    std::set<const google::protobuf::FileDescriptor*>* file_descriptors) const {
  type_proto->Clear();

  FileDescriptorSetMap file_descriptor_set_map;
  if (file_descriptors != nullptr && !file_descriptors->empty()) {
    const google::protobuf::DescriptorPool* pool = (*file_descriptors->begin())->pool();
    std::unique_ptr<FileDescriptorEntry> file_descriptor_entry(
        new FileDescriptorEntry);
    file_descriptor_entry->descriptor_set_index = 0;
    if (file_descriptor_set != nullptr) {
      file_descriptor_entry->file_descriptor_set.Swap(file_descriptor_set);
    }
    file_descriptor_entry->file_descriptors.swap(*file_descriptors);
    file_descriptor_set_map.emplace(pool, std::move(file_descriptor_entry));
  }
  // Optimization: skip building the descriptor sets if they would not
  // affect the output at all.
  BuildFileDescriptorSetMapOptions options;
  options.build_file_descriptor_sets =
      file_descriptor_set != nullptr || file_descriptors != nullptr;

  // No limit on FileDescriptorSet size.
  ZETASQL_RETURN_IF_ERROR(SerializeToProtoAndDistinctFileDescriptorsImpl(
      options, type_proto, &file_descriptor_set_map));
  if (file_descriptor_set_map.size() > 1) {
    return MakeSqlError()
           << "Unable to serialize descriptors spanning multiple "
              "DescriptorPools into a single FileDescriptorSet. "
              "Use SerializeToProtoAndDistinctFileDescriptors "
              "instead.";
  } else if (!file_descriptor_set_map.empty()) {
    const std::unique_ptr<FileDescriptorEntry>& file_descriptor_entry =
        file_descriptor_set_map.begin()->second;
    if (file_descriptor_set != nullptr) {
      file_descriptor_set->Swap(&file_descriptor_entry->file_descriptor_set);
    }
    if (file_descriptors != nullptr) {
      file_descriptors->swap(file_descriptor_entry->file_descriptors);
    }
  }
  return absl::OkStatus();
}

absl::Status Type::SerializeToProtoAndDistinctFileDescriptors(
    TypeProto* type_proto,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  // No limit on FileDescriptorSet size.
  return SerializeToProtoAndDistinctFileDescriptors(
      type_proto,
      /*file_descriptor_sets_max_size_bytes=*/absl::optional<int64_t>(),
      file_descriptor_set_map);
}

absl::Status Type::SerializeToProtoAndDistinctFileDescriptors(
    TypeProto* type_proto,
    absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  ZETASQL_RET_CHECK(file_descriptor_set_map != nullptr);
  type_proto->Clear();
  BuildFileDescriptorSetMapOptions options;
  options.file_descriptor_sets_max_size_bytes =
      file_descriptor_sets_max_size_bytes;
  return SerializeToProtoAndDistinctFileDescriptorsImpl(
      options, type_proto, file_descriptor_set_map);
}

absl::Status Type::SerializeToSelfContainedProto(
    TypeProto* type_proto) const {
  type_proto->Clear();
  FileDescriptorSetMap file_descriptor_set_map;
  // No limit on FileDescriptorSet size.  TODO: Allow a limit to be
  // provided here as well.  Maybe this should just call the non-Impl version.
  ZETASQL_RETURN_IF_ERROR(SerializeToProtoAndDistinctFileDescriptorsImpl(
      {}, type_proto, &file_descriptor_set_map));
  // Determine the order of the FileDescriptorSets in the TypeProto.
  std::vector<google::protobuf::FileDescriptorSet*> file_descriptor_sets;
  file_descriptor_sets.resize(file_descriptor_set_map.size());
  for (const auto& pool_and_file_set : file_descriptor_set_map) {
    file_descriptor_sets[pool_and_file_set.second->descriptor_set_index] =
        &pool_and_file_set.second->file_descriptor_set;
  }
  // Now add all of them in order.
  for (google::protobuf::FileDescriptorSet* file_descriptor_set : file_descriptor_sets) {
    ZETASQL_RET_CHECK(file_descriptor_set != nullptr);
    type_proto->add_file_descriptor_set()->Swap(file_descriptor_set);
  }
  return absl::OkStatus();
}

std::string Type::ShortTypeName(ProductMode mode) const {
  return TypeName(mode);
}

std::string Type::DebugString(bool details) const {
  std::string debug_string;

  TypeOrStringVector stack;
  stack.push_back(this);
  while (!stack.empty()) {
    const auto stack_entry = stack.back();
    stack.pop_back();
    if (absl::holds_alternative<std::string>(stack_entry)) {
      absl::StrAppend(&debug_string, absl::get<std::string>(stack_entry));
      continue;
    }
    absl::get<const Type*>(stack_entry)
        ->DebugStringImpl(details, &stack, &debug_string);
  }
  return debug_string;
}

bool Type::SupportsGrouping(const LanguageOptions& language_options,
                            std::string* type_description) const {
  const Type* no_grouping_type;
  const bool supports_grouping =
      this->SupportsGroupingImpl(language_options, &no_grouping_type);
  if (!supports_grouping && type_description != nullptr) {
    if (no_grouping_type == this) {
      *type_description =
          TypeKindToString(this->kind(), language_options.product_mode());
    } else {
      *type_description = absl::StrCat(
          TypeKindToString(this->kind(), language_options.product_mode()),
          " containing ",
          TypeKindToString(no_grouping_type->kind(),
                           language_options.product_mode()));
    }
  }
  return supports_grouping;
}

bool Type::SupportsGroupingImpl(const LanguageOptions& language_options,
                                const Type** no_grouping_type) const {
  if (no_grouping_type != nullptr) {
    *no_grouping_type = this;
  }
  return false;
}

bool Type::SupportsPartitioning(const LanguageOptions& language_options,
                                std::string* type_description) const {
  const Type* no_partitioning_type;
  const bool supports_partitioning =
      this->SupportsPartitioningImpl(language_options, &no_partitioning_type);

  if (!supports_partitioning && type_description != nullptr) {
    if (no_partitioning_type == this) {
      *type_description =
          TypeKindToString(this->kind(), language_options.product_mode());
    } else {
      *type_description = absl::StrCat(
          TypeKindToString(this->kind(), language_options.product_mode()),
          " containing ",
          TypeKindToString(no_partitioning_type->kind(),
                           language_options.product_mode()));
    }
  }
  return supports_partitioning;
}

bool Type::SupportsPartitioningImpl(const LanguageOptions& language_options,
                                    const Type** no_partitioning_type) const {
  bool supports_partitioning =
      !this->IsGeography() && !this->IsFloatingPoint() && !this->IsJson();

  if (no_partitioning_type != nullptr) {
    *no_partitioning_type = supports_partitioning ? nullptr : this;
  }
  return supports_partitioning;
}

bool Type::SupportsOrdering(const LanguageOptions& language_options,
                            std::string* type_description) const {
  bool supports_ordering = !IsGeography() && !IsJson();
  if (supports_ordering) return true;
  if (type_description != nullptr) {
    *type_description = TypeKindToString(this->kind(),
                                         language_options.product_mode());
  }
  return false;
}

bool Type::SupportsOrdering() const {
  return SupportsOrdering(LanguageOptions(), /*type_description=*/nullptr);
}

// Array type equality support is controlled by the language option
// FEATURE_V_1_1_ARRAY_EQUALITY. To test if 'type' supports equality,
// checks the type recursively as array types can be nested under
// struct types or vice versa.
bool Type::SupportsEquality(
    const LanguageOptions& language_options) const {
  if (this->IsArray()) {
    if (language_options.LanguageFeatureEnabled(FEATURE_V_1_1_ARRAY_EQUALITY)) {
      return this->AsArray()->element_type()->SupportsEquality(
          language_options);
    } else {
      return false;
    }
  } else if (this->IsStruct()) {
    for (const StructField& field : this->AsStruct()->fields()) {
      if (!field.type->SupportsEquality(language_options)) {
        return false;
      }
    }
    return true;
  }
  return this->SupportsEquality();
}

void Type::CopyValueContent(const ValueContent& from, ValueContent* to) const {
  *to = from;
}

absl::HashState Type::Hash(absl::HashState state) const {
  // Hash a type's kind.
  state = absl::HashState::combine(std::move(state), kind());

  // Hash a type's parameter.
  return HashTypeParameter(std::move(state));
}

absl::Status Type::TypeMismatchError(const ValueProto& value_proto) const {
  return absl::Status(
      absl::StatusCode::kInternal,
      absl::StrCat("Type mismatch: provided type ", DebugString(),
                   " but proto <",
                   value_proto.ShortDebugString(),
                   "> doesn't have field of that type and is not null"));
}

bool TypeEquals::operator()(const Type* const type1,
                            const Type* const type2) const {
  if (type1 == type2) {
    // Note that two nullptr Type pointers will compare to TRUE.
    return true;
  }
  if (type1 == nullptr || type2 == nullptr) {
    // If one is nullptr and the other not nullptr, then they cannot be equal.
    return false;
  }
  return type1->Equals(type2);
}

bool TypeEquivalent::operator()(const Type* const type1,
                                const Type* const type2) const {
  if (type1 == type2) {
    // Note that two nullptr Type pointers will compare to TRUE.
    return true;
  }
  if (type1 == nullptr || type2 == nullptr) {
    // If one is nullptr and the other not nullptr, then they cannot be equal.
    return false;
  }
  return type1->Equivalent(type2);
}

size_t TypeHash::operator()(const Type* const type) const {
  if (type == nullptr) {
    return 17 * 23;  // Some random number to represent nullptr.
  }

  return absl::Hash<Type>()(*type);
}

absl::StatusOr<TypeParameters> Type::ValidateAndResolveTypeParameters(
    const std::vector<TypeParameterValue>& type_parameter_values,
    ProductMode mode) const {
  return MakeSqlError() << "Type " << ShortTypeName(mode)
                        << "does not support type parameters";
}

absl::Status Type::ValidateResolvedTypeParameters(
    const TypeParameters& type_parameters, ProductMode mode) const {
  ZETASQL_RET_CHECK(type_parameters.IsEmpty())
      << "Type " << ShortTypeName(mode) << "does not support type parameters";
  return absl::OkStatus();
}

}  // namespace zetasql
