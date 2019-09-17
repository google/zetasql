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

#include "zetasql/public/type.h"

#include <stdlib.h>

#include <algorithm>
#include <limits>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.pb.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/proto_helper.h"
#include "zetasql/proto/options.pb.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/functions/normalize_mode.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/proto/type_annotation.pb.h"
#include "zetasql/public/proto/wire_format_annotation.pb.h"
#include "zetasql/public/strings.h"
#include "absl/base/call_once.h"
#include <cstdint>
#include "absl/base/macros.h"
#include "absl/base/optimization.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "zetasql/base/case.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "zetasql/base/cleanup.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

ABSL_FLAG(int32_t, zetasql_type_factory_nesting_depth_limit,
          std::numeric_limits<int32_t>::max(),
          "The maximum nesting depth for types that zetasql::TypeFactory "
          "will allow to be created. Set this to a bounded value to avoid "
          "stack overflows.");

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
static const TypeKindInfo kTypeKindInfo[] {
  /* name              cost, specificity,  simple }, */
  // 0-4
  { "UNKNOWN",            0,           0,   false },
  { "INT32",             12,          12,    true },
  { "INT64",             14,          14,    true },
  { "UINT32",            11,          11,    true },
  { "UINT64",            13,          13,    true },

  // 5-9
  { "BOOL",              10,          10,    true },
  { "FLOAT",             17,          16,    true },
  { "DOUBLE",            16,          17,    true },
  { "STRING",            18,          18,    true },
  { "BYTES",             19,          19,    true },

  // 10-14
  { "DATE",               9,           7,    true },
  { "TIMESTAMP_SECONDS",  7,           3,    true },
  { "TIMESTAMP_MILLIS",   6,           4,    true },
  { "TIMESTAMP_MICROS",   5,           5,    true },
  { "TIMESTAMP_NANOS",    4,           6,    true },

  // 15-19
  { "ENUM",               1,           1,   false },
  { "ARRAY",             22,          22,   false },
  { "STRUCT",            21,          21,   false },
  { "PROTO",             20,          20,   false },
  { "TIMESTAMP",          8,           2,    true },

  // 20-21
  { "TIME",               2,           8,    true },
  { "DATETIME",           3,           9,    true },

  // 22
  { "GEOGRAPHY",         23,          23,    true },

  // 23
  { "NUMERIC",           15,          15,    true }

  // When a new entry is added here, update TypeTest::VerifyCostAndSpecificity.
};

static_assert(ABSL_ARRAYSIZE(kTypeKindInfo) == TypeKind_ARRAYSIZE,
              "kTypeKindInfo wrong size");

// The following condition must hold true or the mapping to kTypeKindInfo
// will not work and the TypeKindToString() function will fail.
// -1 is the __TypeKind__switch_must_have_a_default__ value.
static_assert(TypeKind_MIN == -1 && TypeKind_MAX == TypeKind_ARRAYSIZE -1,
              "TypeKind must go from -1 to ARRAYSIZE -1");

static const std::map<std::string, TypeKind>& SimpleTypeKindMap() {
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
  };
  return *result;
}

// See (broken link) for approved list of externally visible
// types.
static const std::map<std::string, TypeKind>& ExternalModeSimpleTypeKindMap() {
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
  };
  return *result;
}

namespace {
static const std::set<TypeKind> ExternalModeSimpleTypeKinds() {
  std::set<TypeKind> external_mode_simple_type_kinds;
  for (const auto& external_simple_type : ExternalModeSimpleTypeKindMap()) {
    // Note that kExternalModeSimpleTypeKindMap has duplicate TypeKinds, so
    // we use InsertIfNotPresent() here.
    zetasql_base::InsertIfNotPresent(&external_mode_simple_type_kinds,
                            external_simple_type.second);
  }
  return external_mode_simple_type_kinds;
}

}  // namespace

static const std::set<TypeKind>& GetExternalModeSimpleTypeKinds() {
  // We populate this set once and use it ever after.
  static const std::set<TypeKind>* kExternalModeSimpleTypeKinds =
      new std::set<TypeKind>(ExternalModeSimpleTypeKinds());
  return *kExternalModeSimpleTypeKinds;
}

Type::Type(const TypeFactory* factory, TypeKind kind)
    : type_factory_(factory), kind_(kind) {
}

Type::~Type() {
}

// static
bool Type::IsSimpleType(TypeKind kind) {
  if (ABSL_PREDICT_TRUE(kind > TypeKind_MIN && kind <= TypeKind_MAX)) {
    return kTypeKindInfo[kind].simple;
  }
  return false;
}

bool Type::IsSupportedSimpleType(
    const LanguageOptions& language_options) const {
  DCHECK(IsSimpleType(kind()));
  const ProductMode mode = language_options.product_mode();
  // Note that the IsSimpleType() call above guarantees that 'kind' is in
  // range.
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
  return true;
}

bool Type::IsSupportedSimpleTypeKind(TypeKind kind,
                                     const LanguageOptions& language_options) {
  DCHECK(IsSimpleType(kind));
  const zetasql::Type* type = types::TypeFromSimpleTypeKind(kind);
  return type->IsSupportedSimpleType(language_options);
}

bool Type::IsSupportedType(const LanguageOptions& language_options) const {
  if (IsSimpleType()) {
    return IsSupportedSimpleType(language_options);
  }
  switch (kind()) {
    case TYPE_PROTO:
      return language_options.SupportsProtoTypes();
    case TYPE_ENUM: {
      // Enums are generally unsupported in EXTERNAL mode, except for the
      // DateTimestampPart enum that is used in many of the date/time
      // related functions.
      if (language_options.product_mode() == ProductMode::PRODUCT_EXTERNAL &&
          !Equivalent(types::DatePartEnumType()) &&
          !Equivalent(types::NormalizeModeEnumType())) {
        return false;
      }
      return true;
    }
    case TYPE_STRUCT: {
      // A Struct is supported if all of its fields are supported.
      for (const StructField& field : AsStruct()->fields()) {
        if (!field.type->IsSupportedType(language_options)) {
          return false;
        }
      }
      return true;
    }
    case TYPE_ARRAY:
      return AsArray()->element_type()->IsSupportedType(language_options);
    default:
      LOG(FATAL) << "Unexpected type: " << DebugString();
  }
}

bool Type::IsSimpleTypeName(const std::string& type_name, ProductMode mode) {
  if (mode == PRODUCT_EXTERNAL) {
    return zetasql_base::ContainsKey(ExternalModeSimpleTypeKindMap(),
                            absl::AsciiStrToLower(type_name));
  }
  return zetasql_base::ContainsKey(SimpleTypeKindMap(),
                          absl::AsciiStrToLower(type_name));
}

TypeKind Type::SimpleTypeNameToTypeKindOrDie(const std::string& type_name,
                                             ProductMode mode) {
  if (mode == PRODUCT_EXTERNAL) {
    return zetasql_base::FindOrDie(ExternalModeSimpleTypeKindMap(),
                          absl::AsciiStrToLower(type_name));
  }
  return zetasql_base::FindOrDie(SimpleTypeKindMap(), absl::AsciiStrToLower(type_name));
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

int Type::KindSpecificity(TypeKind kind) {
  if (ABSL_PREDICT_TRUE(kind > TypeKind_MIN && kind <= TypeKind_MAX)) {
    return kTypeKindInfo[kind].specificity;
  }

  LOG(FATAL) << "Out of range: " << kind;
}

static int KindCost(TypeKind kind) {
  if (ABSL_PREDICT_TRUE(kind > TypeKind_MIN && kind <= TypeKind_MAX)) {
    return kTypeKindInfo[kind].cost;
  }

  LOG(FATAL) << "Out of range: " << kind;
}

int Type::GetTypeCoercionCost(TypeKind kind1, TypeKind kind2) {
  return abs(KindCost(kind1) - KindCost(kind2));
}

bool Type::KindSpecificityLess(TypeKind kind1, TypeKind kind2) {
  return KindSpecificity(kind1) < KindSpecificity(kind2);
}

zetasql_base::Status Type::SerializeToProtoAndFileDescriptors(
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
  // No limit on FileDescriptorSet size.
  ZETASQL_RETURN_IF_ERROR(SerializeToProtoAndDistinctFileDescriptorsImpl(
      type_proto,
      /*file_descriptor_sets_max_size_bytes=*/absl::optional<int64_t>(),
      &file_descriptor_set_map));
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
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Type::SerializeToProtoAndDistinctFileDescriptors(
    TypeProto* type_proto,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  // No limit on FileDescriptorSet size.
  return SerializeToProtoAndDistinctFileDescriptors(
      type_proto,
      /*file_descriptor_sets_max_size_bytes=*/absl::optional<int64_t>(),
      file_descriptor_set_map);
}

zetasql_base::Status Type::SerializeToProtoAndDistinctFileDescriptors(
    TypeProto* type_proto,
    absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  ZETASQL_RET_CHECK(file_descriptor_set_map != nullptr);
  type_proto->Clear();
  return SerializeToProtoAndDistinctFileDescriptorsImpl(
      type_proto, file_descriptor_sets_max_size_bytes, file_descriptor_set_map);
}

zetasql_base::Status Type::SerializeToSelfContainedProto(
    TypeProto* type_proto) const {
  type_proto->Clear();
  FileDescriptorSetMap file_descriptor_set_map;
  // No limit on FileDescriptorSet size.  TODO: Allow a limit to be
  // provided here as well.  Maybe this should just call the non-Impl version.
  ZETASQL_RETURN_IF_ERROR(SerializeToProtoAndDistinctFileDescriptorsImpl(
      type_proto,
      /*file_descriptor_sets_max_size_bytes=*/absl::optional<int64_t>(),
      &file_descriptor_set_map));
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
  return ::zetasql_base::OkStatus();
}

std::string Type::ShortTypeName(ProductMode mode) const { return TypeName(mode); }

namespace {

// Helper function that finds a field or a named extension with the given name.
// Possible return values are HAS_FIELD if the field exists, HAS_PSEUDO_FIELD
// if the named extension exists, or HAS_NO_FIELD if neither exists.
Type::HasFieldResult HasProtoFieldOrNamedExtension(
    const google::protobuf::Descriptor* descriptor, const std::string& name, int* field_id) {
  const google::protobuf::FieldDescriptor* field =
      ProtoType::FindFieldByNameIgnoreCase(descriptor, name);
  if (field != nullptr) {
    *field_id = field->number();
    return Type::HAS_FIELD;
  }

  return Type::HAS_NO_FIELD;
}

}  // namespace

Type::HasFieldResult Type::HasField(const std::string& name, int* field_id,
                                    bool include_pseudo_fields) const {
  Type::HasFieldResult result = HAS_NO_FIELD;
  constexpr int kNotFound = -1;
  int found_idx = kNotFound;
  if (this->IsStruct()) {
    bool is_ambiguous;
    const StructType::StructField* field =
        this->AsStruct()->FindField(name, &is_ambiguous, &found_idx);
    if (is_ambiguous) {
      result = HAS_AMBIGUOUS_FIELD;
    } else if (field != nullptr) {
      result = HAS_FIELD;
    }
  } else if (this->IsProto()) {
    const google::protobuf::Descriptor* descriptor = this->AsProto()->descriptor();
    if (include_pseudo_fields) {
      // Consider virtual fields in addition to physical fields, which means
      // there may be ambiguity between a built-in field and a virtual field.
      result = HasProtoFieldOrNamedExtension(descriptor, name, &found_idx);
      if (absl::StartsWithIgnoreCase(name, "has_") &&
          HasProtoFieldOrNamedExtension(descriptor, name.substr(4),
                                        &found_idx) != HAS_NO_FIELD) {
        result =
            (result != HAS_NO_FIELD) ? HAS_AMBIGUOUS_FIELD : HAS_PSEUDO_FIELD;
      }
    } else {
      // Look for physical field only, so the result is always unambiguous.
      const google::protobuf::FieldDescriptor* field =
          ProtoType::FindFieldByNameIgnoreCase(descriptor, name);
      if (field != nullptr) {
        found_idx = field->number();
        result = Type::HAS_FIELD;
      }
    }
  }
  if (field_id != nullptr && found_idx != kNotFound) {
    *field_id = found_idx;
  }
  return result;
}

bool Type::HasAnyFields() const {
  if (this->IsStruct()) {
    return this->AsStruct()->num_fields() != 0;
  } else if (this->IsProto()) {
    return this->AsProto()->descriptor()->field_count() != 0;
  } else {
    return false;
  }
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
  const bool supports_grouping =
      !this->IsGeography() &&
      !(this->IsFloatingPoint() && language_options.LanguageFeatureEnabled(
                                       FEATURE_DISALLOW_GROUP_BY_FLOAT));
  if (no_grouping_type != nullptr) {
    *no_grouping_type = supports_grouping ? nullptr : this;
  }
  return supports_grouping;
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
  bool supports_partitioning = !this->IsGeography() && !this->IsFloatingPoint();
  if (no_partitioning_type != nullptr) {
    *no_partitioning_type = supports_partitioning ? nullptr : this;
  }
  return supports_partitioning;
}

bool Type::SupportsOrdering(const LanguageOptions& language_options,
                            std::string* type_description) const {
  if (IsGeography()) {
    if (type_description != nullptr) {
      *type_description = TypeKindToString(this->kind(),
                                           language_options.product_mode());
    }
    return false;
  }
  return true;
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

static int64_t FileDescriptorSetMapTotalSize(
    const FileDescriptorSetMap& file_descriptor_set_map) {
  int64_t total_size = 0;
  for (const auto& entry : file_descriptor_set_map) {
    total_size += entry.second->file_descriptor_set.ByteSizeLong();
  }
  return total_size;
}

// Adds the file descriptor and all of its dependencies to the given map of file
// descriptor sets, indexed by the file descriptor's pool. Returns the 0-based
// <file_descriptor_set_index> corresponding to file descriptor set to which
// the dependencies were added.  Returns an error on out-of-memory.
static zetasql_base::Status PopulateDistinctFileDescriptorSets(
    const google::protobuf::FileDescriptor* file_descr,
    absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
    FileDescriptorSetMap* file_descriptor_set_map,
    int* file_descriptor_set_index) {
  ZETASQL_RET_CHECK(file_descr != nullptr);
  ZETASQL_RET_CHECK(file_descriptor_set_map != nullptr);

  std::unique_ptr<Type::FileDescriptorEntry>& file_descriptor_entry =
      (*file_descriptor_set_map)[file_descr->pool()];
  if (file_descriptor_entry == nullptr) {
    // This is a new entry in the map.
    file_descriptor_entry = absl::make_unique<Type::FileDescriptorEntry>();
    file_descriptor_entry->descriptor_set_index =
        file_descriptor_set_map->size() - 1;
  }
  absl::optional<int64_t> this_file_descriptor_set_max_size;
  if (file_descriptor_sets_max_size_bytes.has_value()) {
    const int64_t map_total_size =
        FileDescriptorSetMapTotalSize(*file_descriptor_set_map);
    this_file_descriptor_set_max_size =
        file_descriptor_sets_max_size_bytes.value() - map_total_size +
        file_descriptor_entry->file_descriptor_set.ByteSizeLong();
  }
  ZETASQL_RETURN_IF_ERROR(PopulateFileDescriptorSet(
      file_descr, this_file_descriptor_set_max_size,
      &file_descriptor_entry->file_descriptor_set,
      &file_descriptor_entry->file_descriptors));
  *file_descriptor_set_index = file_descriptor_entry->descriptor_set_index;
  return zetasql_base::OkStatus();
}

SimpleType::SimpleType(const TypeFactory* factory, TypeKind kind)
    : Type(factory, kind) {
  CHECK(IsSimpleType(kind)) << kind;
}

SimpleType::~SimpleType() {
}

zetasql_base::Status SimpleType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    TypeProto* type_proto,
    absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
    FileDescriptorSetMap* file_descriptor_set_map)
    const {
  type_proto->set_type_kind(kind_);
  return ::zetasql_base::OkStatus();
}

std::string SimpleType::TypeName(ProductMode mode) const {
  return TypeKindToString(kind_, mode);
}

std::string SimpleType::DebugString(bool unused_details) const {
  return TypeKindToString(kind_, PRODUCT_INTERNAL);
}

ArrayType::ArrayType(const TypeFactory* factory, const Type* element_type)
    : Type(factory, TYPE_ARRAY),
      element_type_(element_type) {
  CHECK(!element_type->IsArray());  // Blocked in MakeArrayType.
}

ArrayType::~ArrayType() {
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

zetasql_base::Status ArrayType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    TypeProto* type_proto,
    absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  type_proto->set_type_kind(kind_);
  return element_type()->SerializeToProtoAndDistinctFileDescriptors(
      type_proto->mutable_array_type()->mutable_element_type(),
      file_descriptor_sets_max_size_bytes, file_descriptor_set_map);
}

std::string ArrayType::ShortTypeName(ProductMode mode) const {
  return absl::StrCat("ARRAY<", element_type_->ShortTypeName(mode), ">");
}

std::string ArrayType::TypeName(ProductMode mode) const {
  return absl::StrCat("ARRAY<", element_type_->TypeName(mode), ">");
}

std::string ArrayType::DebugString(bool details) const {
  return absl::StrCat("ARRAY<", element_type_->DebugString(details), ">");
}

StructType::StructType(const TypeFactory* factory,
                       std::vector<StructField> fields, int nesting_depth)
    : Type(factory, TYPE_STRUCT),
      fields_(std::move(fields)),
      nesting_depth_(nesting_depth) {}

bool StructType::SupportsGroupingImpl(const LanguageOptions& language_options,
                                      const Type** no_grouping_type) const {
  if (!language_options.LanguageFeatureEnabled(FEATURE_V_1_2_GROUP_BY_STRUCT)) {
    if (no_grouping_type != nullptr) *no_grouping_type = this;
    return false;
  }

  for (const StructField& field : this->AsStruct()->fields()) {
    if (!field.type->SupportsGroupingImpl(language_options, no_grouping_type)) {
      return false;
    }
  }
  if (no_grouping_type != nullptr) *no_grouping_type = nullptr;
  return true;
}

bool StructType::SupportsPartitioningImpl(
    const LanguageOptions& language_options,
    const Type** no_partitioning_type) const {
  if (!language_options.LanguageFeatureEnabled(FEATURE_V_1_2_GROUP_BY_STRUCT)) {
    if (no_partitioning_type != nullptr) *no_partitioning_type = this;
    return false;
  }

  for (const StructField& field : this->AsStruct()->fields()) {
    if (!field.type->SupportsPartitioningImpl(language_options,
                                              no_partitioning_type)) {
      return false;
    }
  }

  if (no_partitioning_type != nullptr) *no_partitioning_type = nullptr;
  return true;
}

StructType::~StructType() {}

bool StructType::SupportsOrdering(const LanguageOptions& language_options,
                                  std::string* type_description) const {
  if (type_description != nullptr) {
    *type_description = TypeKindToString(this->kind(),
                                         language_options.product_mode());
  }
  return false;
}

bool StructType::SupportsEquality() const {
  for (const StructField& field : fields_) {
    if (!field.type->SupportsEquality()) {
      return false;
    }
  }
  return true;
}

bool StructType::UsingFeatureV12CivilTimeType() const {
  for (const StructField& field : fields_) {
    if (field.type->UsingFeatureV12CivilTimeType()) {
      return true;
    }
  }
  return false;
}

zetasql_base::Status StructType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    TypeProto* type_proto,
    absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  // Note - we cannot type_proto->Clear(), because it might have a
  // FileDescriptorSet that we are trying to populate.
  type_proto->set_type_kind(kind_);
  StructTypeProto* struct_type_proto = type_proto->mutable_struct_type();
  std::set<const google::protobuf::FileDescriptor*> local_file_descrs;
  for (const StructField& field : fields_) {
    StructFieldProto* struct_field_proto = struct_type_proto->add_field();
    struct_field_proto->set_field_name(field.name);
    ZETASQL_RETURN_IF_ERROR(field.type->SerializeToProtoAndDistinctFileDescriptors(
        struct_field_proto->mutable_field_type(),
        file_descriptor_sets_max_size_bytes, file_descriptor_set_map));
  }
  return ::zetasql_base::OkStatus();
}


// TODO DebugString and other recursive methods on struct types
// may cause a stack overflow for deeply nested types.
std::string StructType::DebugStringImpl(
    int field_limit,
    const std::function<std::string(const zetasql::Type*)>& field_debug_fn) const {
  const int num_fields_to_show = std::min<int>(field_limit, fields_.size());
  const bool output_truncated = num_fields_to_show < fields_.size();

  std::string ret = "STRUCT<";
  for (int i = 0; i < num_fields_to_show; ++i) {
    const StructField& field = fields_[i];
    if (i != 0) absl::StrAppend(&ret, ", ");
    if (!field.name.empty()) {
      absl::StrAppend(&ret, ToIdentifierLiteral(field.name), " ");
    }
    absl::StrAppend(&ret, field_debug_fn(field.type));
  }
  if (output_truncated) {
    absl::StrAppend(&ret, ", ...");
  }
  absl::StrAppend(&ret, ">");
  return ret;
}

std::string StructType::ShortTypeName(ProductMode mode) const {
  // Limit the output to three struct fields to avoid long error messages.
  const int field_limit = 3;
  const auto field_debug_fn = [=](const zetasql::Type* type) {
    return type->ShortTypeName(mode);
  };
  return DebugStringImpl(field_limit, field_debug_fn);
}

std::string StructType::TypeName(ProductMode mode) const {
  const auto field_debug_fn = [=](const zetasql::Type* type) {
    return type->TypeName(mode);
  };
  return DebugStringImpl(std::numeric_limits<int>::max(), field_debug_fn);
}

std::string StructType::DebugString(bool details) const {
  const auto field_debug_fn = [=](const zetasql::Type* type) {
    return type->DebugString(details);
  };
  // No limit on the number of struct fields to show.
  return DebugStringImpl(std::numeric_limits<int>::max(), field_debug_fn);
}

const StructType::StructField* StructType::FindField(
    absl::string_view name, bool* is_ambiguous, int* found_idx) const {
  *is_ambiguous = false;
  if (found_idx != nullptr) *found_idx = -1;

  // Empty names indicate unnamed fields, not fields named "".
  if (ABSL_PREDICT_FALSE(name.empty())) {
    return nullptr;
  }

  int field_index;
  {
    absl::MutexLock lock(&mutex_);
    if (ABSL_PREDICT_FALSE(field_name_to_index_map_.empty())) {
      for (int i = 0; i < num_fields(); ++i) {
        const std::string& field_name = field(i).name;
        // Empty names indicate unnamed fields, not fields which can be looked
        // up by name. They are not added to the map.
        if (!field_name.empty()) {
          auto result = field_name_to_index_map_.emplace(field_name, i);
          // If the name has already been added to the map, we know any lookup
          // on that name would be ambiguous.
          if (!result.second) result.first->second = -1;
        }
      }
    }
    const auto iter = field_name_to_index_map_.find(name);
    if (ABSL_PREDICT_FALSE(iter == field_name_to_index_map_.end())) {
      return nullptr;
    }
    field_index = iter->second;
  }

  if (ABSL_PREDICT_FALSE(field_index == -1)) {
    *is_ambiguous = true;
    return nullptr;
  } else {
    if (found_idx != nullptr) *found_idx = field_index;
    return &fields_[field_index];
  }
}

ProtoType::ProtoType(const TypeFactory* factory,
                     const google::protobuf::Descriptor* descriptor)
    : Type(factory, TYPE_PROTO), descriptor_(descriptor) {
  CHECK(descriptor_ != nullptr);
}

ProtoType::~ProtoType() {
}

bool ProtoType::SupportsOrdering(const LanguageOptions& language_options,
                                 std::string* type_description) const {
  if (type_description != nullptr) {
    *type_description = TypeKindToString(this->kind(),
                                         language_options.product_mode());
  }
  return false;
}

const google::protobuf::Descriptor* ProtoType::descriptor() const {
  return descriptor_;
}

zetasql_base::Status ProtoType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    TypeProto* type_proto,
    absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  type_proto->set_type_kind(kind_);
  ProtoTypeProto* proto_type_proto = type_proto->mutable_proto_type();
  proto_type_proto->set_proto_name(descriptor_->full_name());
  proto_type_proto->set_proto_file_name(descriptor_->file()->name());
  // Note that right now we are not supporting extensions to TypeProto, so
  // we do not need to look for all extensions of this proto.  Therefore the
  // FileDescriptorSet can be derived from the descriptor's FileDescriptor
  // dependencies.
  int set_index;
  ZETASQL_RETURN_IF_ERROR(PopulateDistinctFileDescriptorSets(
      descriptor_->file(), file_descriptor_sets_max_size_bytes,
      file_descriptor_set_map, &set_index));
  if (set_index != 0) {
    proto_type_proto->set_file_descriptor_set_index(set_index);
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ProtoType::GetFieldTypeByTagNumber(int number,
                                                TypeFactory* factory,
                                                bool use_obsolete_timestamp,
                                                const Type** type,
                                                std::string* name) const {
  const google::protobuf::FieldDescriptor* field_descr =
      descriptor_->FindFieldByNumber(number);
  if (field_descr == nullptr) {
    return MakeSqlError()
           << "Field number " << number << " not found in descriptor "
           << descriptor_->full_name();
  }
  if (name != nullptr) {
    *name = field_descr->name();
  }
  return factory->GetProtoFieldType(field_descr, use_obsolete_timestamp, type);
}

zetasql_base::Status ProtoType::GetFieldTypeByName(const std::string& name,
                                           TypeFactory* factory,
                                           bool use_obsolete_timestamp,
                                           const Type** type,
                                           int* number) const {
  const google::protobuf::FieldDescriptor* field_descr =
      descriptor_->FindFieldByName(name);
  if (field_descr == nullptr) {
    return MakeSqlError()
           << "Field name " << name << " not found in descriptor "
           << descriptor_->full_name();
  }
  if (number != nullptr) {
    *number = field_descr->number();
  }
  return factory->GetProtoFieldType(field_descr, use_obsolete_timestamp, type);
}

std::string ProtoType::TypeName() const {
  return ToIdentifierLiteral(descriptor_->full_name());
}

std::string ProtoType::ShortTypeName(ProductMode mode_unused) const {
  return descriptor_->full_name();
}

std::string ProtoType::TypeName(ProductMode mode_unused) const {
  return TypeName();
}

std::string ProtoType::DebugString(bool details) const {
  std::string debug_string;
  absl::StrAppend(&debug_string, "PROTO<", descriptor_->full_name());
  if (details) {
    absl::StrAppend(&debug_string, ", file name: ", descriptor_->file()->name(),
                    ", <", descriptor_->DebugString(), ">");
  }
  absl::StrAppend(&debug_string, ">");
  return debug_string;
}

// static
zetasql_base::Status ProtoType::GetTypeKindFromFieldDescriptor(
    const google::protobuf::FieldDescriptor* field, bool ignore_format_annotations,
    bool use_obsolete_timestamp, TypeKind* kind) {
  const google::protobuf::FieldDescriptor::Type field_type = field->type();
  if (!ignore_format_annotations) {
    ZETASQL_RETURN_IF_ERROR(ProtoType::ValidateTypeAnnotations(field));
  }
  const FieldFormat::Format format =
      ignore_format_annotations ? FieldFormat::DEFAULT_FORMAT
                                : ProtoType::GetFormatAnnotation(field);
  switch (field_type) {
    case google::protobuf::FieldDescriptor::TYPE_INT32:
    case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
    case google::protobuf::FieldDescriptor::TYPE_SINT32: {
      switch (format) {
        case FieldFormat::DEFAULT_FORMAT:
          *kind = TYPE_INT32;
          break;
        case FieldFormat::DATE:
        case FieldFormat::DATE_DECIMAL:
          *kind = TYPE_DATE;
          break;
        default:
          // Should not reach this if ValidateTypeAnnotations() is working
          // properly.
          return MakeSqlError()
                 << "Proto " << field->containing_type()->full_name()
                 << " has invalid zetasql.format for INT32 field: "
                 << field->DebugString();
      }
      break;
    }
    case google::protobuf::FieldDescriptor::TYPE_INT64:
    case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
    case google::protobuf::FieldDescriptor::TYPE_SINT64: {
      switch (format) {
        case FieldFormat::DEFAULT_FORMAT:
          *kind = TYPE_INT64;
          break;
        case FieldFormat::DATE:
        case FieldFormat::DATE_DECIMAL:
          *kind = TYPE_DATE;
          break;
        case FieldFormat::TIMESTAMP_SECONDS:
          if (use_obsolete_timestamp) {
          } else {
            *kind = TYPE_TIMESTAMP;
          }
          break;
        case FieldFormat::TIMESTAMP_MILLIS:
          if (use_obsolete_timestamp) {
          } else {
            *kind = TYPE_TIMESTAMP;
          }
          break;
        case FieldFormat::TIMESTAMP_MICROS:
          if (use_obsolete_timestamp) {
          } else {
            *kind = TYPE_TIMESTAMP;
          }
          break;
        case FieldFormat::TIMESTAMP_NANOS:
          *kind = TYPE_TIMESTAMP;
          break;
        case FieldFormat::TIME_MICROS:
          *kind = TYPE_TIME;
          break;
        case FieldFormat::DATETIME_MICROS:
          *kind = TYPE_DATETIME;
          break;
        default:
          // Should not reach this if ValidateTypeAnnotations() is working
          // properly.
          return MakeSqlError()
                 << "Proto " << field->containing_type()->full_name()
                 << " has invalid zetasql.format for INT64 field: "
                 << field->DebugString();
      }
      break;
    }
    case google::protobuf::FieldDescriptor::TYPE_FIXED32:
    case google::protobuf::FieldDescriptor::TYPE_UINT32:
      *kind = TYPE_UINT32;
      break;
    case google::protobuf::FieldDescriptor::TYPE_FIXED64:
      *kind = TYPE_UINT64;
      break;
    case google::protobuf::FieldDescriptor::TYPE_UINT64: {
      switch (format) {
        case FieldFormat::DEFAULT_FORMAT:
          *kind = TYPE_UINT64;
          break;
        case FieldFormat::TIMESTAMP_MICROS:
          if (use_obsolete_timestamp) {
          } else {
            *kind = TYPE_TIMESTAMP;
          }
          break;
        default:
          // Should not reach this if ValidateTypeAnnotations() is working
          // properly.
          return MakeSqlError()
                 << "Proto " << field->containing_type()->full_name()
                 << " has invalid zetasql.format for UINT64 field: "
                 << field->DebugString();
      }
      break;
    }
    case google::protobuf::FieldDescriptor::TYPE_BYTES: {
      switch (format) {
        case FieldFormat::DEFAULT_FORMAT:
          *kind = TYPE_BYTES;
          break;
        case FieldFormat::ST_GEOGRAPHY_ENCODED:
          *kind = TYPE_GEOGRAPHY;
          break;
        case FieldFormat::NUMERIC:
          *kind = TYPE_NUMERIC;
          break;
        default:
          // Should not reach this if ValidateTypeAnnotations() is working
          // properly.
          return MakeSqlError()
                 << "Proto " << field->containing_type()->full_name()
                 << " has invalid zetasql.format for BYTES field: "
                 << field->DebugString();
      }
      break;
    }
    case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
      *kind = TYPE_DOUBLE;
      break;
    case google::protobuf::FieldDescriptor::TYPE_FLOAT:
      *kind = TYPE_FLOAT;
      break;
    case google::protobuf::FieldDescriptor::TYPE_BOOL:
      *kind = TYPE_BOOL;
      break;
    case google::protobuf::FieldDescriptor::TYPE_ENUM:
      *kind = TYPE_ENUM;
      break;
    case google::protobuf::FieldDescriptor::TYPE_STRING:
      *kind = TYPE_STRING;
      break;
    case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
      if (ignore_format_annotations) {
        *kind = TYPE_PROTO;
        break;
      }
      ABSL_FALLTHROUGH_INTENDED;
    case google::protobuf::FieldDescriptor::TYPE_GROUP:
      // NOTE: We are not unwrapping is_wrapper or is_struct here.
      // Use MakeUnwrappedTypeFromProto to get an unwrapped type.
      *kind = TYPE_PROTO;
      break;
    default:
      return MakeSqlError() << "Invalid protocol buffer Type: " << field_type;
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ProtoType::FieldDescriptorToTypeKind(
    bool ignore_annotations, const google::protobuf::FieldDescriptor* field,
    TypeKind* kind) {
  if (field->label() == google::protobuf::FieldDescriptor::LABEL_REPEATED) {
    *kind = TYPE_ARRAY;
  } else {
    ZETASQL_RETURN_IF_ERROR(
        FieldDescriptorToTypeKindBase(ignore_annotations, field, kind));
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ProtoType::FieldDescriptorToTypeKind(
    const google::protobuf::FieldDescriptor* field, bool use_obsolete_timestamp,
    TypeKind* kind) {
  if (field->label() == google::protobuf::FieldDescriptor::LABEL_REPEATED) {
    *kind = TYPE_ARRAY;
  } else {
    ZETASQL_RETURN_IF_ERROR(
        FieldDescriptorToTypeKindBase(field, use_obsolete_timestamp, kind));
  }
  return ::zetasql_base::OkStatus();
}

const google::protobuf::FieldDescriptor* ProtoType::FindFieldByNameIgnoreCase(
    const google::protobuf::Descriptor* descriptor, const std::string& name) {
  // Try the fast way first.
  const google::protobuf::FieldDescriptor* found = descriptor->FindFieldByName(name);
  if (found != nullptr) return found;

  // We don't bother looking for multiple names that match.
  // Protos with duplicate names (case insensitively) do not compile in
  // c++ or java, so we don't worry about them.
  for (int i = 0; i < descriptor->field_count(); ++i) {
    const google::protobuf::FieldDescriptor* field = descriptor->field(i);
    if (zetasql_base::StringCaseEqual(field->name(), name)) {
      return field;
    }
  }
  return nullptr;
}

bool ProtoType::HasFormatAnnotation(
    const google::protobuf::FieldDescriptor* field) {
  return GetFormatAnnotationImpl(field) != FieldFormat::DEFAULT_FORMAT;
}

FieldFormat::Format ProtoType::GetFormatAnnotationImpl(
    const google::protobuf::FieldDescriptor* field) {
  // Read the format encoding, or if it doesn't exist, the type encoding.
  if (field->options().HasExtension(zetasql::format)) {
    return field->options().GetExtension(zetasql::format);
  } else if (field->options().HasExtension(zetasql::type)) {
    return field->options().GetExtension(zetasql::type);
  } else {
    return FieldFormat::DEFAULT_FORMAT;
  }
}

FieldFormat::Format ProtoType::GetFormatAnnotation(
    const google::protobuf::FieldDescriptor* field) {
  // Read the format (or deprecated type) encoding.
  const FieldFormat::Format format = GetFormatAnnotationImpl(field);

  const DeprecatedEncoding::Encoding encoding =
      field->options().GetExtension(zetasql::encoding);
  // If we also have a (valid) deprecated encoding annotation, merge that over
  // top of the type encoding.  Ignore any invalid encoding annotation.
  // This exists for backward compatibility with existing .proto files only.
  if (encoding == DeprecatedEncoding::DATE_DECIMAL &&
      format == FieldFormat::DATE) {
    return FieldFormat::DATE_DECIMAL;
  }
  return format;
}

EnumType::EnumType(const TypeFactory* factory,
                   const google::protobuf::EnumDescriptor* enum_descr)
    : Type(factory, TYPE_ENUM), enum_descriptor_(enum_descr) {
  CHECK(enum_descriptor_ != nullptr);
}

EnumType::~EnumType() {
}

const google::protobuf::EnumDescriptor* EnumType::enum_descriptor() const {
  return enum_descriptor_;
}

zetasql_base::Status EnumType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    TypeProto* type_proto,
    absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  type_proto->set_type_kind(kind_);
  EnumTypeProto* enum_type_proto = type_proto->mutable_enum_type();
  enum_type_proto->set_enum_name(enum_descriptor_->full_name());
  enum_type_proto->set_enum_file_name(enum_descriptor_->file()->name());
  // Note that right now we are not supporting TypeProto extensions.  The
  // FileDescriptorSet can be derived from the enum descriptor's FileDescriptor
  // dependencies.
  int set_index;
  ZETASQL_RETURN_IF_ERROR(PopulateDistinctFileDescriptorSets(
      enum_descriptor_->file(), file_descriptor_sets_max_size_bytes,
      file_descriptor_set_map, &set_index));
  if (set_index != 0) {
    enum_type_proto->set_file_descriptor_set_index(set_index);
  }
  return ::zetasql_base::OkStatus();
}

std::string EnumType::TypeName() const {
  return ToIdentifierLiteral(enum_descriptor_->full_name());
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
  return enum_descriptor_->full_name();
}

std::string EnumType::TypeName(ProductMode mode_unused) const {
  return TypeName();
}

std::string EnumType::DebugString(bool details) const {
  std::string debug_string;
  absl::StrAppend(&debug_string, "ENUM<", enum_descriptor_->full_name());
  if (details) {
    absl::StrAppend(&debug_string,
                    ", file name: ", enum_descriptor_->file()->name(), ", <",
                    enum_descriptor_->DebugString(), ">");
  }
  absl::StrAppend(&debug_string, ">");
  return debug_string;
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

bool Type::EqualsNonSimpleTypes(const Type* that, bool equivalent) const {
  switch (kind()) {
    case TYPE_ARRAY:
      return ArrayType::EqualsImpl(static_cast<const ArrayType*>(this),
                                   static_cast<const ArrayType*>(that),
                                   equivalent);
    case TYPE_STRUCT:
      return StructType::EqualsImpl(static_cast<const StructType*>(this),
                                    static_cast<const StructType*>(that),
                                    equivalent);
    case TYPE_ENUM:
      return EnumType::EqualsImpl(static_cast<const EnumType*>(this),
                                  static_cast<const EnumType*>(that),
                                  equivalent);
    case TYPE_PROTO:
      return ProtoType::EqualsImpl(static_cast<const ProtoType*>(this),
                                   static_cast<const ProtoType*>(that),
                                   equivalent);
    default:
      break;
  }
  return false;
}

bool ArrayType::EqualsImpl(const ArrayType* const type1,
                           const ArrayType* const type2, bool equivalent) {
  return type1->element_type()->EqualsImpl(type2->element_type(), equivalent);
}

bool StructType::FieldEqualsImpl(const StructType::StructField& field1,
                                 const StructType::StructField& field2,
                                 bool equivalent) {
  // Ignore field names if we are doing an equivalence check.
  if (!equivalent && !zetasql_base::StringCaseEqual(field1.name, field2.name)) {
    return false;
  }
  return field1.type->EqualsImpl(field2.type, equivalent);
}

bool StructType::EqualsImpl(const StructType* const type1,
                            const StructType* const type2, bool equivalent) {
  if (type1->num_fields() != type2->num_fields()) {
    return false;
  }
  for (int idx = 0; idx < type1->num_fields(); ++idx) {
    if (!FieldEqualsImpl(type1->field(idx), type2->field(idx), equivalent)) {
      return false;
    }
  }
  return true;
}

bool EnumType::EqualsImpl(const EnumType* const type1,
                          const EnumType* const type2, bool equivalent) {
  if (type1->enum_descriptor() == type2->enum_descriptor()) {
    return true;
  }
  if (equivalent &&
      type1->enum_descriptor()->full_name() ==
      type2->enum_descriptor()->full_name()) {
    return true;
  }
  return false;
}

bool ProtoType::EqualsImpl(const ProtoType* const type1,
                           const ProtoType* const type2, bool equivalent) {
  if (type1->descriptor() == type2->descriptor()) {
    return true;
  }
  if (equivalent &&
      type1->descriptor()->full_name() == type2->descriptor()->full_name()) {
    return true;
  }
  return false;
}

bool TypeEquals::operator()(const Type* const type1,
                            const Type* const type2) const {
  if (type1 == type2) {
    // Note that two NULL types will compare to TRUE.
    return true;
  }
  if (type1 == nullptr || type2 == nullptr) {
    // If one is NULL and the other not NULL, then they cannot be equal.
    return false;
  }
  return type1->Equals(type2);
}

TypeFactory::TypeFactory()
    : cached_simple_types_(),
      nesting_depth_limit_(
          absl::GetFlag(FLAGS_zetasql_type_factory_nesting_depth_limit)) {
  VLOG(2) << "Created TypeFactory " << this
          ;
}

TypeFactory::~TypeFactory() {
  // Need to delete these in a loop because the destructor is only visible
  // via friend declaration on Type.
  for (const Type* type : owned_types_) {
    delete type;
  }

  if (!factories_depending_on_this_.empty()) {
    LOG(DFATAL) << "Destructing TypeFactory " << this
                << " is unsafe because TypeFactory "
                << *factories_depending_on_this_.begin()
                << " depends on it staying alive.\n"
                << "Using --vmodule=type=2 may aid debugging.\n"
                ;
    // Avoid crashing on the TypeFactory dependency reference itself.
    for (const TypeFactory* other : factories_depending_on_this_) {
      absl::MutexLock l(&other->mutex_);
      other->depends_on_factories_.erase(this);
    }
  }

  for (const TypeFactory* other : depends_on_factories_) {
    absl::MutexLock l(&other->mutex_);
    other->factories_depending_on_this_.erase(this);
  }
}

int TypeFactory::nesting_depth_limit() const {
  absl::MutexLock l(&mutex_);
  return nesting_depth_limit_;
}

void TypeFactory::set_nesting_depth_limit(int value) {
  // We don't want to have to check the depth for simple types, so a depth of
  // 0 must be allowed.
  DCHECK_GE(value, 0);
  absl::MutexLock l(&mutex_);
  nesting_depth_limit_ = value;
}

template <class TYPE>
const TYPE* TypeFactory::TakeOwnership(const TYPE* type) {
  absl::MutexLock l(&mutex_);
  return TakeOwnershipLocked(type);
}

template <class TYPE>
const TYPE* TypeFactory::TakeOwnershipLocked(const TYPE* type) {
  DCHECK_EQ(type->type_factory_, this);
  owned_types_.push_back(type);
  return type;
}

const Type* TypeFactory::get_int32() { return MakeSimpleType(TYPE_INT32); }
const Type* TypeFactory::get_int64() { return MakeSimpleType(TYPE_INT64); }
const Type* TypeFactory::get_uint32() { return MakeSimpleType(TYPE_UINT32); }
const Type* TypeFactory::get_uint64() { return MakeSimpleType(TYPE_UINT64); }
const Type* TypeFactory::get_string() { return MakeSimpleType(TYPE_STRING); }
const Type* TypeFactory::get_bytes() { return MakeSimpleType(TYPE_BYTES); }
const Type* TypeFactory::get_bool() { return MakeSimpleType(TYPE_BOOL); }
const Type* TypeFactory::get_float() { return MakeSimpleType(TYPE_FLOAT); }
const Type* TypeFactory::get_double() { return MakeSimpleType(TYPE_DOUBLE); }
const Type* TypeFactory::get_date() { return MakeSimpleType(TYPE_DATE); }
const Type* TypeFactory::get_timestamp() {
  return MakeSimpleType(TYPE_TIMESTAMP);
}
const Type* TypeFactory::get_time() { return MakeSimpleType(TYPE_TIME); }
const Type* TypeFactory::get_datetime() {
  return MakeSimpleType(TYPE_DATETIME);
}
const Type* TypeFactory::get_geography() {
  return MakeSimpleType(TYPE_GEOGRAPHY);
}
const Type* TypeFactory::get_numeric() {
  return MakeSimpleType(TYPE_NUMERIC);
}

const Type* TypeFactory::MakeSimpleType(TypeKind kind) {
  CHECK(Type::IsSimpleType(kind)) << kind;
  absl::MutexLock l(&mutex_);
  if (cached_simple_types_[kind] == nullptr) {
    cached_simple_types_[kind] =
        TakeOwnershipLocked(new SimpleType(this, kind));
  }
  return cached_simple_types_[kind];
}

zetasql_base::Status TypeFactory::MakeArrayType(
    const Type* element_type, const ArrayType** result) {
  *result = nullptr;
  AddDependency(element_type);
  if (element_type->IsArray()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Array of array types are not supported";
  } else {
    const int depth_limit = nesting_depth_limit();
    if (element_type->nesting_depth() + 1 > depth_limit) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Array type would exceed nesting depth limit of "
             << depth_limit;
    }
    *result = TakeOwnership(new ArrayType(this, element_type));
    return ::zetasql_base::OkStatus();
  }
}

zetasql_base::Status TypeFactory::MakeArrayType(
    const Type* element_type, const Type** result) {
  return MakeArrayType(element_type,
                       reinterpret_cast<const ArrayType**>(result));
}

zetasql_base::Status TypeFactory::MakeStructType(
    absl::Span<const StructType::StructField> fields,
    const StructType** result) {
  std::vector<StructType::StructField> new_fields(fields.begin(), fields.end());
  return MakeStructTypeFromVector(std::move(new_fields), result);
}

zetasql_base::Status TypeFactory::MakeStructType(
    absl::Span<const StructType::StructField> fields, const Type** result) {
  return MakeStructType(fields, reinterpret_cast<const StructType**>(result));
}

zetasql_base::Status TypeFactory::MakeStructTypeFromVector(
    std::vector<StructType::StructField> fields, const StructType** result) {
  *result = nullptr;
  const int depth_limit = nesting_depth_limit();
  int max_nesting_depth = 0;
  for (const StructType::StructField& field : fields) {
    const int nesting_depth = field.type->nesting_depth();
    max_nesting_depth = std::max(max_nesting_depth, nesting_depth);
    if (ABSL_PREDICT_FALSE(nesting_depth + 1 > depth_limit)) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Struct type would exceed nesting depth limit of "
             << depth_limit;
    }
    AddDependency(field.type);
  }
  // We calculate <max_nesting_depth> in the previous loop. We also need to
  // increment it to take into account the struct itself.
  *result = TakeOwnership(
      new StructType(this, std::move(fields), max_nesting_depth + 1));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TypeFactory::MakeStructTypeFromVector(
    std::vector<StructType::StructField> fields, const Type** result) {
  return MakeStructTypeFromVector(std::move(fields),
                                  reinterpret_cast<const StructType**>(result));
}

zetasql_base::Status TypeFactory::MakeProtoType(
    const google::protobuf::Descriptor* descriptor, const ProtoType** result) {
  *result = TakeOwnership(new ProtoType(this, descriptor));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TypeFactory::MakeProtoType(
    const google::protobuf::Descriptor* descriptor, const Type** result) {
  return MakeProtoType(descriptor, reinterpret_cast<const ProtoType**>(result));
}

zetasql_base::Status TypeFactory::MakeEnumType(
    const google::protobuf::EnumDescriptor* enum_descriptor, const EnumType** result) {
  *result = TakeOwnership(new EnumType(this, enum_descriptor));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TypeFactory::MakeEnumType(
    const google::protobuf::EnumDescriptor* enum_descriptor, const Type** result) {
  return MakeEnumType(enum_descriptor,
                      reinterpret_cast<const EnumType**>(result));
}

zetasql_base::Status TypeFactory::MakeUnwrappedTypeFromProto(
    const google::protobuf::Descriptor* message, bool use_obsolete_timestamp,
    const Type** result_type) {
  std::set<const google::protobuf::Descriptor*> ancestor_messages;
  return MakeUnwrappedTypeFromProtoImpl(
      message, nullptr /* existing_message_type */, use_obsolete_timestamp,
      result_type, &ancestor_messages);
}

zetasql_base::Status TypeFactory::UnwrapTypeIfAnnotatedProto(
    const Type* input_type, bool use_obsolete_timestamp,
    const Type** result_type) {
  std::set<const google::protobuf::Descriptor*> ancestor_messages;
  return UnwrapTypeIfAnnotatedProtoImpl(input_type, use_obsolete_timestamp,
                                        result_type, &ancestor_messages);
}

zetasql_base::Status TypeFactory::UnwrapTypeIfAnnotatedProtoImpl(
    const Type* input_type, bool use_obsolete_timestamp,
    const Type** result_type,
    std::set<const google::protobuf::Descriptor*>* ancestor_messages) {
  if (input_type->IsArray()) {
    // For Arrays, unwrap the element type inside the array.
    const ArrayType* array_type = input_type->AsArray();
    const Type* element_type = array_type->element_type();
    const Type* unwrapped_element_type;
    // If this is an array<proto>, unwrap the proto element if necessary.
    if (element_type->IsProto()) {
      ZETASQL_RETURN_IF_ERROR(MakeUnwrappedTypeFromProtoImpl(
          element_type->AsProto()->descriptor(), element_type,
          use_obsolete_timestamp, &unwrapped_element_type, ancestor_messages));
      ZETASQL_RETURN_IF_ERROR(MakeArrayType(unwrapped_element_type, &array_type));
    }
    *result_type = array_type;
    return ::zetasql_base::OkStatus();
  } else if (input_type->IsProto()) {
    return MakeUnwrappedTypeFromProtoImpl(input_type->AsProto()->descriptor(),
                                          input_type, use_obsolete_timestamp,
                                          result_type, ancestor_messages);
  } else {
    *result_type = input_type;
    return ::zetasql_base::OkStatus();
  }
}

zetasql_base::Status TypeFactory::MakeUnwrappedTypeFromProtoImpl(
    const google::protobuf::Descriptor* message, const Type* existing_message_type,
    bool use_obsolete_timestamp, const Type** result_type,
    std::set<const google::protobuf::Descriptor*>* ancestor_messages) {
  if (!ancestor_messages->insert(message).second) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid proto " << message->full_name()
           << " has type annotations but is recursive";
  }
  // Always erase 'message' before returning so 'ancestor_messages' contains
  // only ancestors of the current message being unwrapped.
  auto cleanup = ::zetasql_base::MakeCleanup([message, ancestor_messages] {
    ancestor_messages->erase(message);
  });
  zetasql_base::Status return_status;
  if (ProtoType::GetIsWrapperAnnotation(message)) {
    // If we have zetasql.is_wrapper, unwrap the proto and return the type
    // of the contained field.
    if (message->field_count() != 1) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Proto " << message->full_name()
             << " is invalid because it has zetasql.is_wrapper annotation"
                " but does not have exactly one field";
    }
    const google::protobuf::FieldDescriptor* proto_field = message->field(0);
    const Type* field_type;
    ZETASQL_RETURN_IF_ERROR(
        GetProtoFieldType(proto_field, use_obsolete_timestamp, &field_type));
    ZETASQL_RET_CHECK_EQ(field_type->IsArray(), proto_field->is_repeated());
    if (!proto_field->options().GetExtension(zetasql::is_raw_proto)) {
      return_status = UnwrapTypeIfAnnotatedProtoImpl(
          field_type, use_obsolete_timestamp, result_type, ancestor_messages);
    } else {
      *result_type = field_type;
    }
  } else if (ProtoType::GetIsStructAnnotation(message)) {
    // If we have zetasql.is_struct, convert this proto to a struct type.
    std::vector<StructType::StructField> struct_fields;
    for (int i = 0; i < message->field_count(); ++i) {
      const google::protobuf::FieldDescriptor* proto_field = message->field(i);
      const Type* field_type;
      ZETASQL_RETURN_IF_ERROR(
          GetProtoFieldType(proto_field, use_obsolete_timestamp, &field_type));
      if (!proto_field->options().GetExtension(zetasql::is_raw_proto)) {
        const Type* unwrapped_field_type;
        ZETASQL_RETURN_IF_ERROR(UnwrapTypeIfAnnotatedProtoImpl(
            field_type, use_obsolete_timestamp, &unwrapped_field_type,
            ancestor_messages));
        field_type = unwrapped_field_type;
      }

      std::string name = proto_field->name();
      if (ProtoType::HasStructFieldName(proto_field)) {
        name = ProtoType::GetStructFieldName(proto_field);
      }

      struct_fields.emplace_back(name, field_type);
    }
    return_status = MakeStructType(struct_fields, result_type);
  } else if (existing_message_type != nullptr) {
    // Use the message_type we already have allocated.
    DCHECK(existing_message_type->IsProto());
    DCHECK_EQ(message->full_name(),
              existing_message_type->AsProto()->descriptor()->full_name());
    *result_type = existing_message_type;
    return_status = ::zetasql_base::OkStatus();
  } else {
    return_status = MakeProtoType(message, result_type);
  }
  return return_status;
}

bool ProtoType::GetUseDefaultsExtension(
    const google::protobuf::FieldDescriptor* field) {
  if (field->options().HasExtension(zetasql::use_defaults)) {
    // If the field has a use_defaults extension, use that.
    return field->options().GetExtension(zetasql::use_defaults);
  } else {
    // Otherwise, use its message's use_field_defaults extension
    // (which defaults to true if not explicitly set).
    const google::protobuf::Descriptor* parent = field->containing_type();
    return parent->options().GetExtension(zetasql::use_field_defaults);
  }
}

bool ProtoType::GetIsWrapperAnnotation(const google::protobuf::Descriptor* message) {
  return message->options().GetExtension(zetasql::is_wrapper);
}

bool ProtoType::GetIsStructAnnotation(const google::protobuf::Descriptor* message) {
  return message->options().GetExtension(zetasql::is_struct);
}

bool ProtoType::HasStructFieldName(const google::protobuf::FieldDescriptor* field) {
  return field->options().HasExtension(zetasql::struct_field_name);
}

const std::string& ProtoType::GetStructFieldName(
    const google::protobuf::FieldDescriptor* field) {
  return field->options().GetExtension(zetasql::struct_field_name);
}

zetasql_base::Status TypeFactory::GetProtoFieldTypeWithKind(
    const google::protobuf::FieldDescriptor* field_descr, TypeKind kind,
    const Type** type) {
  if (Type::IsSimpleType(kind)) {
    *type = MakeSimpleType(kind);
  } else if (kind == TYPE_ENUM) {
    const EnumType* enum_type;
    ZETASQL_RETURN_IF_ERROR(MakeEnumType(field_descr->enum_type(), &enum_type));
    *type = enum_type;
  } else if (kind == TYPE_PROTO) {
    ZETASQL_RETURN_IF_ERROR(MakeProtoType(field_descr->message_type(), type));
  } else {
    return ::zetasql_base::UnimplementedErrorBuilder()
           << "Unsupported type found: "
           << Type::TypeKindToString(kind, PRODUCT_INTERNAL);
  }
  if (field_descr->is_repeated()) {
    const ArrayType* array_type;
    ZETASQL_RETURN_IF_ERROR(MakeArrayType(*type, &array_type));
    *type = array_type;
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TypeFactory::GetProtoFieldType(
    bool ignore_annotations, const google::protobuf::FieldDescriptor* field_descr,
    const Type** type) {
  TypeKind kind;
  ZETASQL_RETURN_IF_ERROR(ProtoType::FieldDescriptorToTypeKindBase(ignore_annotations,
                                                           field_descr, &kind));
  ZETASQL_RETURN_IF_ERROR(GetProtoFieldTypeWithKind(field_descr, kind, type));
  if (ZETASQL_DEBUG_MODE) {
    // For testing, make sure the TypeKinds we get from
    // FieldDescriptorToTypeKind match the Types returned by this method.
    TypeKind computed_type_kind;
    ZETASQL_RETURN_IF_ERROR(ProtoType::FieldDescriptorToTypeKind(
        ignore_annotations, field_descr, &computed_type_kind));
    ZETASQL_RET_CHECK_EQ((*type)->kind(), computed_type_kind)
        << (*type)->DebugString() << "\n"
        << field_descr->DebugString();
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status TypeFactory::GetProtoFieldType(
    const google::protobuf::FieldDescriptor* field_descr, bool use_obsolete_timestamp,
    const Type** type) {
  TypeKind kind;
  ZETASQL_RETURN_IF_ERROR(ProtoType::FieldDescriptorToTypeKindBase(
      field_descr, use_obsolete_timestamp, &kind));

  ZETASQL_RETURN_IF_ERROR(GetProtoFieldTypeWithKind(field_descr, kind, type));
  if (ZETASQL_DEBUG_MODE) {
    // For testing, make sure the TypeKinds we get from
    // FieldDescriptorToTypeKind match the Types returned by this method.
    TypeKind computed_type_kind;
    ZETASQL_RETURN_IF_ERROR(ProtoType::FieldDescriptorToTypeKind(
        field_descr, use_obsolete_timestamp, &computed_type_kind));
    ZETASQL_RET_CHECK_EQ((*type)->kind(), computed_type_kind)
        << (*type)->DebugString() << "\n" << field_descr->DebugString();
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TypeFactory::DeserializeFromProtoUsingExistingPool(
    const TypeProto& type_proto,
    const google::protobuf::DescriptorPool* pool,
    const Type** type) {
  return DeserializeFromProtoUsingExistingPools(type_proto, {pool}, type);
}

zetasql_base::Status TypeFactory::DeserializeFromProtoUsingExistingPools(
    const TypeProto& type_proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    const Type** type) {
  *type = nullptr;
  if (!type_proto.has_type_kind() ||
      (type_proto.type_kind() == TYPE_ARRAY) != type_proto.has_array_type() ||
      (type_proto.type_kind() == TYPE_ENUM) != type_proto.has_enum_type() ||
      (type_proto.type_kind() == TYPE_PROTO) != type_proto.has_proto_type() ||
      (type_proto.type_kind() == TYPE_STRUCT) != type_proto.has_struct_type() ||
      type_proto.type_kind() == __TypeKind__switch_must_have_a_default__) {
    // TODO: Temporary hack to allow deserialization of existing
    // zetasql::TypeProto with TYPE_GEOGRAPHY which have proto_type for
    // stlib.proto.Geography in them.
    if (type_proto.type_kind() != TYPE_GEOGRAPHY) {
      return MakeSqlError()
             << "Invalid TypeProto provided for deserialization: "
             << type_proto.DebugString();
    }
  }
  if (Type::IsSimpleType(type_proto.type_kind())) {
    *type = MakeSimpleType(type_proto.type_kind());
    return ::zetasql_base::OkStatus();
  }
  switch (type_proto.type_kind()) {
    case TYPE_ARRAY: {
      const Type* element_type;
      const ArrayType* array_type;
      ZETASQL_RETURN_IF_ERROR(DeserializeFromProtoUsingExistingPools(
          type_proto.array_type().element_type(), pools, &element_type));
      ZETASQL_RETURN_IF_ERROR(MakeArrayType(element_type, &array_type));
      *type = array_type;
    } break;
    case TYPE_STRUCT: {
      std::vector<StructType::StructField> fields;
      const StructType* struct_type;
      for (int idx = 0; idx < type_proto.struct_type().field_size(); ++idx) {
        const StructFieldProto& field_proto =
            type_proto.struct_type().field(idx);
        const Type* field_type;
        ZETASQL_RETURN_IF_ERROR(DeserializeFromProtoUsingExistingPools(
            field_proto.field_type(), pools, &field_type));
        StructType::StructField struct_field(field_proto.field_name(),
                                             field_type);
        fields.push_back(struct_field);
      }
      ZETASQL_RETURN_IF_ERROR(MakeStructType(fields, &struct_type));
      *type = struct_type;
    } break;
    case TYPE_ENUM: {
      const EnumType* enum_type;
      const int set_index = type_proto.enum_type().file_descriptor_set_index();
      if (set_index < 0 || set_index >= pools.size()) {
        return MakeSqlError()
               << "Descriptor pool index " << set_index
               << " is out of range for the provided pools of size "
               << pools.size();
      }
      const google::protobuf::DescriptorPool* pool = pools[set_index];
      const google::protobuf::EnumDescriptor* enum_descr =
          pool->FindEnumTypeByName(type_proto.enum_type().enum_name());
      if (enum_descr == nullptr) {
        return MakeSqlError()
               << "Enum type name not found in the specified DescriptorPool: "
               << type_proto.enum_type().enum_name();
      }
      if (enum_descr->file()->name() !=
          type_proto.enum_type().enum_file_name()) {
        return MakeSqlError()
               << "Enum " << type_proto.enum_type().enum_name() << " found in "
               << enum_descr->file()->name() << ", not "
               << type_proto.enum_type().enum_file_name() << " as specified.";
      }
      ZETASQL_RETURN_IF_ERROR(MakeEnumType(enum_descr, &enum_type));
      *type = enum_type;
    } break;
    case TYPE_PROTO: {
      const ProtoType* proto_type;
      const int set_index = type_proto.proto_type().file_descriptor_set_index();
      if (set_index < 0 || set_index >= pools.size()) {
        return MakeSqlError()
               << "Descriptor pool index " << set_index
               << " is out of range for the provided pools of size "
               << pools.size();
      }
      const google::protobuf::DescriptorPool* pool = pools[set_index];
      const google::protobuf::Descriptor* proto_descr =
          pool->FindMessageTypeByName(type_proto.proto_type().proto_name());
      if (proto_descr == nullptr) {
        return MakeSqlError()
               << "Proto type name not found in the specified DescriptorPool: "
               << type_proto.proto_type().proto_name();
      }
      if (proto_descr->file()->name() !=
          type_proto.proto_type().proto_file_name()) {
        return MakeSqlError()
               << "Proto " << type_proto.proto_type().proto_name()
               << " found in " << proto_descr->file()->name() << ", not "
               << type_proto.proto_type().proto_file_name() << " as specified.";
      }
      ZETASQL_RETURN_IF_ERROR(MakeProtoType(proto_descr, &proto_type));
      *type = proto_type;
    } break;
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Making Type of kind "
             << Type::TypeKindToString(type_proto.type_kind(), PRODUCT_INTERNAL)
             << " from TypeProto is not implemented.";
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TypeFactory::DeserializeFromSelfContainedProto(
      const TypeProto& type_proto,
      google::protobuf::DescriptorPool* pool,
      const Type** type) {
  if (type_proto.file_descriptor_set_size() > 1) {
    return MakeSqlError()
           << "DeserializeFromSelfContainedProto cannot be used to deserialize "
              "types that rely on multiple FileDescriptorSets. Use "
              "DeserializeFromSelfContainedProtoWithDistinctFiles instead";
  }
  return DeserializeFromSelfContainedProtoWithDistinctFiles(type_proto, {pool},
                                                            type);
}

zetasql_base::Status TypeFactory::DeserializeFromSelfContainedProtoWithDistinctFiles(
      const TypeProto& type_proto,
      const std::vector<google::protobuf::DescriptorPool*>& pools,
      const Type** type) {
  if (!type_proto.file_descriptor_set().empty() &&
      type_proto.file_descriptor_set_size() != pools.size()) {
    return MakeSqlError()
           << "Expected the number of provided FileDescriptorSets "
              "and DescriptorPools to match. Found "
           << type_proto.file_descriptor_set_size()
           << " FileDescriptorSets and " << pools.size() << " DescriptorPools";
  }
  for (int i = 0; i < type_proto.file_descriptor_set_size(); ++i) {
    ZETASQL_RETURN_IF_ERROR(AddFileDescriptorSetToPool(
        &type_proto.file_descriptor_set(i), pools[i]));
  }
  const std::vector<const google::protobuf::DescriptorPool*> const_pools(pools.begin(),
                                                          pools.end());
  return DeserializeFromProtoUsingExistingPools(type_proto, const_pools, type);
}

bool IsValidTypeKind(int kind) {
  return TypeKind_IsValid(kind) &&
      kind != __TypeKind__switch_must_have_a_default__;
}

namespace types {

static absl::once_flag s_static_init_once;
// These variables are initialized by absl::call_once
static TypeFactory* s_type_factory = nullptr;
static const Type* s_int32_type = nullptr;
static const Type* s_int64_type = nullptr;
static const Type* s_uint32_type = nullptr;
static const Type* s_uint64_type = nullptr;
static const Type* s_bool_type = nullptr;
static const Type* s_float_type = nullptr;
static const Type* s_double_type = nullptr;
static const Type* s_string_type = nullptr;
static const Type* s_bytes_type = nullptr;
static const Type* s_timestamp_type = nullptr;
static const Type* s_date_type = nullptr;
static const Type* s_time_type = nullptr;
static const Type* s_datetime_type = nullptr;
static const Type* s_geography_type = nullptr;
static const Type* s_numeric_type = nullptr;
static const EnumType* s_date_part_enum_type = nullptr;
static const EnumType* s_normalize_mode_enum_type = nullptr;
static const StructType* s_empty_struct_type = nullptr;
static const ArrayType* s_int32_array_type = nullptr;
static const ArrayType* s_int64_array_type = nullptr;
static const ArrayType* s_uint32_array_type = nullptr;
static const ArrayType* s_uint64_array_type = nullptr;
static const ArrayType* s_bool_array_type = nullptr;
static const ArrayType* s_float_array_type = nullptr;
static const ArrayType* s_double_array_type = nullptr;
static const ArrayType* s_string_array_type = nullptr;
static const ArrayType* s_bytes_array_type = nullptr;
static const ArrayType* s_timestamp_array_type = nullptr;
static const ArrayType* s_date_array_type = nullptr;
static const ArrayType* s_datetime_array_type = nullptr;
static const ArrayType* s_time_array_type = nullptr;
static const ArrayType* s_geography_array_type = nullptr;
static const ArrayType* s_numeric_array_type = nullptr;
static const absl::Time* kBaseTimeMin = nullptr;
static const absl::Time* kBaseTimeMax = nullptr;

static const ArrayType* MakeArrayType(const Type* element_type) {
  const ArrayType* array_type;
  ZETASQL_CHECK_OK(s_type_factory->MakeArrayType(element_type, &array_type));
  return array_type;
}

static void InitStatic() {
  s_type_factory = new TypeFactory();
  s_int32_type = s_type_factory->get_int32();
  s_int64_type = s_type_factory->get_int64();
  s_uint32_type = s_type_factory->get_uint32();
  s_uint64_type = s_type_factory->get_uint64();
  s_bool_type = s_type_factory->get_bool();
  s_float_type = s_type_factory->get_float();
  s_double_type = s_type_factory->get_double();
  s_string_type = s_type_factory->get_string();
  s_bytes_type = s_type_factory->get_bytes();
  s_timestamp_type = s_type_factory->get_timestamp();
  s_date_type = s_type_factory->get_date();
  s_time_type = s_type_factory->get_time();
  s_datetime_type = s_type_factory->get_datetime();
  s_geography_type = s_type_factory->get_geography();
  s_numeric_type = s_type_factory->get_numeric();
  ZETASQL_CHECK_OK(s_type_factory->MakeStructType({}, &s_empty_struct_type));

  s_int32_array_type = MakeArrayType(s_type_factory->get_int32());
  s_int64_array_type = MakeArrayType(s_type_factory->get_int64());
  s_uint32_array_type = MakeArrayType(s_type_factory->get_uint32());
  s_uint64_array_type = MakeArrayType(s_type_factory->get_uint64());
  s_bool_array_type = MakeArrayType(s_type_factory->get_bool());
  s_float_array_type = MakeArrayType(s_type_factory->get_float());
  s_double_array_type = MakeArrayType(s_type_factory->get_double());
  s_string_array_type = MakeArrayType(s_type_factory->get_string());
  s_bytes_array_type = MakeArrayType(s_type_factory->get_bytes());
  s_timestamp_array_type = MakeArrayType(s_type_factory->get_timestamp());
  s_date_array_type = MakeArrayType(s_type_factory->get_date());
  s_datetime_array_type = MakeArrayType(s_type_factory->get_datetime());
  s_time_array_type = MakeArrayType(s_type_factory->get_time());
  s_geography_array_type = MakeArrayType(s_type_factory->get_geography());
  s_numeric_array_type = MakeArrayType(s_type_factory->get_numeric());
  kBaseTimeMin = new absl::Time(absl::FromUnixMicros(kTimestampMin));
  kBaseTimeMax = new absl::Time(absl::FromUnixMicros(kTimestampMax) +
                                absl::Nanoseconds(999));

  const google::protobuf::EnumDescriptor* datepart_descr =
      functions::DateTimestampPart_descriptor();
  ZETASQL_CHECK_OK(s_type_factory->MakeEnumType(datepart_descr,
                                        &s_date_part_enum_type));

  const google::protobuf::EnumDescriptor* normalize_mode_descriptor =
      functions::NormalizeMode_descriptor();
  ZETASQL_CHECK_OK(s_type_factory->MakeEnumType(normalize_mode_descriptor,
                                        &s_normalize_mode_enum_type));
}

const Type* Int32Type() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_int32_type;
}
const Type* Int64Type() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_int64_type;
}
const Type* Uint32Type() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_uint32_type;
}
const Type* Uint64Type() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_uint64_type;
}
const Type* BoolType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_bool_type;
}
const Type* FloatType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_float_type;
}
const Type* DoubleType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_double_type;
}
const Type* StringType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_string_type;
}
const Type* BytesType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_bytes_type;
}
const Type* DateType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_date_type;
}
const Type* TimestampType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_timestamp_type;
}
const Type* TimeType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_time_type;
}
const Type* DatetimeType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_datetime_type;
}
const Type* GeographyType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_geography_type;
}
const Type* NumericType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_numeric_type;
}
const StructType* EmptyStructType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_empty_struct_type;
}
const EnumType* DatePartEnumType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_date_part_enum_type;
}
const EnumType* NormalizeModeEnumType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_normalize_mode_enum_type;
}

const ArrayType* Int32ArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_int32_array_type;
}
const ArrayType* Int64ArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_int64_array_type;
}
const ArrayType* Uint32ArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_uint32_array_type;
}
const ArrayType* Uint64ArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_uint64_array_type;
}
const ArrayType* BoolArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_bool_array_type;
}
const ArrayType* FloatArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_float_array_type;
}
const ArrayType* DoubleArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_double_array_type;
}
const ArrayType* StringArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_string_array_type;
}
const ArrayType* BytesArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_bytes_array_type;
}

const ArrayType* TimestampArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_timestamp_array_type;
}
const ArrayType* DateArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_date_array_type;
}

const ArrayType* DatetimeArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_datetime_array_type;
}

const ArrayType* TimeArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_time_array_type;
}

const ArrayType* GeographyArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_geography_array_type;
}

const ArrayType* NumericArrayType() {
  absl::call_once(s_static_init_once, &InitStatic);
  return s_numeric_array_type;
}

const Type* TypeFromSimpleTypeKind(TypeKind type_kind) {
  switch (type_kind) {
    case TYPE_INT32:
      return Int32Type();
    case TYPE_INT64:
      return Int64Type();
    case TYPE_UINT32:
      return Uint32Type();
    case TYPE_UINT64:
      return Uint64Type();
    case TYPE_BOOL:
      return BoolType();
    case TYPE_FLOAT:
      return FloatType();
    case TYPE_DOUBLE:
      return DoubleType();
    case TYPE_STRING:
      return StringType();
    case TYPE_BYTES:
      return BytesType();
    case TYPE_TIMESTAMP:
      return TimestampType();
    case TYPE_DATE:
      return DateType();
    case TYPE_TIME:
      return TimeType();
    case TYPE_DATETIME:
      return DatetimeType();
    case TYPE_GEOGRAPHY:
      return GeographyType();
    case TYPE_NUMERIC:
      return NumericType();
    default:
      VLOG(1) << "Could not build static Type from type: "
              << Type::TypeKindToString(type_kind, PRODUCT_INTERNAL);
      return nullptr;
  }
}

const ArrayType* ArrayTypeFromSimpleTypeKind(TypeKind type_kind) {
  switch (type_kind) {
    case TYPE_INT32:
      return Int32ArrayType();
    case TYPE_INT64:
      return Int64ArrayType();
    case TYPE_UINT32:
      return Uint32ArrayType();
    case TYPE_UINT64:
      return Uint64ArrayType();
    case TYPE_BOOL:
      return BoolArrayType();
    case TYPE_FLOAT:
      return FloatArrayType();
    case TYPE_DOUBLE:
      return DoubleArrayType();
    case TYPE_STRING:
      return StringArrayType();
    case TYPE_BYTES:
      return BytesArrayType();
    case TYPE_TIMESTAMP:
      return TimestampArrayType();
    case TYPE_DATE:
      return DateArrayType();
    case TYPE_TIME:
      return TimeArrayType();
    case TYPE_DATETIME:
      return DatetimeArrayType();
    case TYPE_GEOGRAPHY:
      return GeographyArrayType();
    case TYPE_NUMERIC:
      return NumericArrayType();
    default:
      VLOG(1) << "Could not build static ArrayType from type: "
              << Type::TypeKindToString(type_kind, PRODUCT_INTERNAL);
      return nullptr;
  }
}

absl::Time TimestampMinBaseTime() {
  absl::call_once(s_static_init_once, &InitStatic);
  return *kBaseTimeMin;
}

absl::Time TimestampMaxBaseTime() {
  absl::call_once(s_static_init_once, &InitStatic);
  return *kBaseTimeMax;
}

}  // namespace types

void TypeFactory::AddDependency(const Type* other_type) {
  const TypeFactory* other_factory = other_type->type_factory_;

  // Do not add a dependency if the other factory is the same as this factory or
  // is the static factory (since the static factory is never destroyed).
  if (other_factory == this || other_factory == types::s_type_factory) return;

  {
    absl::MutexLock l(&mutex_);
    if (!zetasql_base::InsertIfNotPresent(&depends_on_factories_, other_factory)) {
      return;  // Already had it.
    }
    VLOG(2) << "Added dependency from TypeFactory " << this << " to "
            << other_factory << " which owns the type "
            << other_type->DebugString() << ":\n"
            ;

    // This detects trivial cycles between two TypeFactories.  It won't detect
    // longer cycles, so those won't give this error message, but the
    // destructor error will still fire because no destruction order is safe.
    if (zetasql_base::ContainsKey(factories_depending_on_this_, other_factory)) {
      LOG(DFATAL) << "Created cyclical dependency between TypeFactories, "
                     "which is not legal because there can be no safe "
                     "destruction order";
    }
  }
  {
    absl::MutexLock l(&other_factory->mutex_);
    zetasql_base::InsertIfNotPresent(&other_factory->factories_depending_on_this_, this);
  }
}

}  // namespace zetasql
