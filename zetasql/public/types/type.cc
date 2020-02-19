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

#include "zetasql/public/types/type.h"

#include "zetasql/public/language_options.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/strings/match.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/map_util.h"

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
    /* name              cost, specificity,  simple }, */
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
    {"BIGNUMERIC",         16,          16,    true }

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
      {"bignumeric", zetasql::TYPE_BIGNUMERIC},
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
      {"bignumeric", zetasql::TYPE_BIGNUMERIC},
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
  if (kind() == TYPE_BIGNUMERIC &&
      !language_options.LanguageFeatureEnabled(FEATURE_BIGNUMERIC_TYPE)) {
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

std::string Type::ShortTypeName(ProductMode mode) const {
  return TypeName(mode);
}

std::string Type::DebugString(bool details) const {
  std::string debug_string;

  std::vector<absl::variant<const Type*, std::string>> stack;
  stack.push_back(this);
  while (!stack.empty()) {
    const auto stack_entry = stack.back();
    stack.pop_back();
    if (absl::holds_alternative<std::string>(stack_entry)) {
      absl::StrAppend(&debug_string, absl::get<std::string>(stack_entry));
      continue;
    }
    const Type* type = absl::get<const Type*>(stack_entry);
    switch (type->kind()) {
      case TypeKind::TYPE_ARRAY:
        absl::StrAppend(&debug_string, "ARRAY<");
        stack.push_back(">");
        stack.push_back(type->AsArray()->element_type());
        break;
      case TypeKind::TYPE_STRUCT: {
        const StructType* struct_type = type->AsStruct();
        absl::StrAppend(&debug_string, "STRUCT<");
        stack.push_back(">");
        for (int i = struct_type->num_fields() - 1; i >= 0; --i) {
          const StructField& field = struct_type->field(i);
          stack.push_back(field.type);
          std::string prefix = (i > 0) ? ", " : "";
          if (!field.name.empty()) {
            absl::StrAppend(&prefix, ToIdentifierLiteral(field.name), " ");
          }
          stack.push_back(prefix);
        }
        break;
      }
      case TypeKind::TYPE_PROTO: {
        const google::protobuf::Descriptor* descriptor = type->AsProto()->descriptor();
        absl::StrAppend(&debug_string, "PROTO<", descriptor->full_name());
        if (details) {
          absl::StrAppend(&debug_string,
                          ", file name: ", descriptor->file()->name(), ", <",
                          descriptor->DebugString(), ">");
        }
        absl::StrAppend(&debug_string, ">");
      } break;
      case TypeKind::TYPE_ENUM: {
        const google::protobuf::EnumDescriptor* enum_descriptor =
            type->AsEnum()->enum_descriptor();
        absl::StrAppend(&debug_string, "ENUM<", enum_descriptor->full_name());
        if (details) {
          absl::StrAppend(&debug_string,
                          ", file name: ", enum_descriptor->file()->name(),
                          ", <", enum_descriptor->DebugString(), ">");
        }
        absl::StrAppend(&debug_string, ">");
        break;
      }
      default:
        CHECK(type->IsSimpleType());
        absl::StrAppend(&debug_string,
                        TypeKindToString(type->kind(), PRODUCT_INTERNAL));
        break;
    }
  }
  return debug_string;
}

namespace {

// Helper function that finds a field or a named extension with the given name.
// Possible return values are HAS_FIELD if the field exists, HAS_PSEUDO_FIELD
// if the named extension exists, or HAS_NO_FIELD if neither exists.
Type::HasFieldResult HasProtoFieldOrNamedExtension(
    const google::protobuf::Descriptor* descriptor, const std::string& name,
    int* field_id) {
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

}  // namespace zetasql
