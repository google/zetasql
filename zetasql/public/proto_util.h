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

#ifndef ZETASQL_PUBLIC_PROTO_UTIL_H_
#define ZETASQL_PUBLIC_PROTO_UTIL_H_

#include <cstdint>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/proto/type_annotation.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include <cstdint>
#include "absl/container/flat_hash_map.h"
#include "absl/flags/declare.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "zetasql/base/status.h"

ABSL_DECLARE_FLAG(bool, zetasql_read_proto_field_optimized_path);

namespace zetasql {

struct ProtoFieldDefaultOptions {
  // Whether the 'use_defaults' and 'use_field_defaults' annotations should be
  // ignored. If these annotations are ignored, we will always provide non-null
  // defaults, even when the annotation tells us not to.
  bool ignore_use_default_annotations = false;

  // Whether the format annotations should be ignored.
  bool ignore_format_annotations = false;

  // Whether to always use non-null values for map entry fields.
  bool map_fields_always_nonnull = false;

  // Constructs a ProtoFieldDefaultOptions for a given field and language
  // settings.
  static ProtoFieldDefaultOptions FromFieldAndLanguage(
      const google::protobuf::FieldDescriptor* field,
      const LanguageOptions& language_options);
};

// Gets the default value of for a proto field of a given type. 'options'
// controls edge-case behavior.
absl::Status GetProtoFieldDefault(const ProtoFieldDefaultOptions& options,
                                  const google::protobuf::FieldDescriptor* field,
                                  const Type* type, Value* default_value);

// Gets the type of the field and, if default_value is not null, the
// default_value as well. In debug mode, performs additional validation between
// the type of the field and the default value returned.
absl::Status GetProtoFieldTypeAndDefault(
    const ProtoFieldDefaultOptions& options,
    const google::protobuf::FieldDescriptor* field, TypeFactory* type_factory,
    const Type** type, Value* default_value = nullptr);

// Get the Type and default value for a proto field in one step.
// The returned <type> will be allocated from <type_factory>,
// and will match the type returned by ProtoType::GetProtoFieldType.
// If <default_value> is non-NULL, the computed field default (as described
// above in GetProtoFieldDefault) will be returned.
// For required fields, returns an invalid default_value.
// For repeated fields, returns an empty array for default_value.
//
ABSL_DEPRECATED("Inline me!")
inline absl::Status GetProtoFieldTypeAndDefault(
    const google::protobuf::FieldDescriptor* field, TypeFactory* type_factory,
    const Type** type, Value* default_value = nullptr) {
  return GetProtoFieldTypeAndDefault({}, field, type_factory, type,
                                     default_value);
}

// DEPRECATED: Callers should move to the form above. <use_obsolete_timestamp>
// must be false.
ABSL_DEPRECATED("Inline me!")
inline absl::Status GetProtoFieldTypeAndDefault(
    const google::protobuf::FieldDescriptor* field, TypeFactory* type_factory,
    bool use_obsolete_timestamp, const Type** type,
    Value* default_value = nullptr) {
  ZETASQL_RET_CHECK(!use_obsolete_timestamp);
  return GetProtoFieldTypeAndDefault({}, field, type_factory, type,
                                     default_value);
}

// Represents a proto field access. If 'get_has_bit' is false, 'type' and
// 'default_value' must be populated by GetProtoFieldTypeAndDefault().
struct ProtoFieldInfo {
  const google::protobuf::FieldDescriptor* descriptor = nullptr;
  FieldFormat::Format format = FieldFormat::DEFAULT_FORMAT;
  const Type* type = nullptr;  // Ignored if 'get_has_bit' is true.
  bool get_has_bit = false;
  // Ignored if 'descriptor' is a required field or 'get_has_bit' is
  // true. Otherwise, its type must be 'type'.
  Value default_value;
};

// Maps a ProtoFieldInfo (by its index in a vector) to its corresponding Value,
// or an error (e.g., for a missing required field, or a problem interpreting a
// wire format value according to a FieldFormat). ProtoFieldInfos with
// 'get_has_bit = true' are represented by a Value of type Bool.
using ProtoFieldValueList = std::vector<absl::StatusOr<Value>>;

// Reads the (tag, value) pairs in bytes and sets 'field_value_list' according
// to 'field_infos' (which must be non-empty). The field descriptors in
// 'field_infos' must all come from the same google::protobuf::Descriptor, and the same
// field descriptor can be used multiple times (e.g., to read a field with
// different field formats or with/without the has bit). 'bytes' is a serialized
// proto of the common google::protobuf::Descriptor.
absl::Status ReadProtoFields(
    absl::Span<const ProtoFieldInfo* const> field_infos,
    const absl::Cord& bytes, ProtoFieldValueList* field_value_list);

// Convenience form of ReadProtoFields() for reading a single field. Reads the
// proto field matching tag and type of 'field_descr' from 'bytes' and returns
// the result in 'output_value'. If 'tag' is missing in 'bytes', returns
// 'default_value', or an error if the field was required.  Returns an array
// value for a repeated field. The returned value has type 'type'.  Returns
// 'default_value' and error status upon data corruption and out-of-bounds
// values.  'default_value' may be an uninitialized Value for a required field.
// Internally uses ReadProtoFields if --read_proto_field_optimized_path is
// false, otherwise uses an version optimized for reading a single field.
absl::Status ReadProtoField(const google::protobuf::FieldDescriptor* field_descr,
                            FieldFormat::Format format, const Type* type,
                            const Value& default_value, bool get_has_bit,
                            const absl::Cord& bytes, Value* output_value);
// As above but `get_has_bit` is defaulted to false.
absl::Status ReadProtoField(const google::protobuf::FieldDescriptor* field_descr,
                            FieldFormat::Format format, const Type* type,
                            const Value& default_value, const absl::Cord& bytes,
                            Value* output_value);

// Returns true in '*has_field' if a field with tag 'field_tag' exists in
// 'bytes'. This ignores field type and repeatedness, and returns true as
// soon as one matching tag is found.
// This may return an error if the proto bytes are corrupted.
//
// DEPRECATED: Use ReadProtoField instead.
absl::Status ProtoHasField(
    int32_t field_tag, const absl::Cord& bytes,
    bool* has_field);

// Returns whether Type represents a protocol buffer map. A Type is a protocol
// buffer map if it is an ARRAY<PROTO> where the array element type is a
// protocol buffer map entry descriptor.
bool IsProtoMap(const Type* type);

// A variant containing all the possible value types of a proto map key.
using MapKeyVariant = absl::variant<bool, int64_t, uint64_t, std::string>;

// Copies the elements of array_of_map_entry into the vector output. The first
// element of each pair is the key, the second is the value. Note that
// duplicate keys are not eliminated during this function call. key_type
// or value_type may be null, in which case the Values of that element of each
// pair will be invalid.
//
// REQUIRES: IsProtoMap(array_of_map_entry) is true.
absl::Status ParseProtoMap(const Value& array_of_map_entry,
                           const Type* key_type, const Type* value_type,
                           std::vector<std::pair<Value, Value>>& output);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_PROTO_UTIL_H_
