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

#ifndef ZETASQL_PUBLIC_PROTO_UTIL_H_
#define ZETASQL_PUBLIC_PROTO_UTIL_H_

#include <vector>

#include "google/protobuf/descriptor.h"
#include "zetasql/public/proto/type_annotation.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include <cstdint>
#include "absl/container/flat_hash_map.h"
#include "absl/flags/declare.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"

ABSL_DECLARE_FLAG(bool, zetasql_read_proto_field_optimized_path);

namespace zetasql {

// Get the default value for a proto field. All zetasql annotations will be
// respected for proto2 fields. The 'use_defaults' and 'use_field_defaults'
// annotations will be ignored for proto3 fields. The <type> must be the same
// type as would be computed for <field> using GetProtoFieldTypeAndDefault or
// ProtoType::GetProtoFieldType (with <ignore_annotations> = false). For
// required fields, returns an invalid default_value. For repeated fields,
// returns an empty array for default_value.
zetasql_base::Status GetProtoFieldDefaultV2(const google::protobuf::FieldDescriptor* field,
                                    const Type* type, Value* default_value);
// DEPRECATED: Callers should move to using GetProtoFieldDefaultV2. Note that
// GetProtoFieldDefaultV2 does not respect the 'use_defaults' and
// 'use_field_defaults' annotations for proto3 fields.
zetasql_base::Status GetProtoFieldDefault(const google::protobuf::FieldDescriptor* field,
                                  const Type* type, Value* default_value);
// DEPRECATED: Callers should move to GetProtoFieldDefaultV2.
// <use_obsolete_timestamp> must be false.
zetasql_base::Status GetProtoFieldDefault(const google::protobuf::FieldDescriptor* field,
                                  const Type* type, bool use_obsolete_timestamp,
                                  Value* default_value);

// Get the default value for a proto field, ignoring zetasql format
// annotations as well as the zetasql.use_defaults and
// zetasql.use_field_defaults annotations. The <type> must be the same type as
// would be computed for <field> using ProtoType::GetProtoFieldType (with
// <ignore_annotations> = true). If <type> is non-primitive, returns a NULL
// <default_value>. For required fields, returns an invalid <default_value>.
// For repeated fields, returns an empty array for <default_value> where the
// array element type is the raw field type (i.e., the type of the field with
// format annotations ignored).
zetasql_base::Status GetProtoFieldDefaultRaw(const google::protobuf::FieldDescriptor* field,
                                     const Type* type, Value* default_value);

// Get the Type and default value for a proto field in one step.
// The returned <type> will be allocated from <type_factory>,
// and will match the type returned by ProtoType::GetProtoFieldType.
// If <default_value> is non-NULL, the computed field default (as described
// above in GetProtoFieldDefault) will be returned.
// For required fields, returns an invalid default_value.
// For repeated fields, returns an empty array for default_value.
//
zetasql_base::Status GetProtoFieldTypeAndDefault(const google::protobuf::FieldDescriptor* field,
                                         TypeFactory* type_factory,
                                         const Type** type,
                                         Value* default_value = nullptr);
// DEPRECATED: Callers should move to the form above. <use_obsolete_timestamp>
// must be false.
zetasql_base::Status GetProtoFieldTypeAndDefault(const google::protobuf::FieldDescriptor* field,
                                         TypeFactory* type_factory,
                                         bool use_obsolete_timestamp,
                                         const Type** type,
                                         Value* default_value = nullptr);

// Get the Type and default value, without considering format annotations or any
// automatic conversions, for a proto field. The returned <type> will be
// allocated from <type_factory>, and will match the type returned by
// ProtoType::GetProtoFieldType (with <ignore_annotations> = true). If
// <default_value> is non-NULL, the computed field default (as described above
// in GetProtoFieldDefaultRaw) will be returned. For required fields, returns an
// invalid <default_value>. For repeated fields, returns an empty array for
// <default_value>.
zetasql_base::Status GetProtoFieldTypeAndDefaultRaw(
    const google::protobuf::FieldDescriptor* field, TypeFactory* type_factory,
    const Type** type, Value* default_value = nullptr);

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
using ProtoFieldValueList = std::vector<zetasql_base::StatusOr<Value>>;

// Reads the (tag, value) pairs in bytes and sets 'field_value_list' according
// to 'field_infos' (which must be non-empty). The field descriptors in
// 'field_infos' must all come from the same google::protobuf::Descriptor, and the same
// field descriptor can be used multiple times (e.g., to read a field with
// different field formats or with/without the has bit). 'bytes' is a serialized
// proto of the common google::protobuf::Descriptor.
zetasql_base::Status ReadProtoFields(
    absl::Span<const ProtoFieldInfo* const> field_infos,
    const std::string& bytes,
    ProtoFieldValueList* field_value_list);

// Convenience form of ReadProtoFields() for reading a single field. Reads the
// proto field matching tag and type of 'field_descr' from 'bytes' and returns
// the result in 'output_value'. If 'tag' is missing in 'bytes', returns
// 'default_value', or an error if the field was required.  Returns an array
// value for a repeated field. The returned value has type 'type'.  Returns
// 'default_value' and error status upon data corruption and out-of-bounds
// values.  'default_value' may be an uninitialized Value for a required field.
// Internally uses ReadProtoFields if --read_proto_field_optimized_path is
// false, otherwise uses an version optimized for reading a single field.
zetasql_base::Status ReadProtoField(
    const google::protobuf::FieldDescriptor* field_descr, FieldFormat::Format format,
    const Type* type, const Value& default_value, bool get_has_bit,
    const std::string& bytes,
    Value* output_value);
// As above but `get_has_bit` is defaulted to false.
zetasql_base::Status ReadProtoField(
    const google::protobuf::FieldDescriptor* field_descr, FieldFormat::Format format,
    const Type* type, const Value& default_value,
    const std::string& bytes,
    Value* output_value);

// Returns true in '*has_field' if a field with tag 'field_tag' exists in
// 'bytes'. This ignores field type and repeatedness, and returns true as
// soon as one matching tag is found.
// This may return an error if the proto bytes are corrupted.
//
// DEPRECATED: Use ReadProtoField instead.
zetasql_base::Status ProtoHasField(
    int32_t field_tag,
    const std::string& bytes,
    bool* has_field);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_PROTO_UTIL_H_
