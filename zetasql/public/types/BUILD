#
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

package(default_visibility = ["//zetasql/base:zetasql_implementation"])

cc_library(
    name = "types",
    srcs = [
        "annotation.cc",
        "array_type.cc",
        "collation.cc",
        "enum_type.cc",
        "extended_type.cc",
        "internal_utils.cc",
        "internal_utils.h",
        "proto_type.cc",
        "simple_type.cc",
        "simple_value.cc",
        "struct_type.cc",
        "type.cc",
        "type_deserializer.cc",
        "type_factory.cc",
        "type_parameters.cc",
    ],
    hdrs = [
        "annotation.h",
        "array_type.h",
        "collation.h",
        "enum_type.h",
        "extended_type.h",
        "proto_type.h",
        "simple_type.h",
        "simple_value.h",
        "struct_type.h",
        "type.h",
        "type_deserializer.h",
        "type_factory.h",
        "type_parameters.h",
        "value_equality_check_options.h",
    ],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":timestamp_util",
        ":value_representations",
        "//zetasql/base",
        "//zetasql/base:case",
        "//zetasql/base:map_util",
        "//zetasql/base:refcount",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:time_proto_util",
        "//zetasql/common:errors",
        "//zetasql/common:float_margin",
        "//zetasql/common:proto_helper",
        "//zetasql/common:string_util",
        "//zetasql/public:annotation_cc_proto",
        "//zetasql/public:civil_time",
        "//zetasql/public:collation_cc_proto",
        "//zetasql/public:interval_value",
        "//zetasql/public:language_options",
        "//zetasql/public:numeric_value",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:simple_value_cc_proto",
        "//zetasql/public:strings",
        "//zetasql/public:type_annotation_cc_proto",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:type_parameters_cc_proto",
        "//zetasql/public:value_cc_proto",
        "//zetasql/public:value_content",
        "//zetasql/public/functions:convert_proto",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/public/functions:datetime_cc_proto",
        "//zetasql/public/functions:normalize_mode_cc_proto",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/container:node_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_absl//absl/types:variant",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "value_representations",
    hdrs = [
        "value_representations.h",
    ],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/base:logging",
        "//zetasql/base:refcount",
        "//zetasql/public:interval_value",
        "//zetasql/public:json_value",
        "//zetasql/public:numeric_value",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:variant",
    ],
)

cc_library(
    name = "timestamp_util",
    srcs = [
        "timestamp_util.cc",
    ],
    hdrs = [
        "timestamp_util.h",
    ],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "@com_google_absl//absl/time",
    ],
)
