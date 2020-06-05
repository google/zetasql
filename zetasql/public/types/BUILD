#
# Copyright 2019 ZetaSQL Authors
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

package(default_visibility = ["//:__subpackages__"])

cc_library(
    name = "types",
    srcs = [
        "array_type.cc",
        "enum_type.cc",
        "extended_type.cc",
        "internal_utils.cc",
        "internal_utils.h",
        "proto_type.cc",
        "simple_type.cc",
        "struct_type.cc",
        "type.cc",
        "type_factory.cc",
    ],
    hdrs = [
        "array_type.h",
        "enum_type.h",
        "extended_type.h",
        "proto_type.h",
        "simple_type.h",
        "struct_type.h",
        "type.h",
        "type_factory.h",
    ],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":value_representations",
        "//zetasql/base",
        "//zetasql/base:case",
        "//zetasql/base:cleanup",
        "//zetasql/base:map_util",
        "//zetasql/base:refcount",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:float_margin",
        "//zetasql/common:proto_helper",
        "//zetasql/common:string_util",
        "//zetasql/public:civil_time",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:strings",
        "//zetasql/public:type_annotation_cc_proto",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value_content",
        "//zetasql/public/functions:convert_proto",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/public/functions:datetime_cc_proto",
        "//zetasql/public/functions:normalize_mode_cc_proto",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "//zetasql/public/types:timestamp_util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
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
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        "//zetasql/base:refcount",
        "//zetasql/public:numeric_value",
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
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        "@com_google_absl//absl/time",
    ],
)
