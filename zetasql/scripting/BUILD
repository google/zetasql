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

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")

package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

cc_library(
    name = "control_flow_graph",
    srcs = ["control_flow_graph.cc"],
    hdrs = ["control_flow_graph.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":script_segment",
        "//zetasql/base:map_util",
        "//zetasql/base:status",
        "//zetasql/parser",
        "//zetasql/public:parse_location",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "parsed_script",
    srcs = ["parsed_script.cc"],
    hdrs = [
        "break_continue_context.h",
        "parsed_script.h",
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
        ":control_flow_graph",
        ":error_helpers",
        ":type_aliases",
        "//zetasql/base:map_util",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/common:errors",
        "//zetasql/parser",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
        "//zetasql/public:type",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:variant",
    ],
)

cc_test(
    name = "parsed_script_test",
    srcs = ["parsed_script_test.cc"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":parsed_script",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/parser",
        "//zetasql/public:id_string",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:variant",
    ],
)

cc_library(
    name = "script_segment",
    srcs = ["script_segment.cc"],
    hdrs = ["script_segment.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/parser",
        "//zetasql/public:parse_location",
        "//zetasql/public:parse_resume_location",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "parse_helpers",
    srcs = ["parse_helpers.cc"],
    hdrs = ["parse_helpers.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":parsed_script",
        "//zetasql/base:status",
        "//zetasql/parser",
        "//zetasql/public:options_cc_proto",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "error_helpers",
    srcs = ["error_helpers.cc"],
    hdrs = ["error_helpers.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":script_exception_cc_proto",
        ":script_segment",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:status_payload_utils",
        "//zetasql/parser",
        "//zetasql/public:error_location_cc_proto",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "stack_frame",
    hdrs = ["stack_frame.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":parsed_script",
        "//zetasql/parser",
    ],
)

cc_library(
    name = "type_aliases",
    hdrs = ["type_aliases.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/public:id_string",
        "//zetasql/public:value",
    ],
)

cc_library(
    name = "script_executor",
    srcs = [
        "script_executor.cc",
        "script_executor_impl.cc",
        "script_executor_impl.h",
        "serialization_helpers.cc",
        "serialization_helpers.h",
    ],
    hdrs = [
        "script_executor.h",
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
        ":control_flow_graph",
        ":error_helpers",
        ":parsed_script",
        ":script_exception_cc_proto",
        ":script_executor_state_cc_proto",
        ":script_segment",
        ":stack_frame",
        ":type_aliases",
        ":variable_cc_proto",
        "//zetasql/base",
        "//zetasql/base:flat_set",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:status_payload_utils",
        "//zetasql/parser",
        "//zetasql/public:analyzer",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:coercer",
        "//zetasql/public:evaluator",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:function",
        "//zetasql/public:id_string",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
        "//zetasql/public:type",
        "//zetasql/public/types",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "error_helpers_test",
    srcs = ["error_helpers_test.cc"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":error_helpers",
        ":script_segment",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common:errors",
        "//zetasql/common:status_payload_utils",
        "//zetasql/parser",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:error_helpers",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
    ],
)

proto_library(
    name = "script_exception_proto",
    srcs = ["script_exception.proto"],
)

cc_proto_library(
    name = "script_exception_cc_proto",
    deps = [":script_exception_proto"],
)

proto_library(
    name = "script_executor_state_proto",
    srcs = ["script_executor_state.proto"],
    deps = [
        ":script_exception_proto",
        ":variable_proto",
        "//zetasql/proto:function_proto",
        "@com_google_protobuf//:any_proto",
    ],
)

cc_proto_library(
    name = "script_executor_state_cc_proto",
    deps = [":script_executor_state_proto"],
)

proto_library(
    name = "variable_proto",
    srcs = ["variable.proto"],
    deps = [
        "//zetasql/public:type_parameters_proto",
        "//zetasql/public:type_proto",
        "//zetasql/public:value_proto",
    ],
)

cc_proto_library(
    name = "variable_cc_proto",
    deps = [":variable_proto"],
)

java_proto_library(
    name = "variable_java_proto",
    deps = [":variable_proto"],
)
