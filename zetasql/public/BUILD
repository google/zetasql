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

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "type",
    srcs = [
        "convert_type_to_proto.cc",
    ],
    hdrs = [
        "convert_type_to_proto.h",
        "type.h",
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
        ":type_annotation_cc_proto",
        ":type_cc_proto",
        "//zetasql/base",
        "//zetasql/base:case",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "//zetasql/public/types",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "value_content",
    hdrs = [
        "value_content.h",
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
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "catalog",
    srcs = [
        "catalog.cc",
    ],
    hdrs = [
        "catalog.h",
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
        ":evaluator_table_iterator",
        ":options_cc_proto",
        ":strings",
        ":type",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_absl//absl/types:variant",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "cycle_detector",
    srcs = [
        "cycle_detector.cc",
    ],
    hdrs = ["cycle_detector.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "cycle_detector_test",
    size = "small",
    srcs = ["cycle_detector_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":cycle_detector",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "civil_time",
    srcs = [
        "civil_time.cc",
    ],
    hdrs = [
        "civil_time.h",
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
        "//zetasql/base:mathutil",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "civil_time_test",
    size = "small",
    srcs = ["civil_time_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":civil_time",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "simple_catalog",
    srcs = [
        "simple_catalog.cc",
        "table_from_proto.cc",
    ],
    hdrs = [
        "simple_catalog.h",
        "table_from_proto.h",
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
        ":builtin_function",
        ":catalog",
        ":constant",
        ":function",
        ":simple_constant_cc_proto",
        ":simple_table_cc_proto",
        ":strings",
        ":type",
        ":value",
        "//zetasql/base",
        "//zetasql/base:case",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/common:simple_evaluator_table_iterator",
        "//zetasql/proto:simple_catalog_cc_proto",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "sql_formatter",
    srcs = ["sql_formatter.cc"],
    hdrs = ["sql_formatter.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":error_helpers",
        ":options_cc_proto",
        ":parse_helpers",
        ":parse_location",
        ":parse_resume_location",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/parser",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "sql_formatter_test",
    size = "small",
    srcs = ["sql_formatter_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":sql_formatter",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "coercer",
    srcs = [
        "cast.cc",
        "coercer.cc",
    ],
    hdrs = [
        "cast.h",
        "coercer.h",
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
        ":catalog",
        ":civil_time",
        ":function",
        ":language_options",
        ":numeric_value",
        ":options_cc_proto",
        ":signature_match_result",
        ":strings",
        ":type",
        ":type_cc_proto",
        ":value",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/common:errors",
        "//zetasql/common:internal_value",
        "//zetasql/common:utf_util",
        "//zetasql/public/functions:convert",
        "//zetasql/public/functions:convert_proto",
        "//zetasql/public/functions:convert_string",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/public/functions:datetime_cc_proto",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "signature_match_result",
    srcs = ["signature_match_result.cc"],
    hdrs = ["signature_match_result.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":type",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "numeric_value",
    srcs = ["numeric_value.cc"],
    hdrs = [
        "numeric_value.h",
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
        "//zetasql/base",
        "//zetasql/base:endian",
        "//zetasql/base:mathutil",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/base:stl_util",
        "//zetasql/common:errors",
        "//zetasql/common:fixed_int",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "numeric_value_test_utils",
    testonly = 1,
    hdrs = ["numeric_value_test_utils.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":numeric_value",
        "@com_google_absl//absl/random",
    ],
)

cc_test(
    name = "numeric_value_test_utils_test",
    srcs = ["numeric_value_test_utils_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":numeric_value",
        ":numeric_value_test_utils",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/random",
    ],
)

cc_test(
    name = "numeric_value_test",
    size = "small",
    timeout = "moderate",
    srcs = ["numeric_value_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":numeric_value",
        ":numeric_value_test_utils",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:endian",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common:fixed_int",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:variant",
    ],
)

cc_library(
    name = "value",
    srcs = [
        "proto_util.cc",
        "value.cc",
        "value_inl.h",
    ],
    hdrs = [
        "proto_util.h",
        "value.h",
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
        ":civil_time",
        ":numeric_value",
        ":options_cc_proto",
        ":strings",
        ":type",
        ":type_cc_proto",
        ":value_cc_proto",
        ":value_content",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:refcount",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/base:time_proto_util",
        "//zetasql/common:float_margin",
        "//zetasql/common:string_util",
        "//zetasql/public/functions:arithmetics",
        "//zetasql/public/functions:comparison",
        "//zetasql/public/functions:convert_proto",
        "//zetasql/public/functions:convert_string",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "//zetasql/public/types",
        "//zetasql/public/types:value_representations",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_absl//absl/types:variant",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "value_test",
    size = "medium",
    srcs = ["value_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":analyzer",
        ":evaluator",
        ":language_options",
        ":numeric_value",
        ":simple_catalog",
        ":type",
        ":value",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base",
        "//zetasql/base:statusor",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common:internal_value",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/common/testing:testing_proto_util",
        "//zetasql/public/types",
        "//zetasql/public/types:value_representations",
        "//zetasql/testdata:test_schema_cc_proto",
        "//zetasql/testing:test_value",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "proto_util_test",
    size = "small",
    srcs = ["proto_util_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":civil_time",
        ":type",
        ":type_cc_proto",
        ":value",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/common/testing:testing_proto_util",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/strings:cord",
        "@com_google_protobuf//:protobuf",
    ],
)

proto_library(
    name = "builtin_function_proto",
    srcs = ["builtin_function.proto"],
)

cc_proto_library(
    name = "builtin_function_cc_proto",
    deps = [":builtin_function_proto"],
)

java_proto_library(
    name = "builtin_function_java_proto",
    deps = [":builtin_function_proto"],
)

proto_library(
    name = "deprecation_warning_proto",
    srcs = ["deprecation_warning.proto"],
    deps = [":error_location_proto"],
)

cc_proto_library(
    name = "deprecation_warning_cc_proto",
    deps = [":deprecation_warning_proto"],
)

java_proto_library(
    name = "deprecation_warning_java_proto",
    deps = [":deprecation_warning_proto"],
)

proto_library(
    name = "function_proto",
    srcs = ["function.proto"],
)

cc_proto_library(
    name = "function_cc_proto",
    deps = [":function_proto"],
)

java_proto_library(
    name = "function_java_proto",
    deps = [":function_proto"],
)

proto_library(
    name = "parse_resume_location_proto",
    srcs = ["parse_resume_location.proto"],
)

cc_proto_library(
    name = "parse_resume_location_cc_proto",
    deps = [":parse_resume_location_proto"],
)

java_proto_library(
    name = "parse_resume_location_java_proto",
    deps = [":parse_resume_location_proto"],
)

proto_library(
    name = "options_proto",
    srcs = ["options.proto"],
    deps = ["@com_google_protobuf//:descriptor_proto"],
)

cc_proto_library(
    name = "options_cc_proto",
    deps = [":options_proto"],
)

java_proto_library(
    name = "options_java_proto",
    deps = [":options_proto"],
)

proto_library(
    name = "simple_table_proto",
    srcs = ["simple_table.proto"],
    deps = [":type_proto"],
)

cc_proto_library(
    name = "simple_table_cc_proto",
    deps = [":simple_table_proto"],
)

java_proto_library(
    name = "simple_table_java_proto",
    deps = [":simple_table_proto"],
)

proto_library(
    name = "simple_constant_proto",
    srcs = ["simple_constant.proto"],
    deps = [
        ":type_proto",
        ":value_proto",
    ],
)

cc_proto_library(
    name = "simple_constant_cc_proto",
    deps = [":simple_constant_proto"],
)

java_proto_library(
    name = "simple_constant_java_proto",
    deps = [":simple_constant_proto"],
)

proto_library(
    name = "parse_location_range_proto",
    srcs = ["parse_location_range.proto"],
)

cc_proto_library(
    name = "parse_location_range_cc_proto",
    deps = [":parse_location_range_proto"],
)

java_proto_library(
    name = "parse_location_range_java_proto",
    deps = [":parse_location_range_proto"],
)

proto_library(
    name = "type_proto",
    srcs = ["type.proto"],
    deps = ["@com_google_protobuf//:descriptor_proto"],
)

cc_proto_library(
    name = "type_cc_proto",
    deps = [":type_proto"],
)

java_proto_library(
    name = "type_proto_java_proto",
    deps = [":type_proto"],
)

proto_library(
    name = "type_annotation_proto",
    srcs = ["type_annotation.proto"],
    deps = ["//zetasql/public/proto:type_annotation_proto"],
)

cc_proto_library(
    name = "type_annotation_cc_proto",
    deps = [":type_annotation_proto"],
)

java_proto_library(
    name = "type_annotation_java_proto",
    deps = [":type_annotation_proto"],
)

proto_library(
    name = "value_proto",
    srcs = ["value.proto"],
    deps = [
        "@com_google_protobuf//:timestamp_proto",
    ],
)

cc_proto_library(
    name = "value_cc_proto",
    deps = [":value_proto"],
)

java_proto_library(
    name = "value_java_proto",
    deps = [":value_proto"],
)

proto_library(
    name = "error_location_proto",
    srcs = ["error_location.proto"],
)

cc_proto_library(
    name = "error_location_cc_proto",
    deps = [":error_location_proto"],
)

java_proto_library(
    name = "error_location_java_proto",
    deps = [":error_location_proto"],
)

cc_library(
    name = "error_helpers",
    srcs = ["error_helpers.cc"],
    hdrs = ["error_helpers.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":error_location_cc_proto",
        ":options_cc_proto",
        ":parse_location",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:status_payload_utils",
        "//zetasql/common:utf_util",
        "//zetasql/proto:internal_error_location_cc_proto",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "error_helpers_test",
    size = "small",
    srcs = ["error_helpers_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":error_helpers",
        ":error_location_cc_proto",
        ":parse_location",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base",
        "//zetasql/base:enum_utils",
        "//zetasql/base:map_util",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common:errors",
        "//zetasql/common:status_payload_utils",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "proto_value_conversion",
    srcs = ["proto_value_conversion.cc"],
    hdrs = ["proto_value_conversion.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":numeric_value",
        ":type",
        ":type_cc_proto",
        ":value",
        ":value_cc_proto",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "proto_value_conversion_test",
    size = "small",
    srcs = ["proto_value_conversion_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":analyzer",
        ":language_options",
        ":options_cc_proto",
        ":proto_value_conversion",
        ":simple_catalog",
        ":type",
        ":type_cc_proto",
        ":value",
        "//zetasql/base",
        "//zetasql/base:path",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        # buildcleaner: keep (text format proto)
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "//zetasql/base:map_util",
        "//zetasql/base:status",
        "//zetasql/base:ret_check",
    ],
)

cc_library(
    name = "strings",
    srcs = ["strings.cc"],
    hdrs = ["strings.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":id_string",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:utf_util",
        "//zetasql/parser:keywords",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@icu//:headers",
    ],
)

cc_test(
    name = "strings_test",
    size = "small",
    srcs = ["strings_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":strings",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:map_util",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common:utf_util",
        "//zetasql/parser",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "builtin_function_options",
    hdrs = ["builtin_function_options.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":builtin_function_cc_proto",
        ":function",
        ":language_options",
        ":type",
        "//zetasql/proto:options_cc_proto",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:bind_front",
    ],
)

cc_library(
    name = "builtin_function",
    srcs = ["builtin_function.cc"],
    hdrs = ["builtin_function.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":builtin_function_cc_proto",
        ":builtin_function_options",
        ":catalog",
        ":cycle_detector",
        ":function",
        ":language_options",
        ":options_cc_proto",
        ":type",
        ":value",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/common:builtin_function_internal",
        "//zetasql/common:errors",
        "//zetasql/proto:options_cc_proto",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "function",
    srcs = [
        "function.cc",
        "function_signature.cc",
        "input_argument_type.cc",
        "procedure.cc",
        "table_valued_function.cc",
    ],
    hdrs = [
        "function.h",
        "function_signature.h",
        "input_argument_type.h",
        "procedure.h",
        "table_valued_function.h",
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
        ":catalog",
        ":deprecation_warning_cc_proto",
        ":function_cc_proto",
        ":language_options",
        ":options_cc_proto",
        ":parse_location",
        ":signature_match_result",
        ":strings",
        ":type",
        ":type_cc_proto",
        ":value",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/common:errors",
        "//zetasql/proto:function_cc_proto",
        "//zetasql/resolved_ast:serialization_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "sql_function",
    srcs = ["sql_function.cc"],
    hdrs = ["sql_function.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":error_helpers",
        ":function",
        ":parse_resume_location",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "non_sql_function",
    srcs = ["non_sql_function.cc"],
    hdrs = ["non_sql_function.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":error_helpers",
        ":function",
        ":parse_resume_location",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "templated_sql_function",
    srcs = [
        "templated_sql_function.cc",
    ],
    hdrs = [
        "templated_sql_function.h",
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
        ":error_location_cc_proto",
        ":function",
        ":id_string",
        ":parse_location",
        ":parse_resume_location",
        ":strings",
        ":type",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/proto:function_cc_proto",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "sql_tvf",
    srcs = ["sql_tvf.cc"],
    hdrs = ["sql_tvf.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":function",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/resolved_ast",
    ],
)

cc_library(
    name = "templated_sql_tvf",
    srcs = ["templated_sql_tvf.cc"],
    hdrs = ["templated_sql_tvf.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":analyzer",
        ":catalog",
        ":cycle_detector",
        ":deprecation_warning_cc_proto",
        ":error_helpers",
        ":error_location_cc_proto",
        ":function",
        ":function_cc_proto",
        ":id_string",
        ":options_cc_proto",
        ":parse_location",
        ":parse_resume_location",
        ":strings",
        ":type",
        "//zetasql/analyzer",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/common:errors",
        "//zetasql/common:status_payload_utils",
        "//zetasql/parser",
        "//zetasql/proto:function_cc_proto",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_ast_enums_cc_proto",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
    alwayslink = 1,
)

# :collator_lite is a basic collator implementation optimized for binary size,
# supporting only the "unicode:cs" binary-comparison collation. You most likely
# want :collator instead.
cc_library(
    name = "collator_lite",
    srcs = ["collator_lite.cc"],
    hdrs = ["collator.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        "//zetasql/base:statusor",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "collator_lite_test",
    srcs = ["collator_lite_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":collator_lite",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "collator",
    srcs = ["collator.cc"],
    hdrs = ["collator.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":collator_lite",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@icu",
        "@icu//:common",
    ],
)

cc_library(
    name = "constant",
    hdrs = ["constant.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":type",
        ":value",
        "//zetasql/base",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "function_test",
    size = "small",
    srcs = ["function_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":builtin_function",
        ":deprecation_warning_cc_proto",
        ":error_location_cc_proto",
        ":function",
        ":function_cc_proto",
        ":language_options",
        ":parse_location",
        ":sql_function",
        ":type",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/proto:function_cc_proto",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "function_signature_test",
    size = "small",
    srcs = ["function_signature_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":error_location_cc_proto",
        ":function",
        ":function_cc_proto",
        ":type",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "@com_google_absl//absl/memory",
    ],
)

cc_test(
    name = "input_argument_type_test",
    size = "small",
    srcs = ["input_argument_type_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":function",
        ":type",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_test(
    name = "procedure_test",
    size = "small",
    srcs = ["procedure_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":function",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common/testing:proto_matchers",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "collator_test",
    size = "small",
    srcs = ["collator_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":collator",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base/testing:status_matchers",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "parse_resume_location",
    hdrs = ["parse_resume_location.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":parse_resume_location_cc_proto",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "parse_resume_location_test",
    size = "small",
    srcs = ["parse_resume_location_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":parse_resume_location",
        ":parse_resume_location_cc_proto",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "parse_location",
    srcs = ["parse_location.cc"],
    hdrs = ["parse_location.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":parse_location_range_cc_proto",
        "//zetasql/base",
        "//zetasql/base:mathutil",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/proto:internal_error_location_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@icu//:common",
    ],
)

cc_test(
    name = "parse_location_test",
    size = "small",
    srcs = ["parse_location_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":parse_location",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/proto:internal_error_location_cc_proto",
    ],
)

cc_library(
    name = "parse_helpers",
    srcs = [
        "parse_helpers.cc",
        "parse_tokens.cc",
    ],
    hdrs = [
        "parse_helpers.h",
        "parse_tokens.h",
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
        ":error_helpers",
        ":options_cc_proto",
        ":parse_location",
        ":parse_resume_location",
        ":strings",
        ":value",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/parser",
        "//zetasql/parser:bison_parser_generated_lib",
        "//zetasql/parser:keywords",
        "//zetasql/public/functions:convert_string",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "parse_tokens_test",
    size = "small",
    srcs = ["parse_tokens_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":error_helpers",
        ":options_cc_proto",
        ":parse_helpers",
        ":parse_resume_location",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base",
        "//zetasql/base:path",
        "//zetasql/base/testing:status_matchers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "language_options",
    srcs = ["language_options.cc"],
    hdrs = ["language_options.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":options_cc_proto",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/proto:options_cc_proto",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "language_options_test",
    size = "small",
    srcs = ["language_options_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":language_options",
        ":options_cc_proto",
        "@com_google_googletest//:gtest_main",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

exports_files(["analyzer.h"])

cc_library(
    name = "analyzer",
    hdrs = ["analyzer.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":catalog",
        ":id_string",
        ":language_options",
        ":options_cc_proto",
        ":type",
        ":value",
        "//zetasql/analyzer",  # buildcleaner: keep
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/proto:options_cc_proto",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:node_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

# Abstract base classes for the full and lite evaluators.
# Use either :evaluator or :evaluator_lite instead.
cc_library(
    name = "evaluator_base",
    srcs = ["evaluator_base.cc"],
    hdrs = ["evaluator_base.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":analyzer",
        ":catalog",
        ":evaluator_table_iterator",
        ":language_options",
        ":options_cc_proto",
        ":simple_catalog",
        ":strings",
        ":type",
        ":value",
        "//zetasql/base",
        "//zetasql/base:clock",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/reference_impl:algebrizer",
        "//zetasql/reference_impl:common",
        "//zetasql/reference_impl:evaluation",
        "//zetasql/reference_impl:parameters",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "//zetasql/resolved_ast:validator",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_absl//absl/types:variant",
    ],
)

# Full evaluator implementation.
cc_library(
    name = "evaluator",
    srcs = ["evaluator.cc"],
    hdrs = ["evaluator.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":evaluator_base",
        "//zetasql/common:evaluator_registration_utils",
    ],
)

# Lite evaluator implementation; optimized for executable size at the expense
# of some features. See evaluator_lite.h for pointers on what is excluded
# and how to selectively reenable features.
cc_library(
    name = "evaluator_lite",
    hdrs = ["evaluator_lite.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":evaluator_base",
    ],
)

cc_test(
    name = "evaluator_test",
    size = "small",
    srcs = ["evaluator_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":analyzer",
        ":civil_time",
        ":evaluator",
        ":function",
        ":function_cc_proto",
        ":id_string",
        ":language_options",
        ":options_cc_proto",
        ":simple_catalog",
        ":type",
        ":type_cc_proto",
        ":value",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base",
        "//zetasql/base:clock",
        "//zetasql/base:ret_check",
        "//zetasql/base:stl_util",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common:evaluator_test_table",
        "//zetasql/common/testing:testing_proto_util",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/reference_impl:evaluation",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "//zetasql/testdata:populate_sample_tables",
        "//zetasql/testdata:sample_catalog",
        "//zetasql/testdata:test_schema_cc_proto",
        "//zetasql/testing:test_value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "evaluator_lite_test",
    size = "small",
    srcs = ["evaluator_lite_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":evaluator_lite",
        ":value",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "evaluator_table_iterator",
    hdrs = ["evaluator_table_iterator.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":value",
        "//zetasql/base:status",
    ],
)

cc_library(
    name = "id_string",
    srcs = ["id_string.cc"],
    hdrs = ["id_string.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        "//zetasql/base",
        "//zetasql/base:case",
        "//zetasql/base:endian",
        "//zetasql/base:map_util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "id_string_test",
    size = "small",
    srcs = ["id_string_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":id_string",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:map_util",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:node_hash_set",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "table_valued_function_test",
    srcs = ["table_valued_function_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":function",
        ":type",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:statusor",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/proto:function_cc_proto",
    ],
)

exports_files(["type_annotation.proto"])
