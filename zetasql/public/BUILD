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

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "anonymization_utils",
    srcs = [
        "anonymization_utils.cc",
    ],
    hdrs = [
        "anonymization_utils.h",
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
        ":type",
        ":value",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_cc_differential_privacy//algorithms:partition-selection",
    ],
)

cc_test(
    name = "anonymization_utils_test",
    size = "small",
    srcs = ["anonymization_utils_test.cc"],
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
        ":anonymization_utils",
        ":value",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/status:statusor",
    ],
)

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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":type_annotation_cc_proto",
        ":type_cc_proto",
        "//zetasql/base",
        "//zetasql/base:case",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:strings",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "catalog",
    srcs = [
        "catalog.cc",
        "catalog_helper.cc",
    ],
    hdrs = [
        "catalog.h",
        "catalog_helper.h",
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
        ":evaluator_table_iterator",
        ":options_cc_proto",
        ":strings",
        ":type",
        "//zetasql/base",
        "//zetasql/base:edit_distance",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status:statusor",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":cycle_detector",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "multi_catalog",
    srcs = ["multi_catalog.cc"],
    hdrs = ["multi_catalog.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":catalog",
        ":constant",
        ":function",
        ":type",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":civil_time",
        "//zetasql/base",
        "//zetasql/base/testing:zetasql_gtest_main",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "//zetasql/base:strings",
        "//zetasql/common:simple_evaluator_table_iterator",
        "//zetasql/proto:simple_catalog_cc_proto",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "//zetasql/public/types",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status:statusor",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":sql_formatter",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":catalog",
        ":civil_time",
        ":function",
        ":language_options",
        ":numeric_value",
        ":options_cc_proto",
        ":proto_value_conversion",
        ":signature_match_result",
        ":strings",
        ":type",
        ":type_cc_proto",
        ":value",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:internal_value",
        "//zetasql/common:utf_util",
        "//zetasql/public/functions:cast_date_time",
        "//zetasql/public/functions:convert",
        "//zetasql/public/functions:convert_proto",
        "//zetasql/public/functions:convert_string",
        "//zetasql/public/functions:convert_string_with_format",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/public/functions:datetime_cc_proto",
        "//zetasql/public/functions:string",
        "//zetasql/public/types",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "signature_match_result",
    srcs = ["signature_match_result.cc"],
    hdrs = ["signature_match_result.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":type",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "numeric_constants",
    hdrs = ["numeric_constants.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "numeric_parser",
    srcs = ["numeric_parser.cc"],
    hdrs = ["numeric_parser.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":numeric_constants",
        "//zetasql/base:status",
        "//zetasql/common:multiprecision_int",
        "@com_google_absl//absl/status",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":numeric_constants",
        ":numeric_parser",
        "//zetasql/base",
        "//zetasql/base:endian",
        "//zetasql/base:mathutil",
        "//zetasql/base:status",
        "//zetasql/base:stl_util",
        "//zetasql/common:errors",
        "//zetasql/common:multiprecision_int",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "numeric_value_test_utils",
    testonly = 1,
    hdrs = ["numeric_value_test_utils.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":numeric_value",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "numeric_value_test_utils_test",
    srcs = ["numeric_value_test_utils_test.cc"],
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
        ":numeric_value",
        ":numeric_value_test_utils",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "numeric_value_test",
    size = "small",
    timeout = "moderate",
    srcs = ["numeric_value_test.cc"],
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
        ":numeric_value",
        ":numeric_value_test_utils",
        "//zetasql/base:bits",
        "//zetasql/base:endian",
        "//zetasql/base:mathutil",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common:multiprecision_int",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:variant",
    ],
)

cc_library(
    name = "json_value",
    srcs = ["json_value.cc"],
    hdrs = [
        "json_value.h",
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
        ":numeric_parser",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:json_parser",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@json",
    ],
)

cc_test(
    name = "json_value_test",
    srcs = ["json_value_test.cc"],
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
        ":json_value",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "interval_value",
    srcs = ["interval_value.cc"],
    hdrs = [
        "interval_value.h",
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
        "//zetasql/base:endian",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:multiprecision_int",
        "//zetasql/public/functions:arithmetics",
        "//zetasql/public/functions:datetime_cc_proto",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_library(
    name = "interval_value_test_util",
    testonly = 1,
    hdrs = [
        "interval_value_test_util.h",
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
        ":interval_value",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
    ],
)

cc_test(
    name = "interval_value_test",
    srcs = ["interval_value_test.cc"],
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
        ":interval_value",
        ":interval_value_test_util",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public/functions:datetime_cc_proto",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":civil_time",
        ":interval_value",
        ":json_value",
        ":language_options",
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
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
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
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":analyzer",
        ":evaluator",
        ":interval_value",
        ":interval_value_test_util",
        ":json_value",
        ":language_options",
        ":numeric_value",
        ":options_cc_proto",
        ":simple_catalog",
        ":type",
        ":value",
        "//zetasql/base",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
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
        "@com_google_absl//absl/status:statusor",
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
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":civil_time",
        ":type",
        ":type_cc_proto",
        ":value",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/common/testing:testing_proto_util",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "//zetasql/public/types",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status:statusor",
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
    deps = [
        ":annotation_proto",
        ":type_proto",
    ],
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
    name = "type_parameters_proto",
    srcs = ["type_parameters.proto"],
    deps = [
        ":simple_value_proto",
    ],
)

cc_proto_library(
    name = "type_parameters_cc_proto",
    deps = [":type_parameters_proto"],
)

java_proto_library(
    name = "type_parameters_java_proto",
    deps = [":type_parameters_proto"],
)

proto_library(
    name = "collation_proto",
    srcs = ["collation.proto"],
)

cc_proto_library(
    name = "collation_cc_proto",
    deps = [":collation_proto"],
)

java_proto_library(
    name = "collation_java_proto",
    deps = [":collation_proto"],
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "@com_google_absl//absl/status:statusor",
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
        ":error_location_cc_proto",
        ":parse_location",
        "//zetasql/base",
        "//zetasql/base:enum_utils",
        "//zetasql/base:map_util",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common:errors",
        "//zetasql/common:status_payload_utils",
        "//zetasql/common/testing:proto_matchers",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":json_value",
        ":numeric_value",
        ":type",
        ":type_cc_proto",
        ":value",
        ":value_cc_proto",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status:statusor",
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
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "@com_google_protobuf//:protobuf",
        "@com_google_protobuf//:cc_wkt_protos",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        # buildcleaner: keep (text format proto)
        "//zetasql/testdata:test_schema_cc_proto",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/status:statusor",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":id_string",
        ":language_options",
        "//zetasql/base",
        "//zetasql/base:case",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/common:errors",
        "//zetasql/common:utf_util",
        "//zetasql/parser:keywords",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status:statusor",
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
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":language_options",
        ":options_cc_proto",
        ":strings",
        "//zetasql/base:map_util",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "//zetasql/base:strings",
        "//zetasql/common:builtin_function_internal",
        "//zetasql/common:errors",
        "//zetasql/proto:options_cc_proto",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status:statusor",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":catalog",
        ":deprecation_warning_cc_proto",
        ":function_cc_proto",
        ":language_options",
        ":options_cc_proto",
        ":parse_location",
        ":signature_match_result",
        ":simple_table_cc_proto",
        ":strings",
        ":type",
        ":type_cc_proto",
        ":value",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/common:errors",
        "//zetasql/proto:function_cc_proto",
        "//zetasql/public/types",
        "//zetasql/resolved_ast:serialization_cc_proto",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
    name = "anon_function",
    srcs = ["anon_function.cc"],
    hdrs = ["anon_function.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":function",
        ":function_cc_proto",
        ":language_options",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "sql_tvf",
    srcs = ["sql_tvf.cc"],
    hdrs = ["sql_tvf.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        ":simple_table_cc_proto",
        ":strings",
        ":type",
        "//zetasql/analyzer:name_scope",
        "//zetasql/analyzer:resolver",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:status_payload_utils",
        "//zetasql/parser",
        "//zetasql/proto:function_cc_proto",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_ast_enums_cc_proto",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status:statusor",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@icu//:headers",
    ],
)

cc_test(
    name = "collator_lite_test",
    srcs = ["collator_lite_test.cc"],
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
        ":collator_lite",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "collator",
    srcs = ["collator.cc"],
    hdrs = ["collator.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":collator_lite",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@icu",  # buildcleaner: keep
        "@icu//:common",
    ],
)

cc_library(
    name = "constant",
    hdrs = ["constant.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":type",
        ":value",
        "//zetasql/base",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "cast_test",
    size = "small",
    srcs = ["cast_test.cc"],
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
        ":coercer",
        ":function",
        ":language_options",
        ":options_cc_proto",
        ":type",
        ":value",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public/types",
        "//zetasql/testdata:test_schema_cc_proto",
        "//zetasql/testing:test_function",
        "//zetasql/testing:test_value",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "function_test",
    size = "small",
    srcs = ["function_test.cc"],
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
        ":builtin_function",
        ":deprecation_warning_cc_proto",
        ":error_location_cc_proto",
        ":function",
        ":function_cc_proto",
        ":language_options",
        ":parse_location",
        ":parse_location_range_cc_proto",
        ":sql_function",
        ":type",
        "//zetasql/base",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/common/testing:testing_proto_util",
        "//zetasql/proto:function_cc_proto",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "function_signature_test",
    size = "small",
    srcs = ["function_signature_test.cc"],
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
        ":error_location_cc_proto",
        ":function",
        ":function_cc_proto",
        ":options_cc_proto",
        ":type",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/proto:function_cc_proto",
        "//zetasql/public/types",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "input_argument_type_test",
    size = "small",
    srcs = ["input_argument_type_test.cc"],
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
        ":function",
        ":type",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
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
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":function",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/public/types",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "collator_test",
    size = "small",
    srcs = ["collator_test.cc"],
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
        ":collator",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "parse_resume_location",
    hdrs = ["parse_resume_location.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":parse_resume_location",
        ":parse_resume_location_cc_proto",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "parse_location",
    srcs = ["parse_location.cc"],
    hdrs = ["parse_location.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":parse_location_range_cc_proto",
        "//zetasql/base",
        "//zetasql/base:mathutil",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/proto:internal_error_location_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@icu//:common",
    ],
)

cc_test(
    name = "parse_location_test",
    size = "small",
    srcs = ["parse_location_test.cc"],
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
        ":parse_location",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "//zetasql/base:strings",
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
        ":options_cc_proto",
        ":parse_helpers",
        ":parse_resume_location",
        "//zetasql/base:path",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "language_options",
    srcs = ["language_options.cc"],
    hdrs = ["language_options.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":options_cc_proto",
        "//zetasql/base",
        "//zetasql/base:case",
        "//zetasql/base:map_util",
        "//zetasql/base:status",
        "//zetasql/parser:keywords",
        "//zetasql/proto:options_cc_proto",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "language_options_test",
    size = "small",
    srcs = ["language_options_test.cc"],
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
        ":language_options",
        ":options_cc_proto",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "analyzer_options",
    srcs = ["analyzer_options.cc"],
    hdrs = ["analyzer_options.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":catalog",
        ":id_string",
        ":type",
        "//zetasql/base",
        "//zetasql/base:strings",
        "//zetasql/parser",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:btree",
    ],
)

cc_library(
    name = "analyzer_output_properties",
    hdrs = ["analyzer_output_properties.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":options_cc_proto",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_map",
    ],
)

cc_library(
    name = "analyzer_output",
    srcs = ["analyzer_output.cc"],
    hdrs = ["analyzer_output.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":analyzer_options",
        ":analyzer_output_properties",
        ":id_string",
        "//zetasql/base:arena",
        "//zetasql/parser",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "analyzer",
    srcs = [
        "analyzer.cc",
        "table_name_resolver.cc",
        "table_name_resolver.h",
    ],
    hdrs = [
        "analyzer.h",
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
        ":analyzer_options",
        ":analyzer_output",
        ":catalog",
        ":language_options",
        ":options_cc_proto",
        ":parse_helpers",
        ":parse_resume_location",
        ":type",
        ":type_cc_proto",
        ":value",
        "//zetasql/analyzer:all_rewriters",
        "//zetasql/analyzer:analyzer_impl",
        "//zetasql/analyzer:resolver",
        "//zetasql/analyzer:rewrite_resolved_ast",
        "//zetasql/base",
        "//zetasql/base:arena",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/common:errors",
        "//zetasql/parser",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "//zetasql/resolved_ast:validator",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "literal_remover",
    srcs = ["literal_remover.cc"],
    hdrs = ["literal_remover.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":analyzer_options",
        ":analyzer_output",
        ":language_options",
        ":parse_location",
        ":type",
        ":value",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_google_absl//absl/container:node_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

# Abstract base classes for the full and lite evaluators.
# Use either :evaluator or :evaluator_lite instead.
cc_library(
    name = "evaluator_base",
    srcs = ["evaluator_base.cc"],
    hdrs = ["evaluator_base.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "//zetasql/base:strings",
        "//zetasql/reference_impl:algebrizer",
        "//zetasql/reference_impl:common",
        "//zetasql/reference_impl:evaluation",
        "//zetasql/reference_impl:parameters",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "//zetasql/resolved_ast:validator",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:node_hash_map",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":evaluator_base",
    ],
)

cc_test(
    name = "evaluator_test",
    size = "small",
    timeout = "moderate",
    srcs = ["evaluator_test.cc"],
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
        ":analyzer",
        ":civil_time",
        ":evaluator",
        ":evaluator_base",
        ":function",
        ":function_cc_proto",
        ":id_string",
        ":language_options",
        ":options_cc_proto",
        ":simple_catalog",
        ":type",
        ":type_cc_proto",
        ":value",
        "//zetasql/base",
        "//zetasql/base:clock",
        "//zetasql/base:ret_check",
        "//zetasql/base:stl_util",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common:evaluator_test_table",
        "//zetasql/common/testing:testing_proto_util",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/public/types",
        "//zetasql/reference_impl:evaluation",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "//zetasql/resolved_ast:sql_builder",
        "//zetasql/testdata:populate_sample_tables",
        "//zetasql/testdata:sample_catalog",
        "//zetasql/testdata:test_schema_cc_proto",
        "//zetasql/testing:test_value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:str_format",
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
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":evaluator_lite",
        ":value",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
    ],
)

cc_library(
    name = "evaluator_table_iterator",
    hdrs = ["evaluator_table_iterator.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/base",
        "//zetasql/base:arena",
        "//zetasql/base:case",
        "//zetasql/base:endian",
        "//zetasql/base:map_util",
        "//zetasql/base:strings",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "id_string_test",
    size = "small",
    srcs = ["id_string_test.cc"],
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
        ":id_string",
        "//zetasql/base:map_util",
        "//zetasql/base:strings",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:node_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "table_valued_function_test",
    srcs = ["table_valued_function_test.cc"],
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
        ":function",
        ":language_options",
        ":type",
        ":type_cc_proto",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/proto:function_cc_proto",
        "@com_google_absl//absl/status:statusor",
    ],
)

proto_library(
    name = "annotation_proto",
    srcs = ["annotation.proto"],
    deps = [":simple_value_proto"],
)

cc_proto_library(
    name = "annotation_cc_proto",
    deps = [":annotation_proto"],
)

java_proto_library(
    name = "annotation_java_proto",
    deps = [":annotation_proto"],
)

proto_library(
    name = "simple_value_proto",
    srcs = ["simple_value.proto"],
)

cc_proto_library(
    name = "simple_value_cc_proto",
    deps = [":simple_value_proto"],
)

java_proto_library(
    name = "simple_value_java_proto",
    deps = [":simple_value_proto"],
)

exports_files(["type_annotation.proto"])

cc_test(
    name = "table_name_resolver_test",
    size = "small",
    srcs = ["table_name_resolver_test.cc"],
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
        ":analyzer",
        ":options_cc_proto",
        ":simple_catalog",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/parser",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
    ],
)
