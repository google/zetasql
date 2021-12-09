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
    name = "util",
    srcs = ["util.cc"],
    hdrs = ["util.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/base:status",
        "//zetasql/common:utf_util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "util_test",
    size = "small",
    srcs = ["util_test.cc"],
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
        ":util",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
    ],
)

cc_library(
    name = "convert_internal",
    hdrs = ["convert_internal.h"],
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
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_test(
    name = "convert_internal_test",
    size = "small",
    srcs = ["convert_internal_test.cc"],
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
        ":convert_internal",
        "//zetasql/base/testing:zetasql_gtest_main",
    ],
)

cc_library(
    name = "convert",
    srcs = ["convert.cc"],
    hdrs = ["convert.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":convert_internal",
        ":util",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/public:numeric_value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "convert_test",
    size = "small",
    srcs = ["convert_test.cc"],
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
        ":convert",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:numeric_value",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "convert_proto",
    srcs = ["convert_proto.cc"],
    hdrs = ["convert_proto.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "convert_proto_test",
    size = "small",
    srcs = ["convert_proto_test.cc"],
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
        ":convert_proto",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/testdata:test_schema_cc_proto",
    ],
)

cc_library(
    name = "convert_string",
    srcs = ["convert_string.cc"],
    hdrs = ["convert_string.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":util",
        "//zetasql/base:case",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/common:string_util",
        "//zetasql/public:numeric_value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "convert_string_test",
    size = "small",
    srcs = ["convert_string_test.cc"],
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
        ":convert_string",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "convert_string_with_format",
    srcs = ["convert_string_with_format.cc"],
    hdrs = ["convert_string_with_format.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":convert",
        ":format_max_output_width",
        ":string_format",
        ":util",
        "//zetasql/base:status",
        "//zetasql/public:value",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "convert_string_with_format_test",
    size = "small",
    srcs = ["convert_string_with_format_test.cc"],
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
        ":convert_string",
        ":convert_string_with_format",
        ":format_max_output_width",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:numeric_value",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "like",
    srcs = ["like.cc"],
    hdrs = ["like.h"],
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
        "//zetasql/base:status",
        "//zetasql/public:type_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "like_test",
    size = "small",
    srcs = ["like_test.cc"],
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
        ":like",
        "//zetasql/base:status",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/strings",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_library(
    name = "format_max_output_width",
    srcs = ["format_max_output_width.cc"],
    hdrs = ["format_max_output_width.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "@com_google_absl//absl/flags:flag",
    ],
)

cc_library(
    name = "input_format_string_max_width",
    srcs = ["input_format_string_max_width.cc"],
    hdrs = ["input_format_string_max_width.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = ["@com_google_absl//absl/flags:flag"],
)

cc_library(
    name = "string_format",
    srcs = ["string_format.cc"],
    hdrs = ["string_format.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":convert_proto",
        ":format_max_output_width",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:utf_util",
        "//zetasql/public:numeric_value",
        "//zetasql/public:strings",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
        "@com_googlesource_code_re2//:re2",
        "@icu//:common",
    ],
)

cc_test(
    name = "format_test",
    size = "medium",
    srcs = ["format_test.cc"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    shard_count = 6,
    deps = [
        ":format_max_output_width",
        ":string_format",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:civil_time",
        "//zetasql/testdata:test_schema_cc_proto",
        "//zetasql/testing:test_function",
        "//zetasql/testing:test_value",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "string_format_test",
    size = "small",
    srcs = ["string_format_test.cc"],
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
        ":string_format",
        "//zetasql/base",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:testing_proto_util",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "arithmetics_internal",
    hdrs = ["arithmetics_internal.h"],
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
    ],
)

cc_test(
    name = "arithmetics_internal_test",
    size = "small",
    srcs = ["arithmetics_internal_test.cc"],
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
        ":arithmetics_internal",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "arithmetics",
    hdrs = ["arithmetics.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":arithmetics_internal",
        ":convert",
        ":util",
        "//zetasql/base:status",
        "//zetasql/public:numeric_value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "arithmetics_test",
    size = "small",
    srcs = ["arithmetics_test.cc"],
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
        ":arithmetics",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "bitwise",
    srcs = ["bitwise.cc"],
    hdrs = ["bitwise.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":util",
        "//zetasql/base",
        "//zetasql/base:bits",
        "//zetasql/base:status",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "bitwise_test",
    size = "small",
    srcs = ["bitwise_test.cc"],
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
        ":bitwise",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/testing:test_function",
    ],
)

cc_library(
    name = "bitcast",
    hdrs = ["bitcast.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/base:status",
        "@com_google_absl//absl/base",
    ],
)

cc_test(
    name = "bitcast_test",
    size = "small",
    srcs = ["bitcast_test.cc"],
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
        ":bitcast",
        "//zetasql/base:status",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "comparison",
    hdrs = ["comparison.h"],
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

cc_test(
    name = "comparison_test",
    size = "small",
    srcs = ["comparison_test.cc"],
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
        ":comparison",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/random",
    ],
)

cc_library(
    name = "date_time_util_internal",
    srcs = ["date_time_util_internal.cc"],
    hdrs = ["date_time_util_internal.h"],
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
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "date_time_util_internal_test",
    size = "small",
    srcs = ["date_time_util_internal_test.cc"],
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
        ":date_time_util_internal",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "date_time_util",
    srcs = ["date_time_util.cc"],
    hdrs = ["date_time_util.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":arithmetics",
        ":date_time_util_internal",
        ":datetime_cc_proto",
        "//zetasql/base",
        "//zetasql/base:mathutil",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/base:time_proto_util",
        "//zetasql/common:errors",
        "//zetasql/common:utf_util",
        "//zetasql/public:civil_time",
        "//zetasql/public:interval_value",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "//zetasql/public/types:timestamp_util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
        "@com_googleapis_googleapis//:date_cc_proto",
    ],
)

cc_library(
    name = "parse_date_time_utils",
    srcs = ["parse_date_time_utils.cc"],
    hdrs = ["parse_date_time_utils.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":date_time_util",
        "//zetasql/base",
        "//zetasql/public:strings",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "parse_date_time",
    srcs = ["parse_date_time.cc"],
    hdrs = ["parse_date_time.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":date_time_util",
        ":datetime_cc_proto",
        ":parse_date_time_utils",
        "//zetasql/base",
        "//zetasql/base:mathutil",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/public:civil_time",
        "//zetasql/public:strings",
        "//zetasql/public:type",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "cast_date_time",
    srcs = ["cast_date_time.cc"],
    hdrs = ["cast_date_time.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":date_time_util",
        ":datetime_cc_proto",
        ":input_format_string_max_width",
        ":parse_date_time_utils",
        "//zetasql/base",
        "//zetasql/base:general_trie",
        "//zetasql/base:map_util",
        "//zetasql/base:mathutil",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:utf_util",
        "//zetasql/public:strings",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@icu//:headers",
    ],
)

cc_library(
    name = "math",
    srcs = ["math.cc"],
    hdrs = ["math.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":arithmetics",
        ":util",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/public:numeric_value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "math_test",
    size = "small",
    srcs = ["math_test.cc"],
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
        ":math",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common:float_margin",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:numeric_value",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "net",
    srcs = ["net.cc"],
    hdrs = ["net.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":util",
        "//zetasql/base:status",
        "//zetasql/base/net:idn",
        "//zetasql/base/net:ipaddress",
        "//zetasql/base/net:public_suffix",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "net_test",
    size = "small",
    srcs = ["net_test.cc"],
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
        ":net",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/testing:test_function",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_library(
    name = "numeric",
    srcs = ["numeric.cc"],
    hdrs = ["numeric.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":util",
        "//zetasql/base",
        "//zetasql/public:numeric_value",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "numeric_test",
    size = "small",
    srcs = ["numeric_test.cc"],
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
        ":numeric",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "string",
    srcs = ["string.cc"],
    hdrs = ["string.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":normalize_mode_cc_proto",
        ":util",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:stl_util",
        "//zetasql/base:strings",
        "//zetasql/common:utf_util",
        "//zetasql/public:strings",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "string_with_collation",
    srcs = ["string_with_collation.cc"],
    hdrs = ["string_with_collation.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":string",
        ":util",
        "//zetasql/base:status",
        "//zetasql/common:utf_util",
        "//zetasql/public:collator_lite",
        "//zetasql/public/functions:arithmetics",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "uuid",
    srcs = ["uuid.cc"],
    hdrs = ["uuid.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/base:status",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:bit_gen_ref",
    ],
)

cc_test(
    name = "uuid_test",
    size = "small",
    srcs = ["uuid_test.cc"],
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
        ":uuid",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:mock_distributions",
        "@com_google_absl//absl/random:mocking_bit_gen",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "string_test",
    size = "small",
    srcs = ["string_test.cc"],
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
        ":normalize_mode_cc_proto",
        ":string",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "string_with_collation_test",
    size = "small",
    srcs = ["string_with_collation_test.cc"],
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
        ":string_with_collation",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:collator",  # buildcleaner: keep
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "regexp",
    srcs = ["regexp.cc"],
    hdrs = ["regexp.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":string",
        ":util",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/common:utf_util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_googlesource_code_re2//:re2",
        "@icu//:headers",
    ],
)

cc_test(
    name = "regexp_test",
    size = "small",
    srcs = ["regexp_test.cc"],
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
        ":regexp",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/status",
    ],
)

proto_library(
    name = "datetime_proto",
    srcs = ["datetime.proto"],
)

cc_proto_library(
    name = "datetime_cc_proto",
    deps = ["datetime_proto"],
)

java_proto_library(
    name = "datetime_java_proto",
    deps = [":datetime_proto"],
)

proto_library(
    name = "normalize_mode_proto",
    srcs = ["normalize_mode.proto"],
)

cc_proto_library(
    name = "normalize_mode_cc_proto",
    deps = ["normalize_mode_proto"],
)

java_proto_library(
    name = "normalize_mode_java_proto",
    deps = [":normalize_mode_proto"],
)

cc_library(
    name = "json_internal",
    srcs = ["json_internal.cc"],
    hdrs = ["json_internal.h"],
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
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/common:json_parser",
        "//zetasql/common:json_util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_library(
    name = "json",
    srcs = ["json.cc"],
    hdrs = ["json.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":json_format",
        ":json_internal",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/common:errors",
        "//zetasql/common:int_ops_util",
        "//zetasql/public:json_value",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "json_test",
    srcs = ["json_test.cc"],
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
        ":json",
        ":json_internal",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "generate_array",
    hdrs = ["generate_array.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":arithmetics",
        ":date_time_util",
        ":datetime_cc_proto",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/public:numeric_value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "generate_array_test",
    srcs = ["generate_array_test.cc"],
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
        ":generate_array",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common:float_margin",
        "//zetasql/common:internal_value",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:numeric_value",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "common_proto",
    srcs = ["common_proto.cc"],
    hdrs = ["common_proto.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":date_time_util",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:utf_util",
        "//zetasql/public:civil_time",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:cord",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_googleapis_googleapis//:latlng_cc_proto",
        "@com_googleapis_googleapis//:timeofday_cc_proto",
    ],
)

cc_test(
    name = "common_proto_test",
    srcs = ["common_proto_test.cc"],
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
        ":common_proto",
        ":date_time_util",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:civil_time",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_googleapis_googleapis//:timeofday_cc_proto",
    ],
)

cc_library(
    name = "hash",
    srcs = ["hash.cc"],
    hdrs = ["hash.h"],
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
        "//zetasql/base:endian",
        "@boringssl//:crypto",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_farmhash//:farmhash_fingerprint",
    ],
)

cc_test(
    name = "hash_test",
    srcs = ["hash_test.cc"],
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
        ":hash",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "json_format",
    srcs = ["json_format.cc"],
    hdrs = ["json_format.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":date_time_util",
        "//zetasql/base",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:json_util",
        "//zetasql/common:string_util",
        "//zetasql/public:civil_time",
        "//zetasql/public:json_value",
        "//zetasql/public:numeric_value",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "json_format_test",
    srcs = ["json_format_test.cc"],
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
        ":json_format",
        "//zetasql/base:map_util",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "to_json",
    srcs = ["to_json.cc"],
    hdrs = ["to_json.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":json_format",
        "//zetasql/base:logging",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/public:interval_value",
        "//zetasql/public:json_value",
        "//zetasql/public:language_options",
        "//zetasql/public:numeric_value",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "to_json_test",
    srcs = ["to_json_test.cc"],
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
        ":to_json",
        "//zetasql/base:map_util",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common:float_margin",
        "//zetasql/common:internal_value",
        "//zetasql/compliance:functions_testlib",
        "//zetasql/public:json_value",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:value",
        "//zetasql/testing:test_function",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "percentile",
    srcs = ["percentile.cc"],
    hdrs = ["percentile.h"],
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
        "//zetasql/base:mathutil",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:multiprecision_int",
        "//zetasql/public:numeric_value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "percentile_test",
    size = "small",
    srcs = ["percentile_test.cc"],
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
        ":percentile",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common:string_util",
        "//zetasql/public:numeric_value",
        "@com_google_absl//absl/strings",
    ],
)
