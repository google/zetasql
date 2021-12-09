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

package(default_visibility = ["//zetasql/base:zetasql_implementation"])

cc_library(
    name = "sql_test_base",
    testonly = 1,
    srcs = ["sql_test_base.cc"],
    hdrs = ["sql_test_base.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":known_error_cc_proto",
        ":legal_runtime_errors",
        ":matchers",
        ":parameters_test_util",
        ":test_driver",
        ":test_util",
        "//zetasql/base",
        "//zetasql/base:file_util",
        "//zetasql/base:map_util",
        "//zetasql/base:path",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common:float_margin",
        "//zetasql/common:internal_value",
        "//zetasql/common:status_payload_utils",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_helpers",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public/functions:string",
        "//zetasql/public/types",
        "//zetasql/reference_impl:evaluation",
        "//zetasql/reference_impl:reference_driver",
        "//zetasql/testing:type_util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:node_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_farmhash//:farmhash_fingerprint",
        "@com_google_file_based_test_driver//file_based_test_driver",
        "@com_google_file_based_test_driver//file_based_test_driver:run_test_case_result",
        "@com_google_file_based_test_driver//file_based_test_driver:test_case_options",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:protobuf",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_library(
    name = "compliance_test_base",
    testonly = 1,
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    data = glob(["testdata/*.test"]) + [
        "//zetasql/testdata:test_schema_proto",
        "//zetasql/testdata:test_proto3_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@com_google_protobuf//:wrappers_proto",
        "@com_googleapis_googleapis//:latlng_proto",
        "@com_googleapis_googleapis//:date_proto",
        "@com_googleapis_googleapis//:timeofday_proto",
    ],
    deps = [
        ":sql_test_base",
    ],
)

cc_library(
    name = "test_driver",
    testonly = 1,
    srcs = ["test_driver.cc"],
    hdrs = ["test_driver.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":test_driver_cc_proto",
        "//zetasql/base",
        "//zetasql/base:file_util",
        "//zetasql/base:path",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/public:annotation_cc_proto",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/public/types",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "test_driver_test",
    size = "small",
    srcs = ["test_driver_test.cc"],
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
        ":test_driver",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:value",
    ],
)

proto_library(
    name = "test_driver_proto",
    srcs = ["test_driver.proto"],
    deps = [
        "//zetasql/public:annotation_proto",
        "//zetasql/public:options_proto",
        "//zetasql/public:type_proto",
        "//zetasql/public:value_proto",
    ],
)

cc_proto_library(
    name = "test_driver_cc_proto",
    deps = [":test_driver_proto"],
)

java_proto_library(
    name = "test_driver_java_proto",
    deps = [":test_driver_proto"],
)

cc_library(
    name = "test_util",
    testonly = 1,
    srcs = ["test_util.cc"],
    hdrs = ["test_util.h"],
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
        "//zetasql/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/flags:commandlineflag",
        "@com_google_absl//absl/flags:reflection",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "parameters_test_util",
    testonly = 1,
    srcs = ["parameters_test_util.cc"],
    hdrs = ["parameters_test_util.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":test_driver",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:language_options",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/reference_impl:reference_driver",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "compliance_test_cases",
    testonly = 1,
    srcs = ["compliance_test_cases.cc"],
    hdrs = ["compliance_test_cases.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        # Required runtime dependency
        # buildcleaner: keep
        ":compliance_test_base",
        ":functions_testlib",
        ":sql_test_base",
        ":test_driver",
        "//zetasql/base",
        "//zetasql/base:path",
        "@com_google_protobuf//:protobuf",
        "//zetasql/common:float_margin",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common/testing:testing_proto_util",
        "//zetasql/public:civil_time",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:strings",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/reference_impl:reference_driver",
        "//zetasql/testdata:test_schema_cc_proto",
        "//zetasql/testing:test_function",
        "//zetasql/testing:test_value",
        "//zetasql/testing:type_util",
        "@com_google_googletest//:gtest",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "//zetasql/base:file_util",
        "//zetasql/base:map_util",
        "//zetasql/base:status",
    ],
)

cc_library(
    name = "matchers",
    testonly = 1,
    srcs = ["matchers.cc"],
    hdrs = ["matchers.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/base:map_util",
        "//zetasql/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:fixed_array",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "matchers_test",
    size = "small",
    srcs = ["matchers_test.cc"],
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
        ":matchers",
        "//zetasql/base",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "runtime_expected_errors",
    testonly = 1,
    srcs = ["runtime_expected_errors.cc"],
    hdrs = ["runtime_expected_errors.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":matchers",
        "//zetasql/base:status",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "functions_testlib",
    testonly = 1,
    srcs = [
        "functions_testlib_1.cc",
        "functions_testlib_2.cc",
        "functions_testlib_cast.cc",
        "functions_testlib_common.cc",
        "functions_testlib_format.cc",
        "functions_testlib_format_floating_point.cc",
        "functions_testlib_format_integral.cc",
        "functions_testlib_format_json.cc",
        "functions_testlib_format_nulls.cc",
        "functions_testlib_format_numeric.cc",
        "functions_testlib_format_strings.cc",
        "functions_testlib_generate_array.cc",
        "functions_testlib_interval.cc",
        "functions_testlib_json.cc",
        "functions_testlib_json_extract_array.cc",
        "functions_testlib_like.cc",
        "functions_testlib_math.cc",
        "functions_testlib_net.cc",
        "functions_testlib_numeric.cc",
        "functions_testlib_regex.cc",
        "functions_testlib_string_1.cc",
        "functions_testlib_string_2.cc",
        "functions_testlib_string_3.cc",
        "functions_testlib_string_4.cc",
        "functions_testlib_string_5.cc",
        "functions_testlib_timestamp.cc",
        "functions_testlib_tojson.cc",
    ],
    hdrs = [
        "functions_testlib.h",
        "functions_testlib_common.h",
    ],
    copts = [
        "$(STACK_FRAME_UNLIMITED)",
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
        "//zetasql/base:status",
        "//zetasql/base:stl_util",
        "//zetasql/base:strings",
        "//zetasql/common:float_margin",
        "//zetasql/common:status_payload_utils",
        "//zetasql/common/testing:testing_proto_util",
        "//zetasql/public:civil_time",
        "//zetasql/public:interval_value",
        "//zetasql/public:interval_value_test_util",
        "//zetasql/public:json_value",
        "//zetasql/public:numeric_value",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public/functions:common_proto",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/public/functions:datetime_cc_proto",
        "//zetasql/public/functions:parse_date_time",
        "//zetasql/public/types",
        "//zetasql/testdata:test_schema_cc_proto",
        "//zetasql/testing:test_function",
        "//zetasql/testing:test_value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
        "@com_googleapis_googleapis//:date_cc_proto",
        "@com_googleapis_googleapis//:latlng_cc_proto",
        "@com_googleapis_googleapis//:timeofday_cc_proto",
        "@com_googlesource_code_re2//:re2",
    ],
)

proto_library(
    name = "known_error_proto",
    srcs = ["known_error.proto"],
)

cc_proto_library(
    name = "known_error_cc_proto",
    deps = [":known_error_proto"],
)

cc_library(
    name = "legal_runtime_errors",
    testonly = 1,
    srcs = ["legal_runtime_errors.cc"],
    hdrs = ["legal_runtime_errors.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":matchers",
        "//zetasql/base:status",
        "@com_google_absl//absl/memory",
    ],
)
