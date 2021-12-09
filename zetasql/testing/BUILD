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
    name = "sql_types_test",
    testonly = 1,
    srcs = ["sql_types_test.cc"],
    hdrs = ["sql_types_test.h"],
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
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:civil_time",
        "//zetasql/public:coercer",
        "//zetasql/public:function",
        "//zetasql/public:language_options",
        "//zetasql/public:numeric_value",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "test_catalog",
    testonly = 1,
    srcs = ["test_catalog.cc"],
    hdrs = ["test_catalog.h"],
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
        "//zetasql/base:strings",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:type",
        "@com_google_absl//absl/container:node_hash_map",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "test_catalog_test",
    size = "small",
    srcs = ["test_catalog_test.cc"],
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
        ":test_catalog",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:function",
        "//zetasql/public:type",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "test_value",
    testonly = 1,
    srcs = ["test_value.cc"],
    hdrs = [
        "test_value.h",
        "using_test_value.cc",
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
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/common:float_margin",
        "//zetasql/common:internal_value",
        "//zetasql/compliance:test_driver",
        "//zetasql/public:numeric_value",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "test_function",
    testonly = 1,
    srcs = ["test_function.cc"],
    hdrs = ["test_function.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":test_value",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:status",
        "//zetasql/common:float_margin",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "type_util",
    testonly = 1,
    srcs = ["type_util.cc"],
    hdrs = ["type_util.h"],
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
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public/types",
        "//zetasql/testdata:test_proto3_cc_proto",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
        "@com_googleapis_googleapis//:date_cc_proto",
        "@com_googleapis_googleapis//:latlng_cc_proto",
        "@com_googleapis_googleapis//:timeofday_cc_proto",
    ],
)

cc_test(
    name = "type_util_test",
    size = "small",
    srcs = ["type_util_test.cc"],
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
        ":sql_types_test",
        ":type_util",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:type",
    ],
)
