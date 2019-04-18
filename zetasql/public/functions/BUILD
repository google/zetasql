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

package(default_visibility = ["//zetasql:__subpackages__"])

cc_library(
    name = "util",
    srcs = ["util.cc"],
    hdrs = ["util.h"],
    copts = ["-Wno-sign-compare"],
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
    copts = ["-Wno-sign-compare"],
    deps = [
        ":util",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "convert_internal",
    hdrs = ["convert_internal.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "//zetasql/base",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_test(
    name = "convert_internal_test",
    size = "small",
    srcs = ["convert_internal_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":convert_internal",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "convert",
    srcs = ["convert.cc"],
    hdrs = ["convert.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":convert_internal",
        ":util",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/public:numeric_value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "convert_test",
    size = "small",
    srcs = ["convert_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":convert",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "convert_proto",
    srcs = ["convert_proto.cc"],
    hdrs = ["convert_proto.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "convert_proto_test",
    size = "small",
    srcs = ["convert_proto_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":convert_proto",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/testdata:test_schema_cc_proto",
    ],
)

cc_library(
    name = "convert_string",
    srcs = ["convert_string.cc"],
    hdrs = ["convert_string.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":util",
        "//zetasql/base:case",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/base:string_numbers",
        "//zetasql/common:string_util",
        "//zetasql/public:numeric_value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "convert_string_test",
    size = "small",
    srcs = ["convert_string_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":convert_string",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "arithmetics_internal",
    hdrs = ["arithmetics_internal.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "//zetasql/base",
    ],
)

cc_test(
    name = "arithmetics_internal_test",
    size = "small",
    srcs = ["arithmetics_internal_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":arithmetics_internal",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "arithmetics",
    hdrs = ["arithmetics.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":arithmetics_internal",
        ":convert",
        ":util",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/public:numeric_value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "comparison",
    hdrs = ["comparison.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "date_time_util_internal",
    srcs = ["date_time_util_internal.cc"],
    hdrs = ["date_time_util_internal.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "//zetasql/base",
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "date_time_util_internal_test",
    size = "small",
    srcs = ["date_time_util_internal_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":date_time_util_internal",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "date_time_util",
    srcs = ["date_time_util.cc"],
    hdrs = ["date_time_util.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":arithmetics",
        ":date_time_util_internal",
        ":datetime_cc_proto",
        "//zetasql/base",
        "//zetasql/base:mathutil",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/base:time_proto_util",
        "//zetasql/common:errors",
        "//zetasql/public:civil_time",
        "//zetasql/public:type",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
        "@com_googleapis_googleapis//:date_cc_proto",
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
