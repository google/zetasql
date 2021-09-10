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

package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

cc_library(
    name = "proto_matchers",
    testonly = 1,
    hdrs = ["proto_matchers.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = ["//zetasql/base/testing:proto_matchers"],
)

cc_library(
    name = "testing_proto_util",
    testonly = 1,
    hdrs = ["testing_proto_util.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/base:path",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/strings:cord",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:protobuf",
    ],
)

proto_library(
    name = "status_payload_matchers_test_proto",
    testonly = 1,
    srcs = ["status_payload_matchers_test.proto"],
)

cc_proto_library(
    name = "status_payload_matchers_test_cc_proto",
    testonly = 1,
    deps = [":status_payload_matchers_test_proto"],
)

cc_library(
    name = "status_payload_matchers",
    testonly = 1,
    srcs = [],
    hdrs = ["status_payload_matchers.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":status_payload_matchers_oss",
    ],
)

cc_library(
    name = "status_payload_matchers_oss",
    testonly = 1,
    srcs = [],
    hdrs = ["status_payload_matchers_oss.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common:status_payload_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "status_payload_matchers_oss_test",
    size = "small",
    srcs = ["status_payload_matchers_oss_test.cc"],
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
        ":status_payload_matchers_oss",
        ":status_payload_matchers_test_cc_proto",
        "//zetasql/base/testing:proto_matchers_oss",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common:status_payload_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)
