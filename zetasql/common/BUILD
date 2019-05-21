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

package(
    default_visibility = ["//zetasql:__subpackages__"],
    features = ["parse_headers"],
)

cc_library(
    name = "status_payload_utils",
    srcs = ["status_payload_utils.cc"],
    hdrs = ["status_payload_utils.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "//zetasql/base:status",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "status_payload_utils_test",
    srcs = ["status_payload_utils_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":status_payload_utils",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:status",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/testdata:test_schema_cc_proto",
    ],
)

cc_library(
    name = "errors",
    srcs = ["errors.cc"],
    hdrs = ["errors.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":status_payload_utils",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:deprecation_warning_cc_proto",
        "//zetasql/public:error_helpers",
        "//zetasql/public:error_location_cc_proto",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "errors_test",
    size = "small",
    srcs = ["errors_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":errors",
        ":status_payload_utils",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/public:error_helpers",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "string_util",
    hdrs = ["string_util.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "utf_util",
    srcs = ["utf_util.cc"],
    hdrs = ["utf_util.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "//zetasql/base",
        "@com_google_absl//absl/strings",
        "@icu//:headers",
    ],
)

cc_test(
    name = "utf_util_test",
    size = "small",
    srcs = ["utf_util_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":utf_util",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "proto_helper",
    srcs = ["proto_helper.cc"],
    hdrs = ["proto_helper.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":errors",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "proto_helper_test",
    size = "small",
    srcs = ["proto_helper_test.cc"],
    copts = ["-Wno-sign-compare"],
    data = [
        "//zetasql/testdata:test_files",
        "//zetasql/testdata:test_schema_proto",
    ],
    deps = [
        ":proto_helper",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:path",
        "//zetasql/base/testing:status_matchers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "float_margin",
    hdrs = [
        "float_margin.h",
    ],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":string_util",
        "//zetasql/base",
        "//zetasql/base:mathutil",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "internal_value",
    hdrs = [
        "internal_value.h",
    ],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":float_margin",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "@com_google_absl//absl/types:span",
    ],
)
