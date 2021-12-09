#
# Copyright 2018 Google LLC
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

licenses(["notice"])  # Apache v2.0

package(
    default_visibility = [":zetasql_implementation"],
)

package_group(
    name = "zetasql_implementation",
    packages = [
        "//java/...",
        "//javatests/...",
        "//zetasql/...",
    ],
)

cc_library(
    name = "base",
    hdrs = [
        "atomic_sequence_num.h",
        "logging.h",
        "varsetter.h",
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
        ":logging",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:log_severity",
    ],
)

cc_library(
    name = "varsetter",
    hdrs = [
        "varsetter.h",
    ],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
)

cc_library(
    name = "logging",
    srcs = ["logging.cc"],
    hdrs = ["logging.h"],
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
        "@com_google_absl//absl/base:log_severity",
    ],
)

cc_library(
    name = "arena",
    srcs = ["arena.cc"],
    hdrs = [
        "arena.h",
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
        ":logging",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "arena_allocator",
    srcs = ["arena_allocator.cc"],
    hdrs = [
        "arena_allocator.h",
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
        ":arena",
        ":logging",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_test(
    name = "arena_test",
    srcs = ["arena_unittest.cc"],
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
        ":arena",
        ":arena_allocator",
        ":logging",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/container:node_hash_set",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "arena_leakage_unittest",
    size = "small",
    srcs = ["arena_leakage_unittest.cc"],
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
        ":arena",
        "//zetasql/base/testing:zetasql_gtest_main",
    ],
)

cc_library(
    name = "source_location",
    hdrs = ["source_location.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = ["@com_google_absl//absl/base:config"],
)

cc_test(
    name = "source_location_test",
    srcs = ["source_location_test.cc"],
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
        ":source_location",
        "//zetasql/base/testing:zetasql_gtest_main",
    ],
)

cc_library(
    name = "endian",
    hdrs = [
        "endian.h",
        "unaligned_access.h",
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
        "@com_google_absl//absl/base:config",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/numeric:int128",
    ],
)

cc_test(
    name = "endian_test",
    srcs = ["endian_test.cc"],
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
        ":endian",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base:config",
    ],
)

cc_library(
    name = "status",
    srcs = [
        "status_builder.cc",
        "status_payload.cc",
    ],
    hdrs = [
        "canonical_errors.h",
        "status.h",
        "status_builder.h",
        "status_macros.h",
        "status_payload.h",
    ],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":logging",
        ":source_location",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:log_severity",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_test(
    name = "status_test",
    srcs = ["status_test.cc"],
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
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "status_payload_test",
    srcs = ["status_payload_test.cc"],
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
        ":status",
        ":test_payload_cc_proto",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "enum_utils",
    hdrs = ["enum_utils.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "@com_google_protobuf//:cc_wkt_protos",
    ],
)

cc_test(
    name = "enum_utils_test",
    srcs = [
        "enum_utils_test.cc",
    ],
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
        ":enum_utils",
        ":test_payload_cc_proto",
        "//zetasql/base/testing:zetasql_gtest_main",
    ],
)

cc_library(
    name = "ret_check",
    srcs = ["ret_check.cc"],
    hdrs = [
        "ret_check.h",
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
        ":logging",
        ":source_location",
        ":status",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "status_macros_test",
    srcs = ["status_macros_test.cc"],
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
        ":source_location",
        ":status",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "no_destructor",
    hdrs = ["no_destructor.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
)

cc_library(
    name = "map_util",
    hdrs = [
        "map_traits.h",
        "map_util.h",
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
        ":logging",
        ":no_destructor",
        "@com_google_absl//absl/meta:type_traits",
    ],
)

cc_test(
    name = "map_util_unittest",
    srcs = [
        "map_util_test.h",
        "map_util_unittest.cc",
    ],
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
        ":logging",
        ":map_util",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/container:node_hash_map",
    ],
)

cc_library(
    name = "stl_util",
    hdrs = [
        "stl_util.h",
    ],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
)

cc_test(
    name = "stl_util_test",
    srcs = [
        "stl_util_test.cc",
    ],
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
        ":stl_util",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_test(
    name = "no_destructor_test",
    size = "small",
    srcs = ["no_destructor_test.cc"],
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
        ":logging",
        ":no_destructor",
        "//zetasql/base/testing:zetasql_gtest_main",
    ],
)

cc_test(
    name = "map_traits_test",
    srcs = ["map_traits_test.cc"],
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
        ":map_util",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/container:node_hash_map",
    ],
)

cc_library(
    name = "mathutil",
    srcs = [
        "mathutil.cc",
    ],
    hdrs = [
        "mathutil.h",
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
        ":bits",
        ":logging",
        ":mathlimits",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_test(
    name = "mathutil_test",
    srcs = [
        "mathutil_test.cc",
    ],
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
        ":logging",
        ":mathutil",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base",
    ],
)

cc_library(
    name = "mathlimits",
    srcs = [
        "mathlimits.cc",
    ],
    hdrs = [
        "mathlimits.h",
    ],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
)

cc_test(
    name = "mathlimits_test",
    srcs = ["mathlimits_test.cc"],
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
        ":logging",
        ":mathlimits",
        ":mathutil",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "bits",
    srcs = ["bits.cc"],
    hdrs = [
        "bits.h",
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
        ":endian",
        ":logging",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "bits_test",
    srcs = ["bits_test.cc"],
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
        ":bits",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/numeric:int128",
    ],
)

cc_test(
    name = "status_builder_test",
    srcs = ["status_builder_test.cc"],
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
        ":source_location",
        ":status",
        ":test_payload_cc_proto",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

proto_library(
    name = "test_payload_proto",
    testonly = 1,
    srcs = ["test_payload.proto"],
)

cc_proto_library(
    name = "test_payload_cc_proto",
    testonly = 1,
    deps = [":test_payload_proto"],
)

alias(
    name = "statusor",
    actual = ":status",
)

alias(
    name = "status_payload",
    actual = ":status",
)

cc_library(
    name = "case",
    srcs = ["case.cc"],
    hdrs = ["case.h"],
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
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "case_test",
    srcs = ["case_test.cc"],
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
        ":case",
        "//zetasql/base/testing:zetasql_gtest_main",
    ],
)

cc_library(
    name = "path",
    srcs = ["path.cc"],
    hdrs = ["path.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "path_test",
    srcs = ["path_test.cc"],
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
        ":path",
        "//zetasql/base/testing:zetasql_gtest_main",
    ],
)

cc_library(
    name = "time_proto_util",
    srcs = ["time_proto_util.cc"],
    hdrs = ["time_proto_util.h"],
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
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "time_proto_util_test",
    srcs = ["time_proto_util_test.cc"],
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
        ":time_proto_util",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "string_numbers",
    srcs = ["string_numbers.cc"],
    hdrs = ["string_numbers.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":logging",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "strings",
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":case",
        ":string_numbers",
    ],
)

cc_test(
    name = "string_numbers_test",
    srcs = ["string_numbers_test.cc"],
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
        ":string_numbers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "refcount",
    hdrs = [
        "simple_reference_counted.h",
    ],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
)

cc_test(
    name = "simple_reference_counted_test",
    srcs = ["simple_reference_counted_test.cc"],
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
        ":refcount",
        "//zetasql/base/testing:zetasql_gtest_main",
    ],
)

cc_library(
    name = "general_trie",
    hdrs = [
        "general_trie.h",
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
        ":base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "general_trie_unittest",
    srcs = ["general_trie_unittest.cc"],
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
        ":general_trie",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "flat_internal",
    srcs = ["flat_internal.cc"],
    hdrs = ["flat_internal.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = ["@com_google_absl//absl/meta:type_traits"],
)

cc_test(
    name = "flat_internal_test",
    srcs = [
        "flat_internal_test.cc",
    ],
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
        ":flat_internal",
        "//zetasql/base/testing:zetasql_gtest_main",
    ],
)

cc_library(
    name = "flat_set",
    hdrs = [
        "flat_set.h",
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
        ":flat_internal",
        "@com_google_absl//absl/meta:type_traits",
    ],
)

cc_test(
    name = "flat_set_test",
    srcs = [
        "flat_set_test.cc",
    ],
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
        ":base",
        ":flat_set",
        ":stl_util",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "clock",
    srcs = [
        "clock.cc",
    ],
    hdrs = [
        "clock.h",
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
        ":base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "clock_test",
    srcs = [
        "clock_test.cc",
    ],
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
        ":clock",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "exactfloat",
    srcs = [
        "exactfloat.cc",
    ],
    hdrs = [
        "exactfloat.h",
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
        ":base",
        "@boringssl//:crypto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:fixed_array",
    ],
)

cc_test(
    name = "exactfloat_test",
    srcs = [
        "exactfloat_test.cc",
    ],
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
        ":base",
        ":exactfloat",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_test(
    name = "exactfloat_underflow_test",
    timeout = "eternal",  # This test does take a long time.
    srcs = [
        "exactfloat_underflow_test.cc",
    ],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    shard_count = 4,
    deps = [
        ":base",
        ":exactfloat",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "file_util",
    hdrs = ["file_util.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":file_util_oss",
    ],
)

cc_library(
    name = "file_util_oss",
    hdrs = ["file_util_oss.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":logging",
        ":path",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "file_util_oss_test",
    srcs = ["file_util_oss_test.cc"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    data = ["file_util_oss_test.input_file"],
    deps = [
        ":file_util_oss",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "edit_distance",
    hdrs = ["edit_distance.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = ["@com_google_absl//absl/container:fixed_array"],
)

cc_test(
    name = "edit_distance_test",
    srcs = ["edit_distance_test.cc"],
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
        ":edit_distance",
        ":logging",
        "//zetasql/base/testing:zetasql_gtest_main",
    ],
)
