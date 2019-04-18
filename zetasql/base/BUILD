#
# Copyright 2018 ZetaSQL Authors
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
    default_visibility = ["//zetasql:__subpackages__"],
)

package_group(
    name = "open_source_only",
    packages = [
        "//zetasql/...",
        "//zetasql/common",
    ],
)

cc_library(
    name = "base",
    hdrs = [
        "arena.h",
        "arena_allocator.h",
        "atomic_sequence_num.h",
        "logging.h",
        "varsetter.h",
    ],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":arena",
        ":logging",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "logging",
    srcs = ["logging.cc"],
    hdrs = ["logging.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "arena",
    srcs = ["arena.cc"],
    hdrs = [
        "arena.h",
        "arena_allocator.h",
    ],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":logging",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "arena_test",
    srcs = ["arena_unittest.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":arena",
        ":logging",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:node_hash_set",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "arena_leakage_unittest",
    size = "small",
    srcs = ["arena_leakage_unittest.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":arena",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "source_location",
    hdrs = ["source_location.h"],
    copts = ["-Wno-sign-compare"],
)

cc_test(
    name = "source_location_test",
    srcs = ["source_location_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":source_location",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "endian",
    hdrs = [
        "endian.h",
        "unaligned_access.h",
    ],
    copts = ["-Wno-sign-compare"],
    deps = [
        "@com_google_absl//absl/base:config",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/numeric:int128",
    ],
)

cc_test(
    name = "endian_test",
    srcs = ["endian_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":endian",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/base:config",
    ],
)

cc_library(
    name = "status",
    srcs = [
        "canonical_errors.cc",
        "status.cc",
        "status_builder.cc",
        "status_payload.cc",
        "statusor.cc",
    ],
    hdrs = [
        "canonical_errors.h",
        "status.h",
        "status_builder.h",
        "status_macros.h",
        "status_payload.h",
        "statusor.h",
        "statusor_internals.h",
    ],
    copts = ["-Wno-sign-compare"],
    visibility = ["//visibility:public"],
    deps = [
        ":logging",
        ":map_util",
        ":source_location",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:node_hash_map",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_test(
    name = "status_test",
    srcs = ["status_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "status_payload_test",
    srcs = ["status_payload_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":status",
        ":test_payload_cc_proto",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "enum_utils",
    hdrs = ["enum_utils.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "@com_google_protobuf//:cc_wkt_protos",
    ],
)

cc_test(
    name = "enum_utils_test",
    srcs = [
        "enum_utils_test.cc",
    ],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":enum_utils",
        ":test_payload_cc_proto",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "ret_check",
    srcs = ["ret_check.cc"],
    hdrs = [
        "ret_check.h",
    ],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":logging",
        ":source_location",
        ":status",
    ],
)

cc_test(
    name = "status_macros_test",
    srcs = ["status_macros_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":source_location",
        ":status",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "map_util",
    hdrs = [
        "map_traits.h",
        "map_util.h",
        "no_destructor.h",
    ],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":logging",
        "@com_google_absl//absl/meta:type_traits",
    ],
)

cc_test(
    name = "map_util_unittest",
    srcs = [
        "map_util_test.h",
        "map_util_unittest.cc",
    ],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":logging",
        ":map_util",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:node_hash_map",
    ],
)

cc_library(
    name = "stl_util",
    hdrs = [
        "stl_util.h",
    ],
    copts = ["-Wno-sign-compare"],
)

cc_test(
    name = "stl_util_test",
    srcs = [
        "stl_util_test.cc",
    ],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":stl_util",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_test(
    name = "no_destructor_test",
    size = "small",
    srcs = ["no_destructor_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":logging",
        ":map_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "map_traits_test",
    srcs = ["map_traits_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":map_util",
        "@com_google_googletest//:gtest_main",
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
    copts = ["-Wno-sign-compare"],
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
    copts = ["-Wno-sign-compare"],
    deps = [
        ":logging",
        ":mathutil",
        "@com_google_googletest//:gtest_main",
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
    copts = ["-Wno-sign-compare"],
)

cc_test(
    name = "mathlimits_test",
    srcs = ["mathlimits_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":logging",
        ":mathlimits",
        ":mathutil",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "bits",
    srcs = ["bits.cc"],
    hdrs = [
        "bits.h",
    ],
    copts = ["-Wno-sign-compare"],
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
    copts = ["-Wno-sign-compare"],
    deps = [
        ":bits",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/numeric:int128",
    ],
)

cc_test(
    name = "status_builder_test",
    srcs = ["status_builder_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":source_location",
        ":status",
        ":test_payload_cc_proto",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base/testing:status_matchers",
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

cc_test(
    name = "statusor_test",
    size = "small",
    srcs = ["statusor_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":source_location",
        ":status",
        "@com_google_googletest//:gtest_main",
        "@com_google_googletest//:gtest",
        "//zetasql/base/testing:status_matchers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "case",
    srcs = ["case.cc"],
    hdrs = ["case.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "case_test",
    srcs = ["case_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":case",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "path",
    srcs = ["path.cc"],
    hdrs = ["path.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "path_test",
    srcs = ["path_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":path",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "cleanup",
    hdrs = ["cleanup.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_test(
    name = "cleanup_test",
    srcs = ["cleanup_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":cleanup",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "time_proto_util",
    srcs = ["time_proto_util.cc"],
    hdrs = ["time_proto_util.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "time_proto_util_test",
    srcs = ["time_proto_util_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":time_proto_util",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base/testing:status_matchers",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "string_numbers",
    srcs = ["string_numbers.cc"],
    hdrs = ["string_numbers.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "string_numbers_test",
    srcs = ["string_numbers_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":string_numbers",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "bind_front",
    srcs = [
        "compressed_tuple.h",
        "front_binder.h",
        "invoke.h",
    ],
    hdrs = [
        "bind_front.h",
    ],
    copts = ["-Wno-sign-compare"],
    deps = [
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/utility",
    ],
)

cc_test(
    name = "bind_front_test",
    srcs = ["bind_front_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":bind_front",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/memory",
    ],
)

cc_library(
    name = "refcount",
    hdrs = [
        "simple_reference_counted.h",
    ],
    copts = ["-Wno-sign-compare"],
)

cc_test(
    name = "simple_reference_counted_test",
    srcs = ["simple_reference_counted_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":refcount",
        "@com_google_googletest//:gtest_main",
    ],
)
