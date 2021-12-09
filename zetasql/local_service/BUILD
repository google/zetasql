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

load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("@io_grpc_grpc_java//:java_grpc_library.bzl", "java_grpc_library")

# ZetaSQL Server
package(default_visibility = ["//zetasql/base:zetasql_implementation"])

cc_library(
    name = "local_service",
    srcs = ["local_service.cc"],
    hdrs = [
        "local_service.h",
        "state.h",
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
        ":local_service_cc_proto",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:proto_helper",
        "//zetasql/parser:parse_tree_cc_proto",
        "//zetasql/parser:parse_tree_serializer",
        "//zetasql/proto:options_cc_proto",
        "//zetasql/proto:simple_catalog_cc_proto",
        "//zetasql/public:analyzer",
        "//zetasql/public:builtin_function",
        "//zetasql/public:evaluator",
        "//zetasql/public:function",
        "//zetasql/public:id_string",
        "//zetasql/public:language_options",
        "//zetasql/public:parse_resume_location",
        "//zetasql/public:parse_resume_location_cc_proto",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:simple_table_cc_proto",
        "//zetasql/public:sql_formatter",
        "//zetasql/public:templated_sql_tvf",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public:value_cc_proto",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:sql_builder",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "local_service_test",
    srcs = ["local_service_test.cc"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    data = [
        "//zetasql/testdata:test_schema_proto",
    ],
    tags = ["requires-net:loopback"],
    deps = [
        ":local_service",
        "//zetasql/base",
        "//zetasql/base:path",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common:status_payload_utils",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/common/testing:testing_proto_util",
        "//zetasql/proto:function_cc_proto",
        "//zetasql/proto:simple_catalog_cc_proto",
        "//zetasql/public:parse_resume_location_cc_proto",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:simple_table_cc_proto",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public:value_cc_proto",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/resolved_ast:resolved_ast_cc_proto",
        "//zetasql/testdata:test_proto3_cc_proto",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "local_service_benchmark",
    srcs = ["local_service_benchmark.cc"],
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
        ":local_service",
        ":local_service_cc_proto",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "@com_github_google_benchmark//:benchmark_main",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/status",
    ],
)

proto_library(
    name = "local_service_proto",
    srcs = ["local_service.proto"],
    deps = [
        "//zetasql/parser:parse_tree_proto",
        "//zetasql/proto:function_proto",
        "//zetasql/proto:options_proto",
        "//zetasql/proto:simple_catalog_proto",
        "//zetasql/public:options_proto",
        "//zetasql/public:parse_resume_location_proto",
        "//zetasql/public:simple_table_proto",
        "//zetasql/public:type_proto",
        "//zetasql/public:value_proto",
        "//zetasql/resolved_ast:resolved_ast_proto",
        "@com_google_protobuf//:descriptor_proto",
        "@com_google_protobuf//:empty_proto",
    ],
)

cc_proto_library(
    name = "local_service_cc_proto",
    deps = [":local_service_proto"],
)

cc_grpc_library(
    name = "local_service_cc_grpc",
    srcs = [":local_service_proto"],
    grpc_only = True,
    deps = [":local_service_cc_proto"],
)

cc_library(
    name = "local_service_grpc",
    srcs = ["local_service_grpc.cc"],
    hdrs = ["local_service_grpc.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":local_service",
        ":local_service_cc_grpc",
        ":local_service_cc_proto",
        "//zetasql/base:status",
        "//zetasql/proto:options_cc_proto",
        "//zetasql/public:parse_resume_location_cc_proto",
        "//zetasql/public:simple_table_cc_proto",
    ],
)

cc_test(
    name = "local_service_grpc_test",
    srcs = ["local_service_grpc_test.cc"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    tags = ["requires-net:loopback"],
    deps = [
        ":local_service",
        ":local_service_cc_grpc",
        ":local_service_cc_proto",
        ":local_service_grpc",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/proto:options_cc_proto",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

java_proto_library(
    name = "local_service_java_proto",
    deps = [":local_service_proto"],
)

java_grpc_library(
    name = "local_service_java_grpc",
    srcs = [":local_service_proto"],
    deps = [
        ":local_service_java_proto",
        "//zetasql/proto:options_java_proto",
        "//zetasql/public:parse_resume_location_java_proto",
        "//zetasql/public:simple_table_java_proto",
    ],
)

cc_library(
    name = "local_service_jni",
    srcs = ["local_service_jni.cc"],
    hdrs = ["local_service_jni.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    linkstatic = 1,
    deps = [
        ":local_service_grpc",
        "//zetasql/jdk:jni",
        "@com_github_grpc_grpc//:grpc++",
    ],
    alwayslink = 1,
)

cc_binary(
    name = "liblocal_service_jni.so",
    linkshared = 1,
    deps = [":local_service_jni"],
)

cc_binary(
    name = "liblocal_service_jni.dylib",
    linkshared = 1,
    deps = [":local_service_jni"],
)
