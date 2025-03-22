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

load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")

package(
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "compiled_pattern",
    srcs = ["compiled_pattern.cc"],
    hdrs = ["compiled_pattern.h"],
    deps = [
        ":compiled_pattern_cc_proto",
        ":match_partition",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common/match_recognize:compiled_nfa",
        "//zetasql/common/match_recognize:nfa",
        "//zetasql/common/match_recognize:nfa_builder",
        "//zetasql/common/match_recognize:nfa_match_partition",
        "//zetasql/public:value",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "match_partition",
    hdrs = ["match_partition.h"],
    deps = [
        "//zetasql/public:value",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
    ],
)

proto_library(
    name = "compiled_pattern_proto",
    srcs = ["compiled_pattern.proto"],
    deps = ["//zetasql/resolved_ast:resolved_ast_enums_proto"],
)

cc_proto_library(
    name = "compiled_pattern_cc_proto",
    deps = [":compiled_pattern_proto"],
)

cc_test(
    name = "compiled_pattern_test",
    srcs = ["compiled_pattern_test.cc"],
    deps = [
        ":match_partition",
        "//zetasql/base:check",
        "//zetasql/base:source_location",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/match_recognize:match_test_result_cc_proto",
        "//zetasql/common/match_recognize:test_matcher",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/public:value",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)
