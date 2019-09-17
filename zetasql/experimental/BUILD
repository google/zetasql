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

# This directory contains temporary and experimental code that won't be
# part of the real ZetaSQL library.

package(
    default_visibility = ["//:__subpackages__"],
)

cc_library(
    name = "output_query_result",
    testonly = 1,
    srcs = ["output_query_result.cc"],
    hdrs = ["output_query_result.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "//zetasql/compliance:type_helpers",
        "//zetasql/public:strings",
        "//zetasql/public:value",
        "//zetasql/reference_impl:algebrizer",
        "//zetasql/reference_impl:evaluation",
        "//zetasql/resolved_ast",
    ],
)

# Examples:
#
###############################################
# Basic
###############################################
#   bazel run //zetasql/experimental:execute_query -- "SELECT 1"
#
cc_binary(
    name = "execute_query",
    testonly = 1,
    srcs = [
        "execute_query.cc",
    ],
    deps = [
        ":output_query_result",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/public:analyzer",
        "//zetasql/public:catalog",
        "//zetasql/public:evaluator",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:language_options",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
    ],
)
