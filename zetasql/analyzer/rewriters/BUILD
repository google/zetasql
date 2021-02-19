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

# Contains rewriters for zetasql analyzers.

package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

cc_library(
    name = "flatten_rewriter",
    srcs = ["flatten_rewriter.cc"],
    hdrs = ["flatten_rewriter.h"],
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
        ":rewriter_interface",
        "//zetasql/parser",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:function",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:rewrite_utils",
        "//zetasql/resolved_ast:validator",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "map_function_rewriter",
    srcs = ["map_function_rewriter.cc"],
    hdrs = ["map_function_rewriter.h"],
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
        ":rewriter_interface",
        "//zetasql/analyzer:substitute",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:value",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:make_node_vector",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "array_functions_rewriter",
    srcs = ["array_functions_rewriter.cc"],
    hdrs = ["array_functions_rewriter.h"],
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
        ":rewriter_interface",
        "//zetasql/analyzer:substitute",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/parser",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:rewrite_utils",
        "//zetasql/resolved_ast:validator",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "unpivot_rewriter",
    srcs = ["unpivot_rewriter.cc"],
    hdrs = ["unpivot_rewriter.h"],
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
        ":rewriter_interface",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "rewriter_interface",
    hdrs = ["rewriter_interface.h"],
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
        "//zetasql/base:statusor",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public/types",
        "@com_google_absl//absl/synchronization",
    ],
)
