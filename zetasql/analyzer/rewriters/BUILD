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
    name = "rewriter_relevance_checker",
    srcs = ["rewriter_relevance_checker.cc"],
    hdrs = ["rewriter_relevance_checker.h"],
    deps = [
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:sql_function",
        "//zetasql/public:sql_tvf",
        "//zetasql/public:sql_view",
        "//zetasql/public:templated_sql_function",
        "//zetasql/public:templated_sql_tvf",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "flatten_rewriter",
    srcs = ["flatten_rewriter.cc"],
    hdrs = ["flatten_rewriter.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:catalog",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_ast_enums_cc_proto",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "with_expr_rewriter",
    srcs = ["with_expr_rewriter.cc"],
    hdrs = ["with_expr_rewriter.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:catalog",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "sql_function_inliner",
    srcs = ["sql_function_inliner.cc"],
    hdrs = ["sql_function_inliner.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:varsetter",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:sql_function",
        "//zetasql/public:sql_tvf",
        "//zetasql/public:templated_sql_function",
        "//zetasql/public:templated_sql_tvf",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_ast_builder",
        "//zetasql/resolved_ast:resolved_ast_enums_cc_proto",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "sql_view_inliner",
    srcs = ["sql_view_inliner.cc"],
    hdrs = ["sql_view_inliner.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:catalog",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:sql_view",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "map_function_rewriter",
    srcs = ["map_function_rewriter.cc"],
    hdrs = ["map_function_rewriter.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/analyzer:substitute",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "array_functions_rewriter",
    srcs = ["array_functions_rewriter.cc"],
    hdrs = ["array_functions_rewriter.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/analyzer:substitute",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:builtin_function",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "typeof_function_rewriter",
    srcs = ["typeof_function_rewriter.cc"],
    hdrs = ["typeof_function_rewriter.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "unpivot_rewriter",
    srcs = ["unpivot_rewriter.cc"],
    hdrs = ["unpivot_rewriter.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:id_string",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "rewriter_interface",
    hdrs = ["rewriter_interface.h"],
    deps = ["@com_google_absl//absl/status:statusor"],
)

cc_library(
    name = "pivot_rewriter",
    srcs = ["pivot_rewriter.cc"],
    hdrs = ["pivot_rewriter.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/analyzer:resolver",
        "//zetasql/analyzer:substitute",
        "//zetasql/base:logging",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:aggregate_null_handling",
        "//zetasql/common:errors",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
        "//zetasql/public:value",
        "//zetasql/public/annotation:collation",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_ast_enums_cc_proto",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "nulliferror_function_rewriter",
    srcs = ["nulliferror_function_rewriter.cc"],
    hdrs = ["nulliferror_function_rewriter.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_ast_builder",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "registration",
    srcs = ["registration.cc"],
    hdrs = ["registration.h"],
    deps = [
        "//zetasql/base:logging",
        "//zetasql/public:options_cc_proto",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "like_any_all_rewriter",
    srcs = ["like_any_all_rewriter.cc"],
    hdrs = ["like_any_all_rewriter.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/analyzer:substitute",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "unary_function_rewriter",
    srcs = ["unary_function_rewriter.cc"],
    hdrs = ["unary_function_rewriter.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/analyzer:substitute",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:function_headers",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "ternary_function_rewriter",
    srcs = ["ternary_function_rewriter.cc"],
    hdrs = ["ternary_function_rewriter.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/analyzer:substitute",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:function_headers",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "binary_function_rewriter",
    srcs = ["binary_function_rewriter.cc"],
    hdrs = ["binary_function_rewriter.h"],
    deps = [
        ":rewriter_interface",
        "//zetasql/analyzer:substitute",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:function_headers",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "registration_test",
    srcs = ["registration_test.cc"],
    deps = [
        ":registration",
        ":rewriter_interface",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:options_cc_proto",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
    ],
)
