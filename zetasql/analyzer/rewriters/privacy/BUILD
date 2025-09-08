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

load("@rules_cc//cc:cc_library.bzl", "cc_library")

# Contains helper classes for privacy rewriters.
package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

cc_library(
    name = "privacy_utility",
    srcs = ["privacy_utility.cc"],
    hdrs = ["privacy_utility.h"],
    deps = [
        "//zetasql/analyzer:name_scope",
        "//zetasql/analyzer:resolver",
        "//zetasql/base:check",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/parser:parse_tree",
        "//zetasql/proto:anon_output_with_report_cc_proto",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:function_cc_proto",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public/functions:differential_privacy_cc_proto",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_ast_enums_cc_proto",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "approx_count_distinct_utility",
    srcs = ["approx_count_distinct_utility.cc"],
    hdrs = ["approx_count_distinct_utility.h"],
    deps = [
        ":privacy_utility",
        "//zetasql/analyzer:resolver",
        "//zetasql/base:check",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/parser:parse_tree",
        "//zetasql/proto:anon_output_with_report_cc_proto",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:function_cc_proto",
        "//zetasql/public:function_headers",
        "//zetasql/public:id_string",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:rewriter_interface",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public/functions:differential_privacy_cc_proto",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:column_factory",
        "//zetasql/resolved_ast:comparator",
        "//zetasql/resolved_ast:resolved_ast_enums_cc_proto",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "//zetasql/resolved_ast:rewrite_utils",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)
