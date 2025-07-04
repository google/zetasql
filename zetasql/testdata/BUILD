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

load("@com_google_protobuf//bazel:java_proto_library.bzl", "java_proto_library")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")

package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

exports_files(["external_extension.proto"])

filegroup(
    name = "test_files",
    srcs = [
        "bad_extension_schema.proto",
        "bad_file_dependency.proto",
        "bad_parse_schema.proto",
        "bad_parse_schema_simple.proto",
        "bad_reference.proto",
        "bad_test_schema.proto",
        "external_extension.proto",
        "keyword_in_package_name.proto",
        "recursive_schema.proto",
        "referenced_schema.proto",
        "referencing_schema.proto",
        "test_proto3.proto",
        "test_schema.proto",
    ],
)

proto_library(
    name = "bad_test_schema_proto",
    testonly = 1,
    srcs = [
        "bad_extension_schema.proto",
        "bad_test_schema.proto",
    ],
    deps = ["//zetasql/public/proto:type_annotation_proto"],
)

cc_proto_library(
    name = "bad_test_schema_cc_proto",
    testonly = 1,
    deps = [":bad_test_schema_proto"],
)

java_proto_library(
    name = "bad_test_schema_java_proto",
    testonly = 1,
    deps = [":bad_test_schema_proto"],
)

proto_library(
    name = "test_schema_proto",
    srcs = [
        "referenced_schema.proto",
        "referencing_schema.proto",
        "test_schema.proto",
    ],
    features = ["-proto_dynamic_mode_static_link"],
    deps = [
        "//zetasql/public/proto:type_annotation_proto",
        "@com_google_protobuf//:descriptor_proto",
    ],
)

cc_proto_library(
    name = "test_schema_cc_proto",
    deps = [":test_schema_proto"],
)

java_proto_library(
    name = "test_schema_java_proto",
    deps = [":test_schema_proto"],
)

proto_library(
    name = "test_proto3_proto",
    srcs = [
        "test_proto3.proto",
    ],
    deps = [
        ":test_schema_proto",
        "//zetasql/public/proto:type_annotation_proto",
        "@com_google_protobuf//:any_proto",
        "@com_google_protobuf//:api_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:empty_proto",
        "@com_google_protobuf//:field_mask_proto",
        "@com_google_protobuf//:source_context_proto",
        "@com_google_protobuf//:struct_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@com_google_protobuf//:wrappers_proto",
    ],
)

cc_proto_library(
    name = "test_proto3_cc_proto",
    deps = [":test_proto3_proto"],
)

java_proto_library(
    name = "test_proto3_java_proto",
    deps = [":test_proto3_proto"],
)

proto_library(
    name = "test_packageless_proto",
    srcs = [
        "packageless.proto",
    ],
    deps = [":test_schema_proto"],
)

cc_proto_library(
    name = "test_packageless_cc_proto",
    deps = [":test_packageless_proto"],
)

java_proto_library(
    name = "test_packageless_java_proto",
    testonly = 1,
    deps = [":test_packageless_proto"],
)

proto_library(
    name = "ambiguous_has_proto",
    srcs = [
        "ambiguous_has.proto",
    ],
)

cc_proto_library(
    name = "ambiguous_has_cc_proto",
    deps = [":ambiguous_has_proto"],
)

java_proto_library(
    name = "ambiguous_has_java_proto",
    testonly = 1,
    deps = [":ambiguous_has_proto"],
)

cc_library(
    name = "sample_annotation",
    srcs = ["sample_annotation.cc"],
    hdrs = ["sample_annotation.h"],
    deps = [
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:type",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "sample_system_variables",
    testonly = 1,
    srcs = ["sample_system_variables.cc"],
    hdrs = ["sample_system_variables.h"],
    deps = [
        ":test_schema_cc_proto",
        "//zetasql/base:check",
        "//zetasql/public:analyzer",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:type",
        "//zetasql/public/types",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "sample_catalog",
    testonly = 1,
    srcs = ["sample_catalog.cc"],
    hdrs = ["sample_catalog.h"],
    deps = [
        ":sample_catalog_impl",
        "//zetasql/base:check",
        "//zetasql/public:builtin_function_options",
        "//zetasql/public:function",
        "//zetasql/public:language_options",
        "//zetasql/public:type",
        "//zetasql/public/types",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "sample_catalog_impl",
    srcs = ["sample_catalog_impl.cc"],
    hdrs = ["sample_catalog_impl.h"],
    deps = [
        ":ambiguous_has_cc_proto",
        ":sample_annotation",
        ":test_packageless_cc_proto",
        ":test_proto3_cc_proto",
        ":test_schema_cc_proto",
        "//zetasql/base",
        "//zetasql/base:check",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:internal_property_graph",
        "//zetasql/common:measure_analysis_utils",
        "//zetasql/public:analyzer",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:anon_function",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:builtin_function_options",
        "//zetasql/public:catalog",
        "//zetasql/public:cycle_detector",
        "//zetasql/public:deprecation_warning_cc_proto",
        "//zetasql/public:error_location_cc_proto",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:function",
        "//zetasql/public:function_cc_proto",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_resume_location",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:simple_catalog_util",
        "//zetasql/public:sql_function",
        "//zetasql/public:sql_tvf",
        "//zetasql/public:strings",
        "//zetasql/public:templated_sql_function",
        "//zetasql/public:templated_sql_tvf",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public/annotation:collation",
        "//zetasql/public/annotation:timestamp_precision",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_ast_enums_cc_proto",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:node_hash_map",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "sample_catalog_test",
    srcs = ["sample_catalog_test.cc"],
    deps = [
        ":sample_catalog",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:function_headers",
        "//zetasql/public:language_options",
        "//zetasql/public:sql_view",
        "//zetasql/public/types",
        "//zetasql/resolved_ast:resolved_ast_enums_cc_proto",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "sample_catalog_property_graph_test",
    srcs = ["sample_catalog_property_graph_test.cc"],
    deps = [
        ":sample_catalog",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:catalog",
        "//zetasql/public:language_options",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public/types",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "error_catalog",
    testonly = 1,
    srcs = ["error_catalog.cc"],
    hdrs = ["error_catalog.h"],
    deps = [
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:catalog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "special_catalog",
    testonly = 1,
    srcs = ["special_catalog.cc"],
    hdrs = ["special_catalog.h"],
    deps = [
        ":test_schema_cc_proto",
        "//zetasql/base:check",
        "//zetasql/base:map_util",
        "//zetasql/public:catalog",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:type",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "populate_sample_tables",
    testonly = 1,
    srcs = ["populate_sample_tables.cc"],
    hdrs = ["populate_sample_tables.h"],
    deps = [
        ":sample_catalog",
        ":test_schema_cc_proto",
        "//zetasql/base:status",
        "//zetasql/public:catalog",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "//zetasql/reference_impl:evaluation",
        "//zetasql/testing:test_value",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
    ],
)
