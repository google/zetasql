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

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("//zetasql/resolved_ast:build_rules.bzl", "gen_resolved_ast_files")

package(default_visibility = ["//zetasql/base:zetasql_implementation"])

py_binary(
    name = "gen_resolved_ast",
    srcs = [
        "gen_resolved_ast.py",
    ],
    deps = [
        ":resolved_ast_enums_py_pb2",
        "@io_abseil_py//absl:app",
        "@io_abseil_py//absl/flags",
        "@io_abseil_py//absl/logging",
        "@jinja//:jinja2",
        "@markupsafe//:markupsafe",
        "//zetasql/parser:generator_utils",
    ],
)

gen_resolved_ast_files(
    name = "run_gen_resolved_ast_proto",
    srcs = [
        "resolved_ast.proto.template",
        "resolved_node_kind.proto.template",
    ],
    outs = [
        "resolved_ast.proto",
        "resolved_node_kind.proto",
    ],
)

gen_resolved_ast_files(
    name = "run_gen_resolved_ast_cc",
    srcs = [
        "resolved_ast.cc.template",
        "resolved_ast.h.template",
        "resolved_ast_deep_copy_visitor.cc.template",
        "resolved_ast_deep_copy_visitor.h.template",
        "resolved_ast_visitor.h.template",
        "resolved_node_kind.h.template",
    ],
    outs = [
        "resolved_ast.cc",
        "resolved_ast.h",
        "resolved_ast_deep_copy_visitor.cc",
        "resolved_ast_deep_copy_visitor.h",
        "resolved_ast_visitor.h",
        "resolved_node_kind.h",
    ],
)

gen_resolved_ast_files(
    name = "run_gen_resolved_ast_md",
    srcs = ["resolved_ast.md.template"],
    outs = ["resolved_ast.md"],
    flags = ["--noallow_hash_prefix"],
)

proto_library(
    name = "resolved_node_kind_proto",
    srcs = ["resolved_node_kind.proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "resolved_node_kind_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":resolved_node_kind_proto"],
)

java_proto_library(
    name = "resolved_node_kind_java_proto",
    visibility = ["//visibility:public"],
    deps = [":resolved_node_kind_proto"],
)

# TODO How do I make the library with generated headers live in public/?
cc_library(
    name = "resolved_ast",
    srcs = [
        "resolved_ast.cc",
        "resolved_ast_deep_copy_visitor.cc",
        "resolved_ast_helper.cc",
        "resolved_collation.cc",
        "resolved_column.cc",
        "resolved_node.cc",
    ],
    hdrs = [
        "resolved_ast.h",
        "resolved_ast_deep_copy_visitor.h",
        "resolved_ast_helper.h",
        "resolved_ast_visitor.h",
        "resolved_collation.h",
        "resolved_column.h",
        "resolved_node.h",
        "resolved_node_kind.h",
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
        ":resolved_ast_cc_proto",
        ":resolved_ast_enums_cc_proto",
        ":resolved_node_kind_cc_proto",
        ":serialization_cc_proto",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/public:annotation_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:constant",
        "//zetasql/public:function",
        "//zetasql/public:id_string",
        "//zetasql/public:parse_location",
        "//zetasql/public:parse_location_range_cc_proto",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:strings",
        "//zetasql/public:type",
        "//zetasql/public:type_annotation_cc_proto",
        "//zetasql/public:type_parameters_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public/proto:type_annotation_cc_proto",
        "//zetasql/public/types",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "make_node_vector",
    srcs = [
        "make_node_vector_internal.h",
    ],
    hdrs = [
        "make_node_vector.h",
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
        ":resolved_ast",
        "@com_google_absl//absl/meta:type_traits",
    ],
)

gen_resolved_ast_files(
    name = "run_gen_resolved_ast_comparator",
    srcs = [
        "resolved_ast_comparator.cc.template",
        "resolved_ast_comparator.h.template",
    ],
    outs = [
        "resolved_ast_comparator.cc",
        "resolved_ast_comparator.h",
    ],
)

cc_library(
    name = "comparator",
    srcs = ["resolved_ast_comparator.cc"],
    hdrs = ["resolved_ast_comparator.h"],
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
        ":resolved_ast",
        "//zetasql/base",
        "//zetasql/base:case",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/public:catalog",
        "@com_google_absl//absl/status:statusor",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "validator",
    srcs = ["validator.cc"],
    hdrs = ["validator.h"],
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
        ":resolved_ast",
        ":resolved_ast_enums_cc_proto",
        ":resolved_node_kind_cc_proto",
        ":rewrite_utils",
        "//zetasql/analyzer:filter_fields_path_validator",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:stl_util",
        "//zetasql/base:strings",
        "//zetasql/base:varsetter",
        "//zetasql/common:errors",
        "//zetasql/public:catalog",
        "//zetasql/public:constant",
        "//zetasql/public:function",
        "//zetasql/public:function_cc_proto",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:strings",
        "//zetasql/public:templated_sql_function",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "sql_builder",
    srcs = [
        "query_expression.cc",
        "sql_builder.cc",
    ],
    hdrs = [
        "query_expression.h",
        "sql_builder.h",
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
        ":resolved_ast",
        ":resolved_ast_enums_cc_proto",
        ":resolved_node_kind_cc_proto",
        ":rewrite_utils",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/public:analyzer",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:catalog",
        "//zetasql/public:constant",
        "//zetasql/public:function",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:strings",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/public/functions:datetime_cc_proto",
        "//zetasql/public/functions:normalize_mode_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "resolved_ast_test",
    size = "small",
    srcs = ["resolved_ast_test.cc"],
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
        ":make_node_vector",
        ":resolved_ast",
        ":serialization_cc_proto",
        ":test_utils",
        ":validator",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:analyzer",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:id_string",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:templated_sql_tvf",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public:value_cc_proto",
    ],
)

cc_test(
    name = "resolved_node_test",
    size = "small",
    srcs = ["resolved_node_test.cc"],
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
        ":resolved_ast",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public/types",
    ],
)

cc_test(
    name = "make_node_vector_test",
    size = "small",
    srcs = ["make_node_vector_test.cc"],
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
        ":make_node_vector",
        ":resolved_ast",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/memory",
    ],
)

cc_test(
    name = "resolved_ast_deep_copy_visitor_test",
    size = "small",
    srcs = ["resolved_ast_deep_copy_visitor_test.cc"],
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
        ":make_node_vector",
        ":resolved_ast",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:analyzer",
        "//zetasql/public:builtin_function",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:function",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:templated_sql_tvf",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
    ],
)

cc_test(
    name = "resolved_ast_helper_test",
    size = "small",
    srcs = ["resolved_ast_helper_test.cc"],
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
        ":make_node_vector",
        ":resolved_ast",
        "//zetasql/analyzer:resolver",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:function",
        "//zetasql/public:templated_sql_tvf",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/memory",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "resolved_column_test",
    size = "small",
    srcs = ["resolved_column_test.cc"],
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
        ":resolved_ast",
        "//zetasql/base:status",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:templated_sql_tvf",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public/annotation:collation",
        "//zetasql/public/types",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "resolved_collation_test",
    size = "small",
    srcs = ["resolved_collation_test.cc"],
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
        ":resolved_ast",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:templated_sql_tvf",
        "//zetasql/public/types",
    ],
)

proto_library(
    name = "resolved_ast_enums_proto",
    srcs = ["resolved_ast_enums.proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "resolved_ast_enums_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":resolved_ast_enums_proto"],
)

java_proto_library(
    name = "resolved_ast_enums_java_proto",
    visibility = ["//visibility:public"],
    deps = [":resolved_ast_enums_proto"],
)

load("@com_google_protobuf//:protobuf.bzl", "py_proto_library")

py_proto_library(
    name = "resolved_ast_enums_py_pb2",
    srcs = ["resolved_ast_enums.proto"],
)

proto_library(
    name = "resolved_ast_proto",
    srcs = ["resolved_ast.proto"],
    deps = [
        ":resolved_ast_enums_proto",
        ":resolved_node_kind_proto",
        ":serialization_proto",
        "//zetasql/proto:function_proto",
        "//zetasql/public:annotation_proto",
        "//zetasql/public:type_parameters_proto",
        "//zetasql/public:type_proto",
        "//zetasql/public/proto:type_annotation_proto",
        "@com_google_protobuf//:descriptor_proto",
    ],
)

cc_proto_library(
    name = "resolved_ast_cc_proto",
    deps = [":resolved_ast_proto"],
)

java_proto_library(
    name = "resolved_ast_java_proto",
    deps = [":resolved_ast_proto"],
)

proto_library(
    name = "serialization_proto",
    srcs = ["serialization.proto"],
    deps = [
        "//zetasql/public:annotation_proto",
        "//zetasql/public:parse_location_range_proto",
        "//zetasql/public:type_proto",
        "//zetasql/public:value_proto",
    ],
)

cc_proto_library(
    name = "serialization_cc_proto",
    deps = [":serialization_proto"],
)

java_proto_library(
    name = "serialization_java_proto",
    deps = [":serialization_proto"],
)

cc_library(
    name = "rewrite_utils",
    srcs = ["rewrite_utils.cc"],
    hdrs = ["rewrite_utils.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":resolved_ast",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:builtin_function_cc_proto",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "rewrite_utils_test",
    size = "small",
    srcs = ["rewrite_utils_test.cc"],
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
        ":rewrite_utils",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:simple_catalog",
        "//zetasql/public/types",
    ],
)

cc_test(
    name = "validator_test",
    srcs = ["validator_test.cc"],
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
        ":make_node_vector",
        ":resolved_ast",
        ":test_utils",
        ":validator",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:simple_catalog",
        "//zetasql/public/types",
    ],
)

cc_library(
    name = "test_utils",
    srcs = ["test_utils.cc"],
    hdrs = ["test_utils.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":make_node_vector",
        ":resolved_ast",
        "//zetasql/base:logging",
    ],
)
