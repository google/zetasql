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

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("//zetasql/tools/execute_query:build_rules.bzl", "execute_query_test")

package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

exports_files(["run_execute_query_test.sh"])

proto_library(
    name = "execute_query_proto",
    srcs = ["execute_query.proto"],
)

cc_proto_library(
    name = "execute_query_cc_proto",
    deps = [":execute_query_proto"],
)

cc_library(
    name = "output_query_result",
    srcs = ["output_query_result.cc"],
    hdrs = ["output_query_result.h"],
    deps = [
        "//zetasql/base:status",
        "//zetasql/public:strings",
        "//zetasql/public:value",
        "//zetasql/public/functions:string",
        "//zetasql/public/types",
        "//zetasql/reference_impl:evaluation",
        "//zetasql/reference_impl:type_helpers",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "execute_query_tool_test",
    srcs = ["execute_query_tool_test.cc"],
    data = [
        "testdata/KitchenSinkPB.textproto",
        "testdata/execute_query_tool.test",
        "testdata/test.csv",
    ],
    deps = [
        ":execute_query_tool",
        ":execute_query_writer",
        "//zetasql/base:path",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common:options_utils",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:builtin_function_options",
        "//zetasql/public:catalog",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:commandlineflag",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:reflection",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_file_based_test_driver//file_based_test_driver",
        "@com_google_file_based_test_driver//file_based_test_driver:test_case_options",
        "@com_google_protobuf//:protobuf",
    ],
)
#
###############################################
# Basic
###############################################
#   bazel run //zetasql/tools/execute_query:execute_query -- "SELECT 1"
#
###############################################
# CSV
###############################################
# First generate some data in csv format:
#   printf '"a","b","c"\n5, 1.2, "r1"\n6, 1.5, "r2"\n7, 3.3, "r3"' > sample.csv
# Then query that data (note, all columns are STRING type)
#   bazel run //zetasql/tools/execute_query:execute_query --\
#       --table_spec="data=csv:$(pwd)/sample.csv" "select * from data"
# Output:
#  +---+------+-----+
#  | a | b    | c   |
#  +---+------+-----+
#  | 5 |  1.2 |  r1 |
#  | 6 |  1.5 |  r2 |
#  | 7 |  3.3 |  r3 |
#  +---+------+-----+
#

cc_binary(
    name = "execute_query",
    srcs = [
        "execute_query.cc",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":execute_query_tool",
        ":execute_query_web",
        ":execute_query_writer",
        ":homedir",
        "//zetasql/base",
        "//zetasql/base:status",
        "@com_google_absl//absl/flags:config",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/flags:usage",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/log:initialize",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "execute_query_prompt",
    srcs = ["execute_query_prompt.cc"],
    hdrs = ["execute_query_prompt.h"],
    deps = [
        ":execute_query_cc_proto",
        ":execute_query_tool",
        "//zetasql/base:status",
        "//zetasql/common:status_payload_utils",
        "//zetasql/public:language_options",
        "//zetasql/public:parse_helpers",
        "//zetasql/public:parse_resume_location",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "execute_query_prompt_test",
    srcs = ["execute_query_prompt_test.cc"],
    deps = [
        ":execute_query_cc_proto",
        ":execute_query_prompt",
        ":execute_query_prompt_testutils",
        ":execute_query_tool",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/common/testing:status_payload_matchers",
        "//zetasql/public:options_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "execute_query_prompt_testutils",
    testonly = 1,
    srcs = ["execute_query_prompt_testutils.cc"],
    hdrs = ["execute_query_prompt_testutils.h"],
    deps = [
        ":execute_query_prompt",
        "//zetasql/base:logging",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "execute_query_prompt_testutils_test",
    srcs = ["execute_query_prompt_testutils_test.cc"],
    deps = [
        ":execute_query_prompt_testutils",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "homedir",
    srcs = ["homedir.cc"],
    hdrs = ["homedir.h"],
    deps = [
        "@com_google_absl//absl/types:optional",
    ],
)

cc_test(
    name = "homedir_test",
    srcs = ["homedir_test.cc"],
    deps = [
        ":homedir",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/types:optional",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "execute_query_writer",
    srcs = ["execute_query_writer.cc"],
    hdrs = ["execute_query_writer.h"],
    deps = [
        ":output_query_result",
        "//zetasql/base:status",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "execute_query_writer_test",
    srcs = ["execute_query_writer_test.cc"],
    deps = [
        ":execute_query_writer",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:analyzer",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "execute_query_proto_writer",
    srcs = ["execute_query_proto_writer.cc"],
    hdrs = ["execute_query_proto_writer.h"],
    deps = [
        ":execute_query_writer",
        "//zetasql/base:logging",
        "//zetasql/base:status",
        "//zetasql/common:proto_from_iterator",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:type",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "execute_query_proto_writer_test",
    srcs = ["execute_query_proto_writer_test.cc"],
    deps = [
        ":execute_query_proto_writer",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:json_value",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "execute_query_loop",
    srcs = ["execute_query_loop.cc"],
    hdrs = ["execute_query_loop.h"],
    deps = [
        ":execute_query_cc_proto",
        ":execute_query_prompt",
        ":execute_query_tool",
        ":execute_query_writer",
        "//zetasql/base:status",
        "//zetasql/common:status_payload_utils",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "execute_query_loop_test",
    srcs = ["execute_query_loop_test.cc"],
    deps = [
        ":execute_query_cc_proto",
        ":execute_query_loop",
        ":execute_query_prompt",
        ":execute_query_tool",
        ":execute_query_writer",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/common/testing:status_payload_matchers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "selectable_catalog",
    srcs = ["selectable_catalog.cc"],
    hdrs = ["selectable_catalog.h"],
    deps = [
        "//zetasql/base:status",
        "//zetasql/examples/tpch/catalog",
        "//zetasql/public:builtin_function_options",
        "//zetasql/public:catalog",
        "//zetasql/public:language_options",
        "//zetasql/public:simple_catalog",
        "//zetasql/testdata:sample_catalog_impl",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "selectable_catalog_test",
    srcs = ["selectable_catalog_test.cc"],
    deps = [
        ":selectable_catalog",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:catalog",
        "//zetasql/public:language_options",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "execute_query_tool",
    srcs = [
        "execute_query_internal_binproto.cc",
        "execute_query_internal_csv.cc",
        "execute_query_internal_textproto.cc",
        "execute_query_tool.cc",
    ],
    hdrs = ["execute_query_tool.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":execute_query_proto_writer",
        ":execute_query_writer",
        ":selectable_catalog",
        ":simple_proto_evaluator_table_iterator",
        ":string_error_collector",
        ":value_as_table_adapter",
        "//zetasql/base",
        "//zetasql/base:check",
        "//zetasql/base:file_util",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/common:options_utils",
        "//zetasql/parser:parse_tree",
        "//zetasql/parser:parse_tree_serializer",
        "//zetasql/parser:parser_mode",
        "//zetasql/parser/macros:macro_catalog",
        "//zetasql/public:analyzer",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:builtin_function_options",
        "//zetasql/public:catalog",
        "//zetasql/public:error_helpers",
        "//zetasql/public:evaluator",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:function",
        "//zetasql/public:multi_catalog",
        "//zetasql/public:parse_helpers",
        "//zetasql/public:parse_resume_location",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:simple_catalog_util",
        "//zetasql/public:sql_constant",
        "//zetasql/public:sql_formatter",
        "//zetasql/public:strings",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public/functions:like",
        "//zetasql/public/types",
        "//zetasql/reference_impl:statement_evaluator",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "//zetasql/resolved_ast:sql_builder",
        "//zetasql/scripting:script_executor",
        "//zetasql/scripting:script_segment",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
        "@com_google_riegeli//riegeli/bytes:fd_reader",
        "@com_google_riegeli//riegeli/csv:csv_reader",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_library(
    name = "simple_proto_evaluator_table_iterator",
    srcs = [
        "simple_proto_evaluator_table_iterator.cc",
    ],
    hdrs = ["simple_proto_evaluator_table_iterator.h"],
    deps = [
        "//zetasql/base:check",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "string_error_collector",
    srcs = ["string_error_collector.cc"],
    hdrs = ["string_error_collector.h"],
    deps = [
        "//zetasql/base",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "string_error_collector_test",
    srcs = ["string_error_collector_test.cc"],
    deps = [
        ":string_error_collector",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "value_as_table_adapter",
    srcs = ["value_as_table_adapter.cc"],
    hdrs = ["value_as_table_adapter.h"],
    deps = [
        "//zetasql/public:catalog",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "value_as_table_adapter_test",
    srcs = ["value_as_table_adapter_test.cc"],
    deps = [
        ":value_as_table_adapter",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:catalog",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "execute_query_web_server",
    hdrs = ["execute_query_web_server.h"],
)

cc_library(
    name = "execute_query_web_handler",
    srcs = [
        "execute_query_web_handler.cc",
        "execute_query_web_writer.cc",
    ],
    hdrs = [
        "execute_query_web_handler.h",
        "execute_query_web_writer.h",
    ],
    deps = [
        ":execute_query_tool",
        ":execute_query_writer",
        ":output_query_result",
        ":selectable_catalog",
        "//zetasql/base:status",
        "//zetasql/common:options_utils",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:value",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:sql_builder",
        "//zetasql/tools/execute_query/web:embedded_resources",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_googlesource_code_re2//:re2",
        "@mstch",
    ],
)

cc_test(
    name = "execute_query_web_handler_test",
    srcs = ["execute_query_web_handler_test.cc"],
    data = [
        "testdata/execute_query_web_handler.test",
    ],
    deps = [
        ":execute_query_tool",
        ":execute_query_web_handler",
        "//zetasql/base:path",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/resolved_ast:sql_builder",
        "//zetasql/tools/execute_query/web:embedded_resources",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_file_based_test_driver//file_based_test_driver",
        "@com_google_file_based_test_driver//file_based_test_driver:run_test_case_result",
        "@com_google_file_based_test_driver//file_based_test_driver:test_case_options",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "execute_query_web",
    srcs = ["execute_query_web.cc"],
    hdrs = ["execute_query_web.h"],
    deps = [
        ":execute_query_tool",
        ":execute_query_web_handler",
        ":execute_query_web_server",
        "//zetasql/tools/execute_query/web:embedded_resources",
        "@civetweb",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)

execute_query_test(
    name = "run_execute_query_test",
    args = ["--catalog=tpch"],
    sql_file = "testdata/run_execute_query_test.sql",
)

execute_query_test(
    name = "run_execute_query_bad_parse_test",
    args = ["--mode=parse"],
    sql_file = "testdata/run_execute_query_test_bad.sql",
)

bzl_library(
    name = "build_rules_bzl",
    srcs = ["build_rules.bzl"],
)
