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

package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

cc_library(
    name = "output_query_result",
    srcs = ["output_query_result.cc"],
    hdrs = ["output_query_result.h"],
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
        "//zetasql/compliance:type_helpers",
        "//zetasql/public:strings",
        "//zetasql/public:value",
        "//zetasql/reference_impl:algebrizer",
        "//zetasql/reference_impl:evaluation",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "execute_query_tool_test",
    srcs = ["execute_query_tool_test.cc"],
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
        "testdata/test.csv",
    ],
    deps = [
        ":execute_query_tool",
        "//zetasql/base:path",
        "//zetasql/base:statusor",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:catalog",
        "//zetasql/public/types",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
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
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:catalog",
        "//zetasql/public:language_options",
        "//zetasql/public:simple_catalog",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "execute_query_tool",
    srcs = [
        "execute_query_internal_csv.cc",
        "execute_query_tool.cc",
    ],
    hdrs = ["execute_query_tool.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":output_query_result",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/public:analyzer",
        "//zetasql/public:analyzer_options",
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
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
        "@com_google_riegeli//riegeli/bytes:fd_reader",
        "@com_google_riegeli//riegeli/csv:csv_reader",
    ],
)
