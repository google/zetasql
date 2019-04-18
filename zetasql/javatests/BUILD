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

# Tests for ZetaSQL public Java API.
package(
    default_testonly = 1,
    default_visibility = ["//zetasql:__subpackages__"],
)

load("//zetasql/javatests:junit_test_suites.bzl", "junit_test_suites")

java_library(
    name = "tests",
    srcs = glob(["*Test.java"]),
    deps = [
        ":util",
        "//zetasql/java:analyzer",
        "//zetasql/java:client",
        "//zetasql/java:types",
        "//zetasql/java/resolvedast",
        "//zetasql/proto:function_java_proto",
        "//zetasql/proto:options_java_proto",
        "//zetasql/proto:simple_catalog_java_proto",
        "//zetasql/public:builtin_function_java_proto",
        "//zetasql/public:function_java_proto",
        "//zetasql/public:options_java_proto",
        "//zetasql/public:parse_resume_location_java_proto",
        "//zetasql/public:simple_table_java_proto",
        "//zetasql/public:type_proto_java_proto",
        "//zetasql/public:value_java_proto",
        "//zetasql/public/proto:type_annotation_java_proto",
        "//zetasql/resolved_ast:resolved_node_kind_java_proto",  # buildcleaner: keep
        "//zetasql/testdata:bad_test_schema_java_proto",
        "//zetasql/testdata:test_schema_java_proto",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf//:protobuf_java",
        "@com_google_truth_truth//jar",
        "@joda_time//jar",
        "@junit_junit//jar",
    ],
)

java_library(
    name = "util",
    srcs = glob(["TestUtil.java"]),
)

junit_test_suites(
    name = "gen_tests_jni",
    runtime_deps = ["//zetasql/java:jni_channel"],
    deps = [":tests"],
)
