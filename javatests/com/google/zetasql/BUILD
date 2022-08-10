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

load("//javatests/com/google/zetasql:junit_test_suites.bzl", "junit_test_suites")

# Tests for ZetaSQL public Java API.
package(
    default_testonly = 1,
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

java_library(
    name = "tests",
    srcs = glob(["*Test.java"]),
    deps = [
        ":util",
        "@com_google_protobuf//:protobuf_java",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_guava_guava_testlib",
        "//java/com/google/zetasql:analyzer",
        "//java/com/google/zetasql:client",
        "//java/com/google/zetasql:types",
        "//java/com/google/zetasql/resolvedast",
        "//zetasql/local_service:local_service_java_proto",
        "//zetasql/proto:function_java_proto",
        "//zetasql/proto:options_java_proto",
        "//zetasql/proto:simple_catalog_java_proto",
        # "//zetasql/public/functions:datetime_java_proto",
        "//zetasql/public:builtin_function_java_proto",
        "//zetasql/public:deprecation_warning_java_proto",
        "//zetasql/public:error_location_java_proto",
        "//zetasql/public:formatter_options_java_proto",
        "//zetasql/public:function_java_proto",
        "//zetasql/public:options_java_proto",
        "//zetasql/public:parse_resume_location_java_proto",
        "//zetasql/public:simple_table_java_proto",
        "//zetasql/public:type_proto_java_proto",
        "//zetasql/public:type_parameters_java_proto",
        "//zetasql/public:value_java_proto",
        "//zetasql/public/proto:type_annotation_java_proto",
        "//zetasql/resolved_ast:resolved_node_kind_java_proto",  # buildcleaner: keep
        "//zetasql/testdata:bad_test_schema_java_proto",
        "//zetasql/testdata:test_schema_java_proto",
        "@maven//:io_grpc_grpc_core",
        "@maven//:joda_time_joda_time",
        "@maven//:junit_junit",
        "@maven//:com_google_truth_truth",
        "@maven//:com_google_truth_extensions_truth_proto_extension",
    ],
)

java_library(
    name = "util",
    srcs = [
        "TestUtil.java",
        "TypeTestBase.java",
    ],
    deps = [
        "//java/com/google/zetasql:types",
        "//zetasql/public:type_proto_java_proto",
        "@com_google_protobuf//:protobuf_java",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:com_google_truth_truth",
    ],
)

junit_test_suites(
    name = "gen_tests_jni",
    runtime_deps = ["//java/com/google/zetasql:jni_channel"],
    deps = [":tests"],
)
