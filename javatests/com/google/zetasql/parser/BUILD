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

# Tests for ZetaSQL parser classes.
package(
    default_testonly = 1,
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

java_library(
    name = "tests",
    srcs = glob(["*.java"]),
    resources =
        glob(["protos/*.textproto"]),
    deps = [
        "//java/com/google/zetasql:client",
        "//java/com/google/zetasql:parser",
        "//java/com/google/zetasql/parser",
        "//zetasql/parser:ast_enums_java_proto",
        "//zetasql/parser:ast_java_proto",  # buildcleaner: keep
        "//zetasql/public:options_java_proto",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf//:protobuf_java",
        "@com_google_truth_truth//jar",
        "@junit_junit//jar",
    ],
)

junit_test_suites(
    name = "gen_tests",
    runtime_deps = ["//java/com/google/zetasql:jni_channel"],
    deps = [":tests"],
)
