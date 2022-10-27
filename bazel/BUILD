#
# Copyright 2018 Google LLC
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
"""
Bazel build files for ZetaSQL.
"""

load("flex.bzl", "flex_header")

licenses(["notice"])  # Apache v2.0

package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

exports_files([
    "bison.bzl",
    "flex.bzl",
    "maven_install.json",
    "zetasql_deps_step_1.bzl",
    "zetasql_deps_step_2.bzl",
    "zetasql_deps_step_3.bzl",
    "zetasql_deps_step_4.bzl",
    "zetasql_java_deps.bzl",
])

flex_header(
    name = "flex",
)
