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
Rules for adding './configure && make' style dependencies.
"""

load("@rules_foreign_cc//foreign_cc:configure.bzl", "configure_make")

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

configure_make(
    name = "m4",
    env = select({
        "@platforms//os:macos": {"AR": ""},
        "//conditions:default": {},
    }),
    lib_source = "@m4//:all",
    out_binaries = [
        "m4",
    ],
)

filegroup(
    name = "m4_bin",
    srcs = [":m4"],
    output_group = "m4",
)

configure_make(
    name = "bison",
    build_data = [":m4_bin"],
    # Without specifying m4 directly, cc_rules_foreign may fallback
    # on the system m4, which might be too old.
    env = select({
        "@platforms//os:macos": {
            "AR": "",
            "M4": "$(execpath :m4_bin)",
        },
        "//conditions:default": {"M4": "$(execpath :m4_bin)"},
    }),
    lib_source = "@bison//:all",
    out_binaries = [
        "bison",
        "yacc",
    ],
)

filegroup(
    name = "bison_bin",
    srcs = [":bison"],
    output_group = "bison",
)

configure_make(
    name = "flex",
    build_data = [":m4_bin"],
    # Flex requires m4 when it _executes_, unlike bison, which only
    # requires it when it is built - this requires some additional handling.
    env = select({
        "@platforms//os:macos": {
            "AR": "",
            "M4": "$(execpath :m4_bin)",
        },
        "//conditions:default": {"M4": "$(execpath :m4_bin)"},
    }),
    lib_source = "@flex//:all",
    out_binaries = [
        "flex",
    ],
)

filegroup(
    name = "flex_bin",
    srcs = [":flex"],
    output_group = "flex",
)
