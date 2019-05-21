#
# Copyright 2018 ZetaSQL Authors
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

load("@rules_foreign_cc//tools/build_defs:configure.bzl", "configure_make")

licenses(["notice"])  # Apache v2.0

package(
    default_visibility = ["//zetasql:__subpackages__"],
)

exports_files([
    "bison.bzl",
    "flex.bzl",
])

configure_make(
    name = "m4",
    binaries = [
        "m4",
    ],
    configure_options = [
        "--disable-nls",
    ],
    lib_source = "@m4//:all",
)

filegroup(
    name = "m4_bin",
    srcs = [":m4"],
    output_group = "m4",
)

configure_make(
    name = "bison",
    binaries = [
        "bison",
        "yacc",
    ],
    configure_env_vars = {"M4": "$$EXT_BUILD_DEPS$$/m4/bin/m4"},
    lib_source = "@bison//:all",
    static_libraries = ["liby.a"],
    deps = [":m4"],
)

filegroup(
    name = "bison_bin",
    srcs = [":bison"],
    output_group = "bison",
)

configure_make(
    name = "flex",
    binaries = [
        "flex",
    ],
    # This seems to be necessary (using tools_dep and weird path) because unlike
    # bison, flex needs to invoke m4 during build (whereas bison needs it only
    # during `configure`).
    configure_env_vars = {"M4": "$$EXT_BUILD_DEPS$$/m4/bin/m4"},
    lib_source = "@flex//:all",
    deps = [":m4"],
)

filegroup(
    name = "flex_bin",
    srcs = [":flex"],
    output_group = "flex",
)
