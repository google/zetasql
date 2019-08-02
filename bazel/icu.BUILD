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
    default_visibility = ["//visibility:public"],
)

# We need to label this for configure_make.
filegroup(
    name = "all",
    srcs = glob(["**"]),
)

configure_env_vars = {
    "CXXFLAGS": "-fPIC",  # For JNI
    "CFLAGS": "-fPIC",  # For JNI
    "LIBS": "$$LDFLAGS$$",
}

darwin_configure_env_vars = {
    "AR": "/usr/bin/ar -q"
}

configure_make(
    name = "icu",
    configure_command = "source/configure",
    configure_options = [
        "--enable-option-checking",
        "--enable-static",
        "--enable-tools",  # needed to build data
        "--disable-shared",
        "--disable-dyload",
        "--disable-extras",
        "--disable-plugins",
        "--disable-tests",
        "--disable-samples",
        "--with-data-packaging=static",
    ],
    configure_env_vars = select({
        "@bazel_tools//src/conditions:darwin": dict(configure_env_vars, **darwin_configure_env_vars),
        "//conditions:default": configure_env_vars,
    }),
    lib_source = "@icu//:all",
    static_libraries = [
        "libicui18n.a",
        "libicuio.a",
        "libicuuc.a",
        "libicudata.a",
    ],
)

cc_library(
    name = "common",
    deps = [
        "icu",
    ],
)

cc_library(
    name = "headers",
    deps = [
        "icu",
    ],
)

cc_library(
    name = "unicode",
    deps = [
        "icu",
    ],
)

exports_files([
    "icu4c/LICENSE",
])
