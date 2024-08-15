# Copyright 2024 Google LLC
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

licenses(["notice"])  # Apache v2.0

package(
    default_visibility = ["//visibility:public"],
)

load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")


filegroup(
    name = "all",
    srcs = glob(["**"]),
)

cmake(
    name = "civetweb",
    cache_entries = {
        "CMAKE_C_FLAGS": "-fPIC",
        "CMAKE_CXX_FLAGS": "-fPIC",
    },
    generate_args = [
        "-DCIVETWEB_ENABLE_CXX=ON",
        "-DBUILD_SHARED_LIBS=OFF",
    ],
    lib_source = ":all",
    out_static_libs = select({
        "@bazel_tools//src/conditions:darwin": [
          "libcivetweb.a",
          "libcivetweb-cpp.a"
    ],
        "//conditions:default": [
          "libcivetweb.a",
          "libcivetweb-cpp.a",
    ]}),
    # We have to disable LTO here because otherwise we hit an internal error in gold linker:
    # "/usr/bin/ld.gold: internal error in set_xindex" when civetweb outputs static libs rather
    # than shared libs.
    #
    # "-ldl" is needed because otherwise we can run into link errors like "error: undefined
    # reference to 'dlopen'" on some platforms.
    linkopts = ["-fno-lto", "-ldl"],
    # TODO: Remove this tag once civetweb have all dependencies without accessing network.
    tags = ["requires-network"],
)
