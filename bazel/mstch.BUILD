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


load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

package(
    default_visibility = ["//visibility:public"],
)

filegroup(
    name = "all",
    srcs = glob(["**"]),
)

cmake(
    name = "mstch",
    deps = ["@boost.variant"],
    cache_entries = {
        "CMAKE_C_FLAGS": "-fPIC",
        "CMAKE_CXX_FLAGS": "-fPIC",
    },
    lib_source = ":all",
    out_static_libs = ["libmstch.a"],
)
