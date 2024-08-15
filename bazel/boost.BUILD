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


load("@rules_foreign_cc//foreign_cc:defs.bzl", "boost_build")

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:private"],
)

boost_build(
    name = "boost",
    bootstrap_options = ["--without-icu"],
    lib_source = ":all_srcs",
    out_static_libs = select({
      "//conditions:default": [
          "libboost_atomic.a",
          "libboost_filesystem.a",
          "libboost_program_options.a",
          "libboost_regex.a",
          "libboost_system.a",
          "libboost_thread.a",
      ],
    }),
    user_options = [
        "-j4",
        "--with-filesystem",
        "--with-program_options",
        "--with-regex",
        "--with-system",
        "--with-thread",
        "variant=release",
        "link=static",
        "threading=multi",
    ],
    visibility = ["//visibility:public"],
)
