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

# Definitions of optional builtin functions for the reference implementation and evaluator library.
# See ../public/evaluator_lite.h for more information.
#
# Static self-registration with alwayslink=1 is a common pattern for this, but
# explicitly calling the registration functions is preferred here to allow
# the linker to prune unused modules.

package(
    default_visibility = ["//:__subpackages__"],
)

cc_library(
    name = "register_all",
    srcs = ["register_all.cc"],
    hdrs = ["register_all.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":hash",
        ":json",
        ":uuid",
        "@com_google_absl//absl/base",
    ],
)

cc_library(
    name = "hash",
    srcs = ["hash.cc"],
    hdrs = ["hash.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        "//zetasql/base:statusor",
        "//zetasql/public:value",
        "//zetasql/public/functions:hash",
        "//zetasql/reference_impl:evaluation",
    ],
)

cc_library(
    name = "json",
    srcs = ["json.cc"],
    hdrs = ["json.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        "//zetasql/public/functions:json",
        "//zetasql/reference_impl:evaluation",
    ],
)

cc_library(
    name = "uuid",
    srcs = ["uuid.cc"],
    hdrs = ["uuid.h"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        "//zetasql/public:value",
        "//zetasql/public/functions:uuid",
        "//zetasql/reference_impl:evaluation",
    ],
)
