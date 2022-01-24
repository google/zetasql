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

# Definitions of optional builtin functions for the reference implementation and evaluator library.
# See ../public/evaluator_lite.h for more information.
#
# Static self-registration with alwayslink=1 is a common pattern for this, but
# explicitly calling the registration functions is preferred here to allow
# the linker to prune unused modules.

package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

cc_library(
    name = "register_all",
    srcs = ["register_all.cc"],
    hdrs = ["register_all.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":hash",
        ":json",
        ":string_with_collation",
        ":uuid",
    ],
)

cc_library(
    name = "hash",
    srcs = ["hash.cc"],
    hdrs = ["hash.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/public:value",
        "//zetasql/public/functions:hash",
        "//zetasql/reference_impl:evaluation",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "json",
    srcs = ["json.cc"],
    hdrs = ["json.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/base:ret_check",
        "//zetasql/common:errors",
        "//zetasql/public:json_value",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public/functions:json",
        "//zetasql/public/functions:json_format",
        "//zetasql/public/functions:to_json",
        "//zetasql/public/types",
        "//zetasql/reference_impl:evaluation",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "string_with_collation",
    srcs = ["string_with_collation.cc"],
    hdrs = ["string_with_collation.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/public:collator",
        "//zetasql/public:value",
        "//zetasql/public/functions:string_with_collation",
        "//zetasql/reference_impl:evaluation",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "uuid",
    srcs = ["uuid.cc"],
    hdrs = ["uuid.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/public:value",
        "//zetasql/public/functions:uuid",
        "//zetasql/reference_impl:evaluation",
        "@com_google_absl//absl/status:statusor",
    ],
)
