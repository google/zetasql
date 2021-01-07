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

# Contains rewriters for zetasql analyzers.

package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

cc_library(
    name = "flatten_rewriter",
    srcs = ["flatten_rewriter.cc"],
    hdrs = ["flatten_rewriter.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/parser",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:function",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:rewrite_utils",
        "//zetasql/resolved_ast:validator",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "registration",
    srcs = ["registration.cc"],
    hdrs = ["registration.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/base:statusor",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public/types",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "registration_test",
    srcs = ["registration_test.cc"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":registration",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/status",
    ],
)
