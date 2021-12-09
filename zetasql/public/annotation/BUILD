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

package(default_visibility = ["//zetasql/base:zetasql_implementation"])

cc_library(
    name = "collation",
    srcs = ["collation.cc"],
    hdrs = ["collation.h"],
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
        "//zetasql/parser:parse_tree",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:function",
        "//zetasql/public:function_cc_proto",
        "//zetasql/public:type",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)
