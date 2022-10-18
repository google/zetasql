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

package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

cc_library(
    name = "chunk",
    srcs = [
        "chunk.cc",
    ],
    hdrs = ["chunk.h"],
    deps = [
        ":fusible_tokens",
        ":token",
        "//zetasql/base",
        "//zetasql/base:die_if_null",
        "//zetasql/base:flat_set",
        "//zetasql/base:status",
        "//zetasql/public:formatter_options",
        "//zetasql/public:parse_helpers",
        "//zetasql/public:parse_location",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "chunk_grouping_strategy",
    srcs = ["chunk_grouping_strategy.cc"],
    hdrs = ["chunk_grouping_strategy.h"],
    deps = [
        ":chunk",
        ":token",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "layout",
    srcs = ["layout.cc"],
    hdrs = ["layout.h"],
    deps = [
        ":chunk",
        ":parsed_file",
        ":token",
        "//zetasql/base",
        "//zetasql/base:flat_set",
        "//zetasql/base:status",
        "//zetasql/public:formatter_options",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "token",
    srcs = ["token.cc"],
    hdrs = ["token.h"],
    deps = [
        "//zetasql/base:flat_set",
        "//zetasql/base:status",
        "//zetasql/common:utf_util",
        "//zetasql/parser:keywords",
        "//zetasql/public:formatter_options",
        "//zetasql/public:language_options",
        "//zetasql/public:parse_helpers",
        "//zetasql/public:parse_location",
        "//zetasql/public:parse_resume_location",
        "//zetasql/public:strings",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_googlesource_code_re2//:re2",
        "@icu//:unicode",
    ],
)

cc_library(
    name = "parsed_file",
    srcs = ["parsed_file.cc"],
    hdrs = ["parsed_file.h"],
    deps = [
        ":chunk",
        ":chunk_grouping_strategy",
        ":range_utils",
        ":token",
        "//zetasql/base:status",
        "//zetasql/common:utf_util",
        "//zetasql/parser",
        "//zetasql/public:error_helpers",
        "//zetasql/public:formatter_options",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
        "//zetasql/public:parse_resume_location",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@icu//:unicode",
    ],
)

cc_library(
    name = "fusible_tokens",
    srcs = ["fusible_tokens.cc"],
    hdrs = ["fusible_tokens.h"],
    deps = [
        ":token",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "range_utils",
    srcs = ["range_utils.cc"],
    hdrs = ["range_utils.h"],
    deps = [
        ":token",
        "//zetasql/base:status",
        "//zetasql/public:formatter_options",
        "//zetasql/public:parse_location",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)
