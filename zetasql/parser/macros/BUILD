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

cc_library(
    name = "quoting",
    srcs = ["quoting.cc"],
    hdrs = ["quoting.h"],
    deps = [
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "quoting_test",
    srcs = ["quoting_test.cc"],
    deps = [
        ":quoting",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "token_with_location",
    srcs = ["token_with_location.cc"],
    hdrs = ["token_with_location.h"],
    visibility = [
        "//zetasql/parser:__pkg__",
        "//zetasql/tools/execute_query:__pkg__",
    ],
    deps = [
        "//zetasql/public:parse_location",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "macro_catalog",
    srcs = ["macro_catalog.cc"],
    hdrs = ["macro_catalog.h"],
    visibility = [
        "//zetasql/parser:__pkg__",
        "//zetasql/tools/execute_query:__pkg__",
    ],
    deps = [
        "//zetasql/public:parse_location",
        "@com_google_absl//absl/container:node_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "token_provider_base",
    hdrs = ["token_provider_base.h"],
    deps = [
        ":token_with_location",
        "//zetasql/base:status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "flex_token_provider",
    srcs = ["flex_token_provider.cc"],
    hdrs = ["flex_token_provider.h"],
    visibility = [
        "//zetasql/parser:__pkg__",
        "//zetasql/tools/execute_query:__pkg__",
    ],
    deps = [
        ":token_provider_base",
        ":token_with_location",
        "//zetasql/base:check",
        "//zetasql/base:status",
        "//zetasql/parser:flex_tokenizer",
        "//zetasql/public:parse_location",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "flex_token_provider_test",
    srcs = ["flex_token_provider_test.cc"],
    deps = [
        ":flex_token_provider",
        ":token_with_location",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/parser:bison_token_codes",
        "//zetasql/parser:token_disambiguator",  # build_cleaner: keep
        "//zetasql/public:parse_location",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "token_splicing_utils",
    srcs = ["token_splicing_utils.cc"],
    hdrs = ["token_splicing_utils.h"],
    deps = [
        ":token_with_location",
        "//zetasql/parser:bison_token_codes",
        "//zetasql/parser:keywords",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "macro_expander",
    srcs = [
        "macro_expander.cc",
    ],
    hdrs = [
        "macro_expander.h",
    ],
    visibility = [
        "//zetasql/parser:__pkg__",
        "//zetasql/tools/execute_query:__pkg__",
    ],
    deps = [
        ":macro_catalog",
        ":quoting",
        ":standalone_macro_expansion",
        ":token_provider_base",
        ":token_splicing_utils",
        ":token_with_location",
        "//zetasql/base:arena",
        "//zetasql/base:arena_allocator",
        "//zetasql/base:check",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:thread_stack",
        "//zetasql/parser:bison_token_codes",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:error_helpers",
        "//zetasql/public:error_location_cc_proto",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "macro_expander_test",
    size = "small",
    srcs = ["macro_expander_test.cc"],
    deps = [
        ":flex_token_provider",
        ":macro_catalog",
        ":macro_expander",
        ":quoting",
        ":standalone_macro_expansion",
        ":token_with_location",
        "//zetasql/base:arena",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/parser",
        "//zetasql/parser:bison_token_codes",
        "//zetasql/parser:token_disambiguator",  # build_cleaner: keep
        "//zetasql/public:error_helpers",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
        "//zetasql/public:parse_resume_location",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "standalone_macro_expansion",
    srcs = ["standalone_macro_expansion.cc"],
    hdrs = ["standalone_macro_expansion.h"],
    visibility = [
        "//zetasql/parser:__pkg__",
        "//zetasql/tools/execute_query:__pkg__",
    ],
    deps = [
        ":token_splicing_utils",
        ":token_with_location",
        "//zetasql/base:check",
        "//zetasql/parser:bison_token_codes",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "standalone_macro_expansion_impl",
    hdrs = ["standalone_macro_expansion_impl.h"],
    visibility = [
        "//zetasql/parser:__pkg__",
        "//zetasql/tools/execute_query:__pkg__",
    ],
    deps = [
        ":flex_token_provider",
        ":macro_catalog",
        ":macro_expander",
        "//zetasql/public:error_helpers",
        "//zetasql/public:language_options",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)
