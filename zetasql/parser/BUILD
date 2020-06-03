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

load("//bazel:bison.bzl", "genyacc")
load("//bazel:flex.bzl", "genlex")

package(
    default_visibility = ["//:__subpackages__"],
)

genrule(
    name = "gen_extra_files",
    srcs = ["parse_tree.h"],
    outs = [
        "parse_tree_visitor.h",
        "parse_tree_decls.h",
        "parse_tree_accept_methods.inc",
    ],
    cmd = "$(location :gen_extra_files_tool) $(SRCS) $(OUTS)",
    tools = [":gen_extra_files_tool"],
)

py_binary(
    name = "gen_extra_files_tool",
    srcs = ["gen_extra_files.py"],
    main = "gen_extra_files.py",
    python_version = "PY3",
    deps = [
    ],
)

cc_library(
    name = "parser",
    srcs = [
        "bison_parser.cc",
        "bison_parser.h",
        "bison_parser_mode.h",
        "flex_tokenizer.cc",
        "flex_tokenizer.h",
        "parser.cc",
        "unparser.cc",
    ],
    hdrs = [
        "ast_node_kind.h",
        "parse_tree.h",
        "parse_tree_decls.h",
        "parse_tree_errors.h",
        "parse_tree_visitor.h",
        "parser.h",
        "statement_properties.h",
        "unparser.h",
    ],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    tags = ["nofixdeps"],  # generated and real files are interdependent
    deps = [
        ":bison_parser_generated_lib",
        ":keywords",
        ":parse_tree",
        "//bazel:flex",
        "//zetasql/base",
        "//zetasql/base:cleanup",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/common:errors",
        "//zetasql/common:utf_util",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:error_helpers",
        "//zetasql/public:error_location_cc_proto",
        "//zetasql/public:id_string",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
        "//zetasql/public:parse_resume_location",
        "//zetasql/public:strings",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_absl//absl/types:variant",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_library(
    name = "bison_parser_generated_lib",
    srcs = [
        "bison_parser.bison.cc",
        "flex_tokenizer.flex.cc",
        "join_proccessor.cc",
    ],
    hdrs = [
        "bison_parser.bison.h",
        "bison_parser.h",
        "bison_parser_mode.h",
        "flex_tokenizer.h",
        "join_proccessor.h",
        "location.hh",
        "position.hh",
        "stack.hh",
        "statement_properties.h",
    ],
    # The Bison skeleton uses implicit fallthrough, so this won't compile
    # unless we turn the warning off.
    # The -O2 is required because the generated bison parser uses a ton of
    # stack space in non-optimized builds. The entire parser is contained in
    # a single giant function, and non-optimized compilation does not reuse
    # stack slots for local variables. Without -O2, clients are likely to
    # fail in fastbuild with default 64KB stack sizes, or to start failing
    # when the parser is extended further.
    copts = [
        "-Wno-implicit-fallthrough",
        "-O2",
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    tags = ["nofixdeps"],  # generated and real files are interdependent
    deps = [
        ":keywords",
        ":parse_tree",
        "//bazel:flex",
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/public:id_string",
        "//zetasql/public:language_options",
        "//zetasql/public:parse_location",
        "//zetasql/public:strings",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "parse_tree",
    srcs = [
        "parse_tree.cc",
        "parse_tree_accept_methods.inc",
        "parse_tree_errors.cc",
    ],
    hdrs = [
        "ast_node_kind.h",
        "parse_tree.h",
        "parse_tree_decls.h",
        "parse_tree_errors.h",
        "parse_tree_visitor.h",
        "visit_result.h",
    ],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    tags = ["nofixdeps"],  # generated and real files are interdependent
    deps = [
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/common:errors",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:deprecation_warning_cc_proto",
        "//zetasql/public:error_helpers",
        "//zetasql/public:error_location_cc_proto",
        "//zetasql/public:id_string",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
        "//zetasql/public:parse_resume_location",
        "//zetasql/public:strings",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "parse_tree_test",
    size = "small",
    srcs = ["parse_tree_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":parse_tree",
        ":parser",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common:errors",
        "//zetasql/common:status_payload_utils",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:error_location_cc_proto",
        "//zetasql/public:parse_location",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "parse_tree_errors_test",
    size = "small",
    srcs = ["parse_tree_errors_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":parse_tree",
        ":parser",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/common:status_payload_utils",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/testdata:test_schema_cc_proto",
    ],
)

cc_test(
    name = "unparser_test",
    size = "small",
    srcs = ["unparser_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":parser",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "keywords",
    srcs = [
        "keywords.cc",
    ],
    hdrs = [
        "keywords.h",
    ],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    deps = [
        ":bison_keyword_token_codes_inc",
        "//zetasql/base",
        "//zetasql/base:case",
        "//zetasql/base:map_util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "bison_keyword_token_codes_inc",
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    textual_hdrs = ["bison_keyword_token_codes.inc"],
)

genrule(
    name = "generate_bison_keyword_token_codes_inc",
    srcs = ["bison_parser.bison.h"],
    outs = ["bison_keyword_token_codes.inc"],
    cmd = "grep '    KW_.* = ' $(location bison_parser.bison.h) > $@",
)

cc_test(
    name = "keywords_test",
    srcs = ["keywords_test.cc"],
    copts = [
        "-Wno-pessimizing-move",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
    ],
    data = [
        "bison_parser.y",
        "flex_tokenizer.l",
    ],
    deps = [
        ":keywords",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base",
        "//zetasql/base:path",
        "@com_google_absl//absl/strings",
        "@com_googlesource_code_re2//:re2",
    ],
)

# Flex lexer
genlex(
    name = "flex_tokenizer_generated",
    src = "flex_tokenizer.l",
    out = "flex_tokenizer.flex.cc",
    includes = [
        "bison_parser.bison.h",
    ],
)

# Bison parser
genyacc(
    name = "bison_parser_generated",
    src = "bison_parser.y",
    extra_outs = [
        "location.hh",
        "position.hh",
        "stack.hh",
    ],
    header_out = "bison_parser.bison.h",
    source_out = "bison_parser.bison.cc",
)

exports_files([
    "keywords.h",
    "unparser.h",
])
