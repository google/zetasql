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

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load(":builddefs.bzl", "gen_parser_test")
load("//bazel:bison.bzl", "genyacc")
load("//bazel:flex.bzl", "genlex")

package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

genrule(
    name = "gen_extra_files",
    srcs = [
        "parse_tree_generated.h",
    ],
    outs = [
        "parse_tree_visitor.h",
        "parse_tree_decls.h",
        "parse_tree_accept_methods.inc",
    ],
    cmd = "$(location :gen_extra_files_tool) $(SRCS) $(OUTS)",
    exec_tools = [":gen_extra_files_tool"],
)

py_binary(
    name = "gen_extra_files_tool",
    srcs = ["gen_extra_files.py"],
    main = "gen_extra_files.py",
    python_version = "PY3",
)

genrule(
    name = "gen_headers",
    srcs = [
        "parse_tree_generated.h.template",
    ],
    outs = ["parse_tree_generated.h"],
    cmd = "$(location :gen_parse_tree) $(OUTS) $(SRCS)",
    exec_tools = [":gen_parse_tree"],
)

genrule(
    name = "gen_protos",
    srcs = [
        "parse_tree.proto.template",
    ],
    outs = ["parse_tree.proto"],
    cmd = "$(location :gen_parse_tree) $(OUTS) $(SRCS)",
    exec_tools = [":gen_parse_tree"],
)

genrule(
    name = "gen_parse_tree_serializer_cc",
    srcs = [
        "parse_tree_serializer.cc.template",
    ],
    outs = ["parse_tree_serializer.cc"],
    cmd = "$(location :gen_parse_tree) $(OUTS) $(SRCS)",
    exec_tools = [":gen_parse_tree"],
)

genrule(
    name = "gen_parse_tree_serializer_headers",
    srcs = [
        "parse_tree_serializer.h.template",
    ],
    outs = ["parse_tree_serializer.h"],
    cmd = "$(location :gen_parse_tree) $(OUTS) $(SRCS)",
    exec_tools = [":gen_parse_tree"],
)

proto_library(
    name = "parse_tree_proto",
    srcs = ["parse_tree.proto"],
    deps = [
        ":ast_enums_proto",
        "//zetasql/public:parse_location_range_proto",
        "//zetasql/public:type_proto",
    ],
)

cc_proto_library(
    name = "parse_tree_cc_proto",
    deps = [":parse_tree_proto"],
)

py_library(
    name = "generator_utils",
    srcs = ["generator_utils.py"],
)

py_test(
    name = "generator_utils_test",
    size = "small",
    srcs = ["generator_utils_test.py"],
    python_version = "PY3",
    deps = [
        ":generator_utils",
        "@io_abseil_py//absl/testing:absltest",
    ],
)

py_binary(
    name = "gen_parse_tree",
    srcs = ["gen_parse_tree.py"],
    main = "gen_parse_tree.py",
    deps = [
        ":ast_enums_py_pb2",
        ":generator_utils",
        "@io_abseil_py//absl:app",
        "@io_abseil_py//absl/flags",
        "@jinja//:jinja2",
    ],
)

proto_library(
    name = "ast_enums_proto",
    srcs = ["ast_enums.proto"],
)

cc_proto_library(
    name = "ast_enums_cc_proto",
    deps = [":ast_enums_proto"],
)

java_proto_library(
    name = "ast_enums_java_proto",
    deps = [":ast_enums_proto"],
)

java_proto_library(
    name = "ast_java_proto",
    deps = [":parse_tree_proto"],
)

load("@com_google_protobuf//:protobuf.bzl", "py_proto_library")

py_proto_library(
    name = "ast_enums_py_pb2",
    srcs = ["ast_enums.proto"],
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
        "ast_node.h",
        "ast_node_kind.h",
        "parse_tree.h",
        "parse_tree_decls.h",
        "parse_tree_errors.h",
        "parse_tree_visitor.h",
        "parser.h",
        "statement_properties.h",
        "unparser.h",
        "visit_result.h",
    ],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    tags = ["nofixdeps"],  # generated and real files are interdependent
    deps = [
        ":ast_enums_cc_proto",
        ":bison_parser_generated_lib",
        ":flex_istream",
        ":keywords",
        ":parse_tree",
        "//bazel:flex",
        "//zetasql/base",
        "//zetasql/base:arena",
        "//zetasql/base:arena_allocator",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:strings",
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
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
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
        "join_processor.cc",
    ],
    hdrs = [
        "bison_parser.bison.h",
        "bison_parser.h",
        "bison_parser_mode.h",
        "flex_tokenizer.h",
        "join_processor.h",
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
        "-Wno-char-subscripts",
        "-Wno-implicit-fallthrough",
        "-O2",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    tags = ["nofixdeps"],  # generated and real files are interdependent
    deps = [
        ":flex_istream",
        ":keywords",
        ":parse_tree",
        "//bazel:flex",
        "//zetasql/base",
        "//zetasql/base:arena",
        "//zetasql/base:arena_allocator",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/common:errors",
        "//zetasql/public:id_string",
        "//zetasql/public:language_options",
        "//zetasql/public:parse_location",
        "//zetasql/public:strings",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "flex_istream",
    hdrs = ["flex_istream.h"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        "//zetasql/base:logging",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "flex_istream_test",
    srcs = ["flex_istream_test.cc"],
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
        ":flex_istream",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:parse_helpers",
        "//zetasql/public:parse_resume_location",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "parse_tree_serializer",
    srcs = ["parse_tree_serializer.cc"],
    hdrs = [
        "parse_tree_serializer.h",
        "parser.h",
        "statement_properties.h",
    ],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":parse_tree",
        ":parse_tree_cc_proto",
        "//zetasql/base:arena",
        "//zetasql/base:arena_allocator",
        "//zetasql/base:status",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:variant",
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
        "ast_node.h",
        "ast_node_kind.h",
        "parse_tree.h",
        "parse_tree_decls.h",
        "parse_tree_errors.h",
        "parse_tree_generated.h",
        "parse_tree_visitor.h",
        "visit_result.h",
    ],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    tags = ["nofixdeps"],  # generated and real files are interdependent
    deps = [
        ":ast_enums_cc_proto",
        "//zetasql/base",
        "//zetasql/base:arena_allocator",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/common:utf_util",
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
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_googlesource_code_re2//:re2",
        "@icu//:common",
    ],
)

cc_test(
    name = "parse_tree_test",
    size = "small",
    srcs = ["parse_tree_test.cc"],
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
        ":parse_tree",
        ":parser",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common:errors",
        "//zetasql/common:status_payload_utils",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:error_location_cc_proto",
        "//zetasql/public:parse_location",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "parse_tree_errors_test",
    size = "small",
    srcs = ["parse_tree_errors_test.cc"],
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
        ":parse_tree",
        ":parser",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common:status_payload_utils",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "run_parser_test_lib",
    testonly = 1,
    srcs = ["run_parser_test.cc"],
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":parser",
        "//zetasql/base:edit_distance",
        "//zetasql/base:logging",
        "//zetasql/base:path",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:error_helpers",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_helpers",
        "//zetasql/public:parse_resume_location",
        "//zetasql/scripting:parse_helpers",
        "//zetasql/scripting:script_segment",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_file_based_test_driver//file_based_test_driver",
        "@com_google_file_based_test_driver//file_based_test_driver:run_test_case_result",
        "@com_google_file_based_test_driver//file_based_test_driver:test_case_options",
        "@com_googlesource_code_re2//:re2",
    ],
    # Otherwise tests in this .cc file may get dropped by the linker when statically linking tests
    # which depend on this library, since these tests aren't referenced from main.
    alwayslink = True,
)

cc_test(
    name = "unparser_test",
    size = "small",
    srcs = ["unparser_test.cc"],
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
        ":parser",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
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
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    deps = [
        ":bison_keyword_token_codes_inc",
        "//zetasql/base",
        "//zetasql/base:case",
        "//zetasql/base:map_util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:variant",
    ],
)

cc_library(
    name = "bison_keyword_token_codes_inc",
    copts = [
        "-Wno-char-subscripts",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
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
        "-Wno-char-subscripts",
        "-Wno-return-type",
        "-Wno-sign-compare",
        "-Wno-switch",
        "-Wno-unused-but-set-parameter",
        "-Wno-unused-function",
        "-Wnonnull-compare",
    ],
    data = [
        "bison_parser.y",
        "flex_tokenizer.l",
    ],
    deps = [
        ":keywords",
        "//zetasql/base",
        "//zetasql/base:path",
        "//zetasql/base:strings",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/flags:flag",
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

[gen_parser_test(
    filename = f.split("/")[-1],
) for f in glob(
    ["testdata/*.test"],
    exclude = ["testdata/parse_tree_serializer.test"],
)]

bzl_library(
    name = "builddefs_bzl",
    srcs = ["builddefs.bzl"],
)

exports_files([
    "keywords.h",
    "unparser.h",
])
