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

load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
    features = ["parse_headers"],
)

cc_library(
    name = "status_payload_utils",
    srcs = ["status_payload_utils.cc"],
    hdrs = ["status_payload_utils.h"],
    deps = [
        "//zetasql/base:status",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_googleapis//google/rpc:status_cc_proto",
    ],
)

cc_test(
    name = "status_payload_utils_test",
    srcs = ["status_payload_utils_test.cc"],
    deps = [
        ":status_payload_utils",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/testdata:test_schema_cc_proto",
    ],
)

cc_library(
    name = "constant_utils",
    srcs = ["constant_utils.cc"],
    hdrs = ["constant_utils.h"],
    deps = [
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public/types:type",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "constant_utils_test",
    srcs = ["constant_utils_test.cc"],
    deps = [
        ":constant_utils",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:analyzer",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:language_options",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "//zetasql/testdata:sample_catalog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "errors",
    srcs = ["errors.cc"],
    hdrs = ["errors.h"],
    features = ["header_modules"],
    deps = [
        ":status_payload_utils",
        "//zetasql/base:check",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:deprecation_warning_cc_proto",
        "//zetasql/public:error_helpers",
        "//zetasql/public:error_location_cc_proto",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_test(
    name = "errors_test",
    size = "small",
    srcs = ["errors_test.cc"],
    deps = [
        ":errors",
        ":status_payload_utils",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:deprecation_warning_cc_proto",
        "//zetasql/public:error_helpers",
        "//zetasql/public:parse_location",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "timer_util",
    hdrs = ["timer_util.h"],
    deps = [
        ":thread_stack",
        "//zetasql/base:check",
        "//zetasql/public/proto:logging_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "timer_util_test",
    srcs = ["timer_util_test.cc"],
    deps = [
        ":timer_util",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "measure_utils",
    srcs = ["measure_utils.cc"],
    hdrs = ["measure_utils.h"],
    deps = [
        "//zetasql/analyzer:name_scope",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/parser:parse_tree",
        "//zetasql/public:id_string",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:strings",
        "//zetasql/public/types:type",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:column_factory",
        "//zetasql/resolved_ast:resolved_ast_rewrite_visitor",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

# `measure_analysis_utils` is separated from `measure_utils` to avoid a circular dependency between
# `measure_utils` and the analyzer.
cc_library(
    name = "measure_analysis_utils",
    srcs = ["measure_analysis_utils.cc"],
    hdrs = ["measure_analysis_utils.h"],
    deps = [
        ":internal_analyzer_options",
        ":measure_utils",
        "//zetasql/base",
        "//zetasql/base:arena",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "//zetasql/public:analyzer",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:catalog",
        "//zetasql/public:id_string",
        "//zetasql/public:language_options",
        "//zetasql/public:simple_catalog",
        "//zetasql/public/types",
        "//zetasql/public/types:type",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:column_factory",
        "//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "string_util",
    hdrs = ["string_util.h"],
    deps = [
        ":canonicalize_signed_zero_to_string",
        "//zetasql/base:string_numbers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "utf_util",
    srcs = ["utf_util.cc"],
    hdrs = ["utf_util.h"],
    deps = [
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@icu//:headers",
    ],
)

cc_library(
    name = "options_utils",
    srcs = ["options_utils.cc"],
    hdrs = ["options_utils.h"],
    deps = [
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:evaluator",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_helpers",
        "//zetasql/public:strings",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "options_utils_test",
    size = "small",
    srcs = ["options_utils_test.cc"],
    deps = [
        ":options_utils",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/common/testing:testing_proto_util",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:builtin_function_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:simple_catalog",
        "//zetasql/testdata:test_schema_cc_proto",
        "//zetasql/testing:test_value",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/flags:commandlineflag",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:reflection",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "utf_util_test",
    size = "small",
    srcs = ["utf_util_test.cc"],
    deps = [
        ":utf_util",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:functions_testlib",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "thread_stack",
    srcs = ["thread_stack.cc"],
    hdrs = ["thread_stack.h"],
    deps = [
        "//zetasql/base:logging",
        "//zetasql/base:status",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:log_severity",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/debugging:stacktrace",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "proto_helper",
    srcs = ["proto_helper.cc"],
    hdrs = ["proto_helper.h"],
    deps = [
        ":errors",
        ":thread_stack",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "proto_helper_test",
    size = "small",
    srcs = ["proto_helper_test.cc"],
    data = [
        "//zetasql/testdata:test_files",
        "//zetasql/testdata:test_schema_proto",
    ],
    deps = [
        ":proto_helper",
        "//zetasql/base:path",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:testing_proto_util",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "parsed_templated_sql_function",
    hdrs = ["parsed_templated_sql_function.h"],
    deps = [
        "//zetasql/parser",
        "//zetasql/public:function_headers",
        "//zetasql/public:templated_sql_function",
    ],
)

cc_library(
    name = "lazy_resolution_catalog",
    srcs = ["lazy_resolution_catalog.cc"],
    hdrs = ["lazy_resolution_catalog.h"],
    deps = [
        ":errors",
        ":parsed_templated_sql_function",
        ":thread_stack",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:stl_util",
        "//zetasql/base:strings",
        "//zetasql/parser",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:analyzer",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:catalog",
        "//zetasql/public:constant",
        "//zetasql/public:cycle_detector",
        "//zetasql/public:error_helpers",
        "//zetasql/public:function",
        "//zetasql/public:function_cc_proto",
        "//zetasql/public:module_details",
        "//zetasql/public:multi_catalog",
        "//zetasql/public:non_sql_function",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
        "//zetasql/public:parse_resume_location",
        "//zetasql/public:remote_tvf_factory",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:sql_constant",
        "//zetasql/public:sql_function",
        "//zetasql/public:sql_tvf",
        "//zetasql/public:sql_view",
        "//zetasql/public:templated_sql_function",
        "//zetasql/public:templated_sql_tvf",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "lazy_resolution_catalog_test",
    size = "small",
    srcs = ["lazy_resolution_catalog_test.cc"],
    deps = [
        ":lazy_resolution_catalog",
        ":parsed_templated_sql_function",
        ":status_payload_utils",
        "//zetasql/base",
        "//zetasql/base:check",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/parser",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:builtin_function",
        "//zetasql/public:builtin_function_options",
        "//zetasql/public:cycle_detector",
        "//zetasql/public:error_helpers",
        "//zetasql/public:function",
        "//zetasql/public:function_cc_proto",
        "//zetasql/public:function_headers",
        "//zetasql/public:language_options",
        "//zetasql/public:module_details",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:sql_constant",
        "//zetasql/public:sql_function",
        "//zetasql/public:sql_view",
        "//zetasql/public:type",
        "//zetasql/resolved_ast",
        "//zetasql/testdata:sample_catalog",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "float_margin",
    hdrs = [
        "float_margin.h",
    ],
    deps = [
        ":string_util",
        "//zetasql/base",
        "//zetasql/base:mathutil",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "multiprecision_int",
    srcs = [
        "multiprecision_int.cc",
        "multiprecision_int_impl.cc",
    ],
    hdrs = [
        "multiprecision_int.h",
        "multiprecision_int_impl.h",
    ],
    features = ["header_modules"],
    deps = [
        "//zetasql/base",
        "//zetasql/base:bits",
        "//zetasql/base:endian",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/numeric:bits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:compare",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "multiprecision_int_test",
    srcs = ["multiprecision_int_test.cc"],
    deps = [
        ":multiprecision_int",
        "//zetasql/base:mathutil",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "internal_value",
    srcs = [
        "internal_value.cc",
    ],
    hdrs = [
        "internal_value.h",
    ],
    deps = [
        ":float_margin",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:language_options",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "//zetasql/public/types:value_representations",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "internal_value_test",
    srcs = [
        "internal_value_test.cc",
    ],
    deps = [
        ":internal_value",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:language_options",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "//zetasql/public/types:type",
        "//zetasql/testing:test_value",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "initialize_required_fields",
    srcs = ["initialize_required_fields.cc"],
    hdrs = ["initialize_required_fields.h"],
    deps = [
        "@com_google_protobuf//:protobuf",
    ],
)

proto_library(
    name = "initialize_required_fields_test_proto",
    testonly = 1,
    srcs = ["initialize_required_fields_test.proto"],
)

cc_proto_library(
    name = "initialize_required_fields_test_cc_proto",
    testonly = 1,
    deps = [":initialize_required_fields_test_proto"],
)

cc_test(
    name = "initialize_required_fields_test",
    size = "small",
    srcs = ["initialize_required_fields_test.cc"],
    deps = [
        ":initialize_required_fields",
        ":initialize_required_fields_test_cc_proto",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "builtins_output_properties",
    hdrs = ["builtins_output_properties.h"],
    deps = [
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:options_cc_proto",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
    ],
)

cc_library(
    name = "builtin_function_internal",
    srcs = [
        "builtin_enum_type.cc",
        "builtin_function_array.cc",
        "builtin_function_differential_privacy.cc",
        "builtin_function_distance.cc",
        "builtin_function_graph.cc",
        "builtin_function_internal_1.cc",
        "builtin_function_internal_2.cc",
        "builtin_function_internal_3.cc",
        "builtin_function_map.cc",
        "builtin_function_match_recognize.cc",
        "builtin_function_measure.cc",
        "builtin_function_range.cc",
        "builtin_function_sketches.cc",
    ],
    hdrs = ["builtin_function_internal.h"],
    deps = [
        ":builtins_output_properties",
        ":errors",
        "//zetasql/base",
        "//zetasql/base:check",
        "//zetasql/base:flat_set",
        "//zetasql/base:map_util",
        "//zetasql/base:no_destructor",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/proto:anon_output_with_report_cc_proto",
        "//zetasql/proto:options_cc_proto",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:anon_function",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:builtin_function_options",
        "//zetasql/public:catalog",
        "//zetasql/public:coercer",
        "//zetasql/public:cycle_detector",
        "//zetasql/public:function",
        "//zetasql/public:function_cc_proto",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:strings",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public/annotation:collation",
        "//zetasql/public/functions:array_zip_mode_cc_proto",
        "//zetasql/public/functions:bitwise_agg_mode_cc_proto",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/public/functions:datetime_cc_proto",
        "//zetasql/public/functions:differential_privacy_cc_proto",
        "//zetasql/public/functions:string_format",
        "//zetasql/public/functions:unsupported_fields_cc_proto",
        "//zetasql/public/types",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_googleapis//google/type:date_cc_proto",
        "@com_google_googleapis//google/type:timeofday_cc_proto",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "simple_evaluator_table_iterator",
    srcs = ["simple_evaluator_table_iterator.cc"],
    hdrs = ["simple_evaluator_table_iterator.h"],
    deps = [
        "//zetasql/base",
        "//zetasql/base:clock",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/public:catalog",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "simple_evaluator_table_iterator_test",
    srcs = ["simple_evaluator_table_iterator_test.cc"],
    deps = [
        ":simple_evaluator_table_iterator",
        "//zetasql/base:clock",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:simple_catalog",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "evaluator_test_table",
    testonly = 1,
    hdrs = ["evaluator_test_table.h"],
    deps = [
        ":simple_evaluator_table_iterator",
        "//zetasql/base",
        "//zetasql/base:clock",
        "//zetasql/base:status",
        "//zetasql/public:catalog",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "unicode_utils",
    srcs = ["unicode_utils.cc"],
    hdrs = ["unicode_utils.h"],
    deps = [
        "//zetasql/base",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/strings",
        "@icu//:common",
    ],
)

cc_test(
    name = "unicode_utils_test",
    size = "small",
    srcs = ["unicode_utils_test.cc"],
    deps = [
        ":unicode_utils",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/flags:flag",
    ],
)

cc_library(
    name = "json_parser",
    srcs = ["json_parser.cc"],
    hdrs = ["json_parser.h"],
    deps = [
        ":thread_stack",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:strings",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_googlesource_code_re2//:re2",
        "@icu//:headers",
    ],
)

cc_test(
    name = "json_parser_test",
    srcs = ["json_parser_test.cc"],
    deps = [
        ":json_parser",
        "//zetasql/base",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/compliance:depth_limit_detector_test_cases",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "json_util",
    srcs = ["json_util.cc"],
    hdrs = ["json_util.h"],
    deps = [
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "json_util_test",
    srcs = ["json_util_test.cc"],
    deps = [
        ":json_util",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "evaluator_registration_utils",
    srcs = ["evaluator_registration_utils.cc"],
    hdrs = ["evaluator_registration_utils.h"],
    deps = [
        "//zetasql/base:status",
        "//zetasql/public:collator",
        "//zetasql/reference_impl/functions:register_all",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "proto_from_iterator",
    srcs = [
        "proto_from_iterator.cc",
    ],
    hdrs = [
        "proto_from_iterator.h",
    ],
    deps = [
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:proto_value_conversion",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "proto_from_iterator_test",
    size = "small",
    srcs = ["proto_from_iterator_test.cc"],
    deps = [
        ":proto_from_iterator",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/public:evaluator_table_iterator",
        "//zetasql/public:simple_catalog",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "aggregate_null_handling",
    srcs = ["aggregate_null_handling.cc"],
    hdrs = ["aggregate_null_handling.h"],
    deps = [
        "//zetasql/base:case",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/container:flat_hash_set",
    ],
)

cc_library(
    name = "function_utils",
    srcs = ["function_utils.cc"],
    hdrs = ["function_utils.h"],
    deps = [
        "//zetasql/base:check",
        "//zetasql/public:function_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "canonicalize_signed_zero_to_string",
    srcs = ["canonicalize_signed_zero_to_string.cc"],
    hdrs = ["canonicalize_signed_zero_to_string.h"],
    deps = ["@com_google_absl//absl/flags:flag"],
)

cc_library(
    name = "internal_analyzer_options",
    hdrs = ["internal_analyzer_options.h"],
    deps = ["//zetasql/public:analyzer_options"],
)

cc_library(
    name = "internal_property_graph",
    hdrs = ["internal_property_graph.h"],
    deps = [
        "//zetasql/base:check",
        "//zetasql/resolved_ast",
    ],
)

cc_library(
    name = "internal_analyzer_output_properties",
    hdrs = ["internal_analyzer_output_properties.h"],
    deps = [
        "//zetasql/public:analyzer_output_properties",
        "//zetasql/resolved_ast",
        "//zetasql/resolved_ast:target_syntax",
    ],
)

cc_test(
    name = "function_utils_test",
    srcs = ["function_utils_test.cc"],
    deps = [
        ":function_utils",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:function",
    ],
)

cc_library(
    name = "graph_element_utils",
    srcs = ["graph_element_utils.cc"],
    hdrs = ["graph_element_utils.h"],
    deps = [
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:json_value",
        "//zetasql/public:value",
        "//zetasql/public/functions:json",
        "//zetasql/public/types",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "graph_element_utils_test",
    srcs = ["graph_element_utils_test.cc"],
    deps = [
        ":graph_element_utils",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:json_value",
        "//zetasql/public/functions:json",
        "//zetasql/public/types",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "aggregate_null_handling_test",
    srcs = ["aggregate_null_handling_test.cc"],
    deps = [
        ":aggregate_null_handling",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:analyzer",
        "//zetasql/public:analyzer_options",
        "//zetasql/public:analyzer_output",
        "//zetasql/public:options_cc_proto",
        "//zetasql/resolved_ast",
        "//zetasql/testdata:sample_catalog",
    ],
)

cc_library(
    name = "function_signature_testutil",
    testonly = True,
    srcs = ["function_signature_testutil.cc"],
    hdrs = ["function_signature_testutil.h"],
    deps = [
        "//zetasql/public:function_cc_proto",
    ],
)

cc_library(
    name = "warning_sink",
    srcs = [
        "warning_sink.cc",
    ],
    hdrs = [
        "warning_sink.h",
    ],
    deps = [
        ":status_payload_utils",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:deprecation_warning_cc_proto",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "warning_sink_test",
    srcs = [
        "warning_sink_test.cc",
    ],
    deps = [
        ":errors",
        ":warning_sink",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:parse_location",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "scope_error_catalog",
    srcs = ["scope_error_catalog.cc"],
    hdrs = ["scope_error_catalog.h"],
    deps = [
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/public:catalog",
        "//zetasql/public:strings",
        "//zetasql/public/types",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "scope_error_catalog_test",
    srcs = [
        "scope_error_catalog_test.cc",
    ],
    deps = [
        ":scope_error_catalog",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:catalog",
        "//zetasql/public:function",
        "//zetasql/public:function_headers",
        "//zetasql/public:simple_catalog",
        "//zetasql/public/types",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "resolution_scope",
    srcs = ["resolution_scope.cc"],
    hdrs = ["resolution_scope.h"],
    deps = [
        "//zetasql/base:ret_check",
        "//zetasql/base:strings",
        "//zetasql/parser:parse_tree",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_test(
    name = "resolution_scope_test",
    srcs = [
        "resolution_scope_test.cc",
    ],
    deps = [
        ":resolution_scope",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/parser",
        "//zetasql/public:language_options",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
    ],
)
