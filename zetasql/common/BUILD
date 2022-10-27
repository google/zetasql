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

package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
    features = ["parse_headers"],
)

cc_library(
    name = "status_payload_utils",
    srcs = ["status_payload_utils.cc"],
    hdrs = ["status_payload_utils.h"],
    deps = [
        "//zetasql/base:logging",
        "//zetasql/base:status",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_protobuf//:protobuf",
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
    name = "errors",
    srcs = ["errors.cc"],
    hdrs = ["errors.h"],
    deps = [
        ":status_payload_utils",
        "//zetasql/base",
        "//zetasql/base:ret_check",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/proto:internal_error_location_cc_proto",
        "//zetasql/public:deprecation_warning_cc_proto",
        "//zetasql/public:error_helpers",
        "//zetasql/public:error_location_cc_proto",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:parse_location",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "errors_test",
    size = "small",
    srcs = ["errors_test.cc"],
    deps = [
        ":errors",
        ":status_payload_utils",
        "//zetasql/base:source_location",
        "//zetasql/base:status",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/common/testing:proto_matchers",
        "//zetasql/public:error_helpers",
        "//zetasql/testdata:test_schema_cc_proto",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "timer_util",
    hdrs = ["timer_util.h"],
    deps = [
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "timer_util_test",
    srcs = ["timer_util_test.cc"],
    deps = [
        ":timer_util",
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "int_ops_util",
    hdrs = ["int_ops_util.h"],
    deps = [
    ],
)

cc_test(
    name = "int_ops_util_test",
    size = "small",
    srcs = ["int_ops_util_test.cc"],
    deps = [
        ":int_ops_util",
        "//zetasql/base/testing:zetasql_gtest_main",
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
    ],
)

cc_library(
    name = "proto_helper",
    srcs = ["proto_helper.cc"],
    hdrs = ["proto_helper.h"],
    deps = [
        ":errors",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:cc_wkt_protos",
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
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
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
    deps = [
        "//zetasql/base",
        "//zetasql/base:bits",
        "//zetasql/base:endian",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/numeric:bits",
        "@com_google_absl//absl/strings",
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
    hdrs = [
        "internal_value.h",
    ],
    deps = [
        ":float_margin",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "//zetasql/public/types",
        "@com_google_absl//absl/types:span",
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
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "builtin_function_internal",
    srcs = [
        "builtin_function_internal_1.cc",
        "builtin_function_internal_2.cc",
        "builtin_function_internal_3.cc",
    ],
    hdrs = ["builtin_function_internal.h"],
    deps = [
        ":errors",
        "//zetasql/base",
        "//zetasql/base:map_util",
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/proto:anon_output_with_report_cc_proto",
        "//zetasql/proto:options_cc_proto",
        "//zetasql/public:anon_function",
        "//zetasql/public:builtin_function_cc_proto",
        "//zetasql/public:builtin_function_options",
        "//zetasql/public:catalog",
        "//zetasql/public:cycle_detector",
        "//zetasql/public:function",
        "//zetasql/public:function_cc_proto",
        "//zetasql/public:language_options",
        "//zetasql/public:options_cc_proto",
        "//zetasql/public:type",
        "//zetasql/public:type_cc_proto",
        "//zetasql/public:value",
        "//zetasql/public/functions:date_time_util",
        "//zetasql/public/functions:datetime_cc_proto",
        "//zetasql/public/functions:string_format",
        "//zetasql/public/types",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googleapis//google/type:date_cc_proto",
        "@com_google_googleapis//google/type:timeofday_cc_proto",
        "@com_google_protobuf//:cc_wkt_protos",
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
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "json_parser",
    srcs = ["json_parser.cc"],
    hdrs = ["json_parser.h"],
    deps = [
        "//zetasql/base",
        "//zetasql/base:strings",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
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
        "//zetasql/base/testing:zetasql_gtest_main",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
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
        "@com_google_protobuf//:cc_wkt_protos",
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
        "//zetasql/base:map_util",
        "//zetasql/resolved_ast",
        "@com_google_absl//absl/container:flat_hash_set",
    ],
)

cc_library(
    name = "function_utils",
    srcs = ["function_utils.cc"],
    hdrs = ["function_utils.h"],
    deps = [
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

cc_test(
    name = "function_utils_test",
    srcs = ["function_utils_test.cc"],
    deps = [
        ":function_utils",
        "//zetasql/base/testing:zetasql_gtest_main",
        "//zetasql/public:function",
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
