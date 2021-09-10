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

# Package for non-public protos.
package(default_visibility = ["//zetasql/base:zetasql_implementation"])

proto_library(
    name = "options_proto",
    srcs = ["options.proto"],
    deps = [
        "//zetasql/public:builtin_function_proto",
        "//zetasql/public:options_proto",
        "//zetasql/public:type_proto",
        "//zetasql/resolved_ast:resolved_node_kind_proto",
    ],
)

cc_proto_library(
    name = "options_cc_proto",
    deps = [":options_proto"],
)

java_proto_library(
    name = "options_java_proto",
    deps = [":options_proto"],
)

proto_library(
    name = "simple_catalog_proto",
    srcs = ["simple_catalog.proto"],
    deps = [
        ":function_proto",
        ":options_proto",
        "//zetasql/public:simple_constant_proto",
        "//zetasql/public:simple_table_proto",
        "//zetasql/public:type_proto",
    ],
)

cc_proto_library(
    name = "simple_catalog_cc_proto",
    deps = [":simple_catalog_proto"],
)

java_proto_library(
    name = "simple_catalog_java_proto",
    deps = [":simple_catalog_proto"],
)

proto_library(
    name = "function_proto",
    srcs = ["function.proto"],
    deps = [
        "//zetasql/public:deprecation_warning_proto",
        "//zetasql/public:function_proto",
        "//zetasql/public:options_proto",
        "//zetasql/public:parse_location_range_proto",
        "//zetasql/public:parse_resume_location_proto",
        "//zetasql/public:simple_table_proto",
        "//zetasql/public:type_proto",
        "//zetasql/public:value_proto",
        "//zetasql/resolved_ast:serialization_proto",
    ],
)

cc_proto_library(
    name = "function_cc_proto",
    deps = [":function_proto"],
)

java_proto_library(
    name = "function_java_proto",
    deps = [":function_proto"],
)

proto_library(
    name = "internal_error_location_proto",
    srcs = ["internal_error_location.proto"],
    deps = ["//zetasql/public:error_location_proto"],
)

cc_proto_library(
    name = "internal_error_location_cc_proto",
    deps = [":internal_error_location_proto"],
)
