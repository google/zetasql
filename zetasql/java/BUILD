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
#

# Java API for ZetaSQL.

package(
    default_visibility = ["//zetasql:__subpackages__"],
)

java_library(
    name = "types",
    srcs = [
        "ArrayType.java",
        "CivilTimeEncoder.java",
        "EnumType.java",
        "FileDescriptorSetsBuilder.java",
        "FunctionArgumentType.java",
        "ProtoType.java",
        "SimpleType.java",
        "StructType.java",
        "Type.java",
        "TypeFactory.java",
        "Value.java",
        "ZetaSQLDescriptorPool.java",
        "ZetaSQLStrings.java",
    ],
    deps = [
        "//zetasql/proto:function_java_proto",
        "//zetasql/public:function_java_proto",
        "//zetasql/public:options_java_proto",
        "//zetasql/public:type_proto_java_proto",
        "//zetasql/public:value_java_proto",
        "//zetasql/public/proto:type_annotation_java_proto",
        "@com_google_code_findbugs_jsr305//jar",
        "@com_google_errorprone_error_prone_annotations//jar",  # buildcleaner: keep
        "@com_google_guava_guava//jar",
        "@com_google_protobuf//:protobuf_java",
        "@com_google_protobuf//:protobuf_java_util",
        "@joda_time//jar",
    ],
)

java_library(
    name = "channel_provider",
    srcs = [
        "ClientChannelProvider.java",
    ],
    deps = [
        "@io_grpc_grpc_java//core",
    ],
)

java_library(
    name = "jni_channel",
    srcs = [
        "JniChannelProvider.java",
    ],
    resources = [
        "//zetasql/local_service:liblocal_service_jni.so",
    ],
    deps = [
        ":channel_provider",
        "//zetasql/local_service:local_service_jni",
        "@com_google_auto_service",
        "@io_grpc_grpc_java//core",
        "@io_grpc_grpc_java//netty",
        "@io_netty_netty_transport//jar",
        "@native_utils",
    ],
)

java_binary(
    name = "zetasql_jni",
    srcs = [
        "JniChannelProvider.java",
    ],
    resources = [
        "//zetasql/local_service:liblocal_service_jni.so",
    ],
    runtime_deps = [
        ":analyzer",
        ":client",
        ":types",
    ],
    deps = [
        ":channel_provider",
        "//zetasql/local_service:local_service_jni",
        "@com_google_auto_service",
        "@io_grpc_grpc_java//core",
        "@io_grpc_grpc_java//netty",
        "@io_netty_netty_transport//jar",
        "@native_utils",
    ],
)

java_library(
    name = "client",
    srcs = [
        "AllowedHintsAndOptions.java",
        "AnalyzerOptions.java",
        "Catalog.java",
        "Client.java",
        "Column.java",
        "Constant.java",
        "Function.java",
        "FunctionSignature.java",
        "LanguageOptions.java",
        "Model.java",
        "NotFoundException.java",
        "ParseLocationRange.java",
        "ParseResumeLocation.java",
        "Procedure.java",
        "ResolvedFunctionCallInfo.java",
        "SimpleCatalog.java",
        "SimpleColumn.java",
        "SimpleModel.java",
        "SimpleTable.java",
        "SqlException.java",
        "SqlFormatter.java",
        "TVFRelation.java",
        "TVFSignature.java",
        "Table.java",
        "TableValuedFunction.java",
        "ZetaSQLBuiltinFunctionOptions.java",
    ],
    deps = [
        ":channel_provider",
        ":types",
        "//zetasql/local_service:local_service_java_grpc",
        "//zetasql/local_service:local_service_java_proto",
        "//zetasql/proto:function_java_proto",
        "//zetasql/proto:options_java_proto",
        "//zetasql/proto:simple_catalog_java_proto",
        "//zetasql/public:builtin_function_java_proto",
        "//zetasql/public:function_java_proto",
        "//zetasql/public:options_java_proto",
        "//zetasql/public:parse_location_range_java_proto",
        "//zetasql/public:parse_resume_location_java_proto",
        "//zetasql/public:simple_constant_java_proto",
        "//zetasql/public:simple_table_java_proto",
        "//zetasql/public:type_proto_java_proto",
        "//zetasql/public/functions:datetime_java_proto",
        "//zetasql/public/functions:normalize_mode_java_proto",
        "//zetasql/resolved_ast:resolved_node_kind_java_proto",  # buildcleaner: keep
        "@com_google_api_grpc_proto_google_common_protos//jar",
        "@com_google_auto_value",
        "@com_google_code_findbugs_jsr305//jar",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf//:protobuf_java",
        "@io_grpc_grpc_java//core",
    ],
)

# This is a separate package to avoid circular dependency.
java_library(
    name = "analyzer",
    srcs = [
        "Analyzer.java",
        "AnalyzerHelper.java",
    ],
    deps = [
        ":client",
        ":types",
        "//zetasql/java/resolvedast",
        "//zetasql/local_service:local_service_java_proto",
        "//zetasql/public/functions:datetime_java_proto",
        "@com_google_guava_guava//jar",
        "@io_grpc_grpc_java//core",
    ],
)

java_library(
    name = "testaccess",
    testonly = 1,
    srcs = ["TestAccess.java"],
    deps = [
        ":types",
        "@com_google_guava_guava//jar",
    ],
)
