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

load("@google_bazel_common//tools/jarjar:jarjar.bzl", "jarjar_library")
load("@google_bazel_common//tools/javadoc:javadoc.bzl", "javadoc_library")
load("//tools:maven.bzl", "pom_file")

TYPES_SRCS = [
    "ArrayType.java",
    "CivilTimeEncoder.java",
    "EnumType.java",
    "FileDescriptorSetsBuilder.java",
    "FunctionArgumentType.java",
    "ZetaSQLDescriptorPool.java",
    "ZetaSQLStrings.java",
    "ProtoType.java",
    "SimpleType.java",
    "StructType.java",
    "Type.java",
    "TypeFactory.java",
    "Value.java",
]

java_library(
    name = "types",
    srcs = TYPES_SRCS,
    tags = ["maven_coordinates=com.google.zetasql:types:${project.version}"],
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

jarjar_library(
    name = "types_jar",
    jars = [
        ":types",
        "//zetasql/proto:function_java_proto",
        "//zetasql/public:deprecation_warning_java_proto",
        "//zetasql/public:error_location_java_proto",
        "//zetasql/public:function_java_proto",
        "//zetasql/public:options_java_proto",
        "//zetasql/public:type_proto_java_proto",
        "//zetasql/public:value_java_proto",
        "//zetasql/public/proto:type_annotation_java_proto",
    ],
    rules = [
        "zap com.google.protobuf.**",
    ],
)

jarjar_library(
    name = "types_src",
    jars = [
        ":libtypes-src.jar",
    ],
)

pom_file(
    name = "types_pom",
    artifact_id = "types",
    artifact_name = "Types",
    targets = [
        ":types",
        "//zetasql/proto:function_java_proto",
        "//zetasql/public:deprecation_warning_java_proto",
        "//zetasql/public:error_location_java_proto",
        "//zetasql/public:function_java_proto",
        "//zetasql/public:options_java_proto",
        "//zetasql/public:type_proto_java_proto",
        "//zetasql/public:value_java_proto",
        "//zetasql/public/proto:type_annotation_java_proto",
    ],
)

CHANNEL_PROVIDER_SRCS = [
    "ClientChannelProvider.java",
]

java_library(
    name = "channel_provider",
    srcs = CHANNEL_PROVIDER_SRCS,
    tags = ["maven_coordinates=com.google.zetasql:client:${project.version}"],
    deps = [
        "@io_grpc_grpc_core//jar",
    ],
)

javadoc_library(
    name = "types_javadoc",
    srcs = TYPES_SRCS,
    root_packages = ["com.google.zetasql"],
    deps = [":types"],
)

JNI_CHANNEL_SRCS = [
    "JniChannelProvider.java",
]

java_library(
    name = "jni_channel",
    srcs = JNI_CHANNEL_SRCS,
    resources = [
        "//zetasql/local_service:liblocal_service_jni.so",
    ],
    tags = ["maven_coordinates=com.google.zetasql:jni-channel:${project.version}"],
    deps = [
        ":channel_provider",
        "//zetasql/local_service:local_service_jni",
        "@com_google_auto_service",
        "@io_grpc_grpc_core//jar",
        "@io_grpc_grpc_netty//jar",
        "@io_netty_netty_transport//jar",
        "@native_utils",
    ],
)

jarjar_library(
    name = "jni_channel_jar",
    jars = [
        ":jni_channel",
        "@native_utils",
    ],
    rules = [
        "rule cz.adamh.utils.** com.google.zetasql.@0",
        "rule io.grpc.** com.google.zetasql.@0",
        "rule io.opencensus.** com.google.zetasql.@0",
    ],
)

jarjar_library(
    name = "jni_channel_src",
    jars = [
        ":libjni_channel-src.jar",
    ],
)

pom_file(
    name = "jni_channel_pom",
    artifact_id = "jni-channel",
    artifact_name = "JNI Channel",
    targets = [
        ":jni_channel",
    ],
)

javadoc_library(
    name = "jni_channel_javadoc",
    srcs = JNI_CHANNEL_SRCS,
    root_packages = ["com.google.zetasql"],
    deps = [":jni_channel"],
)

CLIENT_SRCS = [
    "AllowedHintsAndOptions.java",
    "AnalyzerOptions.java",
    "Catalog.java",
    "Client.java",
    "Column.java",
    "Constant.java",
    "Function.java",
    "FunctionSignature.java",
    "ZetaSQLBuiltinFunctionOptions.java",
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
]

java_library(
    name = "client",
    srcs = CLIENT_SRCS,
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
        "@io_grpc_grpc_core//jar",
    ],
)

# This is a separate package to avoid circular dependency.
ANALYZER_SRCS = [
    "Analyzer.java",
    "AnalyzerHelper.java",
]

java_library(
    name = "analyzer",
    srcs = ANALYZER_SRCS,
    deps = [
        ":client",
        ":types",
        "//zetasql/java/resolvedast",
        "//zetasql/local_service:local_service_java_proto",
        "//zetasql/public/functions:datetime_java_proto",
        "@com_google_guava_guava//jar",
        "@io_grpc_grpc_core//jar",
    ],
)

jarjar_library(
    name = "client_jar",
    jars = [
        ":analyzer",
        ":channel_provider",
        ":client",
        "//zetasql/java/resolvedast",
        "//zetasql/local_service:local_service_java_grpc",
        "//zetasql/local_service:local_service_java_proto",
        "//zetasql/proto:options_java_proto",
        "//zetasql/proto:simple_catalog_java_proto",
        "//zetasql/public:builtin_function_java_proto",
        "//zetasql/public:parse_location_range_java_proto",
        "//zetasql/public:parse_resume_location_java_proto",
        "//zetasql/public:simple_constant_java_proto",
        "//zetasql/public:simple_table_java_proto",
        "//zetasql/public/functions:datetime_java_proto",
        "//zetasql/public/functions:normalize_mode_java_proto",
        "//zetasql/resolved_ast:resolved_ast_enums_java_proto",
        "//zetasql/resolved_ast:resolved_ast_java_proto",
        "//zetasql/resolved_ast:resolved_node_kind_java_proto",
        "//zetasql/resolved_ast:serialization_java_proto",
        "@io_grpc_grpc_context//jar",
        "@io_grpc_grpc_core//jar",
        "@io_grpc_grpc_netty//jar",
        "@io_grpc_grpc_protobuf//jar",
        "@io_grpc_grpc_protobuf_lite//jar",
        "@io_grpc_grpc_stub//jar",
        "@io_opencensus_opencensus_api//jar",
        "@io_opencensus_opencensus_contrib_grpc_metrics//jar",
    ],
    rules = [
        "rule io.grpc.** com.google.zetasql.@0",
        "rule io.opencensus.** com.google.zetasql.@0",
        "zap com.google.protobuf.**",
    ],
)

jarjar_library(
    name = "client_src",
    jars = [
        ":libanalyzer-src.jar",
        ":libchannel_provider-src.jar",
        ":libclient-src.jar",
        "//zetasql/java/resolvedast:libresolvedast-src.jar",
    ],
)

pom_file(
    name = "client_pom",
    artifact_id = "client",
    artifact_name = "Core Client",
    targets = [
        ":analyzer",
        ":channel_provider",
        ":client",
        "//zetasql/java/resolvedast",
        "//zetasql/local_service:local_service_java_grpc",
        "//zetasql/local_service:local_service_java_proto",
        "//zetasql/proto:options_java_proto",
        "//zetasql/proto:simple_catalog_java_proto",
        "//zetasql/public:builtin_function_java_proto",
        "//zetasql/public:parse_location_range_java_proto",
        "//zetasql/public:parse_resume_location_java_proto",
        "//zetasql/public:simple_constant_java_proto",
        "//zetasql/public:simple_table_java_proto",
        "//zetasql/public/functions:datetime_java_proto",
        "//zetasql/public/functions:normalize_mode_java_proto",
        "//zetasql/resolved_ast:resolved_ast_enums_java_proto",
        "//zetasql/resolved_ast:resolved_ast_java_proto",
        "//zetasql/resolved_ast:resolved_node_kind_java_proto",
        "//zetasql/resolved_ast:serialization_java_proto",
        "@io_grpc_grpc_context//jar",
        "@io_grpc_grpc_core//jar",
        "@io_grpc_grpc_netty//jar",
        "@io_grpc_grpc_protobuf//jar",
        "@io_grpc_grpc_protobuf_lite//jar",
        "@io_opencensus_opencensus_api//jar",
        "@io_opencensus_opencensus_contrib_grpc_metrics//jar",
    ],
)

javadoc_library(
    name = "client_javadoc",
    srcs = ANALYZER_SRCS + CHANNEL_PROVIDER_SRCS + CLIENT_SRCS,
    root_packages = ["com.google.zetasql"],
    deps = [
        ":analyzer",
        ":channel_provider",
        ":client",
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
