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

# Java API for ZetaSQL.

load("@google_bazel_common//tools/jarjar:jarjar.bzl", "jarjar_library")
load("@google_bazel_common//tools/javadoc:javadoc.bzl", "javadoc_library")
load("//tools:maven.bzl", "pom_file")

package(
    default_visibility = ["//visibility:public"],
)

TYPES_SRCS = [
    "ArrayType.java",
    "BuiltinDescriptorPool.java",
    "CivilTimeEncoder.java",
    "EnumType.java",
    "FileDescriptorSetsBuilder.java",
    "DescriptorPool.java",
    "ImmutableDescriptorPool.java",
    "ZetaSQLDescriptorPool.java",
    "ZetaSQLStrings.java",
    "IntervalValue.java",
    "ProtoType.java",
    "SimpleType.java",
    "StructType.java",
    "Type.java",
    "TypeFactory.java",
    "TypeParameters.java",
    "TVFRelation.java",
    "Value.java",
]

java_library(
    name = "types",
    srcs = TYPES_SRCS,
    tags = ["maven_coordinates=com.google.zetasql:zetasql-types:${project.version}"],
    deps = [
        "//zetasql/proto:anon_output_with_report_java_proto",
        "//zetasql/proto:function_java_proto",
        "//zetasql/public:options_java_proto",
        "//zetasql/public:type_parameters_java_proto",
        "//zetasql/public:type_proto_java_proto",
        "//zetasql/public:value_java_proto",
        "//zetasql/public/functions:datetime_java_proto",
        "//zetasql/public/functions:normalize_mode_java_proto",
        "//zetasql/public/functions:rounding_mode_java_proto",
        "//zetasql/public/proto:type_annotation_java_proto",
        "@com_google_auto_value",
        "@com_google_protobuf//:protobuf_java",
        "@com_google_protobuf//:protobuf_java_util",
        "@maven//:com_google_api_grpc_proto_google_common_protos",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_errorprone_error_prone_annotations",  # buildcleaner: keep
        "@maven//:com_google_guava_guava",
        "@maven//:joda_time_joda_time",
    ],
)

java_library(
    name = "types_proto_resources",
    resources = [
        "//zetasql/public/proto:type_annotation_filegroup",
    ],
)

jarjar_library(
    name = "types_jar",
    jars = [
        ":types",
        ":types_proto_resources",
        "//zetasql/proto:anon_output_with_report_java_proto",
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
    artifact_id = "zetasql-types",
    artifact_name = "Types",
    targets = [
        ":types",
        "//zetasql/proto:anon_output_with_report_java_proto",
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

javadoc_library(
    name = "types_javadoc",
    srcs = TYPES_SRCS,
    root_packages = ["com.google.zetasql"],
    deps = [":types"],
)

CHANNEL_PROVIDER_SRCS = [
    "ClientChannelProvider.java",
]

java_library(
    name = "channel_provider",
    srcs = CHANNEL_PROVIDER_SRCS,
    tags = ["maven_coordinates=com.google.zetasql:zetasql-client:${project.version}"],
    deps = [
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_core",
    ],
)

JNI_CHANNEL_SRCS = [
    "JniChannelProvider.java",
]

java_library(
    name = "jni_channel_darwin",
    resources = select({
        "@bazel_tools//src/conditions:darwin": ["//zetasql/local_service:liblocal_service_jni.dylib"],
        "//conditions:default": [],
    }),
    tags = ["maven_coordinates=com.google.zetasql:zetasql-jni-channel-darwin:${project.version}"],
)

jarjar_library(
    name = "jni_channel_darwin_jar",
    jars = [
        ":jni_channel_darwin",
    ],
)

jarjar_library(
    name = "jni_channel_darwin_src",
    jars = [
        ":libjni_channel_darwin-src.jar",
    ],
)

pom_file(
    name = "jni_channel_darwin_pom",
    artifact_id = "zetasql-jni-channel-darwin",
    artifact_name = "JNI Channel for Darwin",
    targets = [
        ":jni_channel_darwin",
    ],
)

javadoc_library(
    name = "jni_channel_darwin_javadoc",
    srcs = JNI_CHANNEL_SRCS,
    root_packages = ["com.google.zetasql"],
    deps = [":jni_channel"],
)

java_library(
    name = "jni_channel_linux",
    resources = select({
        "@bazel_tools//src/conditions:linux_x86_64": ["//zetasql/local_service:liblocal_service_jni.so"],
        "//conditions:default": [],
    }),
    tags = ["maven_coordinates=com.google.zetasql:zetasql-jni-channel-linux:${project.version}"],
)

jarjar_library(
    name = "jni_channel_linux_jar",
    jars = [
        ":jni_channel_linux",
    ],
)

jarjar_library(
    name = "jni_channel_linux_src",
    jars = [
        ":libjni_channel_linux-src.jar",
    ],
)

pom_file(
    name = "jni_channel_linux_pom",
    artifact_id = "zetasql-jni-channel-linux",
    artifact_name = "JNI Channel for Linux",
    targets = [
        ":jni_channel_linux",
    ],
)

javadoc_library(
    name = "jni_channel_linux_javadoc",
    srcs = JNI_CHANNEL_SRCS,
    root_packages = ["com.google.zetasql"],
    deps = [":jni_channel"],
)

java_library(
    name = "jni_channel",
    srcs = JNI_CHANNEL_SRCS,
    tags = ["maven_coordinates=com.google.zetasql:zetasql-jni-channel:${project.version}"],
    runtime_deps = [
        ":jni_channel_darwin",
        ":jni_channel_linux",
    ],
    deps = [
        ":channel_provider",
        "//zetasql/local_service:local_service_jni",
        "@com_google_auto_service",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_netty_netty_common",
        "@maven//:io_netty_netty_transport",
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
    artifact_id = "zetasql-jni-channel",
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

COMMON_SRCS = [
    "LanguageOptions.java",
    "Client.java",
]

java_library(
    name = "common",
    srcs = COMMON_SRCS,
    deps = [
        ":channel_provider",
        "//zetasql/local_service:local_service_java_grpc",
        "//zetasql/local_service:local_service_java_proto",
        "//zetasql/proto:options_java_proto",
        "//zetasql/public:options_java_proto",
        "//zetasql/resolved_ast:resolved_node_kind_java_proto",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
    ],
)

CATALOG_SRCS = [
    "AllowedHintsAndOptions.java",
    "Column.java",
    "Connection.java",
    "Constant.java",
    "DescriptorPoolSerializer.java",
    "Function.java",
    "FunctionArgumentType.java",
    "FunctionSignature.java",
    "ZetaSQLBuiltinFunctionOptions.java",
    "Model.java",
    "NotFoundException.java",
    "Parameter.java",
    "ParseLocationRange.java",
    "ParseResumeLocation.java",
    "Procedure.java",
    "ResolvedFunctionCallInfo.java",
    "SqlException.java",
    "SqlFormatter.java",
    "TableContent.java",
    "TVFSignature.java",
    "Table.java",
    "TableValuedFunction.java",
]

SIMPLE_CATALOG_SRCS = [
    "AnalyzerOptions.java",
    "EvaluatorTableModifyResponse.java",
    "PreparedExpression.java",
    "PreparedModify.java",
    "PreparedQuery.java",
    "Catalog.java",
    "SimpleCatalog.java",
    "SimpleColumn.java",
    "SimpleConnection.java",
    "SimpleModel.java",
    "SimpleTable.java",
]

java_library(
    name = "client",
    srcs = COMMON_SRCS + SIMPLE_CATALOG_SRCS + CATALOG_SRCS,
    deps = [
        ":channel_provider",
        ":types",
        "//zetasql/local_service:local_service_java_grpc",
        "//zetasql/local_service:local_service_java_proto",
        "//zetasql/proto:function_java_proto",
        "//zetasql/proto:options_java_proto",
        "//zetasql/proto:simple_catalog_java_proto",
        "//zetasql/public:builtin_function_java_proto",
        "//zetasql/public:deprecation_warning_java_proto",
        "//zetasql/public:error_location_java_proto",
        "//zetasql/public:formatter_options_java_proto",
        "//zetasql/public:function_java_proto",
        "//zetasql/public:options_java_proto",
        "//zetasql/public:parse_location_range_java_proto",
        "//zetasql/public:parse_resume_location_java_proto",
        "//zetasql/public:simple_constant_java_proto",
        "//zetasql/public:simple_table_java_proto",
        "//zetasql/public:type_proto_java_proto",
        "//zetasql/public/functions:datetime_java_proto",  # unuseddeps: keep # buildcleaner: keep
        "//zetasql/public/functions:normalize_mode_java_proto",  # unuseddeps: keep # buildcleaner: keep
        "//zetasql/public/functions:rounding_mode_java_proto",  # unuseddeps: keep # buildcleaner: keep
        "//zetasql/resolved_ast:resolved_node_kind_java_proto",  # buildcleaner: keep
        "//zetasql/resolved_ast:serialization_java_proto",
        "@com_google_auto_value",
        "@com_google_protobuf//:protobuf_java",  # unuseddeps: keep # buildcleaner: keep
        "@maven//:com_google_api_grpc_proto_google_common_protos",  # unuseddeps: keep # buildcleaner: keep
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:javax_annotation_javax_annotation_api",  # unuseddeps: keep # buildcleaner: keep
    ],
)

java_library(
    name = "catalog",
    srcs = CATALOG_SRCS,
    deps = [
        ":common",
        ":types",
        "//zetasql/local_service:local_service_java_proto",
        "//zetasql/proto:function_java_proto",
        "//zetasql/proto:options_java_proto",
        "//zetasql/public:builtin_function_java_proto",
        "//zetasql/public:deprecation_warning_java_proto",
        "//zetasql/public:error_location_java_proto",
        "//zetasql/public:formatter_options_java_proto",
        "//zetasql/public:function_java_proto",
        "//zetasql/public:options_java_proto",
        "//zetasql/public:parse_location_range_java_proto",
        "//zetasql/public:parse_resume_location_java_proto",
        "//zetasql/public:simple_constant_java_proto",
        "//zetasql/public:type_proto_java_proto",
        "//zetasql/resolved_ast:serialization_java_proto",
        "@com_google_auto_value",
        "@com_google_protobuf//:protobuf_java",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_core",
        "@maven//:javax_annotation_javax_annotation_api",  # unuseddeps: keep # buildcleaner: keep
    ],
)

java_library(
    name = "simple_catalog",
    srcs = SIMPLE_CATALOG_SRCS,
    deps = [
        ":catalog",
        ":channel_provider",
        ":common",
        ":types",
        "//zetasql/local_service:local_service_java_grpc",
        "//zetasql/local_service:local_service_java_proto",
        "//zetasql/proto:function_java_proto",
        "//zetasql/proto:options_java_proto",
        "//zetasql/proto:simple_catalog_java_proto",
        "//zetasql/public:options_java_proto",
        "//zetasql/public:simple_constant_java_proto",
        "//zetasql/public:simple_table_java_proto",
        "//zetasql/public:type_proto_java_proto",
        "@com_google_auto_value",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:javax_annotation_javax_annotation_api",  # unuseddeps: keep # buildcleaner: keep
    ],
)

# This is a separate package to avoid circular dependency.
ANALYZER_SRCS = [
    "Analyzer.java",
    "AnalyzerHelper.java",
]

java_library(
    name = "debug_printable_node",
    srcs = ["DebugPrintableNode.java"],
    deps = ["@maven//:com_google_guava_guava"],
)

java_library(
    name = "analyzer",
    srcs = ANALYZER_SRCS,
    deps = [
        ":client",
        ":types",
        "//java/com/google/zetasql/resolvedast",
        "//zetasql/local_service:local_service_java_proto",
        "//zetasql/public/functions:datetime_java_proto",
        "//zetasql/resolved_ast:resolved_ast_java_proto",  # buildcleaner: keep
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_core",
    ],
)

java_library(
    name = "parser",
    srcs = ["Parser.java"],
    deps = [
        ":client",
        "//java/com/google/zetasql/parser",
        "//zetasql/local_service:local_service_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_core",
    ],
)

jarjar_library(
    name = "client_jar",
    jars = [
        ":analyzer",
        ":channel_provider",
        ":client",
        ":debug_printable_node",
        "//java/com/google/zetasql/resolvedast",
        "//java/com/google/zetasql/testing:sql_compliance_service_java_grpc",
        "//java/com/google/zetasql/testing:sql_compliance_service_java_proto",
        "//zetasql/compliance:test_driver_java_proto",
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
        "//zetasql/public/functions:rounding_mode_java_proto",
        "//zetasql/resolved_ast:resolved_ast_enums_java_proto",
        "//zetasql/resolved_ast:resolved_ast_java_proto",
        "//zetasql/resolved_ast:resolved_node_kind_java_proto",
        "//zetasql/resolved_ast:serialization_java_proto",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_protobuf_lite",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:io_grpc_grpc_api",
        "@com_google_code_gson_gson//jar",
        "@maven//:io_opencensus_opencensus_api",
        "@maven//:io_opencensus_opencensus_contrib_grpc_metrics",
    ],
    rules = [
        "rule com.google.gson.** com.google.zetasql.@0",
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
        ":libdebug_printable_node-src.jar",
        "//java/com/google/zetasql/resolvedast:libresolvedast-src.jar",
    ],
)

pom_file(
    name = "client_pom",
    artifact_id = "zetasql-client",
    artifact_name = "Core Client",
    targets = [
        ":analyzer",
        ":channel_provider",
        ":client",
        ":debug_printable_node",
        "//java/com/google/zetasql/resolvedast",
        "//java/com/google/zetasql/testing:sql_compliance_service_java_grpc",
        "//java/com/google/zetasql/testing:sql_compliance_service_java_proto",
        "//zetasql/compliance:test_driver_java_proto",
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
        "//zetasql/public/functions:rounding_mode_java_proto",
        "//zetasql/resolved_ast:resolved_ast_enums_java_proto",
        "//zetasql/resolved_ast:resolved_ast_java_proto",
        "//zetasql/resolved_ast:resolved_node_kind_java_proto",
        "//zetasql/resolved_ast:serialization_java_proto",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_protobuf_lite",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_opencensus_opencensus_api",
        "@maven//:io_opencensus_opencensus_contrib_grpc_metrics",
    ],
)

javadoc_library(
    name = "client_javadoc",
    srcs = ANALYZER_SRCS + CHANNEL_PROVIDER_SRCS + CATALOG_SRCS +
           SIMPLE_CATALOG_SRCS + COMMON_SRCS + ["DebugPrintableNode.java"],
    root_packages = ["com.google.zetasql"],
    deps = [
        ":analyzer",
        ":channel_provider",
        ":client",
        ":debug_printable_node",
    ],
)

java_library(
    name = "testaccess",
    testonly = 1,
    srcs = ["TestAccess.java"],
    deps = [
        ":types",
        "@maven//:com_google_guava_guava",
    ],
)
