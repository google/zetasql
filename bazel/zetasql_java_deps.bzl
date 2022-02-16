#
# Copyright 2022 Google LLC
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

""" Load ZetaSQL Java Dependencies. """

load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:java.bzl", "java_import_external")
load("@io_grpc_grpc_java//:repositories.bzl", "IO_GRPC_GRPC_JAVA_ARTIFACTS")

ZETASQL_MAVEN_ARTIFACTS = [
    "com.google.api.grpc:proto-google-common-protos:1.12.0",
    "com.google.code.findbugs:jsr305:3.0.2",
    "com.google.errorprone:error_prone_annotations:2.11.0",
    "com.google.guava:guava:31.0.1-jre",
    "com.google.guava:guava-testlib:31.0.1-jre",
    "io.grpc:grpc-context:1.43.2",
    "io.grpc:grpc-core:1.43.2",
    "io.grpc:grpc-api:1.43.2",
    "io.grpc:grpc-netty:1.43.2",
    "io.grpc:grpc-protobuf-lite:1.43.2",
    "io.grpc:grpc-protobuf:1.43.2",
    "io.grpc:grpc-stub:1.43.2",
    "io.netty:netty-common:4.1.34.Final",
    "io.netty:netty-transport:4.1.34.Final",
    "io.opencensus:opencensus-api:0.21.0",
    "io.opencensus:opencensus-contrib-grpc-metrics:0.21.0",
    "javax.annotation:javax.annotation-api:1.2",
    "joda-time:joda-time:2.10.13",
    "com.google.code.gson:gson:jar:2.8.9",
    "com.google.protobuf:protobuf-java:3.19.3",
    "com.google.truth:truth:1.1.3",
    "com.google.truth.extensions:truth-proto-extension:1.1.3",
    "junit:junit:4.13.2",
]

def zetasql_java_deps(name = ""):
    """Load dependencies required to build and test ZetaSQL's Java API.

    Args:
      name: Unused.
    """
    maven_install(
        name = "maven",
        artifacts = ZETASQL_MAVEN_ARTIFACTS + IO_GRPC_GRPC_JAVA_ARTIFACTS,
        # For updating instructions, see:
        # https://github.com/bazelbuild/rules_jvm_external#updating-maven_installjson
        repositories = [
            "https://repo1.maven.org/maven2",
            "https://repo.maven.apache.org/maven2",
        ],
        fetch_sources = True,
        generate_compat_repositories = True,
        maven_install_json = "@//bazel:maven_install.json",
        fail_if_repin_required = True,
    )
    native.bind(
        name = "gson",
        actual = "@com_google_code_gson_gson//jar",
    )

    # Auto common
    if not native.existing_rule("com_google_auto_common"):
        java_import_external(
            name = "com_google_auto_common",
            jar_sha256 = "b876b5fddaceeba7d359667f6c4fb8c6f8658da1ab902ffb79ec9a415deede5f",
            jar_urls = [
                "https://repo1.maven.org/maven2/com/google/auto/auto-common/0.10/auto-common-0.10.jar",
                "https://repo.maven.apache.org/maven2/com/google/auto/auto-common/0.10/auto-common-0.10.jar",
            ],
            licenses = ["notice"],  # Apache 2.0
            deps = ["@com_google_guava_guava//jar"],
        )

    # Auto service
    if not native.existing_rule("com_google_auto_service"):
        java_import_external(
            name = "com_google_auto_service",
            jar_sha256 = "46808c92276b4c19e05781963432e6ab3e920b305c0e6df621517d3624a35d71",
            jar_urls = [
                "https://repo1.maven.org/maven2/com/google/auto/service/auto-service/1.0-rc2/auto-service-1.0-rc2.jar",
                "https://repo.maven.apache.org/maven2/com/google/auto/service/auto-service/1.0-rc2/auto-service-1.0-rc2.jar",
            ],
            licenses = ["notice"],  # Apache 2.0
            neverlink = True,
            generated_rule_name = "compile",
            generated_linkable_rule_name = "processor",
            deps = [
                "@com_google_auto_common",
                "@com_google_guava_guava//jar",
            ],
            extra_build_file_content = "\n".join([
                "java_plugin(",
                "    name = \"AutoServiceProcessor\",",
                "    output_licenses = [\"unencumbered\"],",
                "    processor_class = \"com.google.auto.service.processor.AutoServiceProcessor\",",
                "    deps = [\":processor\"],",
                ")",
                "",
                "java_library(",
                "    name = \"com_google_auto_service\",",
                "    exported_plugins = [\":AutoServiceProcessor\"],",
                "    exports = [\":compile\"],",
                ")",
            ]),
        )

    # Auto value
    if not native.existing_rule("com_google_auto_value"):
        # AutoValue 1.6+ shades Guava, Auto Common, and JavaPoet. That's OK
        # because none of these jars become runtime dependencies.
        java_import_external(
            name = "com_google_auto_value",
            jar_sha256 = "8320edb037b62d45bc05ae4e1e21941255ef489e950519ef14d636d66870da64",
            jar_urls = [
                "https://repo1.maven.org/maven2/com/google/auto/value/auto-value/1.7.4/auto-value-1.7.4.jar",
                "https://repo.maven.apache.org/maven2/com/google/auto/value/auto-value/1.7.4/auto-value-1.7.4.jar",
            ],
            licenses = ["notice"],  # Apache 2.0
            generated_rule_name = "processor",
            exports = ["@com_google_auto_value_annotations"],
            extra_build_file_content = "\n".join([
                "java_plugin(",
                "    name = \"AutoAnnotationProcessor\",",
                "    output_licenses = [\"unencumbered\"],",
                "    processor_class = \"com.google.auto.value.processor.AutoAnnotationProcessor\",",
                "    tags = [\"annotation=com.google.auto.value.AutoAnnotation;genclass=${package}.AutoAnnotation_${outerclasses}${classname}_${methodname}\"],",
                "    deps = [\":processor\"],",
                ")",
                "",
                "java_plugin(",
                "    name = \"AutoOneOfProcessor\",",
                "    output_licenses = [\"unencumbered\"],",
                "    processor_class = \"com.google.auto.value.processor.AutoOneOfProcessor\",",
                "    tags = [\"annotation=com.google.auto.value.AutoValue;genclass=${package}.AutoOneOf_${outerclasses}${classname}\"],",
                "    deps = [\":processor\"],",
                ")",
                "",
                "java_plugin(",
                "    name = \"AutoValueProcessor\",",
                "    output_licenses = [\"unencumbered\"],",
                "    processor_class = \"com.google.auto.value.processor.AutoValueProcessor\",",
                "    tags = [\"annotation=com.google.auto.value.AutoValue;genclass=${package}.AutoValue_${outerclasses}${classname}\"],",
                "    deps = [\":processor\"],",
                ")",
                "",
                "java_library(",
                "    name = \"com_google_auto_value\",",
                "    exported_plugins = [",
                "        \":AutoAnnotationProcessor\",",
                "        \":AutoOneOfProcessor\",",
                "        \":AutoValueProcessor\",",
                "    ],",
                "    exports = [\"@com_google_auto_value_annotations\"],",
                ")",
            ]),
        )

    # Auto value annotations
    if not native.existing_rule("com_google_auto_value_annotations"):
        java_import_external(
            name = "com_google_auto_value_annotations",
            jar_sha256 = "fedd59b0b4986c342f6ab2d182f2a4ee9fceb2c7e2d5bdc4dc764c92394a23d3",
            jar_urls = [
                "https://repo1.maven.org/maven2/com/google/auto/value/auto-value-annotations/1.7.4/auto-value-annotations-1.7.4.jar",
                "https://repo.maven.apache.org/maven2/com/google/auto/value/auto-value-annotations/1.7.4/auto-value-annotations-1.7.4.jar",
            ],
            licenses = ["notice"],  # Apache 2.0
            neverlink = True,
            default_visibility = ["@com_google_auto_value//:__pkg__"],
        )

    if not native.existing_rule("native_utils"):
        http_archive(
            name = "native_utils",
            url = "https://github.com/adamheinrich/native-utils/archive/e6a39489662846a77504634b6fafa4995ede3b1d.tar.gz",
            sha256 = "6013c0988ba40600e238e47088580fd562dcecd4afd3fcf26130efe7cb1620de",
            strip_prefix = "native-utils-e6a39489662846a77504634b6fafa4995ede3b1d",
            build_file_content = """licenses(["notice"]) # MIT
java_library(
name = "native_utils",
visibility = ["//visibility:public"],
srcs = glob(["src/main/java/cz/adamh/utils/*.java"]),
)""",
        )
