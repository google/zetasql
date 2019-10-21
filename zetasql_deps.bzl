#
# Copyright 2018 ZetaSQL Authors
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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:jvm.bzl", "jvm_maven_import_external")
load("@bazel_tools//tools/build_defs/repo:java.bzl", "java_import_external")

""" Declares dependencies of ZetaSQL """

def zetasql_deps():
    """Macro to include ZetaSQL's critical dependencies in a WORKSPACE.

    """
    if not native.existing_rule("com_googleapis_googleapis"):
        # Very rarely updated, but just in case, here's how:
        #    COMMIT=<paste commit hex>
        #    PREFIX=googleapis-
        #    REPO=https://github.com/googleapis/googleapis/archive
        #    URL=${REPO}/${COMMIT}.tar.gz
        #    wget $URL
        #    SHA256=$(sha256sum ${COMMIT}.tar.gz | cut -f1 -d' ')
        #    rm ${COMMIT}.tar.gz
        #    echo url = \"$URL\",
        #    echo sha256 = \"$SHA256\",
        #    echo strip_prefix = \"${PREFIX}${COMMIT}\",
        http_archive(
            name = "com_googleapis_googleapis",
            url = "https://github.com/googleapis/googleapis/archive/common-protos-1_3_1.tar.gz",
            sha256 = "9584b7ac21de5b31832faf827f898671cdcb034bd557a36ea3e7fc07e6571dcb",
            strip_prefix = "googleapis-common-protos-1_3_1",
            build_file_content = """
proto_library(
    name = "date_proto",
    visibility = ["//visibility:public"],
    srcs = ["google/type/date.proto"])

cc_proto_library(
    name = "date_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":date_proto"])

proto_library(
    name = "latlng_proto",
    visibility = ["//visibility:public"],
    srcs = ["google/type/latlng.proto"])

cc_proto_library(
    name = "latlng_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":latlng_proto"])

proto_library(
    name = "timeofday_proto",
    visibility = ["//visibility:public"],
    srcs = ["google/type/timeofday.proto"])

cc_proto_library(
    name = "timeofday_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":timeofday_proto"])

    """,
        )

    # Abseil
    if not native.existing_rule("com_google_absl"):
        # How to update:
        # Abseil generally just does daily (or even subdaily) releases. None are
        # special, so just periodically update as necessary.
        #
        #  https://github.com/abseil/abseil-cpp/commits/master
        #  pick a recent release.
        #  Hit the 'clipboard with a left arrow' icon to copy the commit hex
        #    COMMIT=<paste commit hex>
        #    PREFIX=abseil-cpp-
        #    REPO=https://github.com/abseil/abseil-cpp/archive
        #    URL=${REPO}/${COMMIT}.tar.gz
        #    wget $URL
        #    SHA256=$(sha256sum ${COMMIT}.tar.gz | cut -f1 -d' ')
        #    rm ${COMMIT}.tar.gz
        #    echo \# Commit from $(date --iso-8601=date)
        #    echo url = \"$URL\",
        #    echo sha256 = \"$SHA256\",
        #    echo strip_prefix = \"${PREFIX}${COMMIT}\",
        #
        http_archive(
            name = "com_google_absl",
            # Commit from 2019-08-15
            url = "https://github.com/abseil/abseil-cpp/archive/aae8143cf9aa611f70d7ea9b95b8b8b383b2271a.tar.gz",
            sha256 = "ff42d7e0cb00fe8b30bfa7e6793766f4098a096c31e923333ed346d4803f1667",
            strip_prefix = "abseil-cpp-aae8143cf9aa611f70d7ea9b95b8b8b383b2271a",
        )

    # Abseil (Python)
    if not native.existing_rule("com_google_absl_py"):
        # How to update:
        # Abseil generally just does daily (or even subdaily) releases. None are
        # special, so just periodically update as necessary.
        #
        #   https://github.com/abseil/abseil-cpp
        #     navigate to "commits"
        #     pick a recent release.
        #     Hit the 'clipboard with a left arrow' icon to copy the commit hex
        #
        #   COMMITHEX=<commit hex>
        #   URL=https://github.com/google/absl-cpp/archive/${COMMITHEX}.tar.gz
        #   wget $URL absl.tar.gz
        #   sha256sum absl.tar.gz # Spits out checksum of tarball
        #
        # update urls with $URL
        # update sha256 with result of sha256sum
        # update strip_prefix with COMMITHEX
        http_archive(
            name = "io_abseil_py",
            # Non-release commit from April 18, 2018
            urls = [
                "https://github.com/abseil/abseil-py/archive/bd4d245ac1e36439cb44e7ac46cd1b3e48d8edfa.tar.gz",
            ],
            sha256 = "62a536b13840dc7e3adec333c1ea4c483628ce39a9fdd41e7b3e027f961eb371",
            strip_prefix = "abseil-py-bd4d245ac1e36439cb44e7ac46cd1b3e48d8edfa",
        )

    # required by protobuf_python
    if not native.existing_rule("six_archive"):
        http_archive(
            name = "six_archive",
            build_file = "@com_google_protobuf//:six.BUILD",
            # Release 1.10.0
            url = "https://pypi.python.org/packages/source/s/six/six-1.10.0.tar.gz",
            sha256 = "105f8d68616f8248e24bf0e9372ef04d3cc10104f1980f54d57b2ce73a5ad56a",
        )

    native.bind(
        name = "six",
        actual = "@six_archive//:six",
    )

    # Protobuf
    if not native.existing_rule("com_google_protobuf"):
        http_archive(
            name = "com_google_protobuf",
            urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.6.1.3.tar.gz"],
            sha256 = "73fdad358857e120fd0fa19e071a96e15c0f23bb25f85d3f7009abfd4f264a2a",
            strip_prefix = "protobuf-3.6.1.3",
            patches = ["@com_google_zetasql//bazel:protobuf-v3.6.1.3.patch"],
        )

    # gRPC
    if not native.existing_rule("com_github_grpc_grpc"):
        http_archive(
            name = "com_github_grpc_grpc",
            # Release v1.22.1
            url = "https://github.com/grpc/grpc/archive/v1.22.1.tar.gz",
            sha256 = "cce1d4585dd017980d4a407d8c5e9f8fc8c1dbb03f249b99e88a387ebb45a035",
            strip_prefix = "grpc-1.22.1",
        )

    # gRPC Java
    if not native.existing_rule("io_grpc_grpc_java"):
        http_archive(
            name = "io_grpc_grpc_java",
            # Release 1.22.1
            url = "https://github.com/grpc/grpc-java/archive/v1.22.1.tar.gz",
            strip_prefix = "grpc-java-1.22.1",
            sha256 = "6e63bd6f5a82de0b84c802390adb8661013bad9ebf910ad7e1f3f72b5f798832",
        )

    if not native.existing_rule("com_google_code_findbugs_jsr305"):
        jvm_maven_import_external(
            name = "com_google_code_findbugs_jsr305",
            artifact = "com.google.code.findbugs:jsr305:3.0.2",
            tags = ["maven_coordinates=com.google.code.findbugs:jsr305:3.0.2"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "766ad2a0783f2687962c8ad74ceecc38a28b9f72a2d085ee438b7813e928d0c7",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("com_google_errorprone_error_prone_annotations"):
        jvm_maven_import_external(
            name = "com_google_errorprone_error_prone_annotations",
            artifact = "com.google.errorprone:error_prone_annotations:2.3.2",
            tags = ["maven_coordinates=com.google.errorprone:error_prone_annotations:2.3.2"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "357cd6cfb067c969226c442451502aee13800a24e950fdfde77bcdb4565a668d",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("com_google_j2objc_j2objc_annotations"):
        jvm_maven_import_external(
            name = "com_google_j2objc_j2objc_annotations",
            artifact = "com.google.j2objc:j2objc-annotations:1.1",
            tags = ["maven_coordinates=com.google.j2objc:j2objc-annotations:1.1"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "2994a7eb78f2710bd3d3bfb639b2c94e219cedac0d4d084d516e78c16dddecf6",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("org_codehaus_mojo_animal_sniffer_annotations"):
        jvm_maven_import_external(
            name = "org_codehaus_mojo_animal_sniffer_annotations",
            artifact = "org.codehaus.mojo:animal-sniffer-annotations:1.17",
            tags = ["maven_coordinates=org.codehaus.mojo:animal-sniffer-annotations:1.17"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "92654f493ecfec52082e76354f0ebf87648dc3d5cec2e3c3cdb947c016747a53",
            licenses = ["notice"],  # MIT
        )

    if not native.existing_rule("com_google_guava_guava"):
        jvm_maven_import_external(
            name = "com_google_guava_guava",
            artifact = "com.google.guava:guava:26.0-android",
            tags = ["maven_coordinates=com.google.guava:guava:26.0-android"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "1d044ebb866ef08b7d04e998b4260c9b52fab6e6d6b68d207859486bb3686cd5",
            licenses = ["notice"],  # Apache 2.0
        )
        native.bind(
            name = "guava",
            actual = "@com_google_guava_guava//jar",
        )

    if not native.existing_rule("com_google_guava_testlib"):
        jvm_maven_import_external(
            name = "com_google_guava_testlib",
            artifact = "com.google.guava:guava-testlib:26.0-jre",
            tags = ["maven_coordinates=com.google.guava:testlib:26.0-jre"],
            server_urls = ["http://central.maven.org/maven2"],
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("com_google_code_gson_gson"):
        jvm_maven_import_external(
            name = "com_google_code_gson_gson",
            artifact = "com.google.code.gson:gson:jar:2.7",
            tags = ["maven_coordinates=com.google.code.gson:gson:jar:2.7"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "2d43eb5ea9e133d2ee2405cc14f5ee08951b8361302fdd93494a3a997b508d32",
            licenses = ["notice"],  # Apache 2.0
        )
        native.bind(
            name = "gson",
            actual = "@com_google_code_gson_gson//jar",
        )

    if not native.existing_rule("com_google_truth_truth"):
        jvm_maven_import_external(
            name = "com_google_truth_truth",
            testonly = 1,
            artifact = "com.google.truth:truth:0.44",
            tags = ["maven_coordinates=com.google.truth:truth:0.44"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "a9e6796786c9c77a5fe19b08e72fe0a620d53166df423d8861af9ebef4dc4247",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("com_google_truth_proto_extension"):
        jvm_maven_import_external(
            name = "com_google_truth_proto_extension",
            testonly = 1,
            artifact = "com.google.truth.extensions:truth-proto-extension:0.44",
            tags = ["maven_coordinates=com.google.truth.extensions:truth-proto-extension:0.44"],
            server_urls = ["http://central.maven.org/maven2"],
            # artifact_sha256 = "a9e6796786c9c77a5fe19b08e72fe0a620d53166df423d8861af9ebef4dc4247",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("io_netty_netty_buffer"):
        jvm_maven_import_external(
            name = "io_netty_netty_buffer",
            artifact = "io.netty:netty-buffer:4.1.34.Final",
            tags = ["maven_coordinates=io.netty:netty-buffer:4.1.34.Final"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "39dfe88df8505fd01fbf9c1dbb6b6fa9b0297e453c3dc4ce039ea578aea2eaa3",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("io_netty_netty_codec"):
        jvm_maven_import_external(
            name = "io_netty_netty_codec",
            artifact = "io.netty:netty-codec:4.1.34.Final",
            tags = ["maven_coordinates=io.netty:netty-codec:4.1.34.Final"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "52e9eeb3638a8ed0911c72a508c05fa4f9d3391125eae46f287d3a8a0776211d",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("io_netty_netty_codec_http"):
        jvm_maven_import_external(
            name = "io_netty_netty_codec_http",
            artifact = "io.netty:netty-codec-http:4.1.34.Final",
            tags = ["maven_coordinates=io.netty:netty-codec-http:4.1.34.Final"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "5df5556ef6b0e7ce7c72a359e4ca774fcdf8d8fe12f0b6332715eaa44cfe41f8",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("io_netty_netty_codec_http2"):
        jvm_maven_import_external(
            name = "io_netty_netty_codec_http2",
            artifact = "io.netty:netty-codec-http2:4.1.34.Final",
            tags = ["maven_coordinates=io.netty:netty-codec-http2:4.1.34.Final"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "319f66f3ab0d3aac3477febf19c259990ee8c639fc7da8822dfa58e7dab1bdcf",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("io_netty_netty_common"):
        jvm_maven_import_external(
            name = "io_netty_netty_common",
            artifact = "io.netty:netty-common:4.1.34.Final",
            tags = ["maven_coordinates=io.netty:netty-common:4.1.34.Final"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "122931117eacf370b054d0e8a2411efa81de4956a6c3f938b0f0eb915969a425",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("io_netty_netty_handler"):
        jvm_maven_import_external(
            name = "io_netty_netty_handler",
            artifact = "io.netty:netty-handler:4.1.34.Final",
            tags = ["maven_coordinates=io.netty:netty-handler:4.1.34.Final"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "035616801fe9894ca2490832cf9976536dac740f41e90de1cdd4ba46f04263d1",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("io_netty_netty_resolver"):
        jvm_maven_import_external(
            name = "io_netty_netty_resolver",
            artifact = "io.netty:netty-resolver:4.1.34.Final",
            tags = ["maven_coordinates=io.netty:netty-resolver:4.1.34.Final"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "774221ed4c130b532865770b10630bc12d0d400127da617ee0ac8de2a7ac2097",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("io_netty_netty_transport"):
        jvm_maven_import_external(
            name = "io_netty_netty_transport",
            artifact = "io.netty:netty-transport:4.1.34.Final",
            tags = ["maven_coordinates=io.netty:netty-transport:4.1.34.Final"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "2b3f7d3a595101def7d411793a675bf2a325964475fd7bdbbe448e908de09445",
            exports = ["@io_netty_netty_common//jar"],
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("junit_junit"):
        jvm_maven_import_external(
            name = "junit_junit",
            artifact = "junit:junit:4.12",
            tags = ["maven_coordinates=junit:junit:4.12"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "59721f0805e223d84b90677887d9ff567dc534d7c502ca903c0c2b17f05c116a",
            licenses = ["notice"],  # EPL 1.0
        )

    if not native.existing_rule("com_google_api_grpc_proto_google_common_protos"):
        jvm_maven_import_external(
            name = "com_google_api_grpc_proto_google_common_protos",
            artifact = "com.google.api.grpc:proto-google-common-protos:1.12.0",
            tags = ["maven_coordinates=com.google.api.grpc:proto-google-common-protos:1.12.0"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "bd60cd7a423b00fb824c27bdd0293aaf4781be1daba6ed256311103fb4b84108",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("io_grpc_grpc_context"):
        jvm_maven_import_external(
            name = "io_grpc_grpc_context",
            artifact = "io.grpc:grpc-context:1.18.0",
            server_urls = ["http://central.maven.org/maven2"],
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("io_grpc_grpc_core"):
        jvm_maven_import_external(
            name = "io_grpc_grpc_core",
            artifact = "io.grpc:grpc-core:1.18.0",
            server_urls = ["http://central.maven.org/maven2"],
            licenses = ["notice"],  # Apache 2.0
            runtime_deps = [
                "@io_opencensus_opencensus_api//jar",
                "@io_opencensus_opencensus_contrib_grpc_metrics//jar",
            ],
        )

    if not native.existing_rule("io_grpc_grpc_netty"):
        jvm_maven_import_external(
            name = "io_grpc_grpc_netty",
            artifact = "io.grpc:grpc-netty:1.18.0",
            server_urls = ["http://central.maven.org/maven2"],
            licenses = ["notice"],  # Apache 2.0
            runtime_deps = [
                "@io_netty_netty_buffer//jar",
                "@io_netty_netty_codec//jar",
                "@io_netty_netty_codec_http//jar",
                "@io_netty_netty_handler//jar",
                "@io_netty_netty_resolver//jar",
                "@io_netty_netty_transport//jar",
            ],
            deps = [
                "@io_netty_netty_codec_http2//jar",
            ],
        )

    if not native.existing_rule("io_grpc_grpc_stub"):
        jvm_maven_import_external(
            name = "io_grpc_grpc_stub",
            artifact = "io.grpc:grpc-stub:1.18.0",
            server_urls = ["http://central.maven.org/maven2"],
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("io_grpc_grpc_protobuf"):
        jvm_maven_import_external(
            name = "io_grpc_grpc_protobuf",
            artifact = "io.grpc:grpc-protobuf:1.18.0",
            server_urls = ["http://central.maven.org/maven2"],
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("io_grpc_grpc_protobuf_lite"):
        jvm_maven_import_external(
            name = "io_grpc_grpc_protobuf_lite",
            artifact = "io.grpc:grpc-protobuf-lite:1.18.0",
            server_urls = ["http://central.maven.org/maven2"],
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("javax_annotation_javax_annotation_api"):
        jvm_maven_import_external(
            name = "javax_annotation_javax_annotation_api",
            artifact = "javax.annotation:javax.annotation-api:1.2",
            tags = ["maven_coordinates=javax.annotation:javax.annotation-api:1.2"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "5909b396ca3a2be10d0eea32c74ef78d816e1b4ead21de1d78de1f890d033e04",
            licenses = ["reciprocal"],  # CDDL License
        )

    if not native.existing_rule("io_opencensus_opencensus_api"):
        jvm_maven_import_external(
            name = "io_opencensus_opencensus_api",
            artifact = "io.opencensus:opencensus-api:0.21.0",
            tags = ["maven_coordinates=io.opencensus:opencensus-api:0.21.0"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "8e2cb0f6391d8eb0a1bcd01e7748883f0033b1941754f4ed3f19d2c3e4276fc8",
            licenses = ["notice"],  # Apache 2.0
        )

    if not native.existing_rule("io_opencensus_opencensus_contrib_grpc_metrics"):
        jvm_maven_import_external(
            name = "io_opencensus_opencensus_contrib_grpc_metrics",
            artifact = "io.opencensus:opencensus-contrib-grpc-metrics:0.21.0",
            tags = ["maven_coordinates=io.opencensus:opencensus-contrib-grpc-metrics:0.21.0"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "29fc79401082301542cab89d7054d2f0825f184492654c950020553ef4ff0ef8",
            licenses = ["notice"],  # Apache 2.0
        )

    # Auto common
    if not native.existing_rule("com_google_auto_common"):
        java_import_external(
            name = "com_google_auto_common",
            jar_sha256 = "eee75e0d1b1b8f31584dcbe25e7c30752545001b46673d007d468d75cf6b2c52",
            jar_urls = [
                "http://mirror.bazel.build/repo1.maven.org/maven2/com/google/auto/auto-common/0.7/auto-common-0.7.jar",
                "http://repo1.maven.org/maven2/com/google/auto/auto-common/0.7/auto-common-0.7.jar",
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
                "http://mirror.bazel.build/repo1.maven.org/maven2/com/google/auto/service/auto-service/1.0-rc2/auto-service-1.0-rc2.jar",
                "http://repo1.maven.org/maven2/com/google/auto/service/auto-service/1.0-rc2/auto-service-1.0-rc2.jar",
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
            jar_sha256 = "fd811b92bb59ae8a4cf7eb9dedd208300f4ea2b6275d726e4df52d8334aaae9d",
            jar_urls = [
                "https://mirror.bazel.build/repo1.maven.org/maven2/com/google/auto/value/auto-value/1.6/auto-value-1.6.jar",
                "https://repo1.maven.org/maven2/com/google/auto/value/auto-value/1.6/auto-value-1.6.jar",
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
            jar_sha256 = "d095936c432f2afc671beaab67433e7cef50bba4a861b77b9c46561b801fae69",
            jar_urls = [
                "https://mirror.bazel.build/repo1.maven.org/maven2/com/google/auto/value/auto-value-annotations/1.6/auto-value-annotations-1.6.jar",
                "https://repo1.maven.org/maven2/com/google/auto/value/auto-value-annotations/1.6/auto-value-annotations-1.6.jar",
            ],
            licenses = ["notice"],  # Apache 2.0
            neverlink = True,
            default_visibility = ["@com_google_auto_value//:__pkg__"],
        )

    # Joda Time
    if not native.existing_rule("joda_time"):
        jvm_maven_import_external(
            name = "joda_time",
            artifact = "joda-time:joda-time:2.3",
            tags = ["maven_coordinates=joda-time:joda-time:2.3"],
            server_urls = ["http://central.maven.org/maven2"],
            artifact_sha256 = "602fd8006641f8b3afd589acbd9c9b356712bdcf0f9323557ec8648cd234983b",
            licenses = ["notice"],  # Apache 2.0
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

    # GoogleTest/GoogleMock framework. Used by most unit-tests.
    if not native.existing_rule("com_google_googletest"):
        http_archive(
            name = "com_google_googletest",
            # Commit on 2019-07-18
            urls = [
                "https://github.com/google/googletest/archive/b77e5c76252bac322bb82c5b444f050bd0d92451.tar.gz",
            ],
            sha256 = "b666893f592a7728331e10f36e991e5f6090998dc267e511899215c52835512d",
            strip_prefix = "googletest-b77e5c76252bac322bb82c5b444f050bd0d92451",
        )

    # RE2 Regex Framework, mostly used in unit tests.
    if not native.existing_rule("com_google_re2"):
        http_archive(
            name = "com_google_re2",
            urls = [
                "https://github.com/google/re2/archive/2018-09-01.tar.gz",
            ],
            sha256 = "1424b303582f71c6f9e19f3b21d320e3b80f4c37b9d4426270f1f80d11cacf43",
            strip_prefix = "re2-2018-09-01",
        )

    # Jinja2.
    if not native.existing_rule("jinja"):
        http_archive(
            name = "jinja",
            # Jinja release 2.10
            url = "https://github.com/pallets/jinja/archive/2.10.tar.gz",
            strip_prefix = "jinja-2.10",
            sha256 = "0d31d3466c313a9ca014a2d904fed18cdac873a5ba1f7b70b8fd8b206cd860d6",
            build_file_content = """py_library(
    name = "jinja2",
    visibility = ["//visibility:public"],
    srcs = glob(["jinja2/*.py"]),
    deps = ["@markupsafe//:markupsafe"],
)""",
        )

    if not native.existing_rule("markupsafe"):
        http_archive(
            name = "markupsafe",
            urls = [
                "https://github.com/pallets/markupsafe/archive/1.0.tar.gz",
            ],
            sha256 = "dc3938045d9407a73cf9fdd709e2b1defd0588d50ffc85eb0786c095ec846f15",
            strip_prefix = "markupsafe-1.0/markupsafe",
            build_file_content = """py_library(
    name = "markupsafe",
    visibility = ["//visibility:public"],
    srcs = glob(["*.py"])
)""",
        )

    if not native.existing_rule("google_bazel_common"):
        http_archive(
            name = "google_bazel_common",
            strip_prefix = "bazel-common-67e793d85570be5a714de3327add930422b2fa6d",
            urls = ["https://github.com/google/bazel-common/archive/67e793d85570be5a714de3327add930422b2fa6d.zip"],
            sha256 = "62055cdfeb77affca6a02c08aca69934fd34413286b4a2504870de6d9c6848a2",
        )

    ##########################################################################
    # Rules which depend on rules_foreign_cc
    #
    # These require a "./configure && make" style build and depend on an
    # experimental project to allow building from source with non-bazel
    # build systems.
    #
    # All of these archives basically just create filegroups and separate
    # BUILD files introduce the relevant rules.
    ##########################################################################

    all_content = """filegroup(name = "all", srcs = glob(["**"]), visibility = ["//visibility:public"])"""

    http_archive(
        name = "bison",
        build_file_content = all_content,
        strip_prefix = "bison-3.2.4",
        sha256 = "cb673e2298d34b5e46ba7df0641afa734da1457ce47de491863407a587eec79a",
        urls = ["https://ftp.gnu.org/gnu/bison/bison-3.2.4.tar.gz"],
    )

    http_archive(
        name = "flex",
        build_file_content = all_content,
        strip_prefix = "flex-2.6.4",
        sha256 = "e87aae032bf07c26f85ac0ed3250998c37621d95f8bd748b31f15b33c45ee995",
        urls = ["https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz"],
        patches = ["@com_google_zetasql//bazel:flex.patch"],
    )

    http_archive(
        name = "m4",
        build_file_content = all_content,
        strip_prefix = "m4-1.4.18",
        sha256 = "ab2633921a5cd38e48797bf5521ad259bdc4b979078034a3b790d7fec5493fab",
        urls = ["https://ftp.gnu.org/gnu/m4/m4-1.4.18.tar.gz"],
        patches = [
            "@com_google_zetasql//bazel:m4.patch",
            "@com_google_zetasql//bazel:m4-1.4.18.patch"
        ],
    )

    http_archive(
        name = "icu",
        build_file = "@com_google_zetasql//bazel:icu.BUILD",
        strip_prefix = "icu",
        sha256 = "627d5d8478e6d96fc8c90fed4851239079a561a6a8b9e48b0892f24e82d31d6c",
        urls = ["https://github.com/unicode-org/icu/releases/download/release-64-2/icu4c-64_2-src.tgz"],
        patches = ["@com_google_zetasql//bazel:icu4c-64_2.patch"],
    )
