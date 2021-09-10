#
# Copyright 2018 Google LLC
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

""" Step 2 to load ZetaSQL dependencies. """

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:jvm.bzl", "jvm_maven_import_external")
load("@bazel_tools//tools/build_defs/repo:java.bzl", "java_import_external")

# Followup from zetasql_deps_step_1.bzl
load("@rules_foreign_cc//:workspace_definitions.bzl", "rules_foreign_cc_dependencies")

def _load_deps_from_step_1():
    rules_foreign_cc_dependencies()

def zetasql_deps_step_2(
        name = None,
        analyzer_deps = True,
        evaluator_deps = True,
        tools_deps = True,
        java_deps = True,
        testing_deps = True):
    """Step 2 macro to include ZetaSQL's critical dependencies in a WORKSPACE.

    Args:
      name: Unused.
      analyzer_deps: True to include all the analyzer dependencies.
      evaluator_deps: True to include all the analyzer and evaluator dependencies. If it's True, analyzer_deps will also be set to True.
      tools_deps: True to include all the tooling dependencies.
      java_deps: True to include all java, analyzer and evaluator dependencies. If it's True, analyzer_deps and evaluator_deps will also be set to True.
      testing_deps: True to include all dependencies. If it's True, analyzer_deps, evaluator_deps, tools_deps and java_deps will also be set to True.
    """

    # Testing needs everything
    if testing_deps:
        analyzer_deps = True
        evaluator_deps = True
        tools_deps = True
        java_deps = True

    if java_deps or tools_deps:
        analyzer_deps = True
        evaluator_deps = True

    # Evaluator depends on analyzer.
    if evaluator_deps:
        analyzer_deps = True

    if analyzer_deps:
        # Followup from zetasql_deps_step_1.bzl
        _load_deps_from_step_1()
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
                # Commit from 2021-08-06
                url = "https://github.com/abseil/abseil-cpp/archive/bf31a10b65d945665cecfb9d8807702ae4a7fde1.tar.gz",
                sha256 = "3179b97f202d7e6b81ccf1d835693463498d0523bfcfaf3089a6226f19d97a7f",
                strip_prefix = "abseil-cpp-bf31a10b65d945665cecfb9d8807702ae4a7fde1",
            )

    if analyzer_deps:
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

    if tools_deps:
        # Riegeli
        if not native.existing_rule("com_google_riegeli"):
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
                name = "com_google_riegeli",
                # Commit from 2021-06-01
                url = "https://github.com/google/riegeli/archive/baf6376f694d401932cf1b9d34e79a0fae50e7c4.tar.gz",
                sha256 = "15b6da71683520b8c2eadf11eb8180eed567568740562c88ead69a560b8cd219",
                strip_prefix = "riegeli-baf6376f694d401932cf1b9d34e79a0fae50e7c4",
            )
    if evaluator_deps:
        # Differential Privacy
        if not native.existing_rule("com_google_differential_privacy"):
            http_archive(
                name = "com_google_differential_privacy",
                # Release from 2021-04-21
                url = "https://github.com/google/differential-privacy/archive/refs/tags/v1.0.0.tar.gz",
                sha256 = "2ff780cdbffd8e3e84425c2ed38a0a48d0307f4df655dfd7b0d7f5143078d8c8",
                strip_prefix = "differential-privacy-1.0.0",
            )

        # Differential Privacy - cc
        if not native.existing_rule("com_google_cc_differential_privacy"):
            http_archive(
                name = "com_google_cc_differential_privacy",
                # Release from 2021-04-21
                url = "https://github.com/google/differential-privacy/archive/refs/tags/v1.0.0.tar.gz",
                sha256 = "2ff780cdbffd8e3e84425c2ed38a0a48d0307f4df655dfd7b0d7f5143078d8c8",
                strip_prefix = "differential-privacy-1.0.0/cc",
            )

        # Boringssl
        if not native.existing_rule("boringssl"):
            http_archive(
                name = "boringssl",
                # Commit from 2019 October 29
                urls = [
                    "https://github.com/google/boringssl/archive/9e18928936ccb882192e9779b0fd355bec739bdd.tar.gz",
                ],
                sha256 = "19a951d1706a67be480809f6a6231675d29841be5682a7fe40bbcdf1e16f0147",
                strip_prefix = "boringssl-9e18928936ccb882192e9779b0fd355bec739bdd",
            )

        # Farmhash
        if not native.existing_rule("com_google_farmhash"):
            http_archive(
                name = "com_google_farmhash",
                build_file = "@com_google_zetasql//bazel:farmhash.BUILD",
                url = "https://github.com/google/farmhash/archive/816a4ae622e964763ca0862d9dbd19324a1eaf45.tar.gz",
                sha256 = "6560547c63e4af82b0f202cb710ceabb3f21347a4b996db565a411da5b17aba0",
                strip_prefix = "farmhash-816a4ae622e964763ca0862d9dbd19324a1eaf45",
            )
    if analyzer_deps:
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

    # Required by gRPC
    if not native.existing_rule("build_bazel_rules_apple"):
        http_archive(
            name = "build_bazel_rules_apple",
            urls = ["https://github.com/bazelbuild/rules_apple/archive/0.18.0.tar.gz"],
            sha256 = "53a8f9590b4026fbcfefd02c868e48683b44a314338d03debfb8e8f6c50d1239",
            strip_prefix = "rules_apple-0.18.0",
        )

    # Required by gRPC
    if not native.existing_rule("build_bazel_apple_support"):
        http_archive(
            name = "build_bazel_apple_support",
            urls = ["https://github.com/bazelbuild/apple_support/archive/0.7.1.tar.gz"],
            sha256 = "140fa73e1c712900097aabdb846172ffa0a5e9523b87d6c564c13116a6180a62",
            strip_prefix = "apple_support-0.7.1",
        )

    if testing_deps:
        if not native.existing_rule("com_google_file_based_test_driver"):
            http_archive(
                name = "com_google_file_based_test_driver",
                # Commit from 2020-11-24
                url = "https://github.com/google/file-based-test-driver/archive/fd7661b168f640f68da39f97dad26e426eb6c339.tar.gz",
                sha256 = "b564acb6f083ce6e91fc2734bdad259cc4edf1a95766f93750a16784ff86218a",
                strip_prefix = "file-based-test-driver-fd7661b168f640f68da39f97dad26e426eb6c339",
            )
    if analyzer_deps:
        # gRPC
        if not native.existing_rule("com_github_grpc_grpc"):
            http_archive(
                name = "com_github_grpc_grpc",
                urls = ["https://github.com/grpc/grpc/archive/v1.24.2.tar.gz"],
                sha256 = "fd040f5238ff1e32b468d9d38e50f0d7f8da0828019948c9001e9a03093e1d8f",
                strip_prefix = "grpc-1.24.2",
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

    if java_deps:
        if not native.existing_rule("com_google_code_findbugs_jsr305"):
            jvm_maven_import_external(
                name = "com_google_code_findbugs_jsr305",
                artifact = "com.google.code.findbugs:jsr305:3.0.2",
                tags = ["maven_coordinates=com.google.code.findbugs:jsr305:3.0.2"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "766ad2a0783f2687962c8ad74ceecc38a28b9f72a2d085ee438b7813e928d0c7",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("com_google_errorprone_error_prone_annotations"):
            jvm_maven_import_external(
                name = "com_google_errorprone_error_prone_annotations",
                artifact = "com.google.errorprone:error_prone_annotations:2.3.2",
                tags = ["maven_coordinates=com.google.errorprone:error_prone_annotations:2.3.2"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "357cd6cfb067c969226c442451502aee13800a24e950fdfde77bcdb4565a668d",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("com_google_j2objc_j2objc_annotations"):
            jvm_maven_import_external(
                name = "com_google_j2objc_j2objc_annotations",
                artifact = "com.google.j2objc:j2objc-annotations:1.1",
                tags = ["maven_coordinates=com.google.j2objc:j2objc-annotations:1.1"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "2994a7eb78f2710bd3d3bfb639b2c94e219cedac0d4d084d516e78c16dddecf6",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("org_codehaus_mojo_animal_sniffer_annotations"):
            jvm_maven_import_external(
                name = "org_codehaus_mojo_animal_sniffer_annotations",
                artifact = "org.codehaus.mojo:animal-sniffer-annotations:1.17",
                tags = ["maven_coordinates=org.codehaus.mojo:animal-sniffer-annotations:1.17"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "92654f493ecfec52082e76354f0ebf87648dc3d5cec2e3c3cdb947c016747a53",
                licenses = ["notice"],  # MIT
            )

        if not native.existing_rule("com_google_guava_guava"):
            jvm_maven_import_external(
                name = "com_google_guava_guava",
                artifact = "com.google.guava:guava:29.0-jre",
                tags = ["maven_coordinates=com.google.guava:guava:29.0-jre"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "b22c5fb66d61e7b9522531d04b2f915b5158e80aa0b40ee7282c8bfb07b0da25",
                licenses = ["notice"],  # Apache 2.0
            )
            native.bind(
                name = "guava",
                actual = "@com_google_guava_guava//jar",
            )

        if not native.existing_rule("com_google_guava_testlib"):
            jvm_maven_import_external(
                name = "com_google_guava_testlib",
                artifact = "com.google.guava:guava-testlib:29.0-jre",
                tags = ["maven_coordinates=com.google.guava:testlib:29.0-jre"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "f15908de80ff261adde7823d1df599b7447de4863f7c521068ecf881cb1fc79b",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("com_google_code_gson_gson"):
            jvm_maven_import_external(
                name = "com_google_code_gson_gson",
                artifact = "com.google.code.gson:gson:jar:2.7",
                tags = ["maven_coordinates=com.google.code.gson:gson:jar:2.7"],
                server_urls = ["https://repo1.maven.org/maven2"],
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
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "a9e6796786c9c77a5fe19b08e72fe0a620d53166df423d8861af9ebef4dc4247",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("com_google_truth_proto_extension"):
            jvm_maven_import_external(
                name = "com_google_truth_proto_extension",
                testonly = 1,
                artifact = "com.google.truth.extensions:truth-proto-extension:0.44",
                tags = ["maven_coordinates=com.google.truth.extensions:truth-proto-extension:0.44"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "d964495cee74d6933512c7b414c8723285a6413a4e3f46f558fbaf624dfd7c9f",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("io_netty_netty_buffer"):
            jvm_maven_import_external(
                name = "io_netty_netty_buffer",
                artifact = "io.netty:netty-buffer:4.1.34.Final",
                tags = ["maven_coordinates=io.netty:netty-buffer:4.1.34.Final"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "39dfe88df8505fd01fbf9c1dbb6b6fa9b0297e453c3dc4ce039ea578aea2eaa3",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("io_netty_netty_codec"):
            jvm_maven_import_external(
                name = "io_netty_netty_codec",
                artifact = "io.netty:netty-codec:4.1.34.Final",
                tags = ["maven_coordinates=io.netty:netty-codec:4.1.34.Final"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "52e9eeb3638a8ed0911c72a508c05fa4f9d3391125eae46f287d3a8a0776211d",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("io_netty_netty_codec_http"):
            jvm_maven_import_external(
                name = "io_netty_netty_codec_http",
                artifact = "io.netty:netty-codec-http:4.1.34.Final",
                tags = ["maven_coordinates=io.netty:netty-codec-http:4.1.34.Final"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "5df5556ef6b0e7ce7c72a359e4ca774fcdf8d8fe12f0b6332715eaa44cfe41f8",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("io_netty_netty_codec_http2"):
            jvm_maven_import_external(
                name = "io_netty_netty_codec_http2",
                artifact = "io.netty:netty-codec-http2:4.1.34.Final",
                tags = ["maven_coordinates=io.netty:netty-codec-http2:4.1.34.Final"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "319f66f3ab0d3aac3477febf19c259990ee8c639fc7da8822dfa58e7dab1bdcf",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("io_netty_netty_common"):
            jvm_maven_import_external(
                name = "io_netty_netty_common",
                artifact = "io.netty:netty-common:4.1.34.Final",
                tags = ["maven_coordinates=io.netty:netty-common:4.1.34.Final"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "122931117eacf370b054d0e8a2411efa81de4956a6c3f938b0f0eb915969a425",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("io_netty_netty_handler"):
            jvm_maven_import_external(
                name = "io_netty_netty_handler",
                artifact = "io.netty:netty-handler:4.1.34.Final",
                tags = ["maven_coordinates=io.netty:netty-handler:4.1.34.Final"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "035616801fe9894ca2490832cf9976536dac740f41e90de1cdd4ba46f04263d1",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("io_netty_netty_resolver"):
            jvm_maven_import_external(
                name = "io_netty_netty_resolver",
                artifact = "io.netty:netty-resolver:4.1.34.Final",
                tags = ["maven_coordinates=io.netty:netty-resolver:4.1.34.Final"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "774221ed4c130b532865770b10630bc12d0d400127da617ee0ac8de2a7ac2097",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("io_netty_netty_transport"):
            jvm_maven_import_external(
                name = "io_netty_netty_transport",
                artifact = "io.netty:netty-transport:4.1.34.Final",
                tags = ["maven_coordinates=io.netty:netty-transport:4.1.34.Final"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "2b3f7d3a595101def7d411793a675bf2a325964475fd7bdbbe448e908de09445",
                exports = ["@io_netty_netty_common//jar"],
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("junit_junit"):
            jvm_maven_import_external(
                name = "junit_junit",
                artifact = "junit:junit:4.13",
                tags = ["maven_coordinates=junit:junit:4.13"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "4b8532f63bdc0e0661507f947eb324a954d1dbac631ad19c8aa9a00feed1d863",
                licenses = ["notice"],  # EPL 1.0
            )

        if not native.existing_rule("com_google_api_grpc_proto_google_common_protos"):
            jvm_maven_import_external(
                name = "com_google_api_grpc_proto_google_common_protos",
                artifact = "com.google.api.grpc:proto-google-common-protos:1.12.0",
                tags = ["maven_coordinates=com.google.api.grpc:proto-google-common-protos:1.12.0"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "bd60cd7a423b00fb824c27bdd0293aaf4781be1daba6ed256311103fb4b84108",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("io_grpc_grpc_context"):
            jvm_maven_import_external(
                name = "io_grpc_grpc_context",
                artifact = "io.grpc:grpc-context:1.18.0",
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "12bc83b9fa3aa7550d75c4515b8ae74f124ba14d3692a5ef4737a2e855cbca2f",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("io_grpc_grpc_core"):
            jvm_maven_import_external(
                name = "io_grpc_grpc_core",
                artifact = "io.grpc:grpc-core:1.18.0",
                server_urls = ["https://repo1.maven.org/maven2"],
                licenses = ["notice"],  # Apache 2.0
                runtime_deps = [
                    "@io_opencensus_opencensus_api//jar",
                    "@io_opencensus_opencensus_contrib_grpc_metrics//jar",
                ],
                artifact_sha256 = "fcc02e49bb54771af51470e85611067a8b6718d0126af09da34bbb1e12096f5f",
            )

        if not native.existing_rule("io_grpc_grpc_netty"):
            jvm_maven_import_external(
                name = "io_grpc_grpc_netty",
                artifact = "io.grpc:grpc-netty:1.18.0",
                server_urls = ["https://repo1.maven.org/maven2"],
                licenses = ["notice"],  # Apache 2.0
                runtime_deps = [
                    "@io_netty_netty_buffer//jar",
                    "@io_netty_netty_codec//jar",
                    "@io_netty_netty_codec_http//jar",
                    "@io_netty_netty_handler//jar",
                    "@io_netty_netty_resolver//jar",
                    "@io_netty_netty_transport//jar",
                ],
                artifact_sha256 = "9954db681d8a80c143603712bcf85ab9c76284fb5817b0253bba9ea773bb6803",
                deps = [
                    "@io_netty_netty_codec_http2//jar",
                ],
            )

        if not native.existing_rule("io_grpc_grpc_stub"):
            jvm_maven_import_external(
                name = "io_grpc_grpc_stub",
                artifact = "io.grpc:grpc-stub:1.18.0",
                server_urls = ["https://repo1.maven.org/maven2"],
                licenses = ["notice"],  # Apache 2.0
                artifact_sha256 = "6509fbbcf953f9c426f891021279b2fb5fb21a27c38d9d9ef85fc081714c2450",
            )

        if not native.existing_rule("io_grpc_grpc_protobuf"):
            jvm_maven_import_external(
                name = "io_grpc_grpc_protobuf",
                artifact = "io.grpc:grpc-protobuf:1.18.0",
                artifact_sha256 = "ab714cf4fec2c588f9d8582c2844485c287afa2a3a8da280c62404e312b2d2b1",
                server_urls = ["https://repo1.maven.org/maven2"],
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("io_grpc_grpc_protobuf_lite"):
            jvm_maven_import_external(
                name = "io_grpc_grpc_protobuf_lite",
                artifact = "io.grpc:grpc-protobuf-lite:1.18.0",
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "108a16c2b70df636ee78976916d6de0b8f393b2b45b5b62909fc03c1a928ea9b",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("javax_annotation_javax_annotation_api"):
            jvm_maven_import_external(
                name = "javax_annotation_javax_annotation_api",
                artifact = "javax.annotation:javax.annotation-api:1.2",
                tags = ["maven_coordinates=javax.annotation:javax.annotation-api:1.2"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "5909b396ca3a2be10d0eea32c74ef78d816e1b4ead21de1d78de1f890d033e04",
                licenses = ["reciprocal"],  # CDDL License
            )

        if not native.existing_rule("io_opencensus_opencensus_api"):
            jvm_maven_import_external(
                name = "io_opencensus_opencensus_api",
                artifact = "io.opencensus:opencensus-api:0.21.0",
                tags = ["maven_coordinates=io.opencensus:opencensus-api:0.21.0"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "8e2cb0f6391d8eb0a1bcd01e7748883f0033b1941754f4ed3f19d2c3e4276fc8",
                licenses = ["notice"],  # Apache 2.0
            )

        if not native.existing_rule("io_opencensus_opencensus_contrib_grpc_metrics"):
            jvm_maven_import_external(
                name = "io_opencensus_opencensus_contrib_grpc_metrics",
                artifact = "io.opencensus:opencensus-contrib-grpc-metrics:0.21.0",
                tags = ["maven_coordinates=io.opencensus:opencensus-contrib-grpc-metrics:0.21.0"],
                server_urls = ["https://repo1.maven.org/maven2"],
                artifact_sha256 = "29fc79401082301542cab89d7054d2f0825f184492654c950020553ef4ff0ef8",
                licenses = ["notice"],  # Apache 2.0
            )

        # Auto common
        if not native.existing_rule("com_google_auto_common"):
            java_import_external(
                name = "com_google_auto_common",
                jar_sha256 = "b876b5fddaceeba7d359667f6c4fb8c6f8658da1ab902ffb79ec9a415deede5f",
                jar_urls = [
                    "https://mirror.bazel.build/repo1.maven.org/maven2/com/google/auto/auto-common/0.10/auto-common-0.10.jar",
                    "https://repo1.maven.org/maven2/com/google/auto/auto-common/0.10/auto-common-0.10.jar",
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
                    "https://mirror.bazel.build/repo1.maven.org/maven2/com/google/auto/service/auto-service/1.0-rc2/auto-service-1.0-rc2.jar",
                    "https://repo1.maven.org/maven2/com/google/auto/service/auto-service/1.0-rc2/auto-service-1.0-rc2.jar",
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
                    "https://mirror.bazel.build/repo1.maven.org/maven2/com/google/auto/value/auto-value/1.7.4/auto-value-1.7.4.jar",
                    "https://repo1.maven.org/maven2/com/google/auto/value/auto-value/1.7.4/auto-value-1.7.4.jar",
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
                    "https://mirror.bazel.build/repo1.maven.org/maven2/com/google/auto/value/auto-value-annotations/1.6/auto-value-annotations-1.7.4.jar",
                    "https://repo1.maven.org/maven2/com/google/auto/value/auto-value-annotations/1.7.4/auto-value-annotations-1.7.4.jar",
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
                server_urls = ["https://repo1.maven.org/maven2"],
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
    if analyzer_deps:
        # GoogleTest/GoogleMock framework. Used by most unit-tests.
        if not native.existing_rule("com_google_googletest"):
            # How to update:
            # Googletest generally just does daily (or even subdaily) releases along
            # with occasional numbered releases.
            #
            #  https://github.com/google/googletest/commits/master
            #  pick a recent release.
            #  Hit the 'clipboard with a left arrow' icon to copy the commit hex
            #    COMMIT=<paste commit hex>
            #    PREFIX=googletest-
            #    REPO=https://github.com/google/googletest/archive/
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
                name = "com_google_googletest",
                # Commit from 2020-02-21
                url = "https://github.com/google/googletest/archive//6f5fd0d7199b9a19faa9f499ecc266e6ae0329e7.tar.gz",
                sha256 = "51e6c4b4449aab8f31e69d0ff89565f49a1f3628a42e24f214e8b02b3526e3bc",
                strip_prefix = "googletest-6f5fd0d7199b9a19faa9f499ecc266e6ae0329e7",
            )

    if testing_deps:
        # Google Benchmark framework. Used by benchmark tests.
        if not native.existing_rule("com_github_google_benchmark"):
            http_archive(
                name = "com_github_google_benchmark",
                url = "https://github.com/google/benchmark/archive/v1.5.1.tar.gz",
                sha256 = "23082937d1663a53b90cb5b61df4bcc312f6dee7018da78ba00dd6bd669dfef2",
                strip_prefix = "benchmark-1.5.1",
            )

    if analyzer_deps:
        # RE2 Regex Framework, mostly used in unit tests.
        if not native.existing_rule("com_googlesource_code_re2"):
            http_archive(
                name = "com_googlesource_code_re2",
                urls = [
                    "https://github.com/google/re2/archive/d1394506654e0a19a92f3d8921e26f7c3f4de969.tar.gz",
                ],
                sha256 = "ac855fb93dfa6878f88bc1c399b9a2743fdfcb3dc24b94ea9a568a1c990b1212",
                strip_prefix = "re2-d1394506654e0a19a92f3d8921e26f7c3f4de969",
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

        # Json.
        if not native.existing_rule("json"):
            http_archive(
                name = "json",
                # JSON for Modern C++
                url = "https://github.com/nlohmann/json/archive/v3.7.3.zip",
                strip_prefix = "json-3.7.3",
                sha256 = "e109cd4a9d1d463a62f0a81d7c6719ecd780a52fb80a22b901ed5b6fe43fb45b",
                build_file_content = """cc_library(
    name="json",
    visibility=["//visibility:public"],
    hdrs=["single_include/nlohmann/json.hpp"]
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

    if analyzer_deps:
        if not native.existing_rule("google_bazel_common"):
            http_archive(
                name = "google_bazel_common",
                strip_prefix = "bazel-common-e768dbfea5bac239734b3f59b2a1d7464c6dbd26",
                urls = ["https://github.com/google/bazel-common/archive/e768dbfea5bac239734b3f59b2a1d7464c6dbd26.zip"],
                sha256 = "17f66ba76073a290add024a4ce7f5f92883832b7da85ffd7677e1f5de9a36153",
            )
    if evaluator_deps:
        if not native.existing_rule("org_publicsuffix"):
            http_archive(
                name = "org_publicsuffix",
                strip_prefix = "list-d111481d5931f704c1d9d3a50af19e4e34fc5ba3",
                urls = ["https://github.com/publicsuffix/list/archive/d111481d5931f704c1d9d3a50af19e4e34fc5ba3.zip"],
                sha256 = "2f84929af28e2b712a235ab544fbb4dd7bd5d075ac351de0723915e528c99a38",
                build_file_content = """licenses(["reciprocal"])

exports_files([
    "LICENSE",
    "public_suffix_list.dat",
    "tests/test_psl.txt",
    ],
    visibility = ["//visibility:public"]
)
alias(
    name = "test_psl.txt",
    actual = "tests/test_psl.txt",
    visibility = ["//visibility:public"]
)
""",
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
    if analyzer_deps:
        all_content = """filegroup(name = "all", srcs = glob(["**"]), visibility = ["//visibility:public"])"""
        bison_build_file_content = all_content + """
filegroup(
    name = "bison_runtime_data",
    srcs = glob(["data/**/*"]),
    output_licenses = ["unencumbered"],
    path = "data",
    visibility = ["//visibility:public"],

)
exports_files(["data"])

"""

        http_archive(
            name = "bison",
            build_file_content = bison_build_file_content,
            strip_prefix = "bison-3.6.2",
            sha256 = "e28ed3aad934de2d1df68be209ac0b454f7b6d3c3d6d01126e5cd2cbadba089a",
            urls = [
                "https://ftp.gnu.org/gnu/bison/bison-3.6.2.tar.gz",
                "https://mirrors.kernel.org/gnu/bison/bison-3.6.2.tar.gz",
            ],
        )

        http_archive(
            name = "flex",
            build_file_content = all_content,
            strip_prefix = "flex-2.6.4",
            sha256 = "e87aae032bf07c26f85ac0ed3250998c37621d95f8bd748b31f15b33c45ee995",
            urls = ["https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz"],
            patches = ["@com_google_zetasql//bazel:flex.patch"],
        )

        if not native.existing_rule("m4"):
            http_archive(
                name = "m4",
                build_file_content = all_content,
                strip_prefix = "m4-1.4.18",
                sha256 = "ab2633921a5cd38e48797bf5521ad259bdc4b979078034a3b790d7fec5493fab",
                urls = [
                    "https://ftp.gnu.org/gnu/m4/m4-1.4.18.tar.gz",
                    "https://mirrors.kernel.org/gnu/m4/m4-1.4.18.tar.gz",
                ],
                patches = ["@com_google_zetasql//bazel:m4.patch"],
            )

        http_archive(
            name = "icu",
            build_file = "@com_google_zetasql//bazel:icu.BUILD",
            strip_prefix = "icu",
            sha256 = "53e37466b3d6d6d01ead029e3567d873a43a5d1c668ed2278e253b683136d948",
            urls = ["https://github.com/unicode-org/icu/releases/download/release-65-1/icu4c-65_1-src.tgz"],
            patches = ["@com_google_zetasql//bazel:icu4c-64_2.patch"],
        )
