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
            # Commit from 2019-01-08
            url = "https://github.com/abseil/abseil-cpp/archive/b16aeb6756bdab08cdf12d40baab5b51f7d15b16.tar.gz",
            sha256 = "0d042c15d349bf62586b86e4c70d785cb2d1e1948636920abdd8a79d6027879a",
            strip_prefix = "abseil-cpp-b16aeb6756bdab08cdf12d40baab5b51f7d15b16",
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
        )

    # gRPC
    if not native.existing_rule("com_github_grpc_grpc"):
        http_archive(
            name = "com_github_grpc_grpc",
            # Release 1.18.0
            url = "https://github.com/grpc/grpc/archive/v1.18.0.tar.gz",
            sha256 = "069a52a166382dd7b99bf8e7e805f6af40d797cfcee5f80e530ca3fc75fd06e2",
            strip_prefix = "grpc-1.18.0",
            patches = ["//bazel:grpc-1.18.patch"],
        )

    # gRPC Java
    if not native.existing_rule("io_grpc_grpc_java"):
        http_archive(
            name = "io_grpc_grpc_java",
            # Release 1.18.0
            url = "https://github.com/grpc/grpc-java/archive/v1.18.0.tar.gz",
            strip_prefix = "grpc-java-1.18.0",
            sha256 = "0b86e44f9530fd61eb044b3c64c7579f21857ba96bcd9434046fd22891483a6d",
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
            # Commit on 2019-04-18
            urls = [
                "https://github.com/google/googletest/archive/a53e931dcd00c2556ee181d832e699c9f3c29036.tar.gz",
            ],
            strip_prefix = "googletest-a53e931dcd00c2556ee181d832e699c9f3c29036",
            sha256 = "7850caaf8149a6aded637f472415f84e4246a21d979d3866d71b1e56242f8de2",
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
    )

    http_archive(
        name = "m4",
        build_file_content = all_content,
        strip_prefix = "m4-1.4.13",
        sha256 = "39333a95aaf8620c7f90f1db5527b9e53e993b173de9207df5ea506c91fea549",
        urls = ["https://ftp.gnu.org/gnu/m4/m4-1.4.13.tar.gz"],
    )

    http_archive(
        name = "icu",
        build_file = "//bazel:icu.BUILD",
        strip_prefix = "icu",
        sha256 = "627d5d8478e6d96fc8c90fed4851239079a561a6a8b9e48b0892f24e82d31d6c",
        urls = ["https://github.com/unicode-org/icu/releases/download/release-64-2/icu4c-64_2-src.tgz"],
        patches = ["//bazel:icu4c-64_2.patch"],
    )
