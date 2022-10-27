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

# Followup from zetasql_deps_step_1.bzl
load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")
load("@rules_m4//m4:m4.bzl", "m4_register_toolchains")
load("@rules_flex//flex:flex.bzl", "flex_register_toolchains")
load("@rules_bison//bison:bison.bzl", "bison_register_toolchains")

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

    _load_deps_from_step_1()

    if analyzer_deps:
        # Followup from zetasql_deps_step_1.bzl
        if not native.existing_rule("com_google_googleapis"):
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
                name = "com_google_googleapis",
                url = "https://github.com/googleapis/googleapis/archive/2f9af297c84c55c8b871ba4495e01ade42476c92.tar.gz",
                sha256 = "5bb6b0253ccf64b53d6c7249625a7e3f6c3bc6402abd52d3778bfa48258703a0",
                strip_prefix = "googleapis-2f9af297c84c55c8b871ba4495e01ade42476c92",
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
                # Commit from 2022-09-14
                url = "https://github.com/abseil/abseil-cpp/archive/d423ac0ef052bd7b6fc53fd1a026a44e1713d993.tar.gz",
                sha256 = "18e401ac5f456ea5a757861230c23d0d2a0154eaccfec01bce94b85a013419f5",
                strip_prefix = "abseil-cpp-d423ac0ef052bd7b6fc53fd1a026a44e1713d993",
            )

        # required by many python libraries
        if not native.existing_rule("six_archive"):
            http_archive(
                name = "six_archive",
                url = "https://pypi.python.org/packages/source/s/six/six-1.10.0.tar.gz",
                sha256 = "105f8d68616f8248e24bf0e9372ef04d3cc10104f1980f54d57b2ce73a5ad56a",
                strip_prefix = "six-1.10.0",
                build_file_content = """licenses(["notice"])

exports_files(["LICENSE"])

py_library(
    name = "six",
    srcs_version = "PY2AND3",
    visibility = ["//visibility:public"],
    srcs = glob(["six.py"]),
)""",
            )

        native.bind(
            name = "six",
            actual = "@six_archive//:six",
        )

        # Abseil (Python)
        if not native.existing_rule("io_abseil_py"):
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
                # Non-release commit from Nov 17, 2021
                urls = [
                    "https://github.com/abseil/abseil-py/archive/a1c1af693b9f15bd0f67fe383cb05e7cc955556b.tar.gz",
                ],
                sha256 = "f233de3482bd724a68c4998e03761536ca99dc8b1fc5941fe04f5cf9a39feb54",
                strip_prefix = "abseil-py-a1c1af693b9f15bd0f67fe383cb05e7cc955556b",
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
                # Commit from 2022-02-16
                url = "https://github.com/google/riegeli/archive/934428f44a6d120cb6c065315c788aa3a1be6b66.tar.gz",
                sha256 = "a54dafa634db87723db106bc44ef365b1b442d8862aafbeb5f1d2e922049e587",
                strip_prefix = "riegeli-934428f44a6d120cb6c065315c788aa3a1be6b66",
            )
    if evaluator_deps:
        # Differential Privacy
        if not native.existing_rule("com_google_differential_privacy"):
            http_archive(
                name = "com_google_differential_privacy",
                # Release from 2022-06-02
                url = "https://github.com/google/differential-privacy/archive/5e7cf28bf55ebac52fc65419364388c33ebc01a4.tar.gz",
                sha256 = "fcdbbafe7aa86415b7b8db654a86595ad894b7d264611f7262b804a193a82adc",
                strip_prefix = "differential-privacy-5e7cf28bf55ebac52fc65419364388c33ebc01a4",
            )

        # Differential Privacy - cc
        if not native.existing_rule("com_google_cc_differential_privacy"):
            http_archive(
                name = "com_google_cc_differential_privacy",
                # Release from 2022-06-02
                url = "https://github.com/google/differential-privacy/archive/5e7cf28bf55ebac52fc65419364388c33ebc01a4.tar.gz",
                sha256 = "fcdbbafe7aa86415b7b8db654a86595ad894b7d264611f7262b804a193a82adc",
                strip_prefix = "differential-privacy-5e7cf28bf55ebac52fc65419364388c33ebc01a4/cc",
            )

        # Boringssl
        if not native.existing_rule("boringssl"):
            http_archive(
                name = "boringssl",
                # Commit from 2021-11-01
                urls = [
                    "https://github.com/google/boringssl/archive/4fb158925f7753d80fb858cb0239dff893ef9f15.tar.gz",
                ],
                sha256 = "e168777eb0fc14ea5a65749a2f53c095935a6ea65f38899a289808fb0c221dc4",
                strip_prefix = "boringssl-4fb158925f7753d80fb858cb0239dff893ef9f15",
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
        # Protobuf
        if not native.existing_rule("com_google_protobuf"):
            http_archive(
                name = "com_google_protobuf",
                urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.19.3.tar.gz"],
                sha256 = "390191a0d7884b3e52bb812c440ad1497b9d484241f37bb8e2ccc8c2b72d6c36",
                strip_prefix = "protobuf-3.19.3",
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
                urls = ["https://github.com/grpc/grpc/archive/v1.43.2.tar.gz"],
                sha256 = "b74ce7d26fe187970d1d8e2c06a5d3391122f7bc1fdce569aff5e435fb8fe780",
                strip_prefix = "grpc-1.43.2",
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
                # Commit from 2022-01-12
                url = "https://github.com/google/googletest/archive//2d07f12b607c528b21795ab672cff3afaf64f7a1.tar.gz",
                sha256 = "219132fd586a870ebde5df6007d7f81dbd4b4a411466569301b3a0f55a207b37",
                strip_prefix = "googletest-2d07f12b607c528b21795ab672cff3afaf64f7a1",
            )

    if testing_deps:
        # Google Benchmark framework. Used by benchmark tests.
        if not native.existing_rule("com_github_google_benchmark"):
            http_archive(
                name = "com_github_google_benchmark",
                url = "https://github.com/google/benchmark/archive/v1.6.1.tar.gz",
                sha256 = "6132883bc8c9b0df5375b16ab520fac1a85dc9e4cf5be59480448ece74b278d4",
                strip_prefix = "benchmark-1.6.1",
            )

    if analyzer_deps:
        # RE2 Regex Framework, mostly used in unit tests.
        if not native.existing_rule("com_googlesource_code_re2"):
            # 2022-01-19
            http_archive(
                name = "com_googlesource_code_re2",
                urls = [
                    "https://github.com/google/re2/archive/e8cb5ecb8ee1066611aa937a42fa10514edf30fb.tar.gz",
                ],
                sha256 = "c5f46950cdf33175f0668f454d9b6b4fe1b5a71ffd9283213e77fb04461af099",
                strip_prefix = "re2-e8cb5ecb8ee1066611aa937a42fa10514edf30fb",
            )

        # Jinja2.
        if not native.existing_rule("jinja"):
            http_archive(
                name = "jinja",
                url = "https://github.com/pallets/jinja/archive/3.0.1.tar.gz",
                strip_prefix = "jinja-3.0.1/src",
                sha256 = "1e37a6f86c29fa8ace108ea72b41d2d5c5bd67d79be14bfeca3ba6eb37d789de",
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
                    "https://github.com/pallets/markupsafe/archive/2.1.1.tar.gz",
                ],
                sha256 = "0f83b6d1bf6fa65546221d42715034e7e654845583a84906c5936590f9a7ad8f",
                strip_prefix = "markupsafe-2.1.1/src/markupsafe",
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

        m4_register_toolchains(version = "1.4.18")
        flex_register_toolchains(version = "2.6.4")
        bison_register_toolchains(version = "3.3.2")

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

        http_archive(
            name = "icu",
            build_file = "@com_google_zetasql//bazel:icu.BUILD",
            strip_prefix = "icu",
            sha256 = "53e37466b3d6d6d01ead029e3567d873a43a5d1c668ed2278e253b683136d948",
            urls = ["https://github.com/unicode-org/icu/releases/download/release-65-1/icu4c-65_1-src.tgz"],
            patches = ["@com_google_zetasql//bazel:icu4c-64_2.patch"],
        )
