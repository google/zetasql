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

""" Workspace for Open Source ZetaSQL library """

workspace(name = "com_google_zetasql")

# Bazel doesn't support recursively loading dependencies.
# The recommended pattern is for a repo to provide a 'my_repo_deps()' method
# which will download all dependencies. Thus, a _direct dependency of 'my_repo'
# can ask it to load it dependencies. However, if 'my_repo' has dependencies
# which themselves have dependencies, and provide a 'child_repo_deps()',
# there is no way to compose a workspace such that 'my_repo_deps' calls
# 'child_repo_deps' (since this would represent a serialization of
# load-then-statement, which is forbidden).  So, we take the tactic of providing
# a serialized sequence of numbered steps that must be invoked in series to
# load all dependencies.  Copy the following code exactly into your WORKSPACE
# to fully download all dependencies. The exact nature of what happens at
# each step may change over time (and additional steps may be added in the
# future).

# Skip to step_1 if you don't require java support. If you _only_ need
# java support, you still must include all steps.

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "toolchains_llvm",
    canonical_id = "1.0.0",
    sha256 = "e91c4361f99011a54814e1afbe5c436e0d329871146a3cd58c23a2b4afb50737",
    strip_prefix = "toolchains_llvm-1.0.0",
    url = "https://github.com/bazel-contrib/toolchains_llvm/releases/download/1.0.0/toolchains_llvm-1.0.0.tar.gz",
)

http_archive(
    name = "rules_jvm_external",
    sha256 = "b17d7388feb9bfa7f2fa09031b32707df529f26c91ab9e5d909eb1676badd9a6",
    strip_prefix = "rules_jvm_external-4.5",
    urls = ["https://github.com/bazelbuild/rules_jvm_external/archive/4.5.zip"],
)

# gRPC Java
http_archive(
    name = "io_grpc_grpc_java",
    sha256 = "301e0de87c7659cc790bd2a7265970a71632d55773128c98768385091c0a1a97",
    strip_prefix = "grpc-java-1.61.0",
    url = "https://github.com/grpc/grpc-java/archive/v1.61.0.zip",
)

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")

rules_jvm_external_deps()

load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")

rules_jvm_external_setup()

load("@com_google_zetasql//bazel:zetasql_java_deps.bzl", "zetasql_java_deps")
zetasql_java_deps()

load("@maven//:defs.bzl", "pinned_maven_install")
pinned_maven_install()

# Creates an alias of the form @com_foo_bar//jar for each
# @maven//:com_foo_bar
load("@maven//:compat.bzl", "compat_repositories")
compat_repositories()

# If java support is not required, copy starting from here
load("@com_google_zetasql//bazel:zetasql_deps_step_1.bzl", "zetasql_deps_step_1")

zetasql_deps_step_1()

load("@com_google_zetasql//bazel:zetasql_deps_step_2.bzl", "zetasql_deps_step_2")

zetasql_deps_step_2()

load("@com_google_zetasql//bazel:zetasql_deps_step_3.bzl", "zetasql_deps_step_3")

zetasql_deps_step_3()

# Required only for java builds
load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")
grpc_java_repositories()

load("@com_google_zetasql//bazel:zetasql_deps_step_4.bzl", "zetasql_deps_step_4")

zetasql_deps_step_4()

