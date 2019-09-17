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

""" Workspace for Open Source ZetaSQL library """

workspace(name = "com_google_zetasql")

###
# Setup `rules_foreign_cc`
# This package allows us to take source-dependencies on non-bazelified packages.
# In particular, it supports `./configure && make` style packages.
###
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
   name = "rules_foreign_cc",
   strip_prefix = "rules_foreign_cc-c3d5405cbc570257e7c9f75f902fab42241e6a53",
   urls = [
      "https://github.com/bazelbuild/rules_foreign_cc/archive/c3d5405cbc570257e7c9f75f902fab42241e6a53.tar.gz",
   ],
   sha256 = "4a643853f5be24458696b347e4507a8868369fb88d18df4c9edcf5f40394943c"
)

load("@rules_foreign_cc//:workspace_definitions.bzl", "rules_foreign_cc_dependencies")

rules_foreign_cc_dependencies()

###
# Load Dependencies
# Note this must be loaded _after_ rules_foreign_cc setup, because it
# implicitly depends on it.
###
load(":zetasql_deps.bzl", "zetasql_deps")

# Download and Build dependencies.
zetasql_deps()

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
protobuf_deps()

load("@google_bazel_common//:workspace_defs.bzl", "google_common_workspace_rules")
google_common_workspace_rules()

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")
grpc_deps()

load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")
grpc_java_repositories()


