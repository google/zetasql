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
   strip_prefix = "rules_foreign_cc-e3f4b5e0bc9dac9cf036616c13de25e6cd5051a2",
   urls = [
      "https://github.com/bazelbuild/rules_foreign_cc/archive/e3f4b5e0bc9dac9cf036616c13de25e6cd5051a2.tar.gz",
   ],
   sha256 = "136470a38dcd00c7890230402b43004dc947bf1e3dd0289dd1bd2bfb1e0a3484"
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

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")

grpc_java_repositories()

