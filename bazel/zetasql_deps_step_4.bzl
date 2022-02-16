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

""" Step 4 to load ZetaSQL dependencies. """

# This must be loaded _after_ com_github_grpc_grpc, so we move it to this file

# Needed by (at least) com_github_grpc_grpc
load("@build_bazel_rules_apple//apple:repositories.bzl", "apple_rules_dependencies")

# Needed by (at least) com_github_grpc_grpc
load("@build_bazel_apple_support//lib:repositories.bzl", "apple_support_dependencies")

def zetasql_deps_step_4():
    apple_rules_dependencies()
    apple_support_dependencies()
