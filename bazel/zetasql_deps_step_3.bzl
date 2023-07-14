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

""" Step 3 to load ZetaSQL dependencies. """

load("@google_bazel_common//:workspace_defs.bzl", "google_common_workspace_rules")
load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")
load("@com_google_googleapis//:repository_rules.bzl", "switched_rules_by_language")

def zetasql_deps_step_3():
    protobuf_deps()
    grpc_deps()
    switched_rules_by_language(
        name = "com_google_googleapis_imports",
        java = True,
        cc = True,
    )
    google_common_workspace_rules()
