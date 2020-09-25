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

""" Workarounds for the 'bazel_version' rule. """

# Makes Bazel version available in BUILD files as bazel_version.
def _impl(repository_ctx):
    repository_ctx.file("bazel_version.bzl", "bazel_version='" + native.bazel_version + "'")
    repository_ctx.file("def.bzl", "BAZEL_VERSION='" + native.bazel_version + "'")
    repository_ctx.file(
        "BUILD",
        "exports_files(['bazel_version.bzl', 'def.bzl'])",
    )

def zetasql_bazel_version():
    # Required by gRPC
    if not native.existing_rule("bazel_version"):
        _bazel_version_repository = repository_rule(
            implementation = _impl,
            local = True,
        )
        _bazel_version_repository(name = "bazel_version")
