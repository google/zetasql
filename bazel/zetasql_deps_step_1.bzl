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

""" Step 1 to load ZetaSQL dependencies. """

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# rules_foreign_cc and grpc (transitively) both require a "bazel_version" repo
# but depend on them being something different. So we have to override them both
# by defining the repo first.
load("@com_google_zetasql//bazel:zetasql_bazel_version.bzl", "zetasql_bazel_version")

def zetasql_deps_step_1(add_bazel_version = True):
    if add_bazel_version:
        zetasql_bazel_version()
    if not native.existing_rule("rules_foreign_cc"):
        http_archive(
            name = "rules_foreign_cc",
            strip_prefix = "rules_foreign_cc-2f9965077f3f237bb73a451a8f367194930a7f24",
            urls = [
                "https://github.com/bazelbuild/rules_foreign_cc/archive/2f9965077f3f237bb73a451a8f367194930a7f24.tar.gz",
            ],
            sha256 = "378603f9775669b6f409e7f08c3267cfbba4707aaf92c770df6ac96f78485de3",
        )
