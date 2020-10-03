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
# to fully download all dependencies. The exactly nature of what happens at
# each step may change over time (and additional steps may be added in the
# future).

# such that 'my_repo_deps'
load("@com_google_zetasql//bazel:zetasql_deps_step_1.bzl", "zetasql_deps_step_1")

zetasql_deps_step_1()

load("@com_google_zetasql//bazel:zetasql_deps_step_2.bzl", "zetasql_deps_step_2")

zetasql_deps_step_2()

load("@com_google_zetasql//bazel:zetasql_deps_step_3.bzl", "zetasql_deps_step_3")

zetasql_deps_step_3()

load("@com_google_zetasql//bazel:zetasql_deps_step_4.bzl", "zetasql_deps_step_4")

zetasql_deps_step_4()
