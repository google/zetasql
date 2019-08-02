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
"""This module provides access to the jni headers."""

licenses(["notice"])  # Apache v2.0

package(default_visibility = ["//:__subpackages__"])

cc_library(
    name = "jni",
    srcs = select({
        "@bazel_tools//src/conditions:darwin": [
            "@local_jdk//:jni_header",
            "@local_jdk//:jni_md_header-darwin",
        ],
        "@bazel_tools//src/conditions:linux_x86_64": [
            "@local_jdk//:jni_header",
            "@local_jdk//:jni_md_header-linux",
        ],
    }),
    copts = ["-Wno-sign-compare"],
    includes = select({
        "@bazel_tools//src/conditions:darwin": [
            "../../external/local_jdk/include",
            "../../external/local_jdk/include/darwin",
        ],
        "@bazel_tools//src/conditions:linux_x86_64": [
            "../../external/local_jdk/include",
            "../../external/local_jdk/include/linux",
        ],
    }),
    linkstatic = 1,
)
