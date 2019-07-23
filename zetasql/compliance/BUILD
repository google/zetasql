#
# Copyright 2019 ZetaSQL Authors
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

package(default_visibility = ["//zetasql:__subpackages__"])

cc_library(
    name = "type_helpers",
    srcs = ["type_helpers.cc"],
    hdrs = ["type_helpers.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        "//zetasql/base:ret_check",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/public:strings",
        "//zetasql/public:type",
        "//zetasql/resolved_ast",
    ],
)

cc_test(
    name = "type_helpers_test",
    size = "small",
    srcs = ["type_helpers_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":type_helpers",
        "@com_google_googletest//:gtest_main",
        "//zetasql/base/testing:status_matchers",
        "//zetasql/testing:test_value",
    ],
)
