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
    name = "test_value",
    testonly = 1,
    srcs = ["test_value.cc"],
    hdrs = [
        "test_value.h",
        "using_test_value.cc",
    ],
    copts = ["-Wno-sign-compare"],
    deps = [
        "//zetasql/base",
        "//zetasql/base:status",
        "//zetasql/base:statusor",
        "//zetasql/common:float_margin",
        "//zetasql/common:internal_value",
        "//zetasql/public:numeric_value",
        "//zetasql/public:type",
        "//zetasql/public:value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)
