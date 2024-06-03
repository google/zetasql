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

# Contains helper classes for privacy rewriters.
package(
    default_visibility = ["//zetasql/base:zetasql_implementation"],
)

cc_library(
    name = "privacy_utility",
    srcs = ["privacy_utility.cc"],
    hdrs = ["privacy_utility.h"],
    deps = [
        "//zetasql/base:status",
        "//zetasql/common:errors",
        "//zetasql/resolved_ast",
    ],
)
