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

package(default_visibility = ["//zetasql/base:zetasql_implementation"])

filegroup(
    name = "type_annotation_filegroup",
    srcs = [
        "type_annotation.proto",
        "wire_format_annotation.proto",
    ],
)

proto_library(
    name = "type_annotation_proto",
    srcs = [
        "type_annotation.proto",
        "wire_format_annotation.proto",
    ],
    features = ["-proto_dynamic_mode_static_link"],
    deps = ["@com_google_protobuf//:descriptor_proto"],
)

cc_proto_library(
    name = "type_annotation_cc_proto",
    deps = [":type_annotation_proto"],
)

java_proto_library(
    name = "type_annotation_java_proto",
    deps = [":type_annotation_proto"],
)

exports_files([
    "wire_format_annotation.proto",
    "type_annotation.proto",
])
