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

# Description:
#   ZetaSQL ResolvedAST Java library.

package(
    default_visibility = ["//zetasql:__subpackages__"],
)

load("//zetasql/resolved_ast:build_rules.bzl", "gen_resolved_ast_files")

gen_resolved_ast_files(
    name = "run_gen_resolved_ast_java",
    srcs = [
        "ResolvedNodes.java.template",
    ],
    outs = [
        "ResolvedNodes.java",
    ],
)

java_library(
    name = "resolvedast",
    srcs = [
        "DebugStrings.java",
        "DeserializationHelper.java",
        "ResolvedColumn.java",
        "ResolvedNode.java",
        "ResolvedNodes.java",
    ],
    deps = [
        "//zetasql/java:client",
        "//zetasql/java:types",
        "//zetasql/proto:function_java_proto",
        "//zetasql/public:type_proto_java_proto",
        "//zetasql/public/proto:type_annotation_java_proto",
        "//zetasql/resolved_ast:resolved_ast_enums_java_proto",
        "//zetasql/resolved_ast:resolved_ast_java_proto",
        "//zetasql/resolved_ast:resolved_node_kind_java_proto",
        "//zetasql/resolved_ast:serialization_java_proto",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf//:protobuf_java",
    ],
)
