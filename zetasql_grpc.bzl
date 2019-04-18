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

""" Rule for gRPC C++ API generation """

load("@com_github_grpc_grpc//bazel:generate_cc.bzl", "generate_cc")

def cc_grpc_library(name, srcs, deps, **kwargs):
    if len(srcs) > 1:
        fail("Only one srcs value supported", "srcs")

    codegen_grpc_target = name + "__grpc_codegen"

    generate_cc(
        name = codegen_grpc_target,
        srcs = srcs,
        plugin = "@com_github_grpc_grpc//:grpc_cpp_plugin",
        well_known_protos = True,
        **kwargs
    )

    native.cc_library(
        name = name,
        srcs = [codegen_grpc_target],
        hdrs = [codegen_grpc_target],
        deps = ["@com_github_grpc_grpc//:grpc++_codegen_proto"] + deps,
        **kwargs
    )
