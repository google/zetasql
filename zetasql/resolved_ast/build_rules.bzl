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

""" Defines blaze extensions for use testing zetasql engines. """

def gen_resolved_ast_files(
        name,
        srcs = [],
        outs = [],
        **extra_args):
    cmd = "$(location //zetasql/resolved_ast:gen_resolved_ast) "
    for t in srcs:
        cmd += "$(location %s) " % t
    for o in outs:
        cmd += "$(location %s) " % o

    native.genrule(
        name = name,
        srcs = srcs,
        outs = outs,
        cmd = cmd,
        tools = ["//zetasql/resolved_ast:gen_resolved_ast"],
        **extra_args
    )
