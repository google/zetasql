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

""" Defines blaze extensions for use testing zetasql engines. """

def gen_resolved_ast_files(
        name,
        srcs = [],
        outs = [],
        data = [],
        flags = [],
        **extra_args):
    """Creates a rule to generate the ResolvedAST files.

    Args:
      name: name of the rule
      srcs: input top-level template files
      outs: output files, corresponding in order to srcs
      data: resource files, including shared logic imported from srcs
      flags: any flags to pass to the rule
      **extra_args: any extra args to pass to the rule
    """
    cmd = "$(location //zetasql/resolved_ast:gen_resolved_ast) "
    for f in flags:
        cmd += f + " "
    cmd += ' --input_templates="'
    for t in srcs:
        cmd += "$(location %s) " % t
    cmd += '" --output_files="'
    for o in outs:
        cmd += "$(location %s) " % o
    cmd += '"'
    if len(data) > 0:
        cmd += ' --data_files="'
        for s in data:
            cmd += "$(location %s) " % s
        cmd += '"'
    native.genrule(
        name = name,
        srcs = srcs + data,
        outs = outs,
        cmd = cmd,
        tools = ["//zetasql/resolved_ast:gen_resolved_ast"],
        **extra_args
    )
