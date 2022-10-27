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

"""Build rule for generating C or C++ sources with Flex.

IMPORTANT: we _strongly recommend_ that you include a unique and project-
specific `%option prefix="myproject"` directive in your scanner spec to avoid
very hard-to-debug symbol name conflict problems if two scanners are linked
into the same dynamically-linked executable.

By default, flex includes the definition of a static function `yyunput` in its
output. If you never use the lex `unput` function in your lex rules, however,
`yyunput` will never be called. This causes problems building the output file,
as llvm issues warnings about static functions that are never called. To avoid
this problem, use `%option nounput` in the declarations section if your lex
rules never use `unput`.

Note that if you use the c++ mode of flex, you will need to include the
boilerplate header `FlexLexer.h` file in any `cc_library` which includes the
generated flex scanner directly.  This is typically done by
`#include <FlexLexer.h>` which typically is installed in /usr/include
when flex is installed.

Examples
--------

This is a simple example.
```
genlex(
    name = "html_lex_lex",
    src = "html.lex",
    out = "html_lexer.c",
)
```

This example uses a `.tab.hh` file.
```
genlex(
    name = "rules_l",
    src = "rules.lex",
    includes = [
        "rules.tab.hh",
    ],
    out = "rules.yy.cc",
)
```
"""

load("@rules_flex//flex:flex.bzl", "FLEX_TOOLCHAIN_TYPE", "flex_toolchain")
load("@rules_m4//m4:m4.bzl", "M4_TOOLCHAIN_TYPE")

def _genlex_impl(ctx):
    """Implementation for genlex rule."""
    flex = flex_toolchain(ctx)

    # Compute the prefix, if not specified.
    if ctx.attr.prefix:
        prefix = ctx.attr.prefix
    else:
        prefix = ctx.file.src.basename.partition(".")[0]

    # Construct the arguments.
    args = ctx.actions.args()
    args.add("-o", ctx.outputs.out)
    args.add("-P", prefix)
    args.add_all(ctx.attr.lexopts)
    args.add(ctx.file.src)

    ctx.actions.run(
        executable = flex.flex_tool,
        env = flex.flex_env,
        arguments = [args],
        inputs = ctx.files.src + ctx.files.includes,
        outputs = [ctx.outputs.out],
        mnemonic = "Flex",
        progress_message = "Generating %s from %s" % (
            ctx.outputs.out.short_path,
            ctx.file.src.short_path,
        ),
    )

genlex = rule(
    implementation = _genlex_impl,
    doc = "Generate C/C++-language sources from a lex file using Flex.",
    attrs = {
        "src": attr.label(
            mandatory = True,
            allow_single_file = [".l", ".ll", ".lex", ".lpp"],
            doc = "The .lex source file for this rule",
        ),
        "includes": attr.label_list(
            allow_files = True,
            doc = "A list of headers that are included by the .lex file",
        ),
        "out": attr.output(mandatory = True, doc = "The generated source file"),
        "prefix": attr.string(
            doc = "External symbol prefix for Flex. This string is " +
                  "passed to flex as the -P option, causing the resulting C " +
                  "file to define external functions named 'prefix'text, " +
                  "'prefix'in, etc.  The default is the basename of the source" +
                  "file without the .lex extension.",
        ),
        "lexopts": attr.string_list(
            doc = "A list of options to be added to the flex command line.",
        ),
    },
    toolchains = [
        FLEX_TOOLCHAIN_TYPE,
        M4_TOOLCHAIN_TYPE,
    ],
    output_to_genfiles = True,
)

def _flex_header(ctx):
    flex = flex_toolchain(ctx)

    return [
        CcInfo(compilation_context = cc_common.create_compilation_context(
            includes = depset([flex.flex_lexer_h.dirname]),
            headers = depset([flex.flex_lexer_h]),
        )),
    ]

flex_header = rule(
    implementation = _flex_header,
    output_to_genfiles = True,
    toolchains = [
        FLEX_TOOLCHAIN_TYPE,
        M4_TOOLCHAIN_TYPE,
    ],
)
