# Copyright 2023 Google LLC
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

"""Definitions to generate parsers through Textmapper"""

def tm_syntax(
        name,
        src,
        outs,
        visibility = None,
        compatible_with = None,
        templates = None,
        textmapper = "@com_github_inspirer_textmapper//cmd/textmapper:textmapper"):
    """Generates the requested outs from Textmapper sources.

    Example usage:

      tm_syntax(
        name = "syntax",
        src = "java.tm",
        outs = [
            "lexer.go",
            "lexer_tables.go",
            "token.go",
        ],
      )

    Outs should enumerate all expected outputs for Textmapper.

    Args:
      name: Unique identifier for the build rule.
      src: Textmapper file for the language.
      outs: Expected outputs from Textmapper.
      visibility: Visibility of the rule.
      compatible_with: Standard Blaze compatible_with attribute.
      templates: Location of the Textmapper templates to use.
      textmapper: Build target for Textmapper tool.
    """

    if not templates:
        tmpl_arg = ""
        srcs = [src]
    else:
        tmpl_arg = "-i $$(dirname $$(echo $(locations {templates}) | cut --delimiter ' ' --fields 1))".format(
            templates = templates,
        )
        srcs = [src, templates]
    tm_cmd = """
        $(location {textmapper}) generate -o $(@D) {tmpl_arg} $(location {src})
    """.format(
        src = src,
        textmapper = textmapper,
        tmpl_arg = tmpl_arg,
    )
    native.genrule(
        name = name,
        srcs = srcs,
        outs = outs,
        cmd = tm_cmd,
        visibility = visibility,
        compatible_with = compatible_with,
        tools = [textmapper],
        exec_compatible_with = ["@platforms//os:linux", "@platforms//os:macos"],
    )
