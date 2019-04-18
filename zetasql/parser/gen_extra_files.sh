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

#!/bin/bash

if [[ "$#" != 4 ]]; then
  # Expected args are filenames for these:
  #   parse_tree.h                  (input)
  #   ZetaSqlParserVisitor.h      (output)
  #   parse_tree_decls.h            (output)
  #   parse_tree_accept_methods.inc (output)
  echo "Wrong args to gen_extra_files.sh"
  exit 1
fi

# Generate parse_tree_visitor.h
(
echo '#ifndef STORAGE_ZETASQL_PARSER_PARSE_TREE_VISITOR_H_'
echo '#define STORAGE_ZETASQL_PARSER_PARSE_TREE_VISITOR_H_'
echo '#include "zetasql/parser/parse_tree.h"'
echo
echo 'namespace zetasql {'
echo 'class ParseTreeVisitor {'
echo ' public:'
echo '  virtual ~ParseTreeVisitor() {}'
echo '  virtual void visit(const ASTNode *node, void* data) = 0;'

# Generate a visitX method declaration for each concrete class X.
sed -f - "$1" <<EOF
# Matches the start of a final (i.e., concrete) class.
/^class AST[a-zA-Z]* final : public/ {
  # Do a search/replace to make the method definition.
  s/class \(AST[a-zA-Z]*\).*/  virtual void visit\1(const \1* node, void* data) = 0;\n/
  p
}
# Delete everything else so we just keep the new methods.
d
EOF

echo '};'
echo
echo 'class DefaultParseTreeVisitor : public ParseTreeVisitor {'
echo ' public:'
echo '  virtual void defaultVisit(const ASTNode* node, void* data) = 0;'
echo '  void visit(const ASTNode* node, void* data) override {'
echo '    defaultVisit(node, data);'
echo '  }'

# Generate a visitX method implementation for each concrete class X.
sed -f - "$1" <<EOF
# Matches the start of a final (i.e., concrete) class.
/^class AST[a-zA-Z]* final : public/ {
  # Do a search/replace to make the method definition.
  s/class \(AST[a-zA-Z]*\).*/  void visit\1(const \1* node, void* data) override {\n    defaultVisit(node, data);\n  }\n/
  p
}
# Delete everything else so we just keep the new methods.
d
EOF

echo '};'
echo
echo '}  // namespace zetasql'
echo '#endif  // STORAGE_ZETASQL_PARSER_PARSE_TREE_VISITOR_H_'
) > "$2"

# Generate parse_tree_decls.h, which has a forward declaration
# for each AST node class in parse_tree.h.
(
echo '#ifndef STORAGE_ZETASQL_PARSER_PARSE_TREE_DECLS_H'
echo '#define STORAGE_ZETASQL_PARSER_PARSE_TREE_DECLS_H'
echo 'namespace zetasql {'
sed '/^class AST[a-zA-Z]* : public/ {s/ :.*/;/; p} ; d' "$1"
sed '/^class AST[a-zA-Z]* final : public/ {s/ final :.*/;/; p} ; d' "$1"
echo
echo '}  // namespace zetasql'
echo '#endif  // STORAGE_ZETASQL_PARSER_PARSE_TREE_DECLS_H'
) > "$3"

# Generate parse_tree_accept_methods.inc, which has an implementation
# of Accept for each non-abstract AST node class in parse_tree.h.
(
echo '#include "zetasql/parser/parse_tree.h"'
echo 'namespace zetasql {'
# Generate the Accept method bodies for each class.
sed -f - "$1" <<EOF
# Matches the start of a final (i.e., non-abstract) class.
/^class AST[a-zA-Z]* final : public/ {
  # Do a search/replace to make the method definition.
  s/class \(AST[a-zA-Z]*\).*/void \1::Accept(ParseTreeVisitor* visitor, void* data) const {\n  visitor->visit\1(this, data);\n}\n/
  p
}
# Delete everything else so we just keep the new methods.
d
EOF
echo '}  // namespace zetasql'
) > "$4"
