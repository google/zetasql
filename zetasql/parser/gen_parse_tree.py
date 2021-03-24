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

"""Defines parse tree nodes for the ZetaSQL parser.

This program defines parse tree node subclasses of ASTNode. It generates
headers and other files from templates.

Still a work in progress.

"""

import enum
import re

from absl import app
from absl import flags
import jinja2

from zetasql.parser.generator_utils import CleanComment
from zetasql.parser.generator_utils import ScalarType
from zetasql.parser.generator_utils import Trim

_make_enum_name_re = re.compile(r'([a-z])([A-Z])')


def NameToEnumName(name):
  """Convert a camel-case c++ ASTClassName into AST_CLASS_NAME."""
  return _make_enum_name_re.sub(r'\1_\2', name.replace('AST', 'Ast')).upper()

SCALAR_BOOL = ScalarType(
    'bool',
    cpp_default='false')

SCALAR_STRING = ScalarType(
    'std::string')

SCALAR_ID_STRING = ScalarType(
    'IdString')


# Identifies the FieldLoader method used to populate member fields.
# Each node field in a subclass is added to the children_ vector in ASTNode,
# then additionally added to a type-specific field in the subclass using one
# of these methods:
# REQUIRED: The next node in the vector, which must exist, is used for this
#           field.
# OPTIONAL: The next node in the vector, if it exists, is used for this field.
# REST_AS_REPEATED: All remaining nodes, if any, are used for this field,
#           which should be a vector type.
# See Add* methods in ast_node.h for further details.
class FieldLoaderMethod(enum.Enum):
  REQUIRED = 0
  OPTIONAL = 1
  REST_AS_REPEATED = 2


def Field(name,
          ctype,
          field_loader=FieldLoaderMethod.OPTIONAL,
          comment=None,
          gen_setters_and_getters=True):
  """Make a field to put in a node class.

  Args:
    name: field name
    ctype: c++ type for this field
           Should be a ScalarType like an int, string or enum type,
           or the name of a node class type (e.g. ASTExpression).
           Cannot be a pointer type, and should not include modifiers like
           const.
    field_loader: FieldLoaderMethod enum specifies which FieldLoader method
           to use for this field.
    comment: Comment text for this field.  Text will be stripped and
             de-indented.
    gen_setters_and_getters: When False, suppress generation of default
           template-based get and set methods. Non-standard alternatives
           may be supplied via extra_defs.
  Returns:
    The newly created field.

  Raises:
    RuntimeError: If an error is detected in one or more arguments.
  """
  if field_loader == FieldLoaderMethod.REST_AS_REPEATED:
    is_vector = True
  else:
    is_vector = False

  member_name = name + '_'
  if isinstance(ctype, ScalarType):
    member_type = ctype.ctype
    cpp_default = ctype.cpp_default
    is_node_ptr = False
    enum_name = None
    element_storage_type = None
  else:
    element_storage_type = 'const %s*' % ctype
    if is_vector:
      member_type = 'absl::Span<%s const>' % element_storage_type
      cpp_default = ''
      is_node_ptr = False
      enum_name = None
    else:
      member_type = 'const %s*' % ctype
      cpp_default = 'nullptr'
      is_node_ptr = True
      enum_name = NameToEnumName(ctype)
  return {
      'ctype': ctype,
      'cpp_default': cpp_default,
      'member_name': member_name,  # member variable name
      'name': name,  # name without trailing underscore
      'comment': CleanComment(comment, prefix='  // '),
      'member_type': member_type,
      'is_node_ptr': is_node_ptr,
      'field_loader': field_loader.name,
      'enum_name': enum_name,
      'is_vector': is_vector,
      'element_storage_type': element_storage_type,
      'gen_setters_and_getters': gen_setters_and_getters,
  }


class TreeGenerator(object):
  """Generates code to define tree objects.
  """

  def __init__(self):
    self.nodes = []

  def AddNode(self,
              name,
              parent,
              is_abstract=False,
              fields=None,
              extra_defs='',
              comment=None,
              use_custom_debug_string=False,
              force_gen_init_fields=False):
    """Add a node class to be generated.

    Args:
      name: class name for this node
      parent: class name of the parent node
      is_abstract: true if this node is an abstract class
      fields: list of fields in this class; created with Field function
      extra_defs: extra c++ definitions to put in this class.
      comment: Comment text for this node. Text will be stripped and
          de-indented.
      use_custom_debug_string: If True, generate prototype for overridden
          SingleNodeDebugString method.
      force_gen_init_fields: If True, generate the InitFields method even when
          there are no fields to be added, so as to ensure there are no children
    """
    if fields is None:
      fields = []
    if is_abstract:
      class_final = ''
    else:
      class_final = 'final '
    enum_name = NameToEnumName(name)
    # generate init_fields if there is a least one is_node_ptr or
    # is_vector field, or if force_gen_init_fields was requested.
    gen_init_fields = force_gen_init_fields
    for field in fields:
      if field['is_node_ptr'] or field['is_vector']:
        gen_init_fields = True
    node_dict = ({
        'name': name,
        'parent': parent,
        'class_final': class_final,
        'is_abstract': is_abstract,
        'comment': CleanComment(comment, prefix='// '),
        'fields': fields,
        'enum_name': enum_name,
        'extra_defs': extra_defs.rstrip(),
        'use_custom_debug_string': use_custom_debug_string,
        'gen_init_fields': gen_init_fields})

    self.nodes.append(node_dict)

  def Generate(
      self,
      output_path,
      h_template_path=None):
    """Materialize the template to generate the output file."""

    jinja_env = jinja2.Environment(
        undefined=jinja2.StrictUndefined,
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        line_statement_prefix='# ',
        loader=jinja2.FileSystemLoader('', followlinks=True))

    context = {
        'nodes': self.nodes,
        # For when we need to force a blank line and jinja wants to
        # eat blank lines from the template.
        'blank_line': '\n'
    }

    h_template = jinja_env.get_template(h_template_path)
    out = open(output_path, 'wt')
    out.write(Trim(h_template.render(context)))
    out.close()


def main(argv):
  if len(argv) != 3:
    raise Exception(
        'Usage: %s <output/path/to/parse_tree_generated.h> <input/path/to/parse_tree_generated.h.template>'
    )

  output_path = argv[1]
  h_template_path = argv[2]

  gen = TreeGenerator()

  gen.AddNode(
      name='ASTStatement',
      parent='ASTNode',
      is_abstract=True,
      comment="""
    Superclass of all Statements.
      """,
      extra_defs="""
  bool IsStatement() const final { return true; }
  bool IsSqlStatement() const override { return true; }
      """
    )

  gen.AddNode(
      name='ASTQueryExpression',
      parent='ASTNode',
      is_abstract=True,
      comment="""
    Superclass for all query expressions.  These are top-level syntactic
    constructs (outside individual SELECTs) making up a query.  These include
    Query itself, Select, UnionAll, etc.
      """,
      extra_defs="""
  bool IsQueryExpression() const override { return true; }
      """,
      fields=[
          Field(
              'parenthesized',
              SCALAR_BOOL,
              field_loader=FieldLoaderMethod.REQUIRED)
      ])

  gen.AddNode(
      name='ASTQuery',
      parent='ASTQueryExpression',
      fields=[
          Field(
              'with_clause',
              'ASTWithClause',
              comment="""
      If present, the WITH clause wrapping this query.
            """),
          Field(
              'query_expr',
              'ASTQueryExpression',
              field_loader=FieldLoaderMethod.REQUIRED,
              comment="""
      The query_expr can be a single Select, or a more complex structure
      composed out of nodes like SetOperation and Query.
            """),
          Field(
              'order_by',
              'ASTOrderBy',
              comment="""
      If present, applies to the result of <query_expr_> as appropriate.
            """),
          Field(
              'limit_offset',
              'ASTLimitOffset',
              comment="""
      If present, this applies after the result of <query_expr_> and
      <order_by_>.
            """),
          Field('is_nested', SCALAR_BOOL),
          Field(
              'is_pivot_input',
              SCALAR_BOOL,
              comment="""
                True if this query represents the input to a pivot clause.
                """)
      ],
      use_custom_debug_string=True
      )

  gen.AddNode(
      name='ASTExpression',
      parent='ASTNode',
      is_abstract=True,
      extra_defs="""
  bool IsExpression() const override { return true; }

  // Returns true if this expression is allowed to occur as a child of a
  // comparison expression. This is not allowed for unparenthesized comparison
  // expressions and operators with a lower precedence level (AND, OR, and NOT).
  virtual bool IsAllowedInComparison() const { return true; }
      """,
      fields=[
          Field(
              'parenthesized',
              SCALAR_BOOL,
              field_loader=FieldLoaderMethod.REQUIRED)
      ])

  gen.AddNode(
      name='ASTQueryStatement',
      parent='ASTStatement',
      comment="""
    Represents a single query statement.
      """,
      fields=[
          Field(
              'query',
              'ASTQuery',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTSelect',
      parent='ASTQueryExpression',
      use_custom_debug_string=True,
      fields=[
          Field(
              'hint',
              'ASTHint'),
          Field(
              'anonymization_options',
              'ASTOptionsList'),
          Field(
              'distinct',
              SCALAR_BOOL),
          Field(
              'select_as',
              'ASTSelectAs'),
          Field(
              'select_list',
              'ASTSelectList',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'from_clause',
              'ASTFromClause'),
          Field(
              'where_clause',
              'ASTWhereClause'),
          Field(
              'group_by',
              'ASTGroupBy'),
          Field(
              'having',
              'ASTHaving'),
          Field(
              'qualify',
              'ASTQualify'),
          Field(
              'window_clause',
              'ASTWindowClause'),
      ])

  gen.AddNode(
      name='ASTSelectList',
      parent='ASTNode',
      fields=[
          Field(
              'columns',
              'ASTSelectColumn',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTSelectColumn',
      parent='ASTNode',
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'alias',
              'ASTAlias')
      ])

  gen.AddNode(
      name='ASTLeaf',
      parent='ASTExpression',
      is_abstract=True,
      use_custom_debug_string=True,
      extra_defs="""
  // image() references data with the same lifetime as this ASTLeaf object.
  absl::string_view image() const { return image_; }
  void set_image(std::string image) { image_ = std::move(image); }

  bool IsLeaf() const override { return true; }
      """,
      # Triggers check that there were no children.
      force_gen_init_fields=True,
      fields=[
          Field(
              'image',
              SCALAR_STRING,
              gen_setters_and_getters=False)
      ])

  gen.AddNode(
      name='ASTIntLiteral',
      parent='ASTLeaf',
      extra_defs="""

  bool is_hex() const;
      """,
      )

  gen.AddNode(
      name='ASTIdentifier',
      parent='ASTExpression',
      use_custom_debug_string=True,
      extra_defs="""
  // Set the identifier string.  Input <identifier> is the unquoted identifier.
  // There is no validity checking here.  This assumes the identifier was
  // validated and unquoted in zetasql.jjt.
  void SetIdentifier(IdString identifier) {
    id_string_ = identifier;
  }

  // Get the unquoted and unescaped string value of this identifier.
  IdString GetAsIdString() const { return id_string_; }
  std::string GetAsString() const { return id_string_.ToString(); }
  absl::string_view GetAsStringView() const {
    return id_string_.ToStringView();
  }
      """,
      # Triggers check that there were no children.
      force_gen_init_fields=True,
      fields=[
          Field(
              'id_string',
              SCALAR_ID_STRING,
              gen_setters_and_getters=False)
      ])

  gen.AddNode(
      name='ASTAlias',
      parent='ASTNode',
      fields=[
          Field(
              'identifier',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REQUIRED),
      ],
      extra_defs="""
  // Get the unquoted and unescaped string value of this alias.
  std::string GetAsString() const;
  absl::string_view GetAsStringView() const;
  IdString GetAsIdString() const;
      """
    )

  gen.AddNode(
      name='ASTGeneralizedPathExpression',
      parent='ASTExpression',
      is_abstract=True,
      comment="""
 Parent class that corresponds to the subset of ASTExpression nodes that are
 allowed by the <generalized_path_expression> grammar rule. It allows for some
 extra type safety vs. simply passing around ASTExpression as
 <generalized_path_expression>s.

 Only the following node kinds are allowed:
 - AST_PATH_EXPRESSION
 - AST_DOT_GENERALIZED_FIELD where the left hand side is a
   <generalized_path_expression>.
 - AST_DOT_IDENTIFIER where the left hand side is a
   <generalized_path_expression>.
 - AST_ARRAY_ELEMENT where the left hand side is a
   <generalized_path_expression>

 Note that the type system does not capture the "pureness constraint" that,
 e.g., the left hand side of an AST_DOT_GENERALIZED_FIELD must be a
 <generalized_path_expression> in order for the node. However, it is still
 considered a bug to create a variable with type ASTGeneralizedPathExpression
 that does not satisfy the pureness constraint (similarly, it is considered a
 bug to call a function with an ASTGeneralizedPathExpression argument that
 does not satisfy the pureness constraint).
    """,
      extra_defs="""
  // Returns an error if 'path' contains a node that cannot come from the
  // <generalized_path_expression> grammar rule.
  static absl::Status VerifyIsPureGeneralizedPathExpression(
      const ASTExpression* path);
      """)

  gen.AddNode(
      name='ASTPathExpression',
      parent='ASTGeneralizedPathExpression',
      comment="""
 This is used for dotted identifier paths only, not dotting into
 arbitrary expressions (see ASTDotIdentifier below).
      """,
      fields=[
          Field(
              'names',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED,
              gen_setters_and_getters=False),
      ],
      # The existing API unfortunately uses name(int i) rather than names(int i)
      extra_defs="""
  const int num_names() const { return names_.size(); }
  const absl::Span<const ASTIdentifier* const>& names() const {
    return names_;
  }
  const ASTIdentifier* name(int i) const { return names_[i]; }
  const ASTIdentifier* first_name() const { return names_.front(); }
  const ASTIdentifier* last_name() const { return names_.back(); }

  // Return this PathExpression as a dotted SQL identifier string, with
  // quoting if necessary.  If <max_prefix_size> is non-zero, include at most
  // that many identifiers from the prefix of <path>.
  std::string ToIdentifierPathString(size_t max_prefix_size = 0) const;

  // Return the vector of identifier strings (without quoting).
  std::vector<std::string> ToIdentifierVector() const;

  // Similar to ToIdentifierVector(), but returns a vector of IdString's,
  // avoiding the need to make copies.
  std::vector<IdString> ToIdStringVector() const;
      """
    )

  gen.AddNode(
      name='ASTTableExpression',
      parent='ASTNode',
      is_abstract=True,
      comment="""
   Superclass for all table expressions.  These are things that appear in the
   from clause and produce a stream of rows like a table.
   This includes table scans, joins and subqueries.
    """,
      extra_defs="""
  bool IsTableExpression() const override { return true; }

  // Return the alias, if the particular subclass has one.
  virtual const ASTAlias* alias() const { return nullptr; }

  // Return the ASTNode location of the alias for this table expression,
  // if applicable.
  const ASTNode* alias_location() const;
      """
    )

  gen.AddNode(
      name='ASTTablePathExpression',
      parent='ASTTableExpression',
      comment="""
   TablePathExpression are the TableExpressions that introduce a single scan,
   referenced by a path expression or UNNEST, and can optionally have
   aliases, hints, and WITH OFFSET.
    """,
      fields=[
          Field(
              'path_expr',
              'ASTPathExpression',
              comment="""
               One of path_exp or path_exp must be non-NULL but not both.
              """),
          Field(
              'unnest_expr',
              'ASTUnnestExpression'),
          Field(
              'hint',
              'ASTHint'),
          Field(
              'alias',
              'ASTAlias',
              # Existing API getter specifies "override"
              gen_setters_and_getters=False),
          Field(
              'with_offset',
              'ASTWithOffset',
              comment="""
              Present if the scan had WITH OFFSET.
              """),
          Field(
              'pivot_clause',
              'ASTPivotClause',
              comment="""
              One of pivot_clause or unpivot_clause can be present but not both.
              """),
          Field(
              'unpivot_clause',
              'ASTUnpivotClause'),
          Field(
              'for_system_time',
              'ASTForSystemTime'),
          Field(
              'sample_clause',
              'ASTSampleClause'),
      ],
      extra_defs="""
  const ASTAlias* alias() const override { return alias_; }
      """
    )

  gen.AddNode(
      name='ASTFromClause',
      parent='ASTNode',
      fields=[
          Field(
              'table_expression',
              'ASTTableExpression',
              field_loader=FieldLoaderMethod.REQUIRED,
              comment="""
  A FromClause has exactly one TableExpression child.
  If the FROM clause has commas, they will be expressed as a tree
  of ASTJoin nodes with join_type=COMMA.
              """),
      ],
    )

  gen.AddNode(
      name='ASTWhereClause',
      parent='ASTNode',
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ],
    )

  gen.AddNode(
      name='ASTBooleanLiteral',
      parent='ASTLeaf',
      fields=[
          Field(
              'value',
              SCALAR_BOOL),
      ],
    )

  gen.AddNode(
      name='ASTAndExpr',
      parent='ASTExpression',
      fields=[
          Field(
              'conjuncts',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ],
      extra_defs="""
  bool IsAllowedInComparison() const override { return parenthesized(); }
      """
      )

  gen.Generate(
      output_path,
      h_template_path=h_template_path)

if __name__ == '__main__':
  app.run(main)
