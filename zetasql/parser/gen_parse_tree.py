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

from zetasql.parser import ast_enums_pb2
from zetasql.parser.generator_utils import CleanComment
from zetasql.parser.generator_utils import ScalarType
from zetasql.parser.generator_utils import Trim

_make_enum_name_re = re.compile(r'([a-z])([A-Z])')


def NormalCamel(name):
  """Convert C++ ASTClassName into normalized AstClassName.

  A few legacy classes have irregular names requiring special-casing in order to
  be remapped to an ASTNodeKind.

  Args:
    name: name of the C++ class.
  Returns:
    Normalized camel-case equivalent of the class name.
  """
  if name == 'ASTBigNumericLiteral':
    return 'AstBignumericLiteral'
  elif name == 'ASTJSONLiteral':
    return 'AstJsonLiteral'
  else:
    return name.replace('AST', 'Ast')


def NameToNodeKind(name):
  """Convert camel-case C++ ASTClassName to AST_CLASS_NAME in ASTNodeKind."""
  return _make_enum_name_re.sub(r'\1_\2', NormalCamel(name)).upper()

SCALAR_BOOL = ScalarType(
    'bool',
    cpp_default='false')

SCALAR_STRING = ScalarType(
    'std::string')

SCALAR_ID_STRING = ScalarType(
    'IdString')

SCALAR_INT = ScalarType(
    'int',
    cpp_default='0')

# enum in type.proto
SCALAR_TYPE_KIND = ScalarType(ctype='TypeKind', cpp_default='TYPE_UNKNOWN')


def EnumScalarType(enum_name, node_name, cpp_default):
  """Create a ScalarType for enums defined in ast_enums.proto.

  Args:
    enum_name: name of the enum.
    node_name: name of the ASTNode that this enum belongs to.
    cpp_default: default value for c++.
  Returns:
    The ScalarType.
  """
  return ScalarType(
      ctype=enum_name,
      is_enum=True,
      scoped_ctype='%s::%s' % (node_name, enum_name),
      cpp_default=cpp_default)


SCALAR_OP = EnumScalarType('Op', 'ASTBinaryExpression', 'NOT_SET')

SCALAR_ORDERING_SPEC = EnumScalarType('OrderingSpec', 'ASTOrderingExpression',
                                      'UNSPECIFIED')

SCALAR_JOIN_TYPE = EnumScalarType('JoinType', 'ASTJoin', 'DEFAULT_JOIN_TYPE')

SCALAR_JOIN_HINT = EnumScalarType('JoinHint', 'ASTJoin', 'NO_JOIN_HINT')

SCALAR_AS_MODE = EnumScalarType('AsMode', 'ASTSelectAs', 'NOT_SET')

SCALAR_NULL_HANDLING_MODIFIER = EnumScalarType('NullHandlingModifier',
                                               'ASTFunctionCall',
                                               'DEFAULT_NULL_HANDLING')

SCALAR_MODIFIER = EnumScalarType('Modifier', 'ASTExpressionSubquery', 'NONE')

SCALAR_MODIFIER_KIND = EnumScalarType('ModifierKind', 'ASTHavingModifier',
                                      'MAX')
SCALAR_OPERATION_TYPE = EnumScalarType('OperationType', 'ASTSetOperation',
                                       'NOT_SET')


# Identifies the FieldLoader method used to populate member fields.
# Each node field in a subclass is added to the children_ vector in ASTNode,
# then additionally added to a type-specific field in the subclass using one
# of these methods:
# NONE:     This field should not be FieldLoaded
# REQUIRED: The next node in the vector, which must exist, is used for this
#           field.
# OPTIONAL: The next node in the vector, if it exists, is used for this field.
# OPTIONAL_EXPRESSION: The next node in the vector for which IsExpression()
#           is true, if it exists, is used for this field.
# REST_AS_REPEATED: All remaining nodes, if any, are used for this field,
#           which should be a vector type.
# REPEATING_WHILE_IS_NODE_KIND: Appends remaining nodes to the vector, stopping
#           when the node kind of next node is not 'node_kind'.
# REPEATING_WHILE_IS_EXPRESSION: Appends remaining nodes to the vector, stopping
#           when the next node is !IsExpression().
# See Add* methods in ast_node.h for further details.
class FieldLoaderMethod(enum.Enum):
  NONE = 0
  REQUIRED = 1
  OPTIONAL = 2
  REST_AS_REPEATED = 3
  OPTIONAL_EXPRESSION = 4
  REPEATING_WHILE_IS_NODE_KIND = 5
  REPEATING_WHILE_IS_EXPRESSION = 6


def Field(name,
          ctype,
          field_loader=FieldLoaderMethod.OPTIONAL,
          comment=None,
          private_comment=None,
          gen_setters_and_getters=True,
          getter_is_virtual=False):
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
    comment: Comment for this field's public getter/setter method. Text will be
             stripped and de-indented.
    private_comment: Comment for the field in the private section.
    gen_setters_and_getters: When False, suppress generation of default
           template-based get and set methods. Non-standard alternatives
           may be supplied via extra_defs.
    getter_is_virtual: Indicates getter overrides virtual method in superclass.
  Returns:
    The newly created field.

  Raises:
    RuntimeError: If an error is detected in one or more arguments.
  """
  if field_loader in (FieldLoaderMethod.REST_AS_REPEATED,
                      FieldLoaderMethod.REPEATING_WHILE_IS_NODE_KIND,
                      FieldLoaderMethod.REPEATING_WHILE_IS_EXPRESSION):
    is_vector = True
  else:
    is_vector = False
  if not gen_setters_and_getters:
    assert comment is None, ('Comments for %s should be placed in '
                             'extra_private_defs') % name
  member_name = name + '_'
  getter_override = ''
  if getter_is_virtual:
    assert gen_setters_and_getters, ('getter_is_virtual cannot be used when '
                                     'gen_setters_and_getters is false')
    getter_override = 'override '
  if isinstance(ctype, ScalarType):
    member_type = ctype.ctype
    cpp_default = ctype.cpp_default
    is_node_ptr = False
    node_kind = None
    element_storage_type = None
  else:
    element_storage_type = 'const %s*' % ctype
    node_kind = NameToNodeKind(ctype)
    if is_vector:
      member_type = 'absl::Span<%s const>' % element_storage_type
      cpp_default = ''
      is_node_ptr = False
    else:
      member_type = 'const %s*' % ctype
      cpp_default = 'nullptr'
      is_node_ptr = True
  return {
      'ctype': ctype,
      'cpp_default': cpp_default,
      'member_name': member_name,  # member variable name
      'name': name,  # name without trailing underscore
      'comment': CleanComment(comment, prefix='  // '),
      'private_comment': CleanComment(private_comment, prefix='  // '),
      'member_type': member_type,
      'is_node_ptr': is_node_ptr,
      'field_loader': field_loader.name,
      'node_kind': node_kind,
      'is_vector': is_vector,
      'element_storage_type': element_storage_type,
      'gen_setters_and_getters': gen_setters_and_getters,
      'getter_override': getter_override,
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
              extra_private_defs='',
              comment=None,
              use_custom_debug_string=False,
              force_gen_init_fields=False):
    """Add a node class to be generated.

    Args:
      name: class name for this node
      parent: class name of the parent node
      is_abstract: true if this node is an abstract class
      fields: list of fields in this class; created with Field function
      extra_defs: extra public C++ definitions to put in this class.
      extra_private_defs: extra C++ definitions to add to the private
          portion of the class, including additional comments.
      comment: Comment text for this node. Text will be stripped and
          de-indented.
      use_custom_debug_string: If True, generate prototype for overridden
          SingleNodeDebugString method.
      force_gen_init_fields: If True, generate the InitFields method even when
          there are no fields to be added, so as to ensure there are no children
    """
    enum_defs = self._GenEnums(name)
    if fields is None:
      fields = []
    if is_abstract:
      class_final = ''
    else:
      class_final = 'final '
    node_kind = NameToNodeKind(name)
    # generate init_fields if there is a least one is_node_ptr or
    # is_vector field which doesn't have field_loader NONE, or if
    # force_gen_init_fields was requested.
    gen_init_fields = force_gen_init_fields
    for field in fields:
      if (field['is_node_ptr'] or field['is_vector']
         ) and field['field_loader'] != 'NONE':
        gen_init_fields = True
    node_dict = ({
        'name': name,
        'parent': parent,
        'class_final': class_final,
        'is_abstract': is_abstract,
        'comment': CleanComment(comment, prefix='// '),
        'fields': fields,
        'node_kind': node_kind,
        'extra_defs': extra_defs.rstrip(),
        'extra_private_defs': extra_private_defs.lstrip('\n').rstrip(),
        'use_custom_debug_string': use_custom_debug_string,
        'gen_init_fields': gen_init_fields,
        'enum_defs': enum_defs})

    self.nodes.append(node_dict)

  def _GenEnums(self, cpp_class_name):
    """Gen C++ enums from the corresponding <cpp_class_name>Enums proto message.

    Args:
      cpp_class_name: C++ class name the enums should be imported into.

    Returns:
      A list of lines, one per enum, for inclusion in the C++ header file.
    """
    message_types = ast_enums_pb2.DESCRIPTOR.message_types_by_name
    message_name = cpp_class_name + 'Enums'
    if message_name not in message_types:
      return []

    enum_defs = []
    for enum_type in message_types[message_name].enum_types:
      enum_values = []
      comment = '// This enum is equivalent to %s::%s in ast_enums.proto\n' % (
          message_name, enum_type.name)
      for value in enum_type.values:
        enum_val = '\n    %s = %s::%s' % (value.name, message_name, value.name)
        enum_values.append(enum_val)
      enum_def = '%s  enum %s { %s \n  };' % (comment, enum_type.name,
                                              ', '.join(enum_values))
      enum_defs.append(enum_def)
    return enum_defs

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
               Exactly one of path_exp or unnest_expr must be non-NULL.
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
              At most one of pivot_clause or unpivot_clause can be present.
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

  gen.AddNode(
      name='ASTBinaryExpression',
      parent='ASTExpression',
      use_custom_debug_string=True,
      fields=[
          Field('op',
                SCALAR_OP,
                comment="""
                See description of Op values in ast_enums.proto.
                """),
          Field(
              'is_not',
              SCALAR_BOOL,
              comment="""
              Signifies whether the binary operator has a preceding NOT to it.
              For NOT LIKE and IS NOT.
              """),
          Field(
              'lhs',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'rhs',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ],
      extra_defs="""
  // Returns name of the operator in SQL, including the NOT keyword when
  // necessary.
  std::string GetSQLForOperator() const;

  bool IsAllowedInComparison() const override;
      """
      )

  gen.AddNode(
      name='ASTStringLiteral',
      parent='ASTLeaf',
      fields=[
          Field(
              'string_value',
              SCALAR_STRING,
              gen_setters_and_getters=False),
      ],
      extra_defs="""
  // The parsed and validated value of this literal. The raw input value can be
  // found in image().
  const std::string& string_value() const { return string_value_; }
  void set_string_value(std::string string_value) {
    string_value_ = std::move(string_value);
  }
       """
      )

  gen.AddNode(
      name='ASTStar',
      parent='ASTLeaf',
      )

  gen.AddNode(
      name='ASTOrExpr',
      parent='ASTExpression',
      fields=[
          Field(
              'disjuncts',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ],
      extra_defs="""
  bool IsAllowedInComparison() const override { return parenthesized(); }
      """
      )

  gen.AddNode(
      name='ASTGroupingItem',
      parent='ASTNode',
      comment="""
      Represents a grouping item, which is either an expression (a regular
      group by key) or a rollup list.
      """,
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.OPTIONAL_EXPRESSION,
              comment="""
              Exactly one of expression() and rollup() will be non-NULL.
              """),
          Field(
              'rollup',
              'ASTRollup'),
      ])

  gen.AddNode(
      name='ASTGroupBy',
      parent='ASTNode',
      fields=[
          Field(
              'hint',
              'ASTHint'),
          Field(
              'grouping_items',
              'ASTGroupingItem',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTOrderingExpression',
      parent='ASTNode',
      use_custom_debug_string=True,
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'collate',
              'ASTCollate'),
          Field(
              'null_order',
              'ASTNullOrder'),
          Field(
              'ordering_spec',
              SCALAR_ORDERING_SPEC)
      ],
      extra_defs="""
  bool descending() const { return ordering_spec_ == DESC; }
      """,
  )

  gen.AddNode(
      name='ASTOrderBy',
      parent='ASTNode',
      fields=[
          Field(
              'hint',
              'ASTHint'),
          Field(
              'ordering_expressions',
              'ASTOrderingExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTLimitOffset',
      parent='ASTNode',
      fields=[
          Field(
              'limit',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED,
              comment="""
          The LIMIT value. Never NULL.
              """),
          Field(
              'offset',
              'ASTExpression',
              field_loader=FieldLoaderMethod.OPTIONAL_EXPRESSION,
              comment="""
          The OFFSET value. NULL if no OFFSET specified.
              """),
      ])

  gen.AddNode(
      name='ASTFloatLiteral',
      parent='ASTLeaf',
      )

  gen.AddNode(
      name='ASTNullLiteral',
      parent='ASTLeaf',
      )

  gen.AddNode(
      name='ASTOnClause',
      parent='ASTNode',
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED)
      ])

  gen.AddNode(
      name='ASTWithClauseEntry',
      parent='ASTNode',
      fields=[
          Field(
              'alias',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'query',
              'ASTQuery',
              field_loader=FieldLoaderMethod.REQUIRED)
      ])

  gen.AddNode(
      name='ASTJoin',
      parent='ASTTableExpression',
      use_custom_debug_string=True,
      comment="""
      Joins could introduce multiple scans and cannot have aliases.
      It can also represent a JOIN with a list of consecutive ON/USING
      clauses. Such a JOIN is only for internal use, and will never show up in
      the final parse tree.
      """,
      fields=[
          Field(
              'lhs',
              'ASTTableExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'hint',
              'ASTHint'),
          Field(
              'rhs',
              'ASTTableExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'on_clause',
              'ASTOnClause'),
          Field(
              'using_clause',
              'ASTUsingClause'),
          Field(
              'clause_list',
              'ASTOnOrUsingClauseList',
              gen_setters_and_getters=False,
              private_comment="""
      Note that if consecutive ON/USING clauses are encountered, they are saved
      as clause_list_, and both on_clause_ and using_clause_ will be nullptr.
              """),
          Field(
              'join_type',
              SCALAR_JOIN_TYPE),
          Field(
              'join_hint',
              SCALAR_JOIN_HINT),
          Field(
              'natural',
              SCALAR_BOOL),
          Field(
              'unmatched_join_count',
              SCALAR_INT,
              comment="""
      unmatched_join_count_ and transformation_needed are for internal use for
      handling consecutive ON/USING clauses. They are not used in the final AST.
              """,
              private_comment="""
      The number of qualified joins that do not have a matching ON/USING clause.
      See the comment in join_processor.cc for details.
              """),
          Field(
              'transformation_needed',
              SCALAR_BOOL,
              private_comment="""
      Indicates if this node needs to be transformed. See the comment
      in join_processor.cc for details.
      This is true if contains_clause_list_ is true, or if there is a JOIN with
      ON/USING clause list on the lhs side of the tree path.
      For internal use only. See the comment in join_processor.cc for details.
              """),
          Field(
              'contains_comma_join',
              SCALAR_BOOL,
              private_comment="""
      Indicates whether this join contains a COMMA JOIN on the lhs side of the
      tree path.
              """),
      ],
      extra_defs="""
  // Represents a parse error when parsing join expressions.
  // See comments in file join_proccessor.h for more details.
  struct ParseError {
    // The node where the error occurs.
    const ASTNode* error_node;

    std::string message;
  };

  const ParseError* parse_error() const {
    return parse_error_.get();
  }
  void set_parse_error(std::unique_ptr<ParseError> parse_error) {
    parse_error_ = std::move(parse_error);
  }

  // The join type and hint strings
  std::string GetSQLForJoinType() const;
  std::string GetSQLForJoinHint() const;
       """,
      extra_private_defs="""
  std::unique_ptr<ParseError> parse_error_ = nullptr;
       """
      )

  gen.AddNode(
      name='ASTWithClause',
      parent='ASTNode',
      use_custom_debug_string=True,
      fields=[
          Field(
              'with',
              'ASTWithClauseEntry',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
          Field(
              'recursive',
              SCALAR_BOOL)
      ])

  gen.AddNode(
      name='ASTHaving',
      parent='ASTNode',
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTType',
      parent='ASTNode',
      is_abstract=True,
      extra_defs="""
  bool IsType() const override { return true; }

  virtual const ASTTypeParameterList* type_parameters() const = 0;
      """,
      )

  gen.AddNode(
      name='ASTSimpleType',
      parent='ASTType',
      comment="""
 TODO This takes a PathExpression and isn't really a simple type.
 Calling this NamedType or TypeName may be more appropriate.
      """,
      fields=[
          Field(
              'type_name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'type_parameters',
              'ASTTypeParameterList',
              getter_is_virtual=True,
              field_loader=FieldLoaderMethod.OPTIONAL),
      ])

  gen.AddNode(
      name='ASTArrayType',
      parent='ASTType',
      fields=[
          Field(
              'element_type',
              'ASTType',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'type_parameters',
              'ASTTypeParameterList',
              getter_is_virtual=True,
              field_loader=FieldLoaderMethod.OPTIONAL),
      ])

  gen.AddNode(
      name='ASTStructField',
      parent='ASTNode',
      fields=[
          Field(
              'name',
              'ASTIdentifier',
              comment="""
     name_ will be NULL for anonymous fields like in STRUCT<int, string>.
              """),
          Field(
              'type',
              'ASTType',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTStructType',
      parent='ASTType',
      fields=[
          Field(
              'struct_fields',
              'ASTStructField',
              field_loader=FieldLoaderMethod.REPEATING_WHILE_IS_NODE_KIND),
          Field(
              'type_parameters',
              'ASTTypeParameterList',
              getter_is_virtual=True,
              field_loader=FieldLoaderMethod.OPTIONAL),
      ])

  gen.AddNode(
      name='ASTCastExpression',
      parent='ASTExpression',
      use_custom_debug_string=True,
      fields=[
          Field(
              'expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'type',
              'ASTType',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'format',
              'ASTFormatClause'),
          Field(
              'is_safe_cast',
              SCALAR_BOOL),
      ])

  gen.AddNode(
      name='ASTSelectAs',
      parent='ASTNode',
      comment="""
   This represents a SELECT with an AS clause giving it an output type.
     SELECT AS STRUCT ...
     SELECT AS VALUE ...
     SELECT AS <type_name> ...
   Exactly one of these is present.
      """,
      use_custom_debug_string=True,
      fields=[
          Field(
              'type_name',
              'ASTPathExpression'),
          Field(
              'as_mode',
              SCALAR_AS_MODE,
              comment="""
              Set if as_mode() == kTypeName;
              """),
      ],
      extra_defs="""

  bool is_select_as_struct() const { return as_mode_ == STRUCT; }
  bool is_select_as_value() const { return as_mode_ == VALUE; }
      """
      )

  gen.AddNode(
      name='ASTRollup',
      parent='ASTNode',
      fields=[
          Field(
              'expressions',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTFunctionCall',
      parent='ASTExpression',
      use_custom_debug_string=True,
      fields=[
          Field(
              'function',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'arguments',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REPEATING_WHILE_IS_EXPRESSION),
          Field(
              'having_modifier',
              'ASTHavingModifier',
              private_comment="""
       Set if the function was called with FUNC(args HAVING {MAX|MIN} expr).
              """),
          Field(
              'clamped_between_modifier',
              'ASTClampedBetweenModifier',
              comment="""
      If present, applies to the inputs of anonimized aggregate functions.
              """,
              private_comment="""
      Set if the function was called with
      FUNC(args CLAMPED BETWEEN low AND high).
              """),
          Field(
              'order_by',
              'ASTOrderBy',
              comment="""
      If present, applies to the inputs of aggregate functions.
              """,
              private_comment="""
      Set if the function was called with FUNC(args ORDER BY cols).
              """),
          Field(
              'limit_offset',
              'ASTLimitOffset',
              comment="""
      If present, this applies to the inputs of aggregate functions.
              """,
              private_comment="""
      Set if the function was called with FUNC(args LIMIT N).
              """),
          Field(
              'hint',
              'ASTHint',
              comment="""
      hint if not null.
              """,
              private_comment="""
              Optional hint.
              """),
          Field(
              'with_group_rows',
              'ASTWithGroupRows',
              private_comment="""
      Set if the function was called WITH GROUP_ROWS(...).
              """),
          Field(
              'null_handling_modifier',
              SCALAR_NULL_HANDLING_MODIFIER,
              comment="""
      If present, modifies the input behavior of aggregate functions.
              """,
              private_comment="""
      Set if the function was called with FUNC(args {IGNORE|RESPECT} NULLS).
              """),
          Field(
              'distinct',
              SCALAR_BOOL,
              private_comment="""
      True if the function was called with FUNC(DISTINCT args).
              """),
          Field(
              'is_current_date_time_without_parentheses',
              SCALAR_BOOL,
              comment="""
      Used by the Bison parser to mark CURRENT_<date/time> functions to which no
      parentheses have yet been applied.
              """,
              private_comment="""
      This is set by the Bison parser to indicate a parentheses-less call to
      CURRENT_* functions. The parser parses them as function calls even without
      the parentheses, but then still allows function call parentheses to be
      applied.
              """),
      ],
      extra_defs="""
  // Convenience method that returns true if any modifiers are set. Useful for
  // places in the resolver where function call syntax is used for purposes
  // other than a function call (e.g., <array>[OFFSET(<expr>) or WEEK(MONDAY)]).
  bool HasModifiers() const {
    return distinct_ || null_handling_modifier_ != DEFAULT_NULL_HANDLING ||
           having_modifier_ != nullptr ||
           clamped_between_modifier_ != nullptr || order_by_ != nullptr ||
           limit_offset_ != nullptr || with_group_rows_ != nullptr;
  }
      """)

  gen.AddNode(
      name='ASTArrayConstructor',
      parent='ASTExpression',
      fields=[
          Field(
              'type',
              'ASTArrayType',
              comment="""
              May return NULL. Occurs only if the array is constructed through
              ARRAY<type>[...] syntax and not ARRAY[...] or [...].
          """),
          Field(
              'elements',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ],
      # legacy code uses element() instead of elements() for getter
      extra_defs="""
  // DEPRECATED - use elements(int i)
  const ASTExpression* element(int i) const { return elements_[i]; }
      """)

  gen.AddNode(
      name='ASTStructConstructorArg',
      parent='ASTNode',
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'alias',
              'ASTAlias',
              field_loader=FieldLoaderMethod.OPTIONAL),
      ])

  gen.AddNode(
      name='ASTStructConstructorWithParens',
      parent='ASTExpression',
      comment="""
      This node results from structs constructed with (expr, expr, ...).
      This will only occur when there are at least two expressions.
      """,
      fields=[
          Field(
              'field_expressions',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTStructConstructorWithKeyword',
      parent='ASTExpression',
      comment="""
      This node results from structs constructed with the STRUCT keyword.
        STRUCT(expr [AS alias], ...)
        STRUCT<...>(expr [AS alias], ...)
      Both forms support empty field lists.
      The struct_type_ child will be non-NULL for the second form,
      which includes the struct's field list.
      """,
      fields=[
          Field(
              'struct_type',
              'ASTStructType',
              private_comment="""
              May be NULL.
              """),
          Field(
              'fields',
              'ASTStructConstructorArg',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ],
      extra_defs="""
  // Deprecated - use fields(int i)
  const ASTStructConstructorArg* field(int idx) const { return fields_[idx]; }
      """)

  gen.AddNode(
      name='ASTInExpression',
      parent='ASTExpression',
      use_custom_debug_string=True,
      fields=[
          Field(
              'lhs',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
      Expression for which we need to verify whether its resolved result matches
      any of the resolved results of the expressions present in the in_list_.
              """),
          Field(
              'hint',
              'ASTHint',
              comment="""
      Hints specified on IN clause.
      This can be set only if IN clause has subquery as RHS.
              """,
              private_comment="""
      Hints specified on IN clause
              """),
          Field(
              'in_list',
              'ASTInList',
              comment="""
      Exactly one of in_list, query or unnest_expr is present.
              """,
              private_comment="""
      List of expressions to check against for the presence of lhs_.
              """),
          Field(
              'query',
              'ASTQuery',
              private_comment="""
      Query returns the row values to check against for the presence of lhs_.
              """),
          Field(
              'unnest_expr',
              'ASTUnnestExpression',
              private_comment="""
      Check if lhs_ is an element of the array value inside Unnest.
              """),
          Field(
              'is_not',
              SCALAR_BOOL,
              comment="""
      Signifies whether the IN operator has a preceding NOT to it.
              """),
      ],
      extra_defs="""
  bool IsAllowedInComparison() const override { return parenthesized(); }
      """
      )

  gen.AddNode(
      name='ASTInList',
      parent='ASTNode',
      comment="""
      This implementation is shared with the IN operator and LIKE ANY/SOME/ALL.
      """,
      fields=[
          Field(
              'list',
              'ASTExpression',
              private_comment="""
              List of expressions present in the InList node.
              """,
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTBetweenExpression',
      parent='ASTExpression',
      use_custom_debug_string=True,
      fields=[
          Field(
              'lhs',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
               Represents <lhs_> BETWEEN <low_> AND <high_>
              """),
          Field(
              'low',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'high',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'is_not',
              SCALAR_BOOL,
              comment="""
              Signifies whether the BETWEEN operator has a preceding NOT to it.
              """),
      ],
      extra_defs="""
  bool IsAllowedInComparison() const override { return parenthesized(); }
      """)

  gen.AddNode(
      name='ASTNumericLiteral',
      parent='ASTLeaf')

  gen.AddNode(
      name='ASTBigNumericLiteral',
      parent='ASTLeaf')

  gen.AddNode(
      name='ASTBytesLiteral',
      parent='ASTLeaf',
      extra_defs="""
  // The parsed and validated value of this literal. The raw input value can be
  // found in image().
  const std::string& bytes_value() const { return bytes_value_; }
  void set_bytes_value(std::string bytes_value) {
    bytes_value_ = std::move(bytes_value);
  }
      """,
      extra_private_defs="""
  std::string bytes_value_;
      """)

  gen.AddNode(
      name='ASTDateOrTimeLiteral',
      parent='ASTExpression',
      use_custom_debug_string=True,
      fields=[
          Field(
              'string_literal',
              'ASTStringLiteral',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'type_kind',
              SCALAR_TYPE_KIND),
      ])

  gen.AddNode(
      name='ASTMaxLiteral',
      parent='ASTLeaf',
      comment="""
      This represents the value MAX that shows up in type parameter lists.
      It will not show up as a general expression anywhere else.
      """)

  gen.AddNode(
      name='ASTJSONLiteral',
      parent='ASTLeaf')

  gen.AddNode(
      name='ASTCaseValueExpression',
      parent='ASTExpression',
      fields=[
          Field(
              'arguments',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTCaseNoValueExpression',
      parent='ASTExpression',
      fields=[
          Field(
              'arguments',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTArrayElement',
      parent='ASTGeneralizedPathExpression',
      fields=[
          Field(
              'array',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'position',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTBitwiseShiftExpression',
      parent='ASTExpression',
      use_custom_debug_string=True,
      fields=[
          Field(
              'lhs',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'rhs',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'is_left_shift',
              SCALAR_BOOL,
              comment="""
         Signifies whether the bitwise shift is of left shift type "<<" or right
         shift type ">>".
              """),
      ])

  gen.AddNode(
      name='ASTCollate',
      parent='ASTNode',
      fields=[
          Field(
              'collation_name',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTDotGeneralizedField',
      parent='ASTGeneralizedPathExpression',
      comment="""
      This is a generalized form of extracting a field from an expression.
      It uses a parenthesized path_expression instead of a single identifier
      to select the field.
      """
      ,
      fields=[
          Field(
              'expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'path',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTDotIdentifier',
      parent='ASTGeneralizedPathExpression',
      comment="""
   This is used for using dot to extract a field from an arbitrary expression.
   In cases where we know the left side is always an identifier path, we
   use ASTPathExpression instead.
      """,
      fields=[
          Field(
              'expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'name',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTDotStar',
      parent='ASTExpression',
      fields=[
          Field(
              'expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTDotStarWithModifiers',
      parent='ASTExpression',
      comment="""
      SELECT x.* EXCEPT(...) REPLACE(...).  See (broken link).
      """,
      fields=[
          Field(
              'expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'modifiers',
              'ASTStarModifiers',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTExpressionSubquery',
      parent='ASTExpression',
      use_custom_debug_string=True,
      comment="""
      A subquery in an expression.  (Not in the FROM clause.)
      """,
      fields=[
          Field(
              'hint',
              'ASTHint'),
          Field(
              'query',
              'ASTQuery',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'modifier',
              SCALAR_MODIFIER,
              comment="""
              The syntactic modifier on this expression subquery.
              """),
      ],
      extra_defs="""
  static std::string ModifierToString(Modifier modifier);

  // Note, this is intended by called from inside bison_parser.  At this stage
  // InitFields has _not_ been set, thus we need to use only children offsets.
  // Returns null on error.
  ASTQuery* GetMutableQueryChildInternal() {
    if (num_children() == 1) {
      return mutable_child(0)->GetAsOrNull<ASTQuery>();
    } else if (num_children() == 2) {
      // Hint is the first child.
      return mutable_child(1)->GetAsOrNull<ASTQuery>();
    } else {
      return nullptr;
    }
  }
      """)

  gen.AddNode(
      name='ASTExtractExpression',
      parent='ASTExpression',
      fields=[
          Field(
              'lhs_expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'rhs_expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'time_zone_expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.OPTIONAL_EXPRESSION),
      ])

  gen.AddNode(
      name='ASTHavingModifier',
      parent='ASTNode',
      fields=[
          Field(
              'expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
              The expression MAX or MIN applies to. Never NULL.
              """),
          Field(
              'modifier_kind',
              SCALAR_MODIFIER_KIND),
      ])

  gen.AddNode(
      name='ASTIntervalExpr',
      parent='ASTExpression',
      fields=[
          Field(
              'interval_value',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'date_part_name',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'date_part_name_to',
              'ASTIdentifier'),
      ])

  gen.AddNode(
      name='ASTNamedArgument',
      parent='ASTExpression',
      comment="""
     Represents a named function call argument using syntax: name => expression.
     The resolver will match these against available argument names in the
     function signature.
      """,
      fields=[
          Field(
              'name',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
              Required, never NULL.
              """),
          Field(
              'expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
              Required, never NULL.
              """),
      ])

  gen.AddNode(
      name='ASTNullOrder',
      parent='ASTNode',
      use_custom_debug_string=True,
      force_gen_init_fields=True,
      fields=[
          Field(
              'nulls_first',
              SCALAR_BOOL),
      ])

  gen.AddNode(
      name='ASTOnOrUsingClauseList',
      parent='ASTNode',
      fields=[
          Field(
              'on_or_using_clause_list',
              'ASTNode',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED,
              private_comment="""
          Each element in the list must be either ASTOnClause or ASTUsingClause.
              """),
      ])

  gen.AddNode(
      name='ASTParenthesizedJoin',
      parent='ASTTableExpression',
      fields=[
          Field(
              'join',
              'ASTJoin',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
              Required.
              """),
          Field(
              'sample_clause',
              'ASTSampleClause',
              private_comment="""
              Optional.
              """),
      ])

  gen.AddNode(
      name='ASTPartitionBy',
      parent='ASTNode',
      fields=[
          Field(
              'hint',
              'ASTHint'),
          Field(
              'partitioning_expressions',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTSetOperation',
      parent='ASTQueryExpression',
      use_custom_debug_string=True,
      fields=[
          Field(
              'hint',
              'ASTHint'),
          Field(
              'inputs',
              'ASTQueryExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
          Field(
              'op_type',
              SCALAR_OPERATION_TYPE),
          Field(
              'distinct',
              SCALAR_BOOL),
      ],
      extra_defs="""
  std::pair<std::string, std::string> GetSQLForOperationPair() const;

  // Returns the SQL keywords for the underlying set operation eg. UNION ALL,
  // UNION DISTINCT, EXCEPT ALL, INTERSECT DISTINCT etc.
  std::string GetSQLForOperation() const;
      """
      )

  gen.AddNode(
      name='ASTStarExceptList',
      parent='ASTNode',
      fields=[
          Field(
              'identifiers',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTStarModifiers',
      parent='ASTNode',
      comment="""
      SELECT * EXCEPT(...) REPLACE(...).  See (broken link).
      """,
      fields=[
          Field(
              'except_list',
              'ASTStarExceptList'),
          Field(
              'replace_items',
              'ASTStarReplaceItem',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTStarReplaceItem',
      parent='ASTNode',
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'alias',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTStarWithModifiers',
      parent='ASTExpression',
      comment="""
      SELECT * EXCEPT(...) REPLACE(...).  See (broken link).
      """,
      fields=[
          Field(
              'modifiers',
              'ASTStarModifiers',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTTableSubquery',
      parent='ASTTableExpression',
      extra_defs="""
  const ASTAlias* alias() const override { return alias_; }
      """,
      fields=[
          Field(
              'subquery',
              'ASTQuery',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'alias',
              'ASTAlias',
              gen_setters_and_getters=False),
          Field(
              'pivot_clause',
              'ASTPivotClause',
              private_comment="""
              One of pivot_clause or unpivot_clause can be present but not both.
              """),
          Field(
              'unpivot_clause',
              'ASTUnpivotClause'),
          Field(
              'sample_clause',
              'ASTSampleClause'),
      ])

  gen.Generate(
      output_path,
      h_template_path=h_template_path)

if __name__ == '__main__':
  app.run(main)
