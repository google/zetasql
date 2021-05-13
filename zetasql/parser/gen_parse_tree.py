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
from zetasql.parser.generator_utils import CleanIndent
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
  elif name == 'ASTTVFSchema':
    return 'AstTvfSchema'
  elif name == 'ASTTVF':
    return 'AstTvf'
  elif name == 'ASTTVFArgument':
    return 'AstTvfArgument'
  elif name == 'ASTTVFSchemaColumn':
    return 'AstTvfSchemaColumn'
  else:
    return name.replace('AST', 'Ast')


def NameToNodeKind(name):
  """Convert camel-case C++ ASTClassName to AST_CLASS_NAME in ASTNodeKind."""
  return _make_enum_name_re.sub(r'\1_\2', NormalCamel(name)).upper()

SCALAR_BOOL = ScalarType(
    'bool',
    cpp_default='false')

SCALAR_BOOL_DEFAULT_TRUE = ScalarType(
    'bool',
    cpp_default='true')

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


SCALAR_BINARY_OP = EnumScalarType('Op', 'ASTBinaryExpression', 'NOT_SET')

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

SCALAR_OPERATION_TYPE = EnumScalarType('OperationType', 'ASTSetOperation',
                                       'NOT_SET')

SCALAR_UNARY_OP = EnumScalarType('Op', 'ASTUnaryExpression', 'NOT_SET')

SCALAR_FRAME_UNIT = EnumScalarType('FrameUnit', 'ASTWindowFrame', 'RANGE')

SCALAR_BOUNDARY_TYPE = EnumScalarType('BoundaryType', 'ASTWindowFrameExpr',
                                      'UNBOUNDED_PRECEDING')

SCALAR_ANY_SOME_ALL_OP = EnumScalarType('Op', 'ASTAnySomeAllOp',
                                        'kUninitialized')
SCALAR_READ_WRITE_MODE = EnumScalarType('Mode', 'ASTTransactionReadWriteMode',
                                        'INVALID')

SCALAR_IMPORT_KIND = EnumScalarType('ImportKind', 'ASTImportStatement',
                                    'MODULE')

SCALAR_NULL_FILTER = EnumScalarType('NullFilter', 'ASTUnpivotClause',
                                    'kUnspecified')

SCALAR_SCOPE = EnumScalarType('Scope', 'ASTCreateStatement', 'DEFAULT_SCOPE')

SCALAR_SQL_SECURITY = EnumScalarType('SqlSecurity', 'ASTCreateStatement',
                                     'DEFAULT_SCOPE')

SCALAR_PROCEDURE_PARAMETER_MODE = EnumScalarType('ProcedureParameterMode',
                                                 'ASTFunctionParameter',
                                                 'NOT_SET')

SCALAR_TEMPLATED_TYPE_KIND = EnumScalarType('TemplatedTypeKind',
                                            'ASTTemplatedParameterType',
                                            'UNINITIALIZED')

SCALAR_STORED_MODE = EnumScalarType('StoredMode', 'ASTGeneratedColumnInfo',
                                    'NON_STORED')

SCALAR_RELATIVE_POSITION_TYPE = EnumScalarType('RelativePositionType',
                                               'ASTColumnPosition', 'PRECEDING')

SCALAR_INSERT_MODE = EnumScalarType('InsertMode', 'ASTInsertStatement',
                                    'DEFAULT_MODE')

SCALAR_PARSE_PROGRESS = EnumScalarType('ParseProgress', 'ASTInsertStatement',
                                       'kInitial')

SCALAR_ACTION_TYPE = EnumScalarType('ActionType', 'ASTMergeAction', 'NOT_SET')

SCALAR_MATCH_TYPE = EnumScalarType('MatchType', 'ASTMergeWhenClause', 'NOT_SET')


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
# OPTIONAL_TYPE: The next node in the vector for which IsType()
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
  OPTIONAL_TYPE = 5
  REPEATING_WHILE_IS_NODE_KIND = 6
  REPEATING_WHILE_IS_EXPRESSION = 7


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
      'comment': CleanIndent(comment, prefix='  // '),
      'private_comment': CleanIndent(private_comment, prefix='  // '),
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
              custom_debug_string_comment=None,
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
      custom_debug_string_comment: Optional comment for SingleNodeDebugString
          method.
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
    if custom_debug_string_comment:
      assert use_custom_debug_string, ('custom_debug_string_comment should be '
                                       'used with use_custom_debug_string')
      custom_debug_string_comment = CleanIndent(
          custom_debug_string_comment, prefix='// ')

    node_dict = ({
        'name': name,
        'parent': parent,
        'class_final': class_final,
        'is_abstract': is_abstract,
        'comment': CleanIndent(comment, prefix='// '),
        'fields': fields,
        'node_kind': node_kind,
        'extra_defs': extra_defs.rstrip(),
        'extra_private_defs': extra_private_defs.lstrip('\n').rstrip(),
        'use_custom_debug_string': use_custom_debug_string,
        'custom_debug_string_comment': custom_debug_string_comment,
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
                SCALAR_BINARY_OP,
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

  gen.AddNode(
      name='ASTUnaryExpression',
      parent='ASTExpression',
      use_custom_debug_string=True,
      fields=[
          Field(
              'operand',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'op',
              SCALAR_UNARY_OP),
      ],
      extra_defs="""
  bool IsAllowedInComparison() const override {
    return parenthesized() || op_ != NOT;
  }

  std::string GetSQLForOperator() const;
      """)

  gen.AddNode(
      name='ASTUnnestExpression',
      parent='ASTNode',
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTWindowClause',
      parent='ASTNode',
      fields=[
          Field(
              'windows',
              'ASTWindowDefinition',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTWindowDefinition',
      parent='ASTNode',
      fields=[
          Field(
              'name',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
              Required, never NULL.
              """),
          Field(
              'window_spec',
              'ASTWindowSpecification',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
              Required, never NULL.
              """),
      ])

  gen.AddNode(
      name='ASTWindowFrame',
      parent='ASTNode',
      use_custom_debug_string=True,
      fields=[
          Field(
              'start_expr',
              'ASTWindowFrameExpr',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
              Starting boundary expression. Never NULL.
              """),
          Field(
              'end_expr',
              'ASTWindowFrameExpr',
              private_comment="""
              Ending boundary expression. Can be NULL.
              When this is NULL, the implicit ending boundary is CURRENT ROW.
              """),
          Field(
              'frame_unit',
              SCALAR_FRAME_UNIT,
              gen_setters_and_getters=False),
      ],
      extra_defs="""
  void set_unit(FrameUnit frame_unit) { frame_unit_ = frame_unit; }
  FrameUnit frame_unit() const { return frame_unit_; }

  std::string GetFrameUnitString() const;

  static std::string FrameUnitToString(FrameUnit unit);
      """)

  gen.AddNode(
      name='ASTWindowFrameExpr',
      parent='ASTNode',
      use_custom_debug_string=True,
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.OPTIONAL_EXPRESSION,
              private_comment="""
             Expression to specify the boundary as a logical or physical offset
             to current row. Cannot be NULL if boundary_type is OFFSET_PRECEDING
             or OFFSET_FOLLOWING; otherwise, should be NULL.
              """),
          Field(
              'boundary_type',
              SCALAR_BOUNDARY_TYPE),
      ],
      extra_defs="""
  std::string GetBoundaryTypeString() const;
  static std::string BoundaryTypeToString(BoundaryType type);
      """)

  gen.AddNode(
      name='ASTLikeExpression',
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
              'op',
              'ASTAnySomeAllOp',
              field_loader=FieldLoaderMethod.REQUIRED,
              comment="""
      The any, some, or all operation used.
              """,
              private_comment="""
      Any, some, or all operator.
              """),
          Field(
              'hint',
              'ASTHint',
              comment="""
      Hints specified on LIKE clause.
      This can be set only if LIKE clause has subquery as RHS.
              """,
              private_comment="""
       Hints specified on LIKE clause
              """),
          Field(
              'in_list',
              'ASTInList',
              comment="""
       Exactly one of in_list, query or unnest_expr is present
              """,
              private_comment="""
       List of expressions to check against for any/some/all comparison for lhs_.
              """),
          Field(
              'query',
              'ASTQuery',
              private_comment="""
       Query returns the row values to check against for any/some/all comparison
       for lhs_.
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
       Signifies whether the LIKE operator has a preceding NOT to it.
              """),
      ],
      extra_defs="""
  bool IsAllowedInComparison() const override { return parenthesized(); }
      """)

  gen.AddNode(
      name='ASTWindowSpecification',
      parent='ASTNode',
      fields=[
          Field(
              'base_window_name',
              'ASTIdentifier',
              private_comment="""
              All fields are optional, can be NULL.
              """),
          Field(
              'partition_by',
              'ASTPartitionBy'),
          Field(
              'order_by',
              'ASTOrderBy'),
          Field(
              'window_frame',
              'ASTWindowFrame'),
      ])

  gen.AddNode(
      name='ASTWithOffset',
      parent='ASTNode',
      fields=[
          Field(
              'alias',
              'ASTAlias',
              comment="""
               alias may be NULL.
              """),
      ])

  gen.AddNode(
      name='ASTAnySomeAllOp',
      parent='ASTNode',
      use_custom_debug_string=True,
      fields=[
          Field(
              'op',
              SCALAR_ANY_SOME_ALL_OP),
      ],
      extra_defs="""
  std::string GetSQLForOperator() const;
      """,
      extra_private_defs="""
  void InitFields() final {}
      """)

  gen.AddNode(
      name='ASTParameterExprBase',
      parent='ASTExpression',
      is_abstract=True,
      force_gen_init_fields=True)

  gen.AddNode(
      name='ASTStatementList',
      parent='ASTNode',
      comment="""
      Contains a list of statements.  Variable declarations allowed only at the
      start of the list, and only if variable_declarations_allowed() is true.
      """,
      fields=[
          Field(
              'statement_list',
              'ASTStatement',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED,
              private_comment="""
              Repeated
              """),
          Field(
              'variable_declarations_allowed',
              SCALAR_BOOL),
      ],
      extra_defs="""
 protected:
  explicit ASTStatementList(ASTNodeKind node_kind) : ASTNode(node_kind) {}
      """)

  gen.AddNode(
      name='ASTScriptStatement',
      parent='ASTStatement',
      is_abstract=True,
      extra_defs="""
  bool IsScriptStatement() const final { return true; }
  bool IsSqlStatement() const override { return false; }
      """)

  gen.AddNode(
      name='ASTHintedStatement',
      parent='ASTStatement',
      comment="""
      This wraps any other statement to add statement-level hints.
      """,
      fields=[
          Field(
              'hint',
              'ASTHint',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'statement',
              'ASTStatement',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTExplainStatement',
      parent='ASTStatement',
      comment="""
      Represents an EXPLAIN statement.
      """,
      fields=[
          Field(
              'statement',
              'ASTStatement',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTDescribeStatement',
      parent='ASTStatement',
      comment="""
      Represents a DESCRIBE statement.
      """,
      fields=[
          Field(
              'optional_identifier',
              'ASTIdentifier'),
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'optional_from_name',
              'ASTPathExpression'),
      ])

  gen.AddNode(
      name='ASTShowStatement',
      parent='ASTStatement',
      comment="""
      Represents a SHOW statement.
      """,
      fields=[
          Field(
              'identifier',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'optional_name',
              'ASTPathExpression'),
          Field(
              'optional_like_string',
              'ASTStringLiteral'),
      ])

  gen.AddNode(
      name='ASTTransactionMode',
      parent='ASTNode',
      is_abstract=True,
      comment="""
      Base class transaction modifier elements.
      """)

  gen.AddNode(
      name='ASTTransactionIsolationLevel',
      parent='ASTTransactionMode',
      fields=[
          Field(
              'identifier1',
              'ASTIdentifier'),
          Field(
              'identifier2',
              'ASTIdentifier',
              comment="""
         Second identifier can be non-null only if first identifier is non-null.
               """)
      ])

  gen.AddNode(
      name='ASTTransactionReadWriteMode',
      parent='ASTTransactionMode',
      force_gen_init_fields=True,
      fields=[
          Field(
              'mode',
              SCALAR_READ_WRITE_MODE),
      ])

  gen.AddNode(
      name='ASTTransactionModeList',
      parent='ASTNode',
      fields=[
          Field(
              'elements',
              'ASTTransactionMode',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTBeginStatement',
      parent='ASTStatement',
      comment="""
      Represents a BEGIN or START TRANSACTION statement.
      """,
      fields=[
          Field(
              'mode_list',
              'ASTTransactionModeList'),
      ])

  gen.AddNode(
      name='ASTSetTransactionStatement',
      parent='ASTStatement',
      comment="""
      Represents a SET TRANSACTION statement.
      """,
      fields=[
          Field(
              'mode_list',
              'ASTTransactionModeList',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTCommitStatement',
      parent='ASTStatement',
      force_gen_init_fields=True,
      comment="""
      Represents a COMMIT statement.
      """)

  gen.AddNode(
      name='ASTRollbackStatement',
      parent='ASTStatement',
      force_gen_init_fields=True,
      comment="""
      Represents a ROLLBACK statement.
      """)

  gen.AddNode(
      name='ASTStartBatchStatement',
      parent='ASTStatement',
      fields=[
          Field(
              'batch_type',
              'ASTIdentifier'),
      ])

  gen.AddNode(
      name='ASTRunBatchStatement',
      parent='ASTStatement',
      force_gen_init_fields=True)

  gen.AddNode(
      name='ASTAbortBatchStatement',
      parent='ASTStatement',
      force_gen_init_fields=True)

  gen.AddNode(
      name='ASTDdlStatement',
      parent='ASTStatement',
      is_abstract=True,
      comment="""
      Common superclass of DDL statements.
      """,
      extra_defs="""
  bool IsDdlStatement() const override { return true; }

  virtual const ASTPathExpression* GetDdlTarget() const = 0;
      """
      )

  gen.AddNode(
      name='ASTDropEntityStatement',
      parent='ASTDdlStatement',
      use_custom_debug_string=True,
      custom_debug_string_comment="""
      This adds the "if exists" modifier to the node name.
      """,
      comment="""
      Generic DROP statement (broken link).
      """,
      fields=[
          Field(
              'entity_type',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'is_if_exists',
              SCALAR_BOOL),
      ],
      extra_defs="""
  const ASTPathExpression* GetDdlTarget() const override { return name_; }
      """)

  gen.AddNode(
      name='ASTDropFunctionStatement',
      parent='ASTDdlStatement',
      use_custom_debug_string=True,
      custom_debug_string_comment="""
      This adds the "if exists" modifier to the node name.
      """,
      comment="""
      Represents a DROP FUNCTION statement.
      """,
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'parameters',
              'ASTFunctionParameters'),
          Field(
              'is_if_exists',
              SCALAR_BOOL),
      ],
      extra_defs="""
  const ASTPathExpression* GetDdlTarget() const override { return name_; }
      """)

  gen.AddNode(
      name='ASTDropTableFunctionStatement',
      parent='ASTDdlStatement',
      use_custom_debug_string=True,
      custom_debug_string_comment="""
      This adds the "if exists" modifier to the node name.
      """,
      comment="""
      Represents a DROP TABLE FUNCTION statement.
      Note: Table functions don't support overloading so function parameters are
            not accepted in this statement.
            (broken link)
      """,
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'is_if_exists',
              SCALAR_BOOL),
      ],
      extra_defs="""
  const ASTPathExpression* GetDdlTarget() const override { return name_; }
      """)

  gen.AddNode(
      name='ASTDropAllRowAccessPoliciesStatement',
      parent='ASTStatement',
      comment="""
      Represents a DROP ALL ROW ACCESS POLICIES statement.
      """,
      fields=[
          Field(
              'table_name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'has_access_keyword',
              SCALAR_BOOL),
      ])

  gen.AddNode(
      name='ASTDropMaterializedViewStatement',
      parent='ASTDdlStatement',
      use_custom_debug_string=True,
      custom_debug_string_comment="""
      This adds the "if exists" modifier to the node name.
      """,
      comment="""
      Represents a DROP MATERIALIZED VIEW statement.
      """,
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'is_if_exists',
              SCALAR_BOOL),
      ],
      extra_defs="""
  const ASTPathExpression* GetDdlTarget() const override { return name_; }
      """)

  gen.AddNode(
      name='ASTDropSnapshotTableStatement',
      parent='ASTDdlStatement',
      use_custom_debug_string=True,
      custom_debug_string_comment="""
      This adds the "if exists" modifier to the node name.
      """,
      comment="""
      Represents a DROP SNAPSHOT TABLE statement.
      """,
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'is_if_exists',
              SCALAR_BOOL),
      ],
      extra_defs="""
  const ASTPathExpression* GetDdlTarget() const override { return name_; }
      """)

  gen.AddNode(
      name='ASTDropSearchIndexStatement',
      parent='ASTDdlStatement',
      use_custom_debug_string=True,
      custom_debug_string_comment="""
      This adds the "if exists" modifier to the node name.
      """,
      comment="""
      Represents a DROP SEARCH INDEX statement.
      """,
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'table_name',
              'ASTPathExpression'),
          Field(
              'is_if_exists',
              SCALAR_BOOL),
      ],
      extra_defs="""
  const ASTPathExpression* GetDdlTarget() const override { return name_; }
      """)

  gen.AddNode(
      name='ASTRenameStatement',
      parent='ASTStatement',
      comment="""
      Represents a RENAME statement.
      """,
      fields=[
          Field(
              'identifier',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'old_name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'new_name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTImportStatement',
      parent='ASTStatement',
      comment="""
      Represents an IMPORT statement, which currently support MODULE or PROTO
      kind. We want this statement to be a generic import at some point.
      """,
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              private_comment="""
              Exactly one of 'name_' or 'string_value_' will be populated.
              """),
          Field(
              'string_value',
              'ASTStringLiteral'),
          Field(
              'alias',
              'ASTAlias',
              private_comment="""
              At most one of 'alias_' or 'into_alias_' will be populated.
              """),
          Field(
              'into_alias',
              'ASTIntoAlias'),
          Field(
              'options_list',
              'ASTOptionsList',
              private_comment="""
              May be NULL.
              """),
          Field(
              'import_kind',
              SCALAR_IMPORT_KIND
              ),
      ])

  gen.AddNode(
      name='ASTModuleStatement',
      parent='ASTStatement',
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'options_list',
              'ASTOptionsList',
              private_comment="""
              May be NULL
              """),
      ])

  gen.AddNode(
      name='ASTWithConnectionClause',
      parent='ASTNode',
      fields=[
          Field(
              'connection_clause',
              'ASTConnectionClause',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTIntoAlias',
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
      """)

  gen.AddNode(
      name='ASTUnnestExpressionWithOptAliasAndOffset',
      parent='ASTNode',
      comment="""
      A conjunction of the unnest expression and the optional alias and offset.
      """,
      fields=[
          Field(
              'unnest_expression',
              'ASTUnnestExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'optional_alias',
              'ASTAlias'),
          Field(
              'optional_with_offset',
              'ASTWithOffset'),
      ])

  gen.AddNode(
      name='ASTPivotExpression',
      parent='ASTNode',
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'alias',
              'ASTAlias'),
      ])

  gen.AddNode(
      name='ASTPivotValue',
      parent='ASTNode',
      fields=[
          Field(
              'value',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'alias',
              'ASTAlias'),
      ])

  gen.AddNode(
      name='ASTPivotExpressionList',
      parent='ASTNode',
      fields=[
          Field(
              'expressions',
              'ASTPivotExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTPivotValueList',
      parent='ASTNode',
      fields=[
          Field(
              'values',
              'ASTPivotValue',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTPivotClause',
      parent='ASTNode',
      fields=[
          Field(
              'pivot_expressions',
              'ASTPivotExpressionList',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'for_expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'pivot_values',
              'ASTPivotValueList',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'output_alias',
              'ASTAlias'),
      ])

  gen.AddNode(
      name='ASTUnpivotInItem',
      parent='ASTNode',
      fields=[
          Field(
              'unpivot_columns',
              'ASTPathExpressionList',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'alias',
              'ASTUnpivotInItemLabel'),
      ])

  gen.AddNode(
      name='ASTUnpivotInItemList',
      parent='ASTNode',
      fields=[
          Field(
              'in_items',
              'ASTUnpivotInItem',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTUnpivotClause',
      parent='ASTNode',
      use_custom_debug_string=True,
      fields=[
          Field(
              'unpivot_output_value_columns',
              'ASTPathExpressionList',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'unpivot_output_name_column',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'unpivot_in_items',
              'ASTUnpivotInItemList',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'output_alias',
              'ASTAlias'),
          Field(
              'null_filter',
              SCALAR_NULL_FILTER),
      ],
      extra_defs="""
  std::string GetSQLForNullFilter() const;
      """)

  gen.AddNode(
      name='ASTUsingClause',
      parent='ASTNode',
      fields=[
          Field(
              'keys',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTForSystemTime',
      parent='ASTNode',
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTQualify',
      parent='ASTNode',
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTClampedBetweenModifier',
      parent='ASTNode',
      fields=[
          Field(
              'low',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'high',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTFormatClause',
      parent='ASTNode',
      fields=[
          Field(
              'format',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'time_zone_expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.OPTIONAL_EXPRESSION),
      ])

  gen.AddNode(
      name='ASTPathExpressionList',
      parent='ASTNode',
      fields=[
          Field(
              'path_expression_list',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED,
              comment="""
              Guaranteed by the parser to never be empty.
              """),
      ])

  gen.AddNode(
      name='ASTParameterExpr',
      parent='ASTParameterExprBase',
      use_custom_debug_string=True,
      fields=[
          Field(
              'name',
              'ASTIdentifier'),
          Field(
              'position',
              SCALAR_INT,
              private_comment="""
              1-based position of the parameter in the query. Mutually exclusive
              with name_.
              """),
      ])

  gen.AddNode(
      name='ASTSystemVariableExpr',
      parent='ASTParameterExprBase',
      fields=[
          Field(
              'path',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTWithGroupRows',
      parent='ASTNode',
      fields=[
          Field(
              'subquery',
              'ASTQuery',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTLambda',
      parent='ASTExpression',
      comment="""
      Function argument is required to be expression.
      """,
      fields=[
          Field(
              'argument_list',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
              Empty parameter list is represented as empty
              ASTStructConstructorWithParens.
              """),
          Field(
              'body',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
              Required, never NULL.
              """),
      ])

  gen.AddNode(
      name='ASTAnalyticFunctionCall',
      parent='ASTExpression',
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED,
              gen_setters_and_getters=False,
              private_comment="""
              Required, never NULL.
              The expression is has to be either an ASTFunctionCall or an
              ASTFunctionCallWithGroupRows.
              """),
          Field(
              'window_spec',
              'ASTWindowSpecification',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
              Required, never NULL.
              """),
      ],
      extra_defs="""
  // Exactly one of function() or function_with_group_rows() will be non-null.
  //
  // In the normal case, function() is non-null.
  //
  // The function_with_group_rows() case can only happen if
  // FEATURE_V_1_3_WITH_GROUP_ROWS is enabled and one function call has both
  // WITH GROUP_ROWS and an OVER clause.
  const ASTFunctionCall* function() const;
  const ASTFunctionCallWithGroupRows* function_with_group_rows() const;
      """)

  gen.AddNode(
      name='ASTFunctionCallWithGroupRows',
      parent='ASTExpression',
      fields=[
          Field(
              'function',
              'ASTFunctionCall',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
              Required, never NULL.
              """),
          Field(
              'subquery',
              'ASTQuery',
              field_loader=FieldLoaderMethod.REQUIRED,
              private_comment="""
              Required, never NULL.
              """),
      ])

  gen.AddNode(
      name='ASTClusterBy',
      parent='ASTNode',
      fields=[
          Field(
              'clustering_expressions',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTNewConstructorArg',
      parent='ASTNode',
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'optional_identifier',
              'ASTIdentifier',
              comment="""
         At most one of 'optional_identifier' and 'optional_path_expression' are
         set.
               """),
          Field(
              'optional_path_expression',
              'ASTPathExpression'),
      ])

  gen.AddNode(
      name='ASTNewConstructor',
      parent='ASTExpression',
      fields=[
          Field(
              'type_name',
              'ASTSimpleType',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'arguments',
              'ASTNewConstructorArg',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ],
      # legacy non-standard getter
      extra_defs="""
  const ASTNewConstructorArg* argument(int i) const { return arguments_[i]; }
      """)

  gen.AddNode(
      name='ASTOptionsList',
      parent='ASTNode',
      fields=[
          Field(
              'options_entries',
              'ASTOptionsEntry',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTOptionsEntry',
      parent='ASTNode',
      fields=[
          Field(
              'name',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'value',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED,
              comment="""
              Value is always an identifier, literal, or parameter.
              """),
      ])

  gen.AddNode(
      name='ASTCreateStatement',
      parent='ASTDdlStatement',
      is_abstract=True,
      use_custom_debug_string=True,
      custom_debug_string_comment="""
      This adds the modifiers is_temp, etc, to the node name.
      """,
      comment="""
      Common superclass of CREATE statements supporting the common
      modifiers:
        CREATE [OR REPLACE] [TEMP|PUBLIC|PRIVATE] <object> [IF NOT EXISTS].
      """,
      fields=[
          Field(
              'scope',
              SCALAR_SCOPE
              ),
          Field(
              'is_or_replace',
              SCALAR_BOOL
              ),
          Field(
              'is_if_not_exists',
              SCALAR_BOOL
              )
      ],
      extra_defs="""
  bool is_default_scope() const { return scope_ == DEFAULT_SCOPE; }
  bool is_private() const { return scope_ == PRIVATE; }
  bool is_public() const { return scope_ == PUBLIC; }
  bool is_temp() const { return scope_ == TEMPORARY; }

  bool IsCreateStatement() const override { return true; }

 protected:
  virtual void CollectModifiers(std::vector<std::string>* modifiers) const;
      """)

  gen.AddNode(
      name='ASTFunctionParameter',
      parent='ASTNode',
      use_custom_debug_string=True,
      fields=[
          Field(
              'name',
              'ASTIdentifier'),
          Field(
              'type',
              'ASTType',
              field_loader=FieldLoaderMethod.OPTIONAL_TYPE,
              private_comment="""
              Only one of <type_>, <templated_parameter_type_>, or <tvf_schema_>
              will be set.

              This is the type for concrete scalar parameters.
              """),
          Field(
              'templated_parameter_type',
              'ASTTemplatedParameterType',
              private_comment="""
          This indicates a templated parameter type, which may be either a
          templated scalar type (ANY PROTO, ANY STRUCT, etc.) or templated table
          type as indicated by its kind().
              """),
          Field(
              'tvf_schema',
              'ASTTVFSchema',
              private_comment="""
              Only allowed for table-valued functions, indicating a table type
              parameter.
              """),
          Field(
              'alias',
              'ASTAlias'),
          Field(
              'default_value',
              'ASTExpression',
              field_loader=FieldLoaderMethod.OPTIONAL_EXPRESSION,
              private_comment="""
              The default value of the function parameter if specified.
              """),
          Field(
              'procedure_parameter_mode',
              SCALAR_PROCEDURE_PARAMETER_MODE,
              private_comment="""
         Function parameter doesn't use this field and always has value NOT_SET.
         Procedure parameter should have this field set during parsing.
              """),
          Field(
              'is_not_aggregate',
              SCALAR_BOOL,
              private_comment="""
              True if the NOT AGGREGATE modifier is present.
              """),
      ],
      extra_defs="""

  bool IsTableParameter() const;
  bool IsTemplated() const {
    return templated_parameter_type_ != nullptr;
  }

  static std::string ProcedureParameterModeToString(
      ProcedureParameterMode mode);
      """)

  gen.AddNode(
      name='ASTFunctionParameters',
      parent='ASTNode',
      fields=[
          Field(
              'parameter_entries',
              'ASTFunctionParameter',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTFunctionDeclaration',
      parent='ASTNode',
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'parameters',
              'ASTFunctionParameters',
              field_loader=FieldLoaderMethod.REQUIRED),
      ],
      extra_defs="""
  // Returns whether or not any of the <parameters_> are templated.
  bool IsTemplated() const;
      """)

  gen.AddNode(
      name='ASTSqlFunctionBody',
      parent='ASTNode',
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.OPTIONAL_EXPRESSION),
      ])

  gen.AddNode(
      name='ASTTVFArgument',
      parent='ASTNode',
      comment="""
  This represents an argument to a table-valued function (TVF). ZetaSQL can
  parse the argument in one of the following ways:

  (1) ZetaSQL parses the argument as an expression; if any arguments are
      table subqueries then ZetaSQL will parse them as subquery expressions
      and the resolver may interpret them as needed later. In this case the
      expr_ of this class is filled.

  (2) ZetaSQL parses the argument as "TABLE path"; this syntax represents a
      table argument including all columns in the named table. In this case the
      table_clause_ of this class is non-empty.

  (3) ZetaSQL parses the argument as "MODEL path"; this syntax represents a
      model argument. In this case the model_clause_ of this class is
      non-empty.

  (4) ZetaSQL parses the argument as "CONNECTION path"; this syntax
      represents a connection argument. In this case the connection_clause_ of
      this class is non-empty.

  (5) ZetaSQL parses the argument as a named argument; this behaves like when
      the argument is an expression with the extra requirement that the
      resolver rearranges the provided named arguments to match the required
      argument names from the function signature, if present. The named
      argument is stored in the expr_ of this class in this case since an
      ASTNamedArgument is a subclass of ASTExpression.
  (6) ZetaSQL parses the argument as "DESCRIPTOR"; this syntax represents a
     descriptor on a list of columns with optional types.
      """,
      fields=[
          Field(
              'expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.OPTIONAL_EXPRESSION,
              private_comment="""
              Only one of expr, table_clause, model_clause, connection_clause or
              descriptor may be non-null.
              """),
          Field(
              'table_clause',
              'ASTTableClause'),
          Field(
              'model_clause',
              'ASTModelClause'),
          Field(
              'connection_clause',
              'ASTConnectionClause'),
          Field(
              'descriptor',
              'ASTDescriptor'),
      ])

  gen.AddNode(
      name='ASTTVF',
      parent='ASTTableExpression',
      comment="""
    This represents a call to a table-valued function (TVF). Each TVF returns an
    entire output relation instead of a single scalar value. The enclosing query
    may refer to the TVF as if it were a table subquery. The TVF may accept
    scalar arguments and/or other input relations.
      """,
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'argument_entries',
              'ASTTVFArgument',
              field_loader=FieldLoaderMethod.REPEATING_WHILE_IS_NODE_KIND),
          Field(
              'hint',
              'ASTHint'),
          Field(
              'alias',
              'ASTAlias'),
          Field(
              'pivot_clause',
              'ASTPivotClause'),
          Field(
              'unpivot_clause',
              'ASTUnpivotClause'),
          Field(
              'sample',
              'ASTSampleClause'),
      ])

  gen.AddNode(
      name='ASTTableClause',
      parent='ASTNode',
      comment="""
     This represents a clause of form "TABLE <target>", where <target> is either
     a path expression representing a table name, or <target> is a TVF call.
     It is currently only supported for relation arguments to table-valued
     functions.
      """,
      fields=[
          Field(
              'table_path',
              'ASTPathExpression',
              private_comment="""
              Exactly one of these will be non-null.
              """),
          Field(
              'tvf',
              'ASTTVF'),
      ])

  gen.AddNode(
      name='ASTModelClause',
      parent='ASTNode',
      comment="""
    This represents a clause of form "MODEL <target>", where <target> is a model
    name.
      """,
      fields=[
          Field(
              'model_path',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTConnectionClause',
      parent='ASTNode',
      comment="""
     This represents a clause of form "CONNECTION <target>", where <target> is a
     connection name.
      """,
      fields=[
          Field(
              'connection_path',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTTableDataSource',
      parent='ASTTableExpression',
      is_abstract=True,
      fields=[
          Field(
              'path_expr',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'for_system_time',
              'ASTForSystemTime'),
          Field(
              'where_clause',
              'ASTWhereClause'),
      ])

  gen.AddNode(
      name='ASTCloneDataSource',
      parent='ASTTableDataSource')

  gen.AddNode(
      name='ASTCopyDataSource',
      parent='ASTTableDataSource')

  gen.AddNode(
      name='ASTCloneDataSourceList',
      parent='ASTNode',
      fields=[
          Field(
              'data_sources',
              'ASTCloneDataSource',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTCloneDataStatement',
      parent='ASTStatement',
      fields=[
          Field(
              'target_path',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'data_source_list',
              'ASTCloneDataSourceList',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTCreateConstantStatement',
      parent='ASTCreateStatement',
      comment="""
      This represents a CREATE CONSTANT statement, i.e.,
      CREATE [OR REPLACE] [TEMP|TEMPORARY|PUBLIC|PRIVATE] CONSTANT
        [IF NOT EXISTS] <name_path> = <expression>;
      """,
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ],
      extra_defs="""
  const ASTPathExpression* GetDdlTarget() const override { return name_; }
      """)

  gen.AddNode(
      name='ASTCreateDatabaseStatement',
      parent='ASTStatement',
      comment="""
      This represents a CREATE DATABASE statement, i.e.,
      CREATE DATABASE <name> [OPTIONS (name=value, ...)];
      """,
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'options_list',
              'ASTOptionsList'),
      ])

  gen.AddNode(
      name='ASTCreateProcedureStatement',
      parent='ASTCreateStatement',
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'parameters',
              'ASTFunctionParameters',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'options_list',
              'ASTOptionsList'),
          Field(
              'body',
              'ASTScript',
              field_loader=FieldLoaderMethod.REQUIRED,
              comment="""
              The body of a procedure. Always consists of a single BeginEndBlock
              including the BEGIN/END keywords and text in between.
              """),
      ],
      extra_defs="""
  const ASTPathExpression* GetDdlTarget() const override { return name_; }
      """)

  gen.AddNode(
      name='ASTCreateSchemaStatement',
      parent='ASTCreateStatement',
      comment="""
      This represents a CREATE SCHEMA statement, i.e.,
      CREATE SCHEMA <name> [OPTIONS (name=value, ...)];
      """,
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'collate',
              'ASTCollate'),
          Field(
              'options_list',
              'ASTOptionsList'),
      ],
      extra_defs="""
  const ASTPathExpression* GetDdlTarget() const override { return name_; }
      """)

  gen.AddNode(
      name='ASTTransformClause',
      parent='ASTNode',
      fields=[
          Field(
              'select_list',
              'ASTSelectList',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTCreateModelStatement',
      parent='ASTCreateStatement',
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'transform_clause',
              'ASTTransformClause'),
          Field(
              'options_list',
              'ASTOptionsList'),
          Field(
              'query',
              'ASTQuery'),
      ],
      extra_defs="""
  const ASTPathExpression* GetDdlTarget() const override { return name_; }
      """)

  gen.AddNode(
      name='ASTIndexItemList',
      parent='ASTNode',
      comment="""
      Represents the list of expressions used to order an index.
      """,
      fields=[
          Field(
              'ordering_expressions',
              'ASTOrderingExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTIndexStoringExpressionList',
      parent='ASTNode',
      comment="""
      Represents the list of expressions being used in the STORING clause of an
      index.
      """,
      fields=[
          Field(
              'expressions',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])
  gen.AddNode(
      name='ASTIndexUnnestExpressionList',
      parent='ASTNode',
      comment="""
      Represents the list of unnest expressions for create_index.
      """,
      fields=[
          Field(
              'unnest_expressions',
              'ASTUnnestExpressionWithOptAliasAndOffset',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])
  gen.AddNode(
      name='ASTCreateIndexStatement',
      parent='ASTCreateStatement',
      use_custom_debug_string=True,
      comment="""
      Represents a CREATE INDEX statement.
      """,
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'table_name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'optional_table_alias',
              'ASTAlias'),
          Field(
              'optional_index_unnest_expression_list',
              'ASTIndexUnnestExpressionList'),

          Field(
              'index_item_list',
              'ASTIndexItemList',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'optional_index_storing_expressions',
              'ASTIndexStoringExpressionList'),
          Field(
              'options_list',
              'ASTOptionsList'),
          Field(
              'is_unique',
              SCALAR_BOOL),
          Field(
              'is_search',
              SCALAR_BOOL),
      ],
      extra_defs="""
  const ASTPathExpression* GetDdlTarget() const override { return name_; }
      """
      )

  gen.AddNode(
      name='ASTExportDataStatement',
      parent='ASTStatement',
      fields=[
          Field(
              'with_connection_clause',
              'ASTWithConnectionClause'),
          Field(
              'options_list',
              'ASTOptionsList'),
          Field(
              'query',
              'ASTQuery',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTExportModelStatement',
      parent='ASTStatement',
      fields=[
          Field(
              'model_name_path',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'with_connection_clause',
              'ASTWithConnectionClause'),
          Field(
              'options_list',
              'ASTOptionsList'),
      ])

  gen.AddNode(
      name='ASTCallStatement',
      parent='ASTStatement',
      fields=[
          Field(
              'procedure_name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'arguments',
              'ASTTVFArgument',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTDefineTableStatement',
      parent='ASTStatement',
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'options_list',
              'ASTOptionsList',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTWithPartitionColumnsClause',
      parent='ASTNode',
      fields=[
          Field(
              'table_element_list',
              'ASTTableElementList'),
      ])

  gen.AddNode(
      name='ASTCreateSnapshotTableStatement',
      parent='ASTCreateStatement',
      fields=[
          Field(
              'name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'clone_data_source',
              'ASTCloneDataSource',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'options_list',
              'ASTOptionsList'),
      ],
      extra_defs="""
  const ASTPathExpression* GetDdlTarget() const override { return name_; }
      """)

  gen.AddNode(
      name='ASTTypeParameterList',
      parent='ASTNode',
      fields=[
          Field(
              'parameters',
              'ASTLeaf',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTTVFSchema',
      parent='ASTNode',
      comment="""
   This represents a relation argument or return type for a table-valued
   function (TVF). The resolver can convert each ASTTVFSchema directly into a
   TVFRelation object suitable for use in TVF signatures. For more information
   about the TVFRelation object, please refer to public/table_valued_function.h.
   TODO: Change the names of these objects to make them generic and
   re-usable wherever we want to represent the schema of some intermediate or
   final table. Same for ASTTVFSchemaColumn.
      """,
      fields=[
          Field(
              'columns',
              'ASTTVFSchemaColumn',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTTVFSchemaColumn',
      parent='ASTNode',
      comment="""
      This represents one column of a relation argument or return value for a
      table-valued function (TVF). It contains the name and type of the column.
      """,
      fields=[
          Field(
              'name',
              'ASTIdentifier',
              comment="""
              name_ will be NULL for value tables.
              """),
          Field(
              'type',
              'ASTType',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTTableAndColumnInfo',
      parent='ASTNode',
      fields=[
          Field(
              'table_name',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'column_list',
              'ASTColumnList'),
      ])

  gen.AddNode(
      name='ASTTableAndColumnInfoList',
      parent='ASTNode',
      fields=[
          Field(
              'table_and_column_info_entries',
              'ASTTableAndColumnInfo',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTTemplatedParameterType',
      parent='ASTNode',
      force_gen_init_fields=True,
      fields=[
          Field(
              'kind',
              SCALAR_TEMPLATED_TYPE_KIND),
      ])

  gen.AddNode(
      name='ASTDefaultLiteral',
      parent='ASTExpression',
      force_gen_init_fields=True,
      comment="""
      This represents the value DEFAULT that shows up in DML statements.
      It will not show up as a general expression anywhere else.
      """)

  gen.AddNode(
      name='ASTAnalyzeStatement',
      parent='ASTStatement',
      fields=[
          Field(
              'options_list',
              'ASTOptionsList'),
          Field(
              'table_and_column_info_list',
              'ASTTableAndColumnInfoList'),
      ])

  gen.AddNode(
      name='ASTAssertStatement',
      parent='ASTStatement',
      fields=[
          Field(
              'expr',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'description',
              'ASTStringLiteral'),
      ])

  gen.AddNode(
      name='ASTAssertRowsModified',
      parent='ASTNode',
      fields=[
          Field(
              'num_rows',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTReturningClause',
      parent='ASTNode',
      comment="""
      "This represents {THEN RETURN} clause."
      (broken link)
      """,
      fields=[
          Field(
              'select_list',
              'ASTSelectList',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'action_alias',
              'ASTAlias'),
      ])

  gen.AddNode(
      name='ASTDeleteStatement',
      parent='ASTStatement',
      comment="""
      This is used for both top-level DELETE statements and for nested DELETEs
      inside ASTUpdateItem. When used at the top-level, the target is always a
      path expression.
      """,
      fields=[
          Field(
              'target_path',
              'ASTGeneralizedPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'alias',
              'ASTAlias'),
          Field(
              'offset',
              'ASTWithOffset'),
          Field(
              'where',
              'ASTExpression',
              field_loader=FieldLoaderMethod.OPTIONAL_EXPRESSION),
          Field(
              'assert_rows_modified',
              'ASTAssertRowsModified'),
          Field(
              'returning',
              'ASTReturningClause'),
      ],
      extra_defs="""
  // Verifies that the target path is an ASTPathExpression and, if so, returns
  // it. The behavior is undefined when called on a node that represents a
  // nested DELETE.
  zetasql_base::StatusOr<const ASTPathExpression*> GetTargetPathForNonNested() const;

  const ASTGeneralizedPathExpression* GetTargetPathForNested() const {
    return target_path_;
  }
      """)

  gen.AddNode(
      name='ASTColumnAttribute',
      parent='ASTNode',
      is_abstract=True,
      extra_defs="""
  virtual std::string SingleNodeSqlString() const = 0;
      """)

  gen.AddNode(
      name='ASTNotNullColumnAttribute',
      parent='ASTColumnAttribute',
      force_gen_init_fields=True,
      extra_defs="""
  std::string SingleNodeSqlString() const override;
      """)

  gen.AddNode(
      name='ASTHiddenColumnAttribute',
      parent='ASTColumnAttribute',
      force_gen_init_fields=True,
      extra_defs="""
  std::string SingleNodeSqlString() const override;
      """)

  gen.AddNode(
      name='ASTPrimaryKeyColumnAttribute',
      parent='ASTColumnAttribute',
      use_custom_debug_string=True,
      force_gen_init_fields=True,
      fields=[
          Field(
              'enforced',
              SCALAR_BOOL_DEFAULT_TRUE)
      ],
      extra_defs="""
  std::string SingleNodeSqlString() const override;
      """)

  gen.AddNode(
      name='ASTForeignKeyColumnAttribute',
      parent='ASTColumnAttribute',
      fields=[
          Field(
              'constraint_name',
              'ASTIdentifier'),
          Field(
              'reference',
              'ASTForeignKeyReference',
              field_loader=FieldLoaderMethod.REQUIRED),
      ],
      extra_defs="""
  std::string SingleNodeSqlString() const override;
      """)

  gen.AddNode(
      name='ASTColumnAttributeList',
      parent='ASTNode',
      fields=[
          Field(
              'values',
              'ASTColumnAttribute',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTStructColumnField',
      parent='ASTNode',
      fields=[
          Field(
              'name',
              'ASTIdentifier',
              comment="""
            name_ will be NULL for anonymous fields like in STRUCT<int, string>.
              """),
          Field(
              'schema',
              'ASTColumnSchema',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTGeneratedColumnInfo',
      parent='ASTNode',
      use_custom_debug_string=True,
      custom_debug_string_comment="""
      Adds stored_mode (if needed) to the debug string.
      """,
      fields=[
          Field(
              'expression',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'stored_mode',
              SCALAR_STORED_MODE),
      ],
      extra_defs="""
  std::string GetSqlForStoredMode() const;
      """)

  gen.AddNode(
      name='ASTTableElement',
      parent='ASTNode',
      is_abstract=True,
      comment="""
      Base class for CREATE TABLE elements, including column definitions and
      table constraints.
      """)

  gen.AddNode(
      name='ASTColumnDefinition',
      parent='ASTTableElement',
      fields=[
          Field(
              'name',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'schema',
              'ASTColumnSchema',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.AddNode(
      name='ASTTableElementList',
      parent='ASTNode',
      fields=[
          Field(
              'elements',
              'ASTTableElement',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTColumnList',
      parent='ASTNode',
      fields=[
          Field(
              'identifiers',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTColumnPosition',
      parent='ASTNode',
      use_custom_debug_string=True,
      fields=[
          Field(
              'identifier',
              'ASTIdentifier',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'type',
              SCALAR_RELATIVE_POSITION_TYPE),
      ])

  gen.AddNode(
      name='ASTInsertValuesRow',
      parent='ASTNode',
      fields=[
          Field(
              'values',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED,
              comment="""
             A row of values in a VALUES clause.  May include ASTDefaultLiteral.
              """),
      ])

  gen.AddNode(
      name='ASTInsertValuesRowList',
      parent='ASTNode',
      fields=[
          Field(
              'rows',
              'ASTInsertValuesRow',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTInsertStatement',
      parent='ASTStatement',
      use_custom_debug_string=True,
      comment="""
      This is used for both top-level INSERT statements and for nested INSERTs
      inside ASTUpdateItem. When used at the top-level, the target is always a
      path expression.
      """,
      fields=[
          Field(
              'target_path',
              'ASTGeneralizedPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'column_list',
              'ASTColumnList'),
          Field(
              'rows',
              'ASTInsertValuesRowList',
              comment="""
              Non-NULL rows() means we had a VALUES clause.
              This is mutually exclusive with query() and with().
              """,
              private_comment="""
              Exactly one of rows_ or query_ will be present.
              with_ can be present if query_ is present.
              """),
          Field(
              'query',
              'ASTQuery'),
          Field(
              'assert_rows_modified',
              'ASTAssertRowsModified'),
          Field(
              'returning',
              'ASTReturningClause'),
          Field(
              'parse_progress',
              SCALAR_PARSE_PROGRESS,
              comment="""
      This is used by the Bison parser to store the latest element of the INSERT
      syntax that was seen. The INSERT statement is extremely complicated to
      parse in bison because it is very free-form, almost everything is optional
      and almost all of the keywords are also usable as identifiers. So we parse
      it in a very free-form way, and enforce the grammar in code during/after
      parsing.
              """),
          Field(
              'insert_mode',
              SCALAR_INSERT_MODE),
      ],
      extra_defs="""
  const ASTGeneralizedPathExpression* GetTargetPathForNested() const {
     return target_path_;
  }

  std::string GetSQLForInsertMode() const;

  // Verifies that the target path is an ASTPathExpression and, if so, returns
  // it. The behavior is undefined when called on a node that represents a
  // nested INSERT.
  zetasql_base::StatusOr<const ASTPathExpression*> GetTargetPathForNonNested() const;
""")

  gen.AddNode(
      name='ASTUpdateSetValue',
      parent='ASTNode',
      fields=[
          Field(
              'path',
              'ASTGeneralizedPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'value',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED,
              comment="""
              The rhs of SET X=Y.  May be ASTDefaultLiteral.
              """),
      ])

  gen.AddNode(
      name='ASTUpdateItem',
      parent='ASTNode',
      fields=[
          Field(
              'set_value',
              'ASTUpdateSetValue',
              private_comment="""
              Exactly one of set_value, insert_statement, delete_statement
              or update_statement will be non-NULL.
              """),
          Field(
              'insert_statement',
              'ASTInsertStatement'),
          Field(
              'delete_statement',
              'ASTDeleteStatement'),
          Field(
              'update_statement',
              'ASTUpdateStatement'),
      ])

  gen.AddNode(
      name='ASTUpdateItemList',
      parent='ASTNode',
      fields=[
          Field(
              'update_items',
              'ASTUpdateItem',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTUpdateStatement',
      parent='ASTStatement',
      comment="""
      This is used for both top-level UPDATE statements and for nested UPDATEs
      inside ASTUpdateItem. When used at the top-level, the target is always a
      path expression.
      """,
      fields=[
          Field(
              'target_path',
              'ASTGeneralizedPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'alias',
              'ASTAlias'),
          Field(
              'offset',
              'ASTWithOffset'),
          Field(
              'update_item_list',
              'ASTUpdateItemList',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'from_clause',
              'ASTFromClause'),
          Field(
              'where',
              'ASTExpression',
              field_loader=FieldLoaderMethod.OPTIONAL_EXPRESSION),
          Field(
              'assert_rows_modified',
              'ASTAssertRowsModified'),
          Field(
              'returning',
              'ASTReturningClause'),
      ],
      extra_defs="""
  const ASTGeneralizedPathExpression* GetTargetPathForNested() const {
    return target_path_;
  }

  // Verifies that the target path is an ASTPathExpression and, if so, returns
  // it. The behavior is undefined when called on a node that represents a
  // nested UPDATE.
  zetasql_base::StatusOr<const ASTPathExpression*> GetTargetPathForNonNested() const;
      """
      )
  gen.AddNode(
      name='ASTTruncateStatement',
      parent='ASTStatement',
      fields=[
          Field(
              'target_path',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'where',
              'ASTExpression',
              field_loader=FieldLoaderMethod.OPTIONAL_EXPRESSION),
      ],
      extra_defs="""
  // Verifies that the target path is an ASTPathExpression and, if so, returns
  // it. The behavior is undefined when called on a node that represents a
  // nested TRUNCATE (but this is not allowed by the parser).
  zetasql_base::StatusOr<const ASTPathExpression*> GetTargetPathForNonNested() const;
      """)

  gen.AddNode(
      name='ASTMergeAction',
      parent='ASTNode',
      use_custom_debug_string=True,
      fields=[
          Field(
              'insert_column_list',
              'ASTColumnList',
              comment="""
     Exactly one of the INSERT/UPDATE/DELETE operation must be defined in
     following ways,
       -- INSERT, action_type() is INSERT. The insert_column_list() is optional.
          The insert_row() must be non-null, but may have an empty value list.
       -- UPDATE, action_type() is UPDATE. update_item_list() is non-null.
       -- DELETE, action_type() is DELETE.
              """,
              private_comment="""
              For INSERT operation.
              """),
          Field(
              'insert_row',
              'ASTInsertValuesRow'),
          Field(
              'update_item_list',
              'ASTUpdateItemList',
              private_comment="""
              For UPDATE operation.
              """),
          Field(
              'action_type',
              SCALAR_ACTION_TYPE,
              private_comment="""
              Merge action type.
              """),
      ])

  gen.AddNode(
      name='ASTMergeWhenClause',
      parent='ASTNode',
      use_custom_debug_string=True,
      fields=[
          Field(
              'search_condition',
              'ASTExpression',
              field_loader=FieldLoaderMethod.OPTIONAL_EXPRESSION),
          Field(
              'action',
              'ASTMergeAction',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'match_type',
              SCALAR_MATCH_TYPE),
      ],
      extra_defs="""
  std::string GetSQLForMatchType() const;
      """)

  gen.AddNode(
      name='ASTMergeWhenClauseList',
      parent='ASTNode',
      fields=[
          Field(
              'clause_list',
              'ASTMergeWhenClause',
              field_loader=FieldLoaderMethod.REST_AS_REPEATED),
      ])

  gen.AddNode(
      name='ASTMergeStatement',
      parent='ASTStatement',
      fields=[
          Field(
              'target_path',
              'ASTPathExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'alias',
              'ASTAlias'),
          Field(
              'table_expression',
              'ASTTableExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'merge_condition',
              'ASTExpression',
              field_loader=FieldLoaderMethod.REQUIRED),
          Field(
              'when_clauses',
              'ASTMergeWhenClauseList',
              field_loader=FieldLoaderMethod.REQUIRED),
      ])

  gen.Generate(output_path, h_template_path=h_template_path)


if __name__ == '__main__':
  app.run(main)
