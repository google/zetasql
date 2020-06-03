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

"""Generates c++ classes for the ZetaSQL resolved AST representation.

The classes and fields to generate are specified in main() in this python
file. The base class for the tree is hand-written in resolved_node.h. Code
generated files can be viewed at (broken link)
Documentation is generated in resolved_ast.html.
A viewable copy of the documentation is at (broken link).
"""

import operator
import os.path
import re
import time
from absl import app
from absl import flags
from absl import logging
import jinja2
import markupsafe
from zetasql.resolved_ast import resolved_ast_enums_pb2

# Enums indicating whether a field can be ignored without breaking semantics.
# If a field is not ignorable, and the user doesn't look at the field, then
# CheckFieldsAccessed() will return an error saying this query can't run
# because feature X is unimplemented.
NOT_IGNORABLE = 0  # Field can never be ignored. This is the default.
IGNORABLE = 1  # Field can always be ignored.
IGNORABLE_DEFAULT = 2  # Field can be ignored if it has its default value.

NODE_NAME_PREFIX = 'Resolved'
ROOT_NODE_NAME = 'ResolvedNode'

SCALAR_TYPES = {}

# Disabling "Wrong hanging indentation" lint warnings because Field arrays
# are not following correct indentation.
# pylint: disable=bad-continuation


class ScalarType(object):
  """Class used for scalar types as Field ctype parameters."""

  def __init__(self,
               ctype,
               proto_type=None,
               java_type=None,
               java_reference_type=None,
               passed_by_reference=False,
               has_proto_setter=False,
               is_enum=False,
               scoped_ctype=None,
               java_default=None):
    """Create a ScalarType.

    Args:
      ctype: C type name for this ScalarType
      proto_type: The proto field type name used to store this field.  If not
          set, this defaults to using the same name as ctype.
      java_type: Java type name for this ScalarType. Defaults to ctype.
      java_reference_type: Java type name when reference type is needed for this
                           ScalarType. Defaults to java_type.
      passed_by_reference: Specify whether this ScalarType should be passed by
          value or by reference in constructors and getter methods. Types that
          are really classes should be passed by reference. Real scalar types
          (PODs) should be passed by value.
      has_proto_setter: True if fields of this type have a set_X(value) method
          in C++. For example, enum, int64, string, fields do. Message fields
          don't. Always set to True if ctype == proto_type. Otherwise defaults
          to False.
      is_enum: True if this ScalarType represents an Enum normally persisted
          as integers in proto form. This is used to generate serialization
          logic that casts between the enum type and underlying int as
          necessary.
      scoped_ctype: C type, possibly with scope prepended as in the case of
          inner types.  Useful for locally declared enums that need to be
          referenced externally to that class.
          If not set, this defaults to using the same name as ctype.
      java_default: Non-Constructor args require a default value. While java
          field defaults match c++ (for PODS), it's best practice to
          initialize them explicitly.
    """
    self.ctype = ctype
    self.is_enum = is_enum
    self.passed_by_reference = passed_by_reference
    if java_type is None:
      self.java_type = ctype
    else:
      self.java_type = java_type
    if java_reference_type is None:
      self.java_reference_type = self.java_type
    else:
      self.java_reference_type = java_reference_type
    if proto_type is None:
      self.proto_type = ctype
      self.has_proto_setter = True
    else:
      self.proto_type = proto_type
      self.has_proto_setter = has_proto_setter
    if scoped_ctype is None:
      self.scoped_ctype = ctype
    else:
      self.scoped_ctype = scoped_ctype
    self.java_default = java_default
    assert ctype not in SCALAR_TYPES
    SCALAR_TYPES[ctype] = self


SCALAR_TYPE = ScalarType('const Type*', 'TypeProto', 'Type')
SCALAR_VALUE = ScalarType(
    'Value', 'ValueWithTypeProto', passed_by_reference=True)
SCALAR_TABLE = ScalarType('const Table*', 'TableRefProto', 'Table')
SCALAR_MODEL = ScalarType('const Model*', 'ModelRefProto', 'Model')
SCALAR_CONNECTION = ScalarType('const Connection*', 'ConnectionRefProto',
                               'Connection')
SCALAR_RESOLVED_COLUMN = ScalarType(
    'ResolvedColumn', 'ResolvedColumnProto', passed_by_reference=True)
SCALAR_CONSTANT = ScalarType('const Constant*', 'ConstantRefProto', 'Constant')
SCALAR_FUNCTION = ScalarType('const Function*', 'FunctionRefProto', 'Function')
SCALAR_FUNCTION_SIGNATURE = ScalarType(
    'FunctionSignature', 'FunctionSignatureProto', passed_by_reference=True)
SCALAR_FUNCTION_CALL_INFO = ScalarType(
    'std::shared_ptr<ResolvedFunctionCallInfo>',
    'ResolvedFunctionCallInfoProto',
    passed_by_reference=True,
    java_type='ResolvedFunctionCallInfo')
SCALAR_FIELD_DESCRIPTOR = ScalarType('const google::protobuf::FieldDescriptor*',
                                     'FieldDescriptorRefProto',
                                     'ZetaSQLFieldDescriptor')
SCALAR_ENUM_DESCRIPTOR = ScalarType('const google::protobuf::EnumDescriptor*',
                                    'EnumTypeProto', 'ZetaSQLEnumDescriptor')
SCALAR_FIELD_FORMAT = ScalarType(
    'FieldFormat::Format',
    'FieldFormat.Format',
    'FieldFormat.Format',
    has_proto_setter=True)

SCALAR_STRING = ScalarType(
    'std::string',
    proto_type='string',
    java_type='String',
    passed_by_reference=True,
    has_proto_setter=True,
    java_default='')
SCALAR_BOOL = ScalarType(
    'bool',
    java_type='boolean',
    java_reference_type='Boolean',
    java_default='false')
SCALAR_INT = ScalarType(
    'int',
    'int64',
    java_type='long',
    java_reference_type='Long',
    has_proto_setter=True,
    java_default='0')
TABLE_VALUED_FUNCTION = ScalarType(
    'const TableValuedFunction*',
    'TableValuedFunctionRefProto',
    java_type='TableValuedFunction')
TVF_SIGNATURE = ScalarType(
    'std::shared_ptr<TVFSignature>',
    'TVFSignatureProto',
    passed_by_reference=True,
    java_type='TVFSignature')
SCALAR_PROCEDURE = ScalarType('const Procedure*', 'ProcedureRefProto',
                              'Procedure')


def EnumScalarType(enum_name, node_name):
  """Create a ScalarType for enums defined in resolved_ast_enums.proto.

  Args:
    enum_name: name of the enum.
    node_name: name of the ResolvedNode that this enum belongs to.
  Returns:
    The ScalarType.
  """
  return ScalarType(
      ctype=enum_name,
      proto_type='%sEnums.%s' % (node_name, enum_name),
      has_proto_setter=True,
      is_enum=True,
      scoped_ctype='%s::%s' % (node_name, enum_name))


SCALAR_SUBQUERY_TYPE = EnumScalarType('SubqueryType', 'ResolvedSubqueryExpr')
SCALAR_JOIN_TYPE = EnumScalarType('JoinType', 'ResolvedJoinScan')
SCALAR_SET_OPERATION_TYPE = EnumScalarType('SetOperationType',
                                           'ResolvedSetOperationScan')
SCALAR_RECURSIVE_SET_OPERATION_TYPE = EnumScalarType(
    'RecursiveSetOperationType', 'ResolvedRecursiveScan')
SCALAR_SAMPLE_UNIT = EnumScalarType('SampleUnit', 'ResolvedSampleScan')
SCALAR_NULL_ORDER_MODE = EnumScalarType('NullOrderMode', 'ResolvedOrderByItem')
SCALAR_CREATE_MODE = EnumScalarType('CreateMode', 'ResolvedCreateStatement')
SCALAR_CREATE_SCOPE = EnumScalarType('CreateScope', 'ResolvedCreateStatement')
SCALAR_READ_WRITE_TRANSACTION_MODE = EnumScalarType('ReadWriteMode',
                                                    'ResolvedBeginStmt')
SCALAR_FRAME_UNIT = EnumScalarType('FrameUnit', 'ResolvedWindowFrame')
SCALAR_BOUNDARY_TYPE = EnumScalarType('BoundaryType', 'ResolvedWindowFrameExpr')
SCALAR_INSERT_MODE = EnumScalarType('InsertMode', 'ResolvedInsertStmt')
SCALAR_MERGE_ACTION_TYPE = EnumScalarType('ActionType', 'ResolvedMergeWhen')
SCALAR_MERGE_MATCH_TYPE = EnumScalarType('MatchType', 'ResolvedMergeWhen')
SCALAR_ARGUMENT_KIND = EnumScalarType('ArgumentKind', 'ResolvedArgumentDef')
SCALAR_HAVING_MODIFIER_KIND = EnumScalarType('HavingModifierKind',
                                             'ResolvedAggregateHavingModifier')
SCALAR_NULL_HANDLING_MODIFIER_KIND = EnumScalarType(
    'NullHandlingModifier', 'ResolvedNonScalarFunctionCallBase')
SCALAR_IMPORT_KIND = EnumScalarType('ImportKind', 'ResolvedImportStmt')
SCALAR_ERROR_MODE = EnumScalarType('ErrorMode', 'ResolvedFunctionCallBase')
SCALAR_OBJECT_ACCESS = EnumScalarType('ObjectAccess', 'ResolvedStatement')
SCALAR_FOREIGN_KEY_MATCH_MODE = EnumScalarType(
    'MatchMode', 'ResolvedForeignKey')
SCALAR_FOREIGN_KEY_ACTION_OPERATION = EnumScalarType(
    'ActionOperation', 'ResolvedForeignKey')
SCALAR_SQL_SECURITY = EnumScalarType('SqlSecurity', 'ResolvedCreateStatement')
SCALAR_DETERMINISM_LEVEL = EnumScalarType('DeterminismLevel',
                                          'ResolvedCreateStatement')


def _Trim(s):
  """Remove double blank lines and trailing spaces from string."""
  s = re.sub(r'  *\n', r'\n', s)
  s = re.sub(r'\n\n\n*', r'\n\n', s)
  return s


def _JavaDoc(text, indent=0):
  """Returns text for a JavaDoc comment from the given text.

  Args:
    text: comment text
    indent: indent level

  It will be indented by the specified number of spaces.
  """
  if not text:
    return text
  indent_text = ' ' * indent
  content = _CleanComment(text, '%s * ' % indent_text)

  # Prefix <p> to lines that start a new paragraph. The regex finds lines that
  # follow an empty line.
  add_paragraph_re = re.compile(r'\* \n( *\* )(\S)')
  content = add_paragraph_re.sub(r'* \n\1<p> \2', content)

  # Add the leading line (/**) and trailing line (*/)
  return '%s/**\n%s\n%s */' % (indent_text, content, indent_text)


def _CleanComment(text, prefix=''):
  """Remove extra indentation from comments.

  This allows comments to be written as triple-quoted strings with natural
  wrapping and the extra spaces will get stripped.

  Args:
    text: Comment text
    prefix: will be added to the start of each line.

  Returns:
    Comment text with extra indentation removed.
  """
  if text:
    text = text.strip()
    lines = text.split('\n')

    def NumLeadingSpaces(line):
      num = 0
      for c in line:
        if c.isspace():
          num += 1
        else:
          break
      return num

    # Compute the indentation we will strip off.  Do it by looking at the
    # minimum indentation on non-empty lines after the first line.
    # (The first line will have already been stripped.)
    # This is assuming there is a uniform indentation on the left of the
    # comment that we should strip off, but there could be additional
    # indentation on some lines that we'd rather keep.
    non_empty_lines = [line for line in lines[1:] if line.strip()]
    if non_empty_lines:
      leading_spaces = min(NumLeadingSpaces(line) for line in non_empty_lines)
      strip_prefix = ' ' * leading_spaces
    else:
      strip_prefix = ''

    def Unindent(line):
      if line.startswith(strip_prefix):
        line = line[len(strip_prefix):]
      return line

    # Remove leading spaces from each line and add prefix.
    text = '\n'.join(prefix + Unindent(line.rstrip()) for line in lines)
  return text


def Field(name,
          ctype,
          tag_id,
          vector=False,
          ignorable=NOT_IGNORABLE,
          is_constructor_arg=True,
          to_string_method=None,
          java_to_string_method=None,
          comment=None,
          propagate_order=False):
  """Make a field to put in a node class.

  Args:
    name: field name
    ctype: c++ type for this field
           Should be a ScalarType like an int, string or enum type,
           or the name of a node class type (e.g. ResolvedProjectScan).
           Cannot be a pointer type, and should not include modifiers like
           const.
    tag_id: unique tag number for the proto field. This should never change or
            be reused.
    vector: True if this field is a vector of ctype. (Not supported for
            scalars.)
    ignorable: one of the IGNORABLE enums above.
    is_constructor_arg: True if this field should be in the constructor's
                        argument list.
    to_string_method: Override the default ToStringImpl method used to print
                      this object.
    java_to_string_method: Override the default to_string_method method used for
                           java.
    comment: Comment text for this field.  Text will be stripped and
             de-indented.
    propagate_order: If true, this field and the parent node must both be
                     ResolvedScan subclasses, and the is_ordered property of
                     the field will be propagated to the containing scan.
  Returns:
    The newly created field.

  Raises:
    RuntimeError: If an error is detected in one or more arguments.
  """
  assert tag_id > 1, tag_id
  assert ignorable in (IGNORABLE, NOT_IGNORABLE, IGNORABLE_DEFAULT)
  member_name = name + '_'

  if vector:
    assert name.endswith('_list') or name.endswith('_path'), (
        'Vector fields should normally be named <x>_list or <x>_path: %s' %
        name)

  # C++ code requires a large number of variations of the same type declaration.
  # These are:
  #   member_type - type of the class member
  #   member_accessor - get the member variable, unwrapping unique_ptr if needed
  #   setter_arg_type - type when passed to constructor or setter method.
  #   maybe_ptr_setter_arg_type - type when passed to constructor or setter
  #                               method as a bare pointer which transfers
  #                               ownership.
  #   getter_return_type - type returned from a getter method
  #   release_return_type - type returned from a release method, or None to
  #                         indicate there should be no release() method.
  #   element_arg_type - for vectors, the element type for get/set methods
  #   element_storage_type - for vectors, the type inside the vector.
  #   element_getter_return_type - for vectors, the return type for get/set
  #                         methods returning a specific element.
  #   element_unwrapper - '.get()', if necessary, to unwrap unique_ptrs.
  #   is_move_only - setters must use std::move instead of assign/copy as
  #                  for anything wrapped in a unique_ptr/vector<unique_ptr>.
  if isinstance(ctype, ScalarType):
    is_node_type = False
    member_type = ctype.ctype
    proto_type = ctype.proto_type
    has_proto_setter = ctype.has_proto_setter
    java_default = ctype.java_default
    nullable_java_type = ctype.java_reference_type
    member_accessor = member_name
    element_arg_type = None
    element_storage_type = None
    element_getter_return_type = None
    element_unwrapper = ''
    release_return_type = None
    is_enum = ctype.is_enum
    is_move_only = False

    if ctype.passed_by_reference:
      setter_arg_type = 'const %s&' % member_type
      getter_return_type = 'const %s&' % member_type
    else:
      setter_arg_type = member_type
      getter_return_type = ctype.ctype

    if vector:
      element_arg_type = ctype.ctype
      element_getter_return_type = getter_return_type
      element_storage_type = ctype.ctype
      member_type = 'std::vector<%s>' % ctype.ctype
      setter_arg_type = 'const %s&' % member_type
      getter_return_type = setter_arg_type
      java_type = ctype.java_reference_type
      java_default = 'ImmutableList.of()'
    else:
      java_type = ctype.java_type
      java_default = ctype.java_default

    # For scalars, these are always the same
    maybe_ptr_setter_arg_type = setter_arg_type

  else:
    # Non-scalars should all be node types (possibly node vectors).
    assert 'const' not in ctype, ctype
    assert '*' not in ctype, ctype
    if not ctype.startswith(NODE_NAME_PREFIX):
      raise RuntimeError('Invalid field type: %s' % (ctype,))
    is_node_type = True
    has_proto_setter = False
    element_pointer_type = 'const %s*' % ctype
    element_storage_type = 'std::unique_ptr<const %s>' % ctype
    element_unwrapper = '.get()'
    proto_type = '%sProto' % ctype
    java_type = ctype
    nullable_java_type = ctype
    java_default = None
    is_enum = False
    is_move_only = True
    if vector:
      element_arg_type = element_pointer_type
      element_getter_return_type = element_pointer_type
      member_type = 'std::vector<%s>' % element_storage_type
      maybe_ptr_setter_arg_type = (
          'const std::vector<%s>&' % element_pointer_type)
      getter_return_type = 'const %s&' % member_type
      release_return_type = 'std::vector<%s>' % element_pointer_type
      member_accessor = member_name
      java_default = 'ImmutableList.of()'
    else:
      member_type = element_storage_type
      maybe_ptr_setter_arg_type = element_pointer_type
      getter_return_type = element_pointer_type
      release_return_type = element_pointer_type
      element_arg_type = None
      element_storage_type = None
      element_getter_return_type = None
      member_accessor = member_name + '.get()'
      if is_constructor_arg:
        java_default = None
      else:
        # TODO: remove when we add support for optional arguments.
        java_default = 'null'

    # For node vector, we use std::move, which requires setters = member.
    setter_arg_type = member_type

  if vector:
    optional_or_repeated = 'repeated'
    full_java_type = 'ImmutableList<%s>' % java_type
    nullable_java_type = full_java_type
  else:
    optional_or_repeated = 'optional'
    full_java_type = java_type

  if not to_string_method:
    to_string_method = 'ToStringImpl'
  if not java_to_string_method:
    java_to_string_method = to_string_method[0].lower() + to_string_method[1:]

  if is_enum:
    scoped_setter_arg_type = ctype.scoped_ctype
  else:
    scoped_setter_arg_type = setter_arg_type

  if (not is_constructor_arg) and java_default is None:
    logging.fatal(
        'Field %s must either be a constructor arg, or have a java_default. ctype:\n%s',
        name, ctype)

  return {
      'ctype': ctype,
      'java_type': java_type,
      'full_java_type': full_java_type,
      'nullable_java_type': nullable_java_type,
      'java_default': java_default,
      'tag_id': tag_id,
      'member_name': member_name,  # member variable name
      'name': name,  # name without trailing underscore
      'comment': _CleanComment(comment, prefix='  // '),
      'javadoc': _JavaDoc(comment, indent=4),
      'member_accessor': member_accessor,
      'member_type': member_type,
      'proto_type': proto_type,
      'optional_or_repeated': optional_or_repeated,
      'has_proto_setter': has_proto_setter,
      'setter_arg_type': setter_arg_type,
      'maybe_ptr_setter_arg_type': maybe_ptr_setter_arg_type,
      'scoped_setter_arg_type': scoped_setter_arg_type,
      'getter_return_type': getter_return_type,
      'release_return_type': release_return_type,
      'is_vector': vector,
      'element_getter_return_type': element_getter_return_type,
      'element_arg_type': element_arg_type,
      'element_storage_type': element_storage_type,
      'element_unwrapper': element_unwrapper,
      'is_node_ptr': is_node_type and not vector,
      'is_node_vector': is_node_type and vector,
      'is_enum_vector': is_enum and vector,
      'is_move_only': is_move_only,
      'is_not_ignorable': ignorable == NOT_IGNORABLE,
      'is_ignorable_default': ignorable == IGNORABLE_DEFAULT,
      'is_constructor_arg': is_constructor_arg,
      'to_string_method': to_string_method,
      'java_to_string_method': java_to_string_method,
      'propagate_order': propagate_order
  }


class TreeGenerator(object):
  """Generates code to define tree objects.
  """

  def __init__(self):
    self.nodes = []

    # Map node name to node (which is just the dictionary created in AddNode
    # right now).
    self.node_map = {}

    # We don't actually have the real root node ResolvedNode, so this
    # is a list of the first-level children.
    self.root_child_nodes = []

  def AddNode(self,
              name,
              tag_id,
              parent,
              fields,
              is_abstract=False,
              extra_defs='',
              emit_default_constructor=True,
              use_custom_debug_string=False,
              comment=None):
    """Add a node class to be generated.

    Args:
      name: class name for this node
      tag_id: unique tag number for the node as a proto field or an enum value.
          tag_id for each node type is hard coded and should never change.
          Next tag_id: 152.
      parent: class name of the parent node
      fields: list of fields in this class; created with Field function
      is_abstract: true if this node is an abstract class
      extra_defs: extra c++ definitions to put in this class.
      emit_default_constructor: If True, emit default constructor for this class
      use_custom_debug_string: If True, use hand-written
          CollectDebugStringFields and GetNameForDebugString c++ methods for
          DebugString.
      comment: Comment text for this node.  Text will be stripped and
          de-indented.
    """
    assert name not in self.node_map, name
    assert name != ROOT_NODE_NAME, name
    assert parent in self.node_map or parent == ROOT_NODE_NAME, parent
    if parent == ROOT_NODE_NAME:
      # Nodes generally fit into these four top-level categories.
      # Nodes that are part of other nodes are usually ResolvedArguments.
      assert name in ('ResolvedArgument', 'ResolvedExpr', 'ResolvedScan',
                      'ResolvedStatement'), name
    assert len(fields) <= 32, 'More than 32 fields not supported'
    assert name not in SCALAR_TYPES
    fields = [field.copy() for field in fields]
    extra_enum_decls, extra_enum_defs = self._ResolveImportEnums(name)

    make_enum_name_re = re.compile(r'([a-z])([A-Z])')

    def NameToEnumName(name):
      """Convert a camel-case c++ ClassName into CLASS_NAME."""
      return make_enum_name_re.sub(r'\1_\2', name).upper()

    def NameToNodeKindName(name):
      """Returns the name of a node kind, suitable for a tree dump.

      This is simply the name, minus the "Resolved" prefix (if present).
      Args:
        name: name of node class (in C++).
      """
      if name.startswith(NODE_NAME_PREFIX):
        return name[len(NODE_NAME_PREFIX):]
      else:
        return name

    # Add position-related field members.
    for (pos, field) in enumerate(fields):
      field.update({'bitmap': '(1<<%d)' % pos})

    # Compute list of fields inherited from the parent node.
    # Used for constructors that accept and pass on all inherited fields.
    inherited_fields = []
    if parent != ROOT_NODE_NAME:
      inherited_fields = (self.node_map[parent]['inherited_fields'] +
                          self.node_map[parent]['fields'])

    proto_type = '%sProto' % name

    # Marking classes and virtual methods as final may allow the
    # compiler to avoid some virtual function calls.
    if is_abstract:
      class_final = ''
      override_or_final = 'override'
      proto_field_type = 'Any%sProto' % name
    else:
      class_final = 'final'
      override_or_final = 'final'
      proto_field_type = proto_type

    has_node_vector_constructor_arg = any(
        field['is_node_vector'] and field['is_constructor_arg']
        for field in fields + inherited_fields)

    node_dict = ({
        'name': name,
        'proto_type': proto_type,
        'proto_field_type': proto_field_type,
        'enum_name': NameToEnumName(name),
        'member_name': NameToEnumName(name).lower(),
        'node_kind_name': NameToNodeKindName(name),
        'tag_id': tag_id,
        'parent': parent,
        'parent_proto_container_type': 'Any%sProto' % parent,
        'parent_proto_type': '%sProto' % parent,
        'depth': (self.node_map[parent]['depth'] + 1
                  if parent != ROOT_NODE_NAME else 1),
        'is_abstract': is_abstract,
        'class_final': class_final,
        'override_or_final': override_or_final,
        'comment': _CleanComment(comment, prefix='// '),
        'javadoc': _JavaDoc(comment, indent=2),
        'fields': fields,
        'inherited_fields': inherited_fields,
        'extra_enum_defs': extra_enum_defs,
        'extra_defs': extra_enum_decls + extra_defs,
        'emit_default_constructor': emit_default_constructor,
        'has_node_vector_constructor_arg': has_node_vector_constructor_arg,
        'use_custom_debug_string': use_custom_debug_string,
        'subclasses': []
    })
    # This is the name of the defining node rather than a reference to
    # avoid creating a recursive data structure that cannot be compared.
    # TraverseToRoot contains such a comparison.
    #
    # In the template, use the defining_node(field) function to find
    # the node in which this field is defined.
    for field in fields:
      field.update({'defining_node_name': name})

    self.nodes.append(node_dict)
    self.node_map[name] = node_dict

  def _ResolveImportEnums(self, cpp_class_name):
    """Import enums under <cpp_class_name>Enums message into the C++ class.

    Args:
      cpp_class_name: C++ class name the enums should be imported into.

    Returns:
      Tuple of C++ declarations and definitions for the .h and .cc files.

    Load definitions of enums defined under <cpp_class_name>Enums message in
    resolved_ast_enums.proto if any, and convert them to C++ declarations and
    definitions to be included in the generated .h/.cc files, so that the enum
    values can be referred as static consts under the C++ class.
    E.g.: if cpp_class_name='ResolvedWindowFrame' and there's a FrameUnit enum
    defined under ResolvedWindowFrameEnums in resolved_ast_enums.proto as:
    message ResolvedWindowFrameEnums {
      enum FrameUnit {
        ROWS = 0;
        RANGE = 1;
      };
    };
    then this function will return the C++ declarations:
      typedef ResolvedWindowFrameEnums::FrameUnit FrameUnit;
      static const FrameUnit ROWS = ResolvedWindowFrameEnums::ROWS;
      static const FrameUnit RANGE = ResolvedWindowFrameEnums::RANGE;
    which should be included in the class declaration, and C++ definitions:
      const ResolvedWindowFrame::FrameUnit ResolvedWindowFrame::ROWS;
      const ResolvedWindowFrame::FrameUnit ResolvedWindowFrame::RANGE;
    which should be included in the .cc file.

    """
    message_types = resolved_ast_enums_pb2.DESCRIPTOR.message_types_by_name
    message_name = cpp_class_name + 'Enums'

    # Hack: We import the same enum names into ResolvedArgument{Ref,Def}.
    if message_name == 'ResolvedArgumentRefEnums':
      message_name = 'ResolvedArgumentDefEnums'

    if message_name not in message_types:
      return '', ''

    typedefs = []
    declarations = []
    definitions = []

    for enum_type in message_types[message_name].enum_types:
      enum_name = enum_type.name
      typedefs.append('  typedef %s::%s %s;\n' % (message_name, enum_name,
                                                  enum_name))
      for value in enum_type.values:
        declarations.append('  static const %s %s = %s::%s;\n' %
                            (enum_name, value.name, message_name, value.name))
        definitions.append('const %s::%s %s::%s;\n' %
                           (cpp_class_name, enum_name, cpp_class_name,
                            value.name))
    return ''.join(typedefs) + ''.join(declarations), ''.join(definitions)

  def _JavaEnumClasses(self):
    classes = []
    message_types = resolved_ast_enums_pb2.DESCRIPTOR.message_types_by_name
    for name, message in message_types.items():
      for enum_type in message.enum_types:
        classes.append('%s.%s' % (name, enum_type.name))
    return classes

  def _GetNodeByName(self, name):
    return self.node_map[name]

  def _ComputeTreeData(self):
    """Add computed nodes to the tree.

    Traverse the node tree hierarchy and add computed nodes that couldn't be
    built before we have the full tree.
    """
    assert not self.root_child_nodes

    def TraverseToRoot(node):
      """Count the number of leaf nodes existing as descendents of each node.

      Traverses upwards from each leaf, incrementing a counter.

      Args:
        node: Node to count descendents of.
      """
      node.setdefault('num_descendant_leaf_types', 0)
      node['num_descendant_leaf_types'] += 1

      for field in node['fields']:
        field_type = field['ctype']
        if field_type in self.node_map:
          field_node = self._GetNodeByName(field_type)
          field['proto_type'] = field_node['proto_field_type']

      parent_name = node['parent']
      if parent_name != ROOT_NODE_NAME:
        parent_node = self._GetNodeByName(parent_name)
        TraverseToRoot(parent_node)
        parent_subclass_list = parent_node['subclasses']
      else:
        parent_subclass_list = self.root_child_nodes

      # Note: this comparison is by value, so the transitive closure of <node>'s
      # entries cannot contain <node>.
      if node not in parent_subclass_list:
        parent_subclass_list.append(node)

    for node in self.nodes:
      if not node['is_abstract']:
        TraverseToRoot(node)

  def Generate(self, input_file_paths, output_file_paths):
    """Materialize the templates to generate the output files."""

    def ShouldAutoescape(template_name):
      return template_name and '.html' in template_name

    jinja_env = jinja2.Environment(
        undefined=jinja2.StrictUndefined,
        autoescape=ShouldAutoescape,
        trim_blocks=True,
        lstrip_blocks=True,
        line_statement_prefix='# ',
        loader=jinja2.FileSystemLoader(''))

    # This can be used to filter a list of fields to only those with
    # <is_constructor_arg = true>.  Used in templates like this:
    #   for node.fields | is_constructor_arg
    def IsConstructorArg(field_list):
      return [field for field in field_list if field['is_constructor_arg']]

    def IsNodeVectorConstructorArg(field_list):
      return [
          field for field in field_list
          if field['is_node_vector'] and field['is_constructor_arg']
      ]

    jinja_env.filters['is_constructor_arg'] = IsConstructorArg
    jinja_env.filters[
        'is_node_vector_constructor_arg'] = IsNodeVectorConstructorArg

    # These can be used to make relative links and targets within a doc.
    # {{name|as_link}} will make emit name with a link to "#name".
    # {{name|as_target}} will make emit name and make it a target for "#name".
    def AsLink(text):
      # Markup makes this not get re-escaped.
      return markupsafe.Markup('<a href="#%s">%s</a>' % (text, text))

    def AsTarget(text):
      return markupsafe.Markup('<a name="%s">%s</a>' % (text, text))

    jinja_env.filters['as_link'] = AsLink
    jinja_env.filters['as_target'] = AsTarget

    # This can be used to find node names in a string (e.g. a c++ return type)
    # and turn them into relative links inside the doc.
    linkify_re = re.compile('(Resolved[a-zA-Z]*)')

    def LinkifyNodeNames(text):
      # Escape the text which may include strings like std::vector<xxx>,
      # and Markup the output to not be escaped further.
      text = markupsafe.escape(text)
      match = linkify_re.search(text)
      if match:
        name = match.groups()[0]
        return markupsafe.Markup(text.replace(name, AsLink(name)))
      else:
        return text

    jinja_env.filters['linkify_node_names'] = LinkifyNodeNames

    # {{items|sort_by_name}} can be used to sort a list of objects by name.
    def SortByName(items):
      return sorted(items, key=operator.itemgetter('name'))

    jinja_env.filters['sort_by_name'] = SortByName

    # {{items|sort_by_tag}} can be used to sort a list of objects by tag.
    def SortByTagId(items):
      return sorted(items, key=operator.itemgetter('tag_id'))

    jinja_env.filters['sort_by_tag'] = SortByTagId

    # {{string|upper_camel_case}} turns some_string or SOME_STRING into
    # SomeString.
    def UpperCamelCase(value):
      split_value = value.lower().split('_')
      return ''.join([part.capitalize() for part in split_value])

    jinja_env.filters['upper_camel_case'] = UpperCamelCase

    # {{string|lower_camel_case}} turns some_string or SOME_STRING into
    # someString.
    def LowerCamelCase(value):
      split_value = value.lower().split('_')
      result = [split_value[0]]
      for part in split_value[1:]:
        result.append(part.capitalize())
      return ''.join(result)

    jinja_env.filters['lower_camel_case'] = LowerCamelCase

    # {{defining_node(some_field)}} will be the node a field is defined in.
    # We have this, rather than a direct field reference, to prevent the data
    # structure from becoming recursive.
    def DefiningNode(field):
      return self.node_map[field['defining_node_name']]

    jinja_env.globals['defining_node'] = DefiningNode

    self._ComputeTreeData()

    context = {
        'nodes': self.nodes,
        'root_node_name': ROOT_NODE_NAME,
        'root_child_nodes': self.root_child_nodes,
        'java_enum_classes': self._JavaEnumClasses(),
        'timestamp': time.ctime(),
        # For when we need to force a blank line and jinja wants to
        # eat blank lines from the template.
        'blank_line': '\n'
    }
    assert len(input_file_paths) == len(output_file_paths)

    for (in_path, out_path) in zip(input_file_paths, output_file_paths):
      if (os.path.basename(in_path) !=
          os.path.basename(out_path) + '.template'):
        logging.fatal(
            'Input must match output with ".template" postfix.'
            'Saw: input=%s output=%s', os.path.basename(in_path),
            os.path.basename(out_path))

      template = jinja_env.get_template(in_path)
      out = open(out_path, 'wt')
      out.write(_Trim(template.render(context)))
      out.close()


def main(argv):
  usage = """
   gen_resolved_py [base.template]... [base]...
   inputs must be of the form: '<input_path>/<filebase>.template' and have a
   corresponding output file of the form <output_path>/<filebase>
   Example:
     gen_resolved_py \\
        zetasql/resolved_ast/resolved_ast.cc.template \\
        zetasql/resolved_ast/resolved_ast.h.template \\
        blaze-genfiles/zetasql/resolved_ast/resolved_ast.cc \\
        blaze-genfiles/zetasql/resolved_ast/resolved_ast.h
  """
  if len(argv) < 3:
    logging.fatal('Must specify at least one input/output pair\n%s', usage)

  if (len(argv) - 1) % 2 == 1:
    logging.fatal('Must provide an equal number of inputs and outputs\n%s',
                  usage)

  # argv of the script is the list of all srcs of the form
  # '.../<basename.template' followed by a list of all outputs in the same
  # order of the format '.../<basename>'
  # Example:
  #   gen_resolved_py \
  #      resolved_ast.cc.template \
  #      resolved_ast.h.template \
  #      resolved_ast.cc \
  #      resolved_ast.h
  input_file_paths = argv[1:int(1 + len(argv) / 2)]
  output_file_paths = argv[1 + len(input_file_paths):]

  gen = TreeGenerator()

  gen.AddNode(
      name='ResolvedArgument',
      tag_id=1,
      parent='ResolvedNode',
      is_abstract=True,
      comment="""
      Argument nodes are not self-contained nodes in the tree.  They exist
      only to describe parameters to another node (e.g. columns in an OrderBy).
      This node is here for organizational purposes only, to cluster these
      argument nodes.
              """,
      fields=[])

  # Expressions

  gen.AddNode(
      name='ResolvedExpr',
      tag_id=2,
      parent='ResolvedNode',
      is_abstract=True,
      fields=[Field('type', SCALAR_TYPE, tag_id=2, ignorable=IGNORABLE)],
      extra_defs="""  bool IsExpression() const final { return true; }""")

  gen.AddNode(
      name='ResolvedLiteral',
      tag_id=3,
      parent='ResolvedExpr',
      comment="""
      Any literal value, including NULL literals.
      There is a special-cased constructor here that gets the type from the
      Value.
              """,
      fields=[
          Field('value', SCALAR_VALUE, tag_id=2),
          Field(
              'has_explicit_type',
              SCALAR_BOOL,
              tag_id=3,
              ignorable=IGNORABLE,
              comment="""
              If true, then the literal is explicitly typed and cannot be used
              for literal coercions.

              This exists mainly for resolver bookkeeping and should be ignored
              by engines.
                      """),
          Field(
              'float_literal_id',
              SCALAR_INT,
              tag_id=4,
              ignorable=IGNORABLE,
              comment="""
              Distinct ID of the literal, if it is a floating point value,
              within the resolved AST. When coercing from floating point
              to NUMERIC, the resolver uses the float_literal_id to find the
              original image of the literal to avoid precision loss. An ID of 0
              represents a literal without a cached image.
              """)
      ])

  gen.AddNode(
      name='ResolvedParameter',
      tag_id=4,
      parent='ResolvedExpr',
      fields=[
          Field(
              'name',
              SCALAR_STRING,
              tag_id=2,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              If non-empty, the name of the parameter.

              A ResolvedParameter will have either a name or a position but not
              both.
                      """),
          Field(
              'position',
              SCALAR_INT,
              tag_id=5,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              If non-zero, the 1-based position of the positional parameter.

              A ResolvedParameter will have either a name or a position but not
              both.
                      """),
          Field(
              'is_untyped',
              SCALAR_BOOL,
              tag_id=3,
              ignorable=IGNORABLE,
              comment="""
              If true, then the parameter has no specified type.

              This exists mainly for resolver bookkeeping and should be ignored
              by engines.
                      """)
      ])

  gen.AddNode(
      name='ResolvedExpressionColumn',
      tag_id=5,
      parent='ResolvedExpr',
      comment="""
      This represents a column when analyzing a standalone expression.
      This is only used when the analyzer was called using AnalyzeExpression.
      Expression column names and types come from
      AnalyzerOptions::AddExpressionColumn.
      <name> will always be in lowercase.
              """,
      fields=[Field('name', SCALAR_STRING, tag_id=2)])

  gen.AddNode(
      name='ResolvedColumnRef',
      tag_id=6,
      parent='ResolvedExpr',
      comment="""
      An expression referencing the value of some column visible in the
      current Scan node.

      If <is_correlated> is false, this must be a column visible in the Scan
      containing this expression, either because it was produced inside that
      Scan or it is on the <column_list> of some child of this Scan.

      If <is_correlated> is true, this references a column from outside a
      subquery that is visible as a correlated column inside.
      The column referenced here must show up on the parameters list for the
      subquery.  See ResolvedSubqueryExpr.
              """,
      fields=[
          Field('column', SCALAR_RESOLVED_COLUMN, tag_id=2),
          Field(
              'is_correlated', SCALAR_BOOL, tag_id=3, ignorable=IGNORABLE)
      ])

  gen.AddNode(
      name='ResolvedConstant',
      tag_id=103,
      parent='ResolvedExpr',
      use_custom_debug_string=True,
      comment="""
      A reference to a named constant.
              """,
      fields=[
          Field(
              'constant',
              SCALAR_CONSTANT,
              tag_id=2,
              comment="""
        The matching Constant from the Catalog.
            """)
      ])

  gen.AddNode(
      name='ResolvedSystemVariable',
      tag_id=139,
      parent='ResolvedExpr',
      use_custom_debug_string=True,
      comment="""
      A reference to a system variable.
              """,
      fields=[
          Field(
              'name_path',
              SCALAR_STRING,
              vector=True,
              tag_id=2,
              comment="""
        Path to system variable.
            """)
      ])

  gen.AddNode(
      name='ResolvedFunctionCallBase',
      tag_id=7,
      parent='ResolvedExpr',
      emit_default_constructor=False,
      is_abstract=True,
      use_custom_debug_string=True,
      comment="""
      Common base class for scalar and aggregate function calls.
              """,
      fields=[
          Field(
              'function',
              SCALAR_FUNCTION,
              tag_id=2,
              comment="""The matching Function from the Catalog."""),
          Field(
              'signature',
              SCALAR_FUNCTION_SIGNATURE,
              tag_id=3,
              ignorable=IGNORABLE,
              comment="""
              The concrete FunctionSignature reflecting the matching Function
              signature and the function's resolved input <argument_list>.
              The function has the mode AGGREGATE iff it is an aggregate
              function, in which case this node must be either
              ResolvedAggregateFunctionCall or ResolvedAnalyticFunctionCall.
                      """),
          Field('argument_list', 'ResolvedExpr', tag_id=4, vector=True),
          Field(
              'error_mode',
              SCALAR_ERROR_MODE,
              tag_id=5,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              If error_mode=SAFE_ERROR_MODE, and if this function call returns a
              semantic error (based on input data, not transient server
              problems), return NULL instead of an error. This is used for
              functions called using SAFE, as in SAFE.FUNCTION(...).
                      """)
      ])

  gen.AddNode(
      name='ResolvedFunctionCall',
      tag_id=8,
      parent='ResolvedFunctionCallBase',
      emit_default_constructor=False,
      fields=[
          Field(
              'function_call_info',
              SCALAR_FUNCTION_CALL_INFO,
              tag_id=2,
              ignorable=IGNORABLE,
              comment="""
              This contains optional custom information about a particular
              function call.

              If some Function subclass requires computing additional
              information at resolving time, that extra information can be
              stored as a subclass of ResolvedFunctionCallInfo here.
              For example, TemplatedSQLFunction stores the resolved template
              body here as a TemplatedSQLFunctionCall.

              This field is ignorable because for most types of function calls,
              there is no extra information to consider besides the arguments
              and other fields from ResolvedFunctionCallBase.
                      """)
      ],
      comment="""
      A regular function call.  The signature will always have mode SCALAR.
      Most scalar expressions show up as FunctionCalls using builtin signatures.
              """)

  gen.AddNode(
      name='ResolvedNonScalarFunctionCallBase',
      tag_id=86,
      parent='ResolvedFunctionCallBase',
      emit_default_constructor=False,
      is_abstract=True,
      comment="""
      Common base class for scalar and aggregate function calls.
              """,
      fields=[
          Field(
              'distinct',
              SCALAR_BOOL,
              tag_id=2,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Apply DISTINCT to the stream of input values before calling
              function.
                      """),
          Field(
              'null_handling_modifier',
              SCALAR_NULL_HANDLING_MODIFIER_KIND,
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Apply IGNORE/RESPECT NULLS filtering to the stream of input
              values.
                      """)
      ])

  gen.AddNode(
      name='ResolvedAggregateFunctionCall',
      tag_id=9,
      parent='ResolvedNonScalarFunctionCallBase',
      emit_default_constructor=False,
      comment="""
      An aggregate function call.  The signature always has mode AGGREGATE.
      This node only ever shows up as the outer function call in a
      ResolvedAggregateScan::aggregate_list.
              """,
      fields=[
          Field(
              'having_modifier',
              'ResolvedAggregateHavingModifier',
              tag_id=5,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Apply HAVING MAX/MIN filtering to the stream of input values.
                      """),
          Field(
              'order_by_item_list',
              'ResolvedOrderByItem',
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Apply ordering to the stream of input values before calling
              function.
                      """,
              vector=True),
          Field(
              'limit',
              'ResolvedExpr',
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'function_call_info',
              SCALAR_FUNCTION_CALL_INFO,
              tag_id=6,
              ignorable=IGNORABLE,
              comment="""
              This contains optional custom information about a particular
              function call. Functions may introduce subclasses of this class to
              add custom information as needed on a per-function basis.

              This field is ignorable because for most types of function calls,
              there is no extra information to consider besides the arguments
              and other fields from ResolvedFunctionCallBase. However, for
              example, the TemplateSQLFunction in
              zetasql/public/templated_sql_function.h defines the
              TemplatedSQLFunctionCall subclass which includes the
              fully-resolved function body in context of the actual concrete
              types of the arguments provided to the function call.
                      """)
      ],
  )
  gen.AddNode(
      name='ResolvedAnalyticFunctionCall',
      tag_id=10,
      parent='ResolvedNonScalarFunctionCallBase',
      emit_default_constructor=False,
      comment="""
      An analytic function call. The mode of the function is either AGGREGATE
      or ANALYTIC. This node only ever shows up as a function call in a
      ResolvedAnalyticFunctionGroup::analytic_function_list. Its associated
      window is not under this node but as a sibling of its parent node.

      <window_frame> can be NULL.
              """,
      fields=[Field('window_frame', 'ResolvedWindowFrame', tag_id=2)])

  gen.AddNode(
      name='ResolvedExtendedCastInfo',
      tag_id=151,
      parent='ResolvedArgument',
      use_custom_debug_string=True,
      comment="""
      Contains information about the cast provided by an engine through a
      catalog's FindConversion function.
              """,
      fields=[
          Field(
              'function',
              SCALAR_FUNCTION,
              tag_id=2,
              comment="""
              Function that implements the cast.
                      """)
      ])

  gen.AddNode(
      name='ResolvedCast',
      tag_id=11,
      parent='ResolvedExpr',
      use_custom_debug_string=True,
      comment="""
      A cast expression, casting the result of an input expression to the
      target Type.

      Valid casts are defined in the CastHashMap (see cast.cc), which identifies
      valid from-Type, to-Type pairs.  Consumers can access it through
      GetZetaSQLCasts().
              """,
      fields=[
          Field('expr', 'ResolvedExpr', tag_id=2),
          Field(
              'return_null_on_error',
              SCALAR_BOOL,
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Whether to return NULL if the cast fails. This is set to true for
              SAFE_CAST.
                      """),
          Field(
              'extended_cast',
              'ResolvedExtendedCastInfo',
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT,
              is_constructor_arg=False,
              comment="""
              If at least one of conversion's types (source or destination) is
              extended (TYPE_EXTENDED), this field must must be provided to
              describe how this conversion should be executed.
                      """)
      ])

  gen.AddNode(
      name='ResolvedMakeStruct',
      tag_id=12,
      parent='ResolvedExpr',
      comment="""
      Construct a struct value.  <type> is always a StructType.
      <field_list> matches 1:1 with the fields in <type> position-wise.
      Each field's type will match the corresponding field in <type>.
              """,
      fields=[Field('field_list', 'ResolvedExpr', tag_id=2, vector=True)])

  gen.AddNode(
      name='ResolvedMakeProto',
      tag_id=13,
      parent='ResolvedExpr',
      comment="""
      Construct a proto value.  <type> is always a ProtoType.
      <field_list> is a vector of (FieldDescriptor, expr) pairs to write.
      <field_list> will contain all required fields, and no duplicate fields.
              """,
      fields=[
          Field(
              'field_list', 'ResolvedMakeProtoField', tag_id=2, vector=True)
      ])

  gen.AddNode(
      name='ResolvedMakeProtoField',
      tag_id=14,
      parent='ResolvedArgument',
      use_custom_debug_string=True,
      comment="""
      One field assignment in a ResolvedMakeProto expression.
      The type of expr will match with the zetasql type of the proto field.
      The type will be an array iff the field is repeated.

      For NULL values of <expr>, the proto field should be cleared.

      If any value of <expr> cannot be written into the field, this query
      should fail.
              """,
      fields=[
          Field('field_descriptor', SCALAR_FIELD_DESCRIPTOR, tag_id=2),
          Field(
              'format',
              SCALAR_FIELD_FORMAT,
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Provides the Format annotation that should be used when building
              this field.  The annotation specifies both the ZetaSQL type and
              the encoding format for this field.
                      """),
          Field('expr', 'ResolvedExpr', tag_id=4)
      ])

  gen.AddNode(
      name='ResolvedGetStructField',
      tag_id=15,
      parent='ResolvedExpr',
      comment="""
      Get the field in position <field_idx> (0-based) from <expr>, which has a
      STRUCT type.
              """,
      fields=[
          Field('expr', 'ResolvedExpr', tag_id=2),
          Field('field_idx', SCALAR_INT, tag_id=3)
      ])

  gen.AddNode(
      name='ResolvedGetProtoField',
      tag_id=16,
      parent='ResolvedExpr',
      fields=[
          Field('expr', 'ResolvedExpr', tag_id=2),
          Field(
              'field_descriptor',
              SCALAR_FIELD_DESCRIPTOR,
              tag_id=3,
              comment="""
              The proto2 FieldDescriptor to extract.  This provides the tag
              number and wire type.  Additional decoding may be necessary if any
              of the other modifiers below are set.  Consumers should use those
              ZetaSQL-computed modifiers rather than examining field
              annotations directly.

              The field is an extension field iff
              field_descriptor->is_extension() is true.  NOTE: The extended
              descriptor's full_name must match the <expr>'s type's full_name,
              but may not be the same Descriptor. Extension FieldDescriptors may
              come from a different DescriptorPool.

              The field is required if field_descriptor->is_required().  If the
              field is required and not present, an error should result.
                      """),
          Field(
              'default_value',
              SCALAR_VALUE,
              tag_id=4,
              ignorable=IGNORABLE,
              comment="""
              Default value to use when the proto field is not set. The default
              may be NULL (e.g. for proto2 fields with a use_defaults=false
              annotation).

              This will not be filled in (the Value will be uninitialized) if
              get_has_bit is true, or the field is required.

              If field_descriptor->is_required() and the field is not present,
              the engine should return an error.

              If the <expr> itself returns NULL, then extracting a field should
              also return NULL, unless <return_default_value_when_unset> is
              true. In that case, the default value is returned.

              TODO Make un-ignorable after clients migrate to start
              using it.
                      """),
          Field(
              'get_has_bit',
              SCALAR_BOOL,
              tag_id=5,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Indicates whether to return a bool indicating if a value was
              present, rather than return the value (or NULL). Never set for
              repeated fields. This field cannot be set if
              <return_default_value_when_unset> is true, and vice versa.
              Expression type will be BOOL.
                      """),
          Field(
              'format',
              SCALAR_FIELD_FORMAT,
              tag_id=6,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
               Provides the Format annotation that should be used when reading
               this field.  The annotation specifies both the ZetaSQL type and
               the encoding format for this field. This cannot be set when
               get_has_bit is true.
                      """),
          Field(
              'return_default_value_when_unset',
              SCALAR_BOOL,
              tag_id=7,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Indicates that the default value should be returned if <expr>
              (the parent message) is NULL.  Note that this does *not* affect
              the return value when the extracted field itself is unset, in
              which case the return value depends on the extracted field's
              annotations (e.g., use_field_defaults).

              This can only be set for non-message fields. If the field is a
              proto2 field, then it must be annotated with
              zetasql.use_defaults=true. This cannot be set when <get_has_bit>
              is true or the field is required.
                      """),
      ])

  gen.AddNode(
      name='ResolvedFlatten',
      tag_id=149,
      parent='ResolvedExpr',
      comment="""
      Constructs an initial input ARRAY<T> from expr. For each get_field_list
      expr, we evaluate the expression once with each array input element and
      use the output as a new array of inputs for the next get_field_list expr.
      If the result of a single expr is an array, we add each element from that
      array as input to the next step instead of adding the array itself.

      The array elements are evaluated and kept in order. For example, if only
      expr is an array, the result will be equivalent to that array having the
      get_field_list evaluated on each array element retaining order.
              """,
      fields=[
          Field('expr', 'ResolvedExpr', tag_id=2),
          Field(
              'get_field_list',
              'ResolvedExpr',
              vector=True,
              tag_id=3,
              comment="""
              List of 'get' fields to evaluate in order (0 or more struct get
              fields followed by 0 or more proto get fields) starting from expr.
              Each get is evaluated N times where N is the number of array
              elements from the previous get (or expr for the first expression)
              generated.
                      """),
      ])

  gen.AddNode(
      name='ResolvedFlattenedArg',
      tag_id=150,
      parent='ResolvedExpr',
      comment="""
      Argument for a child of ResolvedFlatten. This is a placeholder to indicate
      that it will be invoked once for each array element from ResolvedFlatten's
      expr or previous get_field_list entry.
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedReplaceFieldItem',
      tag_id=128,
      parent='ResolvedArgument',
      comment="""
      An argument to the REPLACE_FIELDS() function which specifies a field path
      and a value that this field will be set to. The field path to be modified
      can be constructed through the <struct_index_path> and <proto_field_path>
      fields. These vectors correspond to field paths in a STRUCT and PROTO,
      respectively. At least one of these vectors must be non-empty.

      If only <struct_index_path> is non-empty, then the field path only
      references top-level and nested struct fields.

      If only <proto_field_path> is non-empty, then the field path only
      references top-level and nested message fields.

      If both <struct_index_path> and <proto_field_path> are non-empty, then the
      field path should be expanded starting with <struct_index_path>. The last
      field in <struct_index_path> will be the proto from which the first field
      in <proto_field_path> is extracted.

      <expr> and the field to be modified must be the same type.
      """,
      fields=[
          Field(
              'expr',
              'ResolvedExpr',
              tag_id=2,
              comment="""
              The value that the final field in <proto_field_path> will be set
              to.

              If <expr> is NULL, the field will be unset. If <proto_field_path>
              is a required field, the engine must return an error if it is set
              to NULL.
              """),
          Field(
              'struct_index_path',
              SCALAR_INT,
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT,
              vector=True,
              to_string_method='ToStringCommaSeparated',
              java_to_string_method='toStringCommaSeparatedForInt',
              comment="""
              A vector of integers that denotes the path to a struct field that
              will be modified. The integer values in this vector correspond to
              field positions (0-based) in a STRUCT. If <proto_field_path>
              is also non-empty, then the field corresponding to the last index
              in this vector should be of proto type.
                      """),
          Field(
              'proto_field_path',
              SCALAR_FIELD_DESCRIPTOR,
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT,
              vector=True,
              to_string_method='ToStringVectorFieldDescriptor',
              java_to_string_method='toStringPeriodSeparatedForFieldDescriptors',
              comment="""
              A vector of FieldDescriptors that denotes the path to a proto
              field that will be modified. If <struct_index_path> is also
              non-empty, then the first element in this vector should be a
              subfield of the proto corresponding to the last element in
              <struct_index_path>.
                      """),
      ])

  gen.AddNode(
      name='ResolvedReplaceField',
      tag_id=129,
      parent='ResolvedExpr',
      comment="""
              Represents a call to the REPLACE_FIELDS() function. This function
              can be used to copy a proto or struct, modify a few fields and
              output the resulting proto or struct. The SQL syntax for this
              function is REPLACE_FIELDS(<expr>, <replace_field_item_list>).

              See (broken link) for more detail.
      """,
      fields=[
          Field(
              'expr',
              'ResolvedExpr',
              tag_id=2,
              comment=""" The proto/struct to modify. """),
          Field(
              'replace_field_item_list',
              'ResolvedReplaceFieldItem',
              tag_id=3,
              vector=True,
              comment="""
              The list of field paths to be modified along with their new
              values.

              Engines must check at evaluation time that the modifications in
              <replace_field_item_list> obey the following rules
              regarding updating protos in ZetaSQL:
              - Modifying a subfield of a NULL-valued proto-valued field is an
                error.
              - Clearing a required field or subfield is an error.
                      """),
      ])

  gen.AddNode(
      name='ResolvedSubqueryExpr',
      tag_id=17,
      parent='ResolvedExpr',
      comment="""
      A subquery in an expression (not a FROM clause).  The subquery runs
      in the context of a single input row and produces a single output value.

      Correlated subqueries can be thought of like functions, with a parameter
      list.  The <parameter_list> gives the set of ResolvedColumns from outside
      the subquery that are used inside.

      Inside the subquery, the only allowed references to values outside the
      subquery are to the named ColumnRefs listed in <parameter_list>.
      Any reference to one of these parameters will be represented as a
      ResolvedColumnRef with <is_correlated> set to true.

      These parameters are only visible through one level of expression
      subquery.  An expression subquery inside an expression has to list
      parameters again if parameters from the outer query are passed down
      further.  (This does not apply for table subqueries inside an expression
      subquery.  Table subqueries are never indicated in the resolved AST, so
      Scan nodes inside an expression query may have come from a nested table
      subquery, and they can still reference the expression subquery's
      parameters.)

      An empty <parameter_list> means that the subquery is uncorrelated.  It is
      permissable to run an uncorrelated subquery only once and reuse the \
result.
      TODO Do we want to specify semantics more firmly here?

      The semantics vary based on SubqueryType:
        SCALAR
          Usage: ( <subquery> )
          If the subquery produces zero rows, the output value is NULL.
          If the subquery produces exactly one row, that row is the output \
value.
          If the subquery produces more than one row, raise a runtime error.

        ARRAY
          Usage: ARRAY( <subquery> )
          The subquery produces an array value with zero or more rows, with
          one array element per subquery row produced.

        EXISTS
          Usage: EXISTS( <subquery> )
          The output type is always bool.  The result is true if the subquery
          produces at least one row, and false otherwise.

        IN
          Usage: <in_expr> [NOT] IN ( <subquery> )
          The output type is always bool.  The result is true when <in_expr> is
          equal to at least one row, and false otherwise.  The <subquery> row
          contains only one column, and the types of <in_expr> and the
          subquery column must exactly match a built-in signature for the
          '$equals' comparison function (they must be the same type or one
          must be INT64 and the other UINT64).  NOT will be expressed as a $not
          FunctionCall wrapping this SubqueryExpr.

      The subquery for a SCALAR or ARRAY or IN subquery must have exactly one
      output column.
      The output type for a SCALAR or ARRAY subquery is that column's type or
      an array of that column's type.  (The subquery scan may include a Project
      with a MakeStruct or MakeProto expression to construct a single value
      from multiple columns.)
              """,
      fields=[
          Field('subquery_type', SCALAR_SUBQUERY_TYPE, tag_id=2),
          Field(
              'parameter_list',
              'ResolvedColumnRef',
              tag_id=3,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'in_expr',
              'ResolvedExpr',
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Field is only populated for subquery of type IN.
                      """),
          Field('subquery', 'ResolvedScan', tag_id=5),
          Field(
              'hint_list',
              'ResolvedOption',
              tag_id=6,
              ignorable=IGNORABLE,
              is_constructor_arg=False,
              vector=True,
              comment="""
              Note: Hints currently happen only for EXISTS or IN subquery but
              not for ARRAY or SCALAR subquery.
                      """)
      ])

  # Scans (anything that shows up in a from clause and produces rows).

  gen.AddNode(
      name='ResolvedScan',
      tag_id=18,
      parent='ResolvedNode',
      is_abstract=True,
      comment="""
      Common superclass for all Scans, which are nodes that produce rows
      (e.g. scans, joins, table subqueries).  A query's FROM clause is
      represented as a single Scan that composes all input sources into
      a single row stream.

      Each Scan has a <column_list> that says what columns are produced.
      The Scan logically produces a stream of output rows, where each row
      has exactly these columns.

      Each Scan may have an attached <hint_list>, storing each hint as
      a ResolvedOption.

      If <is_ordered> is true, this Scan produces an ordered output, either
      by generating order itself (OrderByScan) or by preserving the order
      of its single input scan (LimitOffsetScan, ProjectScan, or WithScan).
              """,
      fields=[
          Field(
              'column_list',
              SCALAR_RESOLVED_COLUMN,
              tag_id=2,
              ignorable=IGNORABLE,
              vector=True),
          Field(
              'hint_list',
              'ResolvedOption',
              tag_id=3,
              ignorable=IGNORABLE,
              is_constructor_arg=False,
              vector=True),
          Field(
              'is_ordered',
              SCALAR_BOOL,
              tag_id=4,
              ignorable=IGNORABLE,
              is_constructor_arg=False)
      ],
      extra_defs="""  bool IsScan() const final { return true; }""")

  gen.AddNode(
      name='ResolvedModel',
      tag_id=109,
      parent='ResolvedArgument',
      comment="""
      Represents a machine learning model as a TVF argument.
      <model> is the machine learning model object known to the resolver
      (usually through the catalog).
              """,
      fields=[
          Field('model', SCALAR_MODEL, tag_id=2),
      ])

  gen.AddNode(
      name='ResolvedConnection',
      tag_id=141,
      parent='ResolvedArgument',
      comment="""
      Represents a connection object as a TVF argument.
      <connection> is the connection object encapsulated metadata to connect to
      an external data source.
              """,
      fields=[
          Field('connection', SCALAR_CONNECTION, tag_id=2),
      ])

  gen.AddNode(
      name='ResolvedDescriptor',
      tag_id=144,
      parent='ResolvedArgument',
      comment="""
      Represents a descriptor object as a TVF argument.
      <descriptor_column_list> contains resolved columns from the related input
      table argument if FunctionArgumentTypeOptions.get_resolve_descriptor_names_table_offset()
      returns a valid argument offset.
      <descriptor_column_name_list> contains strings which represent columns
      names.
      """,
      fields=[
          Field(
              'descriptor_column_list',
              SCALAR_RESOLVED_COLUMN,
              tag_id=2,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'descriptor_column_name_list',
              SCALAR_STRING,
              tag_id=3,
              vector=True,
              ignorable=IGNORABLE_DEFAULT,
              to_string_method='ToStringCommaSeparated'),
      ])

  gen.AddNode(
      name='ResolvedSingleRowScan',
      tag_id=19,
      parent='ResolvedScan',
      fields=[],
      comment="""
      Scan that produces a single row with no columns.  Used for queries without
      a FROM clause, where all output comes from the select list.
              """)

  gen.AddNode(
      name='ResolvedTableScan',
      tag_id=20,
      parent='ResolvedScan',
      comment="""
      Scan a Table.
      The <column_list>[i] should be matched to a Table column by
      <table>.GetColumn(<column_index_list>[i]).

      If AnalyzerOptions::prune_unused_columns is true, the <column_list> and
      <column_index_list> will include only columns that were referenced
      in the user query. (SELECT * counts as referencing all columns.)
      This column_list can then be used for column-level ACL checking on tables.

      for_system_time_expr when non NULL resolves to TIMESTAMP used in
      FOR SYSTEM_TIME AS OF clause. The expression is expected to be constant
      and no columns are visible to it.

      If provided, <alias> refers to an explicit alias which was used to
      reference a Table in the user query. If the Table was given an implicitly
      generated alias, then defaults to "".

      TODO: Enforce <column_index_list> in the constructor arg list. For
      historical reasons, some clients match <column_list> to Table columns by
      name. All code building this should always set_column_index_list() to
      provide the indexes of all columns in <table> right after the construction
      of a ResolvedTableScan.
              """,
      fields=[
          Field('table', SCALAR_TABLE, tag_id=2),
          Field(
              'for_system_time_expr',
              'ResolvedExpr',
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'column_index_list',
              SCALAR_INT,
              tag_id=4,
              ignorable=IGNORABLE,
              vector=True,
              is_constructor_arg=False,
              to_string_method='ToStringCommaSeparated',
              java_to_string_method='toStringCommaSeparatedForInt'),
          Field('alias', SCALAR_STRING, tag_id=5, ignorable=IGNORABLE),
      ])

  gen.AddNode(
      name='ResolvedJoinScan',
      tag_id=21,
      parent='ResolvedScan',
      comment="""
      A Scan that joins two input scans.
      The <column_list> will contain columns selected from the union
      of the input scan's <column_lists>.
      When the join is a LEFT/RIGHT/FULL join, ResolvedColumns that came from
      the non-joined side get NULL values.
              """,
      fields=[
          Field(
              'join_type',
              SCALAR_JOIN_TYPE,
              tag_id=2,
              ignorable=IGNORABLE_DEFAULT),
          Field('left_scan', 'ResolvedScan', tag_id=3),
          Field('right_scan', 'ResolvedScan', tag_id=4),
          Field(
              'join_expr',
              'ResolvedExpr',
              tag_id=5,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedArrayScan',
      tag_id=22,
      parent='ResolvedScan',
      comment="""
      Scan an array value, produced from some expression.

      If input_scan is NULL, this scans the given array value and produces
      one row per array element.  This can occur when using UNNEST(expression).

      If <input_scan> is non-NULL, for each row in the stream produced by
      input_scan, this evaluates the expression <array_expr> (which must return
      an array type) and then produces a stream with one row per array element.

      If <join_expr> is non-NULL, then this condition is evaluated as an ON
      clause for the array join.  The named column produced in <array_expr>
      may be used inside <join_expr>.

      If the array is empty (after evaluating <join_expr>), then
      1. If <is_outer> is false, the scan produces zero rows.
      2. If <is_outer> is true, the scan produces one row with a NULL value for
         the <element_column>.

      <element_column> is the new column produced by this scan that stores the
      array element value for each row.

      If present, <array_offset_column> defines the column produced by this
      scan that stores the array offset (0-based) for the corresponding
      <element_column>.

      This node's column_list can have columns from input_scan, <element_column>
      and <array_offset_column>.
              """,
      fields=[
          Field('input_scan', 'ResolvedScan', tag_id=2),
          Field('array_expr', 'ResolvedExpr', tag_id=3),
          Field('element_column', SCALAR_RESOLVED_COLUMN, tag_id=4),
          Field(
              'array_offset_column',
              'ResolvedColumnHolder',
              tag_id=5,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'join_expr',
              'ResolvedExpr',
              tag_id=6,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'is_outer',
              SCALAR_BOOL,
              tag_id=7,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedColumnHolder',
      tag_id=23,
      parent='ResolvedArgument',
      comment="""
      This wrapper is used for an optional ResolvedColumn inside another node.
              """,
      fields=[Field('column', SCALAR_RESOLVED_COLUMN, tag_id=2)])

  gen.AddNode(
      name='ResolvedFilterScan',
      tag_id=24,
      parent='ResolvedScan',
      comment="""
      Scan rows from input_scan, and emit all rows where filter_expr
      evaluates to true.  filter_expr is always of type bool.
      This node's column_list will be a subset of input_scan's column_list.
              """,
      fields=[
          Field('input_scan', 'ResolvedScan', tag_id=2),
          Field('filter_expr', 'ResolvedExpr', tag_id=3)
      ])

  gen.AddNode(
      name='ResolvedGroupingSet',
      tag_id=93,
      parent='ResolvedArgument',
      comment="""
      List of group by columns that form a grouping set.

      Columns must come from group_by_list in ResolvedAggregateScan.
      group_by_column_list will not contain any duplicates. There may be more
      than one ResolvedGroupingSet in the ResolvedAggregateScan with the same
      columns, however.
              """,
      fields=[
          Field(
              'group_by_column_list',
              'ResolvedColumnRef',
              tag_id=2,
              vector=True)
      ])

  gen.AddNode(
      name='ResolvedAggregateScanBase',
      tag_id=111,
      parent='ResolvedScan',
      is_abstract=True,
      comment="""
      Base class for aggregation scans. Apply aggregation to rows produced from
      input_scan, and output aggregated rows.

      Group by keys in <group_by_list>.  If <group_by_list> is empty,
      aggregate all input rows into one output row.

      Compute all aggregations in <aggregate_list>.  All expressions in
      <aggregate_list> have a ResolvedAggregateFunctionCall with mode
      Function::AGGREGATE as their outermost node.

      The output <column_list> contains only columns produced from
      <group_by_list> and <aggregate_list>.  No other columns are visible after
      aggregation.
              """,
      fields=[
          Field('input_scan', 'ResolvedScan', tag_id=2),
          Field(
              'group_by_list',
              'ResolvedComputedColumn',
              tag_id=3,
              vector=True),
          Field(
              'aggregate_list',
              'ResolvedComputedColumn',
              tag_id=4,
              vector=True)
      ])

  gen.AddNode(
      name='ResolvedAggregateScan',
      tag_id=25,
      parent='ResolvedAggregateScanBase',
      comment="""
      Apply aggregation to rows produced from input_scan, and output aggregated
      rows.

      For each item in <grouping_set_list>, output additional rows computing the
      same <aggregate_list> over the input rows using a particular grouping set.
      The aggregation input values, including <input_scan>, computed columns in
      <group_by_list>, and aggregate function arguments in <aggregate_list>,
      should be computed just once and then reused as aggregation input for each
      grouping set. (This ensures that ROLLUP rows have correct totals, even
      with non-stable functions in the input.) For each grouping set, the
      <group_by_list> elements not included in the <group_by_column_list> are
      replaced with NULL.

      <rollup_column_list> is the original list of columns from
      GROUP BY ROLLUP(...), if there was a ROLLUP clause, and is used only for
      rebuilding equivalent SQL for the resolved AST. Engines should refer to
      <grouping_set_list> rather than <rollup_column_list>.
              """,
      fields=[
          Field(
              'grouping_set_list',
              'ResolvedGroupingSet',
              tag_id=5,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'rollup_column_list',
              'ResolvedColumnRef',
              tag_id=6,
              vector=True,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedSetOperationItem',
      tag_id=94,
      parent='ResolvedArgument',
      comment="""
      This is one input item in a ResolvedSetOperation.
      The <output_column_list> matches 1:1 with the ResolvedSetOperation's
      <column_list> and specifies how columns from <scan> map to output columns.
      Each column from <scan> can map to zero or more output columns.
              """,
      fields=[
          Field('scan', 'ResolvedScan', tag_id=2),
          Field(
              'output_column_list',
              SCALAR_RESOLVED_COLUMN,
              tag_id=3,
              vector=True)
      ],
  )

  gen.AddNode(
      name='ResolvedSetOperationScan',
      tag_id=26,
      parent='ResolvedScan',
      comment="""
      Apply a set operation (specified by <op_type>) on two or more input scans.

      <scan_list> will have at least two elements.

      <column_list> is a set of new ResolvedColumns created by this scan.
      Each input ResolvedSetOperationItem has an <output_column_list> which
      matches 1:1 with <column_list> and specifies how the input <scan>'s
      columns map into the final <column_list>.

      - Results of {UNION, INTERSECT, EXCEPT} ALL can include duplicate rows.
        More precisely, with two input scans, if a given row R appears exactly
        m times in first input and n times in second input (m >= 0, n >= 0):
        For UNION ALL, R will appear exactly m + n times in the result.
        For INTERSECT ALL, R will appear exactly min(m, n) in the result.
        For EXCEPT ALL, R will appear exactly max(m - n, 0) in the result.

      - Results of {UNION, INTERSECT, EXCEPT} DISTINCT cannot contain any
        duplicate rows. For UNION and INTERSECT, the DISTINCT is computed
        after the result above is computed.  For EXCEPT DISTINCT, row R will
        appear once in the output if m > 0 and n = 0.

      - For n (>2) input scans, the above operations generalize so the output is
        the same as if the inputs were combined incrementally from left to \
right.
              """,
      fields=[
          Field('op_type', SCALAR_SET_OPERATION_TYPE, tag_id=2),
          Field(
              'input_item_list',
              'ResolvedSetOperationItem',
              tag_id=4,
              vector=True)
      ])

  gen.AddNode(
      name='ResolvedOrderByScan',
      tag_id=27,
      parent='ResolvedScan',
      comment="""
      Apply ordering to rows produced from input_scan, and output ordered
      rows.

      The <order_by_item_list> must not be empty.  Each element identifies
      a sort column and indicates direction (ascending or descending).

      Order Preservation:
        A ResolvedScan produces an ordered output if it has <is_ordered>=true.
        If <is_ordered>=false, the scan may discard order.  This can happen
        even for a ResolvedOrderByScan, if it is the top-level scan in a
        subquery (which discards order).

      The following Scan nodes may have <is_ordered>=true, producing or
      propagating an ordering:
        * ResolvedOrderByScan
        * ResolvedLimitOffsetScan
        * ResolvedProjectScan
        * ResolvedWithScan
      Other Scan nodes will always discard ordering.
              """,
      fields=[
          Field('input_scan', 'ResolvedScan', tag_id=2),
          Field(
              'order_by_item_list',
              'ResolvedOrderByItem',
              tag_id=3,
              vector=True)
      ])

  gen.AddNode(
      name='ResolvedLimitOffsetScan',
      tag_id=28,
      parent='ResolvedScan',
      comment="""
      Apply a LIMIT and optional OFFSET to the rows from input_scan. Emit all
      rows after OFFSET rows have been scanned and up to LIMIT total rows
      emitted. The offset is the number of rows to skip.
      E.g., OFFSET 1 means to skip one row, so the first row emitted will be the
      second ROW, provided the LIMIT is greater than zero.

      The arguments to LIMIT <int64> OFFSET <int64> must be non-negative
      integer literals or (possibly casted) query parameters.  Query
      parameter values must be checked at run-time by ZetaSQL compliant
      backend systems.

      OFFSET is optional and the absence of OFFSET implies OFFSET 0.
              """,
      fields=[
          Field(
              'input_scan', 'ResolvedScan', tag_id=2, propagate_order=True),
          Field('limit', 'ResolvedExpr', tag_id=3),
          Field('offset', 'ResolvedExpr', tag_id=4)
      ])

  gen.AddNode(
      name='ResolvedWithRefScan',
      tag_id=29,
      parent='ResolvedScan',
      comment="""
      Scan the subquery defined in a WITH statement.
      See ResolvedWithScan for more detail.
      The column_list produced here will match 1:1 with the column_list produced
      by the referenced subquery and will given a new unique name to each
      column produced for this scan.
              """,
      fields=[Field('with_query_name', SCALAR_STRING, tag_id=2)])

  gen.AddNode(
      name='ResolvedAnalyticScan',
      tag_id=30,
      parent='ResolvedScan',
      comment="""
      Apply analytic functions to rows produced from input_scan.

      The set of analytic functions are partitioned into a list of analytic
      function groups <function_group_list> by the window PARTITION BY and the
      window ORDER BY.

      The output <column_list> contains all columns from <input_scan>,
      one column per analytic function. It may also conain partitioning/ordering
      expression columns if they reference to select columns.
              """,
      fields=[
          Field('input_scan', 'ResolvedScan', tag_id=2),
          Field(
              'function_group_list',
              'ResolvedAnalyticFunctionGroup',
              tag_id=3,
              vector=True)
      ])

  gen.AddNode(
      name='ResolvedSampleScan',
      tag_id=31,
      parent='ResolvedScan',
      comment="""
      Samples rows from <input_scan>.
      Specs: (broken link)
      Specs for WITH WEIGHT and PARTITION BY: (broken link)

      <method> is the identifier for the sampling algorithm and will always be
      in lowercase.
      For example BERNOULLI, RESERVOIR, SYSTEM. Engines can also support their
      own implementation-specific set of sampling algorithms.

      <size> and <unit> specifies the sample size.
      If <unit> is "ROWS", <size> must be an <int64> and non-negative.
      If <unit> is "PERCENT", <size> must either be a <double> or an <int64> and
      in the range [0, 100].
      <size> can only be a literal value or a (possibly casted) parameter.

      <repeatable_argument> is present if we had a REPEATABLE(<argument>) in the
      TABLESAMPLE clause and can only be a literal value or a (possibly
      casted) parameter.

      If present, <weight_column> defines the column produced by this scan that
      stores the scaling weight for the corresponding sampled row.

      <partition_by_list> can be empty. If <partition_by_list> is not empty,
      <unit> must be ROWS and <method> must be RESERVOIR.
      """,
      fields=[
          Field('input_scan', 'ResolvedScan', tag_id=2),
          Field('method', SCALAR_STRING, tag_id=3),
          Field('size', 'ResolvedExpr', tag_id=4),
          Field('unit', SCALAR_SAMPLE_UNIT, tag_id=5),
          Field('repeatable_argument', 'ResolvedExpr', tag_id=6),
          Field(
              'weight_column',
              'ResolvedColumnHolder',
              tag_id=7,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'partition_by_list',
              'ResolvedExpr',
              tag_id=8,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
      ])

  # Other nodes

  gen.AddNode(
      name='ResolvedComputedColumn',
      tag_id=32,
      parent='ResolvedArgument',
      use_custom_debug_string=True,
      comment="""
      This is used when an expression is computed and given a name (a new
      ResolvedColumn) that can be referenced elsewhere.  The new ResolvedColumn
      can appear in a column_list or in ResolvedColumnRefs in other expressions,
      when appropriate.  This node is not an expression itself - it is a
      container that holds an expression.
              """,
      fields=[
          Field(
              'column',
              SCALAR_RESOLVED_COLUMN,
              tag_id=2,
              ignorable=IGNORABLE),
          Field('expr', 'ResolvedExpr', tag_id=3)
      ])

  gen.AddNode(
      name='ResolvedOrderByItem',
      tag_id=33,
      parent='ResolvedArgument',
      comment="""
      This represents one column of an ORDER BY clause, with the requested
      ordering direction.

      <collation_name> indicates the COLLATE specific rules of ordering.
      If non-NULL, must be a string literal or a string parameter.
      See (broken link).

      <null_order> indicates the ordering of NULL values relative to non-NULL
      values. NULLS_FIRST indicates that NULLS sort prior to non-NULL values,
      and NULLS_LAST indicates that NULLS sort after non-NULL values.
              """,
      fields=[
          Field('column_ref', 'ResolvedColumnRef', tag_id=2),
          Field(
              'collation_name',
              'ResolvedExpr',
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'is_descending',
              SCALAR_BOOL,
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'null_order',
              SCALAR_NULL_ORDER_MODE,
              tag_id=5,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedColumnAnnotations',
      tag_id=104,
      parent='ResolvedArgument',
      comment="""
      This is used in CREATE TABLE statements to provide column annotations
      such as NOT NULL and OPTIONS().

      This class is recursive. It mirrors the structure of the column type
      except that child_list might be truncated.

      For ARRAY:
        If the element or its subfield has annotations, then child_list.size()
        is 1, and child_list(0) stores the element annotations.
        Otherwise child_list is empty.
      For STRUCT:
        If the i-th field has annotations then child_list(i) stores the
        field annotations.
        Otherwise either child_list.size() <= i or child_list(i) is trivial.
        If none of the fields and none of their subfields has annotations, then
        child_list is empty.
      For other types, child_list is empty.
              """,
      fields=[
          Field(
              'not_null',
              SCALAR_BOOL,
              tag_id=2,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'option_list',
              'ResolvedOption',
              tag_id=3,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'child_list',
              'ResolvedColumnAnnotations',
              tag_id=4,
              vector=True,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedGeneratedColumnInfo',
      tag_id=105,
      parent='ResolvedArgument',
      comment="""

      <expression> indicates the expression that defines the column. The type of
      the expression will always match the type of the column.
        - The <expression> can contain ResolvedColumnRefs corresponding to
        ResolvedColumnDefinition.<column> for any of the
        ResolvedColumnDefinitions in the enclosing statement.
        - The expression can never include a subquery.

      <is_stored> indicates whether the value of the expression should be
      pre-emptively computed to save work at read time. When is_stored is true,
      <expression> cannot contain a volatile function (e.g. RAND).

      <is_on_write> indicates that the value of this column should be calculated
      at write time. As opposed to <is_stored> the <expression> can contain
      volatile functions (e.g. RAND).

      Only one of <is_stored> or <is_on_write> can be true.

      See (broken link).""",
      fields=[
          Field('expression', 'ResolvedExpr', tag_id=2),
          Field('is_stored', SCALAR_BOOL, tag_id=3),
          Field(
              'is_on_write',
              SCALAR_BOOL,
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedColumnDefinition',
      tag_id=91,
      parent='ResolvedArgument',
      comment="""
      This is used in CREATE TABLE statements to provide an explicit column
      definition.

      if <is_hidden> is TRUE, then the column won't show up in SELECT * queries.

      if <generated_column_info> is non-NULL, then this table column is a
      generated column.

      <column> defines an ID for the column, which may appear in expressions in
      the PARTITION BY, CLUSTER BY clause or <generated_column_info> if either
      is present.
              """,
      fields=[
          Field('name', SCALAR_STRING, tag_id=2),
          Field('type', SCALAR_TYPE, tag_id=3),
          Field(
              'annotations',
              'ResolvedColumnAnnotations',
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'is_hidden',
              SCALAR_BOOL,
              tag_id=5,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'column',
              SCALAR_RESOLVED_COLUMN,
              tag_id=6,
              ignorable=IGNORABLE),
          Field(
              'generated_column_info',
              'ResolvedGeneratedColumnInfo',
              tag_id=7,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedPrimaryKey',
      tag_id=92,
      parent='ResolvedArgument',
      comment="""
      This represents the PRIMARY KEY constraint on a table.
      <column_offset_list> provides the offsets of the column definitions that
                           comprise the primary key. This is empty when a
                           0-element primary key is defined.

      <unenforced> specifies whether the constraint is unenforced.
      """,
      fields=[
          Field(
              'column_offset_list',
              SCALAR_INT,
              tag_id=2,
              vector=True,
              to_string_method='ToStringCommaSeparated',
              java_to_string_method='toStringCommaSeparatedForInt'),
          Field(
              'option_list',
              'ResolvedOption',
              tag_id=3,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'unenforced', SCALAR_BOOL, tag_id=4, ignorable=IGNORABLE_DEFAULT),
      ])

  gen.AddNode(
      name='ResolvedForeignKey',
      tag_id=110,
      parent='ResolvedArgument',
      comment="""
      This represents the FOREIGN KEY constraint on a table. It is of the form:

        CONSTRAINT <constraint_name>
        FOREIGN KEY <referencing_column_offset_list>
        REFERENCES <referenced_table> <referenced_column_offset_list>
        <match_mode>
        <update_action>
        <delete_action>
        <enforced>
        <option_list>

      <constraint_name> uniquely identifies the constraint.

      <referencing_column_offset_list> provides the offsets of the column
      definitions for the table defining the foreign key.

      <referenced_table> identifies the table this constraint references.

      <referenced_column_offset_list> provides the offsets of the column
      definitions for the table referenced by the foreign key.

      <match_mode> specifies how referencing keys with null values are handled.

      <update_action> specifies what action to take, if any, when a referenced
      value is updated.

      <delete_action> specifies what action to take, if any, when a row with a
      referenced values is deleted.

      <enforced> specifies whether or not the constraint is enforced.

      <option_list> for foreign key table constraints. Empty for foreign key
      column attributes (see instead ResolvedColumnAnnotations).
      """,
      fields=[
          Field(
              'constraint_name',
              SCALAR_STRING,
              tag_id=2),
          Field(
              'referencing_column_offset_list',
              SCALAR_INT,
              tag_id=3,
              vector=True,
              to_string_method='ToStringCommaSeparated',
              java_to_string_method='toStringCommaSeparatedForInt'),
          Field(
              'referenced_table',
              SCALAR_TABLE,
              tag_id=4),
          Field(
              'referenced_column_offset_list',
              SCALAR_INT,
              tag_id=5,
              vector=True,
              to_string_method='ToStringCommaSeparated',
              java_to_string_method='toStringCommaSeparatedForInt'),
          Field(
              'match_mode',
              SCALAR_FOREIGN_KEY_MATCH_MODE,
              tag_id=6),
          Field(
              'update_action',
              SCALAR_FOREIGN_KEY_ACTION_OPERATION,
              tag_id=7),
          Field(
              'delete_action',
              SCALAR_FOREIGN_KEY_ACTION_OPERATION,
              tag_id=8),
          Field(
              'enforced',
              SCALAR_BOOL,
              tag_id=9),
          Field(
              'option_list',
              'ResolvedOption',
              tag_id=10,
              vector=True)
      ])

  gen.AddNode(
      name='ResolvedCheckConstraint',
      tag_id=113,
      parent='ResolvedArgument',
      comment="""
      This represents the CHECK constraint on a table. It is of the form:

        CONSTRAINT <constraint_name>
        CHECK <expression>
        <enforced>
        <option_list>

      <constraint_name> uniquely identifies the constraint.

      <expression> defines a boolean expression to be evaluated when the row is
      updated. If the result is FALSE, update to the row is not allowed.

      <enforced> specifies whether or not the constraint is enforced.

      <option_list> list of options for check constraint.

      See (broken link).
      """,
      fields=[
          Field('constraint_name', SCALAR_STRING, tag_id=2),
          Field(
              'expression',
              'ResolvedExpr',
              tag_id=3,
          ),
          Field('enforced', SCALAR_BOOL, tag_id=4),
          Field('option_list', 'ResolvedOption', tag_id=5, vector=True)
      ])

  gen.AddNode(
      name='ResolvedOutputColumn',
      tag_id=34,
      parent='ResolvedArgument',
      use_custom_debug_string=True,
      comment="""
      This is used in ResolvedQueryStmt to provide a user-visible name
      for each output column.
              """,
      fields=[
          Field('name', SCALAR_STRING, tag_id=2, ignorable=IGNORABLE),
          Field('column', SCALAR_RESOLVED_COLUMN, tag_id=3)
      ])

  gen.AddNode(
      name='ResolvedProjectScan',
      tag_id=35,
      parent='ResolvedScan',
      comment="""
      A Project node computes new expression values, and possibly drops
      columns from the input Scan's column_list.

      Each entry in <expr_list> is a new column computed from an expression.

      The column_list can include any columns from input_scan, plus these
      newly computed columns.

      NOTE: This scan will propagate the is_ordered property of <input_scan>
      by default.  To make this scan unordered, call set_is_ordered(false).
              """,
      fields=[
          Field(
              'expr_list', 'ResolvedComputedColumn', tag_id=2, vector=True),
          Field(
              'input_scan', 'ResolvedScan', tag_id=3, propagate_order=True)
      ])

  gen.AddNode(
      name='ResolvedTVFScan',
      tag_id=81,
      parent='ResolvedScan',
      emit_default_constructor=False,
      comment="""
      This scan represents a call to a table-valued function (TVF). Each TVF
      returns an entire output relation instead of a single scalar value. The
      enclosing query may refer to the TVF as if it were a table subquery. The
      TVF may accept scalar arguments and/or other input relations.

      Scalar arguments work the same way as arguments for non-table-valued
      functions: in the resolved AST, their types are equal to the required
      argument types specified in the function signature.

      The function signature may also include relation arguments, and any such
      relation argument may specify a required schema. If such a required schema
      is present, then in the resolved AST, the ResolvedScan for each relational
      ResolvedTVFArgument is guaranteed to have the same number of columns as
      the required schema, and the provided columns match position-wise with the
      required columns. Each provided column has the same name and type as the
      corresponding required column.

      <column_list> is a set of new ResolvedColumns created by this scan.
      These output columns match positionally with the columns in the output
      schema of <signature>.

      <tvf> The TableValuedFunction entry that the catalog returned for this TVF
            scan. Contains non-concrete function signatures which may include
            arguments with templated types.
      <signature> The concrete table function signature for this TVF call,
                  including the types of all scalar arguments and the
                  number and types of columns of all table-valued
                  arguments. An engine may also subclass this object to
                  provide extra custom information and return an instance
                  of the subclass from the TableValuedFunction::Resolve
                  method.
      <argument_list> The vector of resolved arguments for this TVF call.
      <alias> The AS alias for the scan, or empty if none.
              """,
      fields=[
          Field('tvf', TABLE_VALUED_FUNCTION, tag_id=2),
          Field('signature', TVF_SIGNATURE, tag_id=3),
          Field(
              'argument_list', 'ResolvedTVFArgument', tag_id=5,
              vector=True),
          Field('alias', SCALAR_STRING, tag_id=6, ignorable=IGNORABLE)
      ])

  gen.AddNode(
      name='ResolvedTVFArgument',
      tag_id=82,
      parent='ResolvedArgument',
      comment="""
      This represents an argument to a table-valued function (TVF). The argument
      can be semantically scalar, relational, represent a model, a connection or
      a descriptor. Only one of the five fields will be set.

      <expr> The expression representing a scalar TVF argument.
      <scan> The scan representing a relational TVF argument.
      <model> The model representing an ML model TVF argument.
      <connection> The connection representing a connection object TVF argument.
      <descriptor_arg> The descriptor representing a descriptor object TVF
      argument.

      <argument_column_list> maps columns from <scan> into specific columns
      of the TVF argument's input schema, matching those columns positionally.
      i.e. <scan>'s column_list may have fewer columns or out-of-order columns,
      and this vector maps those columns into specific TVF input columns.
              """,
      fields=[
          Field(
              'expr',
              'ResolvedExpr',
              ignorable=IGNORABLE_DEFAULT,
              tag_id=2),
          Field(
              'scan',
              'ResolvedScan',
              ignorable=IGNORABLE_DEFAULT,
              tag_id=3),
          Field(
              'model',
              'ResolvedModel',
              ignorable=IGNORABLE_DEFAULT,
              tag_id=5),
          Field(
              'connection',
              'ResolvedConnection',
              ignorable=IGNORABLE_DEFAULT,
              tag_id=6),
          Field(
              # Can't name it 'descriptor' because of conflict in protos.
              'descriptor_arg',
              'ResolvedDescriptor',
              ignorable=IGNORABLE_DEFAULT,
              tag_id=7),
          Field(
              'argument_column_list',
              SCALAR_RESOLVED_COLUMN,
              tag_id=4,
              vector=True,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedStatement',
      tag_id=36,
      parent='ResolvedNode',
      is_abstract=True,
      comment="""
      The superclass of all ZetaSQL statements.
              """,
      fields=[
          Field(
              'hint_list',
              'ResolvedOption',
              tag_id=2,
              ignorable=IGNORABLE,
              is_constructor_arg=False,
              vector=True)
      ],
      extra_defs="""  bool IsStatement() const final { return true; }""")

  gen.AddNode(
      name='ResolvedExplainStmt',
      tag_id=37,
      parent='ResolvedStatement',
      comment="""
      An Explain statement. This is always the root of a statement hierarchy.
      Its child may be any statement type except another ResolvedExplainStmt.

      It is implementation dependent what action a back end system takes for an
      ExplainStatement.
              """,
      fields=[Field('statement', 'ResolvedStatement', tag_id=2)])

  gen.AddNode(
      name='ResolvedQueryStmt',
      tag_id=38,
      parent='ResolvedStatement',
      comment="""
      A SQL query statement.  This is the outermost query statement that runs
      and produces rows of output, like a SELECT.  (The contained query may be
      a Scan corresponding to a non-Select top-level operation like UNION ALL
      or WITH.)

      <output_column_list> gives the user-visible column names that should be
      returned in the API or query tools.  There may be duplicate names, and
      multiple output columns may reference the same column from <query>.
              """,
      fields=[
          Field(
              'output_column_list',
              'ResolvedOutputColumn',
              tag_id=2,
              vector=True),
          Field(
              'is_value_table',
              SCALAR_BOOL,
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              If true, the result of this query is a value table. Rather than
              producing rows with named columns, it produces rows with a single
              unnamed value type.  output_column_list will have exactly one
              column, with an empty name. See (broken link).
                      """),
          Field('query', 'ResolvedScan', tag_id=4)
      ])

  gen.AddNode(
      name='ResolvedCreateDatabaseStmt',
      tag_id=95,
      parent='ResolvedStatement',
      comment="""
      This statement:
        CREATE DATABASE <name> [OPTIONS (...)]
      <name_path> is a vector giving the identifier path in the database name.
      <option_list> specifies the options of the database.
              """,
      fields=[
          Field('name_path', SCALAR_STRING, tag_id=2, vector=True),
          Field(
              'option_list',
              'ResolvedOption',
              tag_id=3,
              vector=True,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedCreateStatement',
      tag_id=39,
      parent='ResolvedStatement',
      is_abstract=True,
      comment="""
      Common superclass for CREATE statements with standard modifiers like
              CREATE [OR REPLACE] [TEMP|TEMPORARY|PUBLIC|PRIVATE] <object type>
              [IF NOT EXISTS] <name> ...

      <name_path> is a vector giving the identifier path in the table name.
      <create_scope> is the relevant scope, i.e., DEFAULT, TEMP, PUBLIC,
                     or PRIVATE.  PUBLIC/PRIVATE are only valid in module
                     resolution context, see (broken link)
                     for details.
      <create_mode> indicates if this was CREATE, CREATE OR REPLACE, or
                    CREATE IF NOT EXISTS.
              """,
      fields=[
          Field(
              'name_path',
              SCALAR_STRING,
              tag_id=2,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'create_scope',
              SCALAR_CREATE_SCOPE,
              tag_id=5,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'create_mode',
              SCALAR_CREATE_MODE,
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedIndexItem',
      tag_id=96,
      parent='ResolvedArgument',
      comment="""
      Represents one of indexed items in CREATE INDEX statement, with the
      ordering direction specified.
              """,
      fields=[
          Field('column_ref', 'ResolvedColumnRef', tag_id=2),
          Field('descending', SCALAR_BOOL, tag_id=3)
      ])

  gen.AddNode(
      name='ResolvedUnnestItem',
      tag_id=126,
      parent='ResolvedArgument',
      comment="""
      This is used in CREATE INDEX STMT to represent the unnest operation
      performed on the base table. The produced element columns or array offset
      columns (optional) can appear in other ResolvedUnnestItem or index keys.

      <array_expr> is the expression of the array field, e.g., t.array_field.
      <element_column> is the new column produced by this unnest item that
                       stores the array element value for each row.
      <array_offset_column> is optional. If present, it defines the column
                            produced by this unnest item that stores the array
                            offset (0-based) for the corresponding
                            <element_column>.
              """,
      fields=[
          Field('array_expr', 'ResolvedExpr', tag_id=2),
          Field('element_column', SCALAR_RESOLVED_COLUMN, tag_id=3),
          Field(
              'array_offset_column',
              'ResolvedColumnHolder',
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT),
      ])

  gen.AddNode(
      name='ResolvedCreateIndexStmt',
      tag_id=97,
      parent='ResolvedCreateStatement',
      comment="""
      This statement:
      CREATE [OR REPLACE] [UNIQUE] INDEX [IF NOT EXISTS] <index_name_path>
      ON <table_name_path>
      [STORING (Expression, ...)]
      [UNNEST(path_expression) [[AS] alias] [WITH OFFSET [[AS] alias]], ...]
      (path_expression [ASC|DESC], ...) [OPTIONS (name=value, ...)];

      <table_name_path> is the name of table being indexed.
      <table_scan> is a TableScan on the table being indexed.
      <is_unique> specifies if the index has unique entries.
      <index_item_list> has the columns being indexed, specified as references
                        to 'computed_columns_list' entries or the columns of
                        'table_scan'.
      <storing_expression_list> has the expressions in the storing clause.
      <option_list> has engine-specific directives for how and where to
                    materialize this index.
      <computed_columns_list> has computed columns derived from the columns of
                              'table_scan' or 'unnest_expressions_list'. For
                              example, the extracted field (e.g., x.y.z).
      <unnest_expressions_list> has unnest expressions derived from
                                'table_scan' or previous unnest expressions in
                                the list. So the list order is significant.
              """,
      fields=[
          Field(
              'table_name_path', SCALAR_STRING, tag_id=2, vector=True),
          Field(
              'table_scan',
              'ResolvedTableScan',
              tag_id=3,
              ignorable=IGNORABLE),
          Field('is_unique', SCALAR_BOOL, tag_id=4),
          Field(
              'index_item_list', 'ResolvedIndexItem', tag_id=5,
              vector=True),
          Field(
              'storing_expression_list',
              'ResolvedExpr',
              tag_id=9,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'option_list',
              'ResolvedOption',
              tag_id=6,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'computed_columns_list',
              'ResolvedComputedColumn',
              tag_id=7,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'unnest_expressions_list',
              'ResolvedUnnestItem',
              tag_id=8,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
      ])

  gen.AddNode(
      name='ResolvedCreateTableStmtBase',
      tag_id=106,
      parent='ResolvedCreateStatement',
      is_abstract=True,
      comment="""
      This statement:
        CREATE [TEMP] TABLE <name> (column type, ...)
        [PARTITION BY expr, ...] [CLUSTER BY expr, ...]
        [OPTIONS (...)]

      <option_list> has engine-specific directives for how and where to
                    materialize this table.
      <column_definition_list> has the names and types of the columns in the
                               created table. If <is_value_table> is true, it
                               must contain exactly one column, with a generated
                               name such as "$struct".
      <pseudo_column_list> is a list of some pseudo-columns expected to be
                           present on the created table (provided by
                           AnalyzerOptions::SetDdlPseudoColumns*).  These can be
                           referenced in expressions in <partition_by_list> and
                           <cluster_by_list>.
      <primary_key> specifies the PRIMARY KEY constraint on the table, it is
                    nullptr when no PRIMARY KEY is specified.
      <foreign_key_list> specifies the FOREIGN KEY constraints on the table.
      <check_constraint_list> specifies the CHECK constraints on the table.
      <partition_by_list> specifies the partitioning expressions for the table.
      <cluster_by_list> specifies the clustering expressions for the table.
      <is_value_table> specifies whether the table is a value table.
                       See (broken link).
              """,
      fields=[
          Field(
              'option_list',
              'ResolvedOption',
              tag_id=2,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'column_definition_list',
              'ResolvedColumnDefinition',
              tag_id=3,
              vector=True,
              # This is ignorable for backwards compatibility.
              ignorable=IGNORABLE),
          Field(
              'pseudo_column_list',
              SCALAR_RESOLVED_COLUMN,
              tag_id=7,
              vector=True,
              ignorable=IGNORABLE),
          Field(
              'primary_key',
              'ResolvedPrimaryKey',
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'foreign_key_list',
              'ResolvedForeignKey',
              tag_id=9,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'check_constraint_list',
              'ResolvedCheckConstraint',
              tag_id=10,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'partition_by_list',
              'ResolvedExpr',
              tag_id=5,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'cluster_by_list',
              'ResolvedExpr',
              tag_id=6,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'is_value_table',
              SCALAR_BOOL,
              tag_id=8,
              ignorable=IGNORABLE_DEFAULT),
      ])

  gen.AddNode(
      name='ResolvedCreateTableStmt',
      tag_id=90,
      parent='ResolvedCreateTableStmtBase',
      comment="""
      This statement:
        CREATE [TEMP] TABLE <name> (column schema, ...)
        [PARTITION BY expr, ...] [CLUSTER BY expr, ...]
        [OPTIONS (...)]
              """,
      fields=[]
      )

  gen.AddNode(
      name='ResolvedCreateTableAsSelectStmt',
      tag_id=40,
      parent='ResolvedCreateTableStmtBase',
      comment="""
      This statement:
        CREATE [TEMP] TABLE <name> [(column schema, ...)]
        [PARTITION BY expr, ...] [CLUSTER BY expr, ...] [OPTIONS (...)]
        AS SELECT ...

      The <output_column_list> matches 1:1 with the <column_definition_list> in
      ResolvedCreateTableStmtBase, and maps ResolvedColumns produced by <query>
      into specific columns of the created table.  The output column names and
      types must match the column definition names and types.  If the table is
      a value table, <output_column_list> must have exactly one column, with a
      generated name such as "$struct".

      <output_column_list> does not contain all table schema information that
      <column_definition_list> does. For example, NOT NULL annotations, column
      OPTIONS, and primary keys are only available in <column_definition_list>.
      Consumers are encouraged to read from <column_definition_list> rather
      than than <output_column_list> to determine the table schema, if possible.

      <query> is the query to run.
              """,
      fields=[
          Field(
              'output_column_list',
              'ResolvedOutputColumn',
              tag_id=2,
              vector=True),
          Field('query', 'ResolvedScan', tag_id=3)
      ])

  gen.AddNode(
      name='ResolvedCreateModelStmt',
      tag_id=107,
      parent='ResolvedCreateStatement',
      comment="""
      This statement:
        CREATE [TEMP] MODEL <name> [TRANSFORM(...)] [OPTIONS (...)] AS SELECT ..

      <option_list> has engine-specific directives for how to train this model.
      <output_column_list> matches 1:1 with the <query>'s column_list and the
                           <column_definition_list>, and identifies the names
                           and types of the columns output from the select
                           statement.
      <query> is the select statement.
      <transform_input_column_list> introduces new ResolvedColumns that have the
        same names and types of the columns in the <output_column_list>. The
        transform expressions resolve against these ResolvedColumns. It's only
        set when <transform_list> is non-empty.
      <transform_list> is the list of ResolvedComputedColumn in TRANSFORM
        clause.
      <transform_output_column_list> matches 1:1 with <transform_list> output.
        It records the names of the output columns from TRANSFORM clause.
      <transform_analytic_function_group_list> is the list of
        AnalyticFunctionGroup for analytic functions inside TRANSFORM clause.
        It records the input expression of the analytic functions. It can
        see all the columns from <transform_input_column_list>. The only valid
        group is for the full, unbounded window generated from empty OVER()
        clause.
        For example, CREATE MODEL statement
        "create model Z
          transform (max(c) over() as d)
          options ()
          as select 1 c, 2 b;"
        will generate transform_analytic_function_group_list:
        +-transform_analytic_function_group_list=
          +-AnalyticFunctionGroup
            +-analytic_function_list=
              +-d#5 :=
                +-AnalyticFunctionCall(ZetaSQL:max(INT64) -> INT64)
                  +-ColumnRef(type=INT64, column=Z.c#3)
                  +-window_frame=
                    +-WindowFrame(frame_unit=ROWS)
                      +-start_expr=
                      | +-WindowFrameExpr(boundary_type=UNBOUNDED PRECEDING)
                      +-end_expr=
                        +-WindowFrameExpr(boundary_type=UNBOUNDED FOLLOWING)
              """,
      fields=[
          Field(
              'option_list',
              'ResolvedOption',
              tag_id=2,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'output_column_list',
              'ResolvedOutputColumn',
              tag_id=3,
              vector=True),
          Field('query', 'ResolvedScan', tag_id=4),
          Field(
              'transform_input_column_list',
              'ResolvedColumnDefinition',
              tag_id=8,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'transform_list',
              'ResolvedComputedColumn',
              tag_id=5,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'transform_output_column_list',
              'ResolvedOutputColumn',
              tag_id=6,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'transform_analytic_function_group_list',
              'ResolvedAnalyticFunctionGroup',
              tag_id=7,
              vector=True,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedCreateViewBase',
      tag_id=108,
      parent='ResolvedCreateStatement',
      is_abstract=True,
      comment="""
      Common superclass for CREATE view/materialized view:
        CREATE [TEMP|MATERIALIZED] [RECURSIVE] VIEW <name> [OPTIONS (...)]
          AS SELECT ...

      <option_list> has engine-specific directives for options attached to
                    this view.
      <output_column_list> has the names and types of the columns in the
                           created view, and maps from <query>'s column_list
                           to these output columns.
      <query> is the query to run.
      <sql> is the view query text.
      <sql_security> is the declared security mode for the function. Values
         include 'INVOKER', 'DEFINER'.
      <recursive> specifies whether or not the view is created with the
        RECURSIVE keyword.

      Note that <query> and <sql> are both marked as IGNORABLE because
      an engine could look at either one (but might not look at both).
      An engine must look at one (and cannot ignore both) to be
      semantically valid, but there is currently no way to enforce that.

      The view must produce named columns with unique names.
              """,
      fields=[
          Field(
              'option_list',
              'ResolvedOption',
              tag_id=2,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'output_column_list',
              'ResolvedOutputColumn',
              tag_id=3,
              vector=True),
          Field('query', 'ResolvedScan', tag_id=5, ignorable=IGNORABLE),
          Field('sql', SCALAR_STRING, tag_id=6, ignorable=IGNORABLE),
          Field(
              'sql_security',
              SCALAR_SQL_SECURITY,
              tag_id=7,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'is_value_table',
              SCALAR_BOOL,
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              If true, this view produces a value table. Rather than producing
              rows with named columns, it produces rows with a single unnamed
              value type.  output_column_list will have exactly one column, with
              an empty name. See (broken link).
                      """),
          Field(
              'recursive',
              SCALAR_BOOL,
              tag_id=8,
              ignorable=IGNORABLE,
              comment="""
                True if the view uses the RECURSIVE keyword. <query>
                can be a ResolvedRecursiveScan only if this is true.""")
      ])

  gen.AddNode(
      name='ResolvedCreateViewStmt',
      tag_id=41,
      parent='ResolvedCreateViewBase',
      comment="""
      This statement:
        CREATE [TEMP] VIEW <name> [OPTIONS (...)] AS SELECT ...
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedCreateExternalTableStmt',
      tag_id=42,
      parent='ResolvedCreateTableStmtBase',
      comment="""
      This statement:
        CREATE [TEMP] EXTERNAL TABLE <name> [(column type, ...)]
        [PARTITION BY expr, ...] [CLUSTER BY expr, ...] OPTIONS (...)
            """,
      fields=[])

  gen.AddNode(
      name='ResolvedExportDataStmt',
      tag_id=43,
      parent='ResolvedStatement',
      comment="""
      This statement:
        EXPORT DATA [WITH CONNECTION] <connection> (<option_list>) AS SELECT ...
      which is used to run a query and export its result somewhere
      without giving the result a table name.
      <connection> connection reference for accessing destination source.
      <option_list> has engine-specific directives for how and where to
                    materialize the query result.
      <output_column_list> has the names and types of the columns produced by
                           the query, and maps from <query>'s column_list
                           to these output columns.  The engine may ignore
                           the column names depending on the output format.
      <query> is the query to run.

      The query must produce named columns with unique names.
              """,
      fields=[
          Field(
              'connection',
              'ResolvedConnection',
              tag_id=6,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'option_list',
              'ResolvedOption',
              tag_id=2,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'output_column_list',
              'ResolvedOutputColumn',
              tag_id=3,
              vector=True),
          Field(
              'is_value_table',
              SCALAR_BOOL,
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              If true, the result of this query is a value table. Rather than
              producing rows with named columns, it produces rows with a single
              unnamed value type.  output_column_list will have exactly one
              column, with an empty name. See (broken link).
                      """),
          Field('query', 'ResolvedScan', tag_id=5)
      ])

  gen.AddNode(
      name='ResolvedDefineTableStmt',
      tag_id=44,
      parent='ResolvedStatement',
      comment="""
      This statement: DEFINE TABLE name (...);

      <name_path> is a vector giving the identifier path in the table name.
      <option_list> has engine-specific options of how the table is defined.

      DEFINE TABLE normally has the same effect as CREATE TEMP EXTERNAL TABLE.
              """,
      fields=[
          Field('name_path', SCALAR_STRING, tag_id=2, vector=True),
          Field('option_list', 'ResolvedOption', tag_id=3, vector=True)
      ])

  gen.AddNode(
      name='ResolvedDescribeStmt',
      tag_id=45,
      parent='ResolvedStatement',
      comment="""
      This statement: DESCRIBE [<object_type>] <name> [FROM <from_name_path>];

      <object_type> is an optional string identifier,
                    e.g., "INDEX", "FUNCTION", "TYPE", etc.
      <name_path> is a vector giving the identifier path for the object to be
                  described.
      <from_name_path> is an optional vector giving the identifier path of a
                         containing object, e.g. a table.
              """,
      fields=[
          Field('object_type', SCALAR_STRING, tag_id=2),
          Field('name_path', SCALAR_STRING, tag_id=3, vector=True),
          Field(
              'from_name_path',
              SCALAR_STRING,
              tag_id=4,
              vector=True,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedShowStmt',
      tag_id=46,
      parent='ResolvedStatement',
      comment="""
      This statement: SHOW <identifier> [FROM <name_path>] [LIKE <like_expr>];

      <identifier> is a string that determines the type of objects to be shown,
                   e.g., TABLES, COLUMNS, INDEXES, STATUS,
      <name_path> is an optional path to an object from which <identifier>
                  objects will be shown, e.g., if <identifier> = INDEXES and
                  <name> = table_name, the indexes of "table_name" will be
                  shown,
      <like_expr> is an optional ResolvedLiteral of type string that if present
                  restricts the objects shown to have a name like this string.
              """,
      fields=[
          Field('identifier', SCALAR_STRING, tag_id=2),
          Field(
              'name_path',
              SCALAR_STRING,
              tag_id=3,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'like_expr',
              'ResolvedLiteral',
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedBeginStmt',
      tag_id=47,
      parent='ResolvedStatement',
      comment="""
      This statement: BEGIN [TRANSACTION] [ <transaction_mode> [, ...] ]

      Where transaction_mode is one of:
           READ ONLY
           READ WRITE
           <isolation_level>

      <isolation_level> is a string vector storing the identifiers after
            ISOLATION LEVEL. The strings inside vector could be one of the
            SQL standard isolation levels:

                        READ UNCOMMITTED
                        READ COMMITTED
                        READ REPEATABLE
                        SERIALIZABLE

            or could be arbitrary strings. ZetaSQL does not validate that
            the string is valid.
              """,
      fields=[
          Field(
              'read_write_mode',
              SCALAR_READ_WRITE_TRANSACTION_MODE,
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'isolation_level_list',
              SCALAR_STRING,
              tag_id=2,
              vector=True,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedSetTransactionStmt',
      tag_id=120,
      parent='ResolvedStatement',
      comment="""
      This statement: SET TRANSACTION <transaction_mode> [, ...]

      Where transaction_mode is one of:
           READ ONLY
           READ WRITE
           <isolation_level>

      <isolation_level> is a string vector storing the identifiers after
            ISOLATION LEVEL. The strings inside vector could be one of the
            SQL standard isolation levels:

                        READ UNCOMMITTED
                        READ COMMITTED
                        READ REPEATABLE
                        SERIALIZABLE

            or could be arbitrary strings. ZetaSQL does not validate that
            the string is valid.
              """,
      fields=[
          Field(
              'read_write_mode',
              SCALAR_READ_WRITE_TRANSACTION_MODE,
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'isolation_level_list',
              SCALAR_STRING,
              tag_id=2,
              vector=True,
              ignorable=IGNORABLE_DEFAULT)
      ],
      extra_defs="""
  typedef ResolvedBeginStmt::ReadWriteMode ReadWriteMode;""")

  gen.AddNode(
      name='ResolvedCommitStmt',
      tag_id=48,
      parent='ResolvedStatement',
      comment="""
      This statement: COMMIT [TRANSACTION];
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedRollbackStmt',
      tag_id=49,
      parent='ResolvedStatement',
      comment="""
      This statement: ROLLBACK [TRANSACTION];
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedStartBatchStmt',
      tag_id=122,
      parent='ResolvedStatement',
      comment="""
      This statement: START BATCH [<batch_type>];

      <batch_type> is an optional string identifier that identifies the type of
                   the batch. (e.g. "DML" or "DDL)
              """,
      fields=[
          Field(
              'batch_type',
              SCALAR_STRING,
              tag_id=2,
              ignorable=IGNORABLE_DEFAULT),
      ])

  gen.AddNode(
      name='ResolvedRunBatchStmt',
      tag_id=123,
      parent='ResolvedStatement',
      comment="""
      This statement: RUN BATCH;
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedAbortBatchStmt',
      tag_id=124,
      parent='ResolvedStatement',
      comment="""
      This statement: ABORT BATCH;
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedDropStmt',
      tag_id=50,
      parent='ResolvedStatement',
      comment="""
      This statement: DROP <object_type> [IF EXISTS] <name_path>;

      <object_type> is an string identifier,
                    e.g., "TABLE", "VIEW", "INDEX", "FUNCTION", "TYPE", etc.
      <name_path> is a vector giving the identifier path for the object to be
                  dropped.
      <is_if_exists> silently ignore the "name_path does not exist" error.
              """,
      fields=[
          Field('object_type', SCALAR_STRING, tag_id=2),
          Field('is_if_exists', SCALAR_BOOL, tag_id=3),
          Field('name_path', SCALAR_STRING, tag_id=4, vector=True)
      ])

  gen.AddNode(
      name='ResolvedDropMaterializedViewStmt',
      tag_id=121,
      parent='ResolvedStatement',
      comment="""
      This statement: DROP MATERIALIZED VIEW [IF EXISTS] <name_path>;

      <name_path> is a vector giving the identifier path for the object to be
                  dropped.
      <is_if_exists> silently ignore the "name_path does not exist" error.
              """,
      fields=[
          Field('is_if_exists', SCALAR_BOOL, tag_id=3),
          Field('name_path', SCALAR_STRING, tag_id=4, vector=True)
      ])

  gen.AddNode(
      name='ResolvedRecursiveRefScan',
      tag_id=147,
      parent='ResolvedScan',
      comment="""
      Scan the previous iteration of the recursive alias currently being
      defined, from inside the recursive subquery which defines it. Such nodes
      can exist only in the recursive term of a ResolvedRecursiveScan node.
      The column_list produced here will match 1:1 with the column_list produced
      by the referenced subquery and will be given a new unique name to each
      column produced for this scan.
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedRecursiveScan',
      tag_id=148,
      parent='ResolvedScan',
      comment="""
      A recursive query inside a WITH RECURSIVE or RECURSIVE VIEW. A
      ResolvedRecursiveScan may appear in a resolved tree only as a top-level
      input scan of a ResolvedWithEntry or ResolvedCreateViewBase.

      Recursive queries must satisfy the form:
          <non-recursive-query> UNION [ALL|DISTINCT] <recursive-query>

      where self-references to table being defined are allowed only in the
      <recursive-query> section.

      <column_list> is a set of new ResolvedColumns created by this scan.
      Each input ResolvedSetOperationItem has an <output_column_list> which
      matches 1:1 with <column_list> and specifies how the input <scan>'s
      columns map into the final <column_list>.

      At runtime, a recursive scan is evaluated using an iterative process:

      Step 1: Evaluate the non-recursive term. If UNION DISTINCT
        is specified, discard duplicates.

      Step 2:
        Repeat until step 2 produces an empty result:
          Evaluate the recursive term, binding the recursive table to the
          new rows produced by previous step. If UNION DISTINCT is specified,
          discard duplicate rows, as well as any rows which match any
          previously-produced result.

      Step 3:
        The final content of the recursive table is the UNION ALL of all results
        produced (step 1, plus all iterations of step 2).

      ResolvedRecursiveScan only supports a recursive WITH entry which
        directly references itself; ZetaSQL does not support mutual recursion
        between multiple with-clause elements.

      See (broken link) for details.
      """,
      fields=[
          Field('op_type', SCALAR_RECURSIVE_SET_OPERATION_TYPE, tag_id=2),
          Field('non_recursive_term', 'ResolvedSetOperationItem', tag_id=3),
          Field('recursive_term', 'ResolvedSetOperationItem', tag_id=4),
      ])

  gen.AddNode(
      name='ResolvedWithScan',
      tag_id=51,
      parent='ResolvedScan',
      comment="""
      This represents a SQL WITH query (or subquery) like
        WITH [RECURSIVE] <with_query_name1> AS (<with_subquery1>),
             <with_query_name2> AS (<with_subquery2>)
        <query>;

      WITH entries are sorted in dependency order so that an entry can only
      reference entries earlier in <with_entry_list>, plus itself if the
      RECURSIVE keyword is used. If the RECURSIVE keyword is not used, this will
      be the same order as in the original query, since an entry which
      references itself or any entry later in the list is not allowed.

      If a WITH subquery is referenced multiple times, the full query should
      behave as if the subquery runs only once and its result is reused.

      There will be one ResolvedWithEntry here for each subquery in the SQL
      WITH statement, in the same order as in the query.

      Inside the resolved <query>, or any <with_entry_list> occurring after
      its definition, a <with_query_name> used as a table scan will be
      represented using a ResolvedWithRefScan.

      The <with_query_name> aliases are always unique within a query, and should
      be used to connect the ResolvedWithRefScan to the original query
      definition.  The subqueries are not inlined and duplicated into the tree.

      In ZetaSQL 1.0, WITH is allowed only on the outermost query and not in
      subqueries, so the ResolvedWithScan node can only occur as the outermost
      scan in a statement (e.g. a QueryStmt or CreateTableAsSelectStmt).

      In ZetaSQL 1.1 (language option FEATURE_V_1_1_WITH_ON_SUBQUERY), WITH
      is allowed on subqueries.  Then, ResolvedWithScan can occur anywhere in
      the tree.  The alias introduced by a ResolvedWithEntry is visible only
      in subsequent ResolvedWithEntry queries and in <query>.  The aliases used
      must be globally unique in the resolved AST however, so consumers do not
      need to implement any scoping for these names.  Because the aliases are
      unique, it is legal to collect all ResolvedWithEntries in the tree and
      treat them as if they were a single WITH clause at the outermost level.

      In ZetaSQL 1.3 (language option FEATURE_V_1_3_WITH_RECURSIVE), WITH
      RECURSIVE is supported, which allows any <with_subquery> to reference
      any <with_query_name>, regardless of order, including WITH entries which
      reference themself. Circular dependency chains of WITH entries are allowed
      only for direct self-references, and only when the corresponding
      <with_subquery> takes the form "<non-recursive-term> UNION [ALL|DISTINCT]
      <recursive-term>", with all references to the current <with_query_name>
      confined to the recursive term.

      The subqueries inside ResolvedWithEntries cannot be correlated.

      If a WITH subquery is defined but never referenced, it will still be
      resolved and still show up here.  Query engines may choose not to run it.

              """,
      fields=[
          Field(
              'with_entry_list',
              'ResolvedWithEntry',
              tag_id=2,
              vector=True),
          Field(
              'query', 'ResolvedScan', tag_id=3, propagate_order=True),
          Field(
              'recursive',
              SCALAR_BOOL,
              tag_id=4,
              ignorable=IGNORABLE,
              comment="""
                True if the WITH clause uses the recursive keyword.""")
      ])

  gen.AddNode(
      name='ResolvedWithEntry',
      tag_id=52,
      parent='ResolvedArgument',
      comment="""
      This represents one aliased subquery introduced in a WITH clause.

      The <with_query_name>s must be globally unique in the full resolved AST.
      The <with_subquery> cannot be correlated and cannot reference any
      columns from outside.  It may reference other WITH subqueries.

      See ResolvedWithScan for full details.
              """,
      fields=[
          Field('with_query_name', SCALAR_STRING, tag_id=2),
          Field('with_subquery', 'ResolvedScan', tag_id=3)
      ])

  gen.AddNode(
      name='ResolvedOption',
      tag_id=53,
      parent='ResolvedArgument',
      use_custom_debug_string=True,
      comment="""
      This represents one SQL hint key/value pair.
      The SQL syntax @{ key1=value1, key2=value2, some_db.key3=value3 }
      will expand to three ResolvedOptions.  Keyword hints (e.g. LOOKUP JOIN)
      are interpreted as shorthand, and will be expanded to a ResolvedOption
      attached to the appropriate node before any explicit long-form hints.

      ResolvedOptions are attached to the ResolvedScan corresponding to the
      operator that the SQL hint was associated with.
      See (broken link) for more detail.
      Hint semantics are implementation defined.

      Each hint is resolved as a [<qualifier>.]<name>:=<value> pair.
        <qualifier> will be empty if no qualifier was present.
        <name> is always non-empty.
        <value> can be a ResolvedLiteral or a ResolvedParameter,
                a cast of a ResolvedParameter (for typed hints only),
                or a general expression (on constant inputs).

      If AllowedHintsAndOptions was set in AnalyzerOptions, and this hint or
      option was included there and had an expected type, the type of <value>
      will match that expected type.  Unknown hints (not listed in
      AllowedHintsAndOptions) are not stripped and will still show up here.

      If non-empty, <qualifier> should be interpreted as a target system name,
      and a database system should ignore any hints targeted to different
      systems.

      The SQL syntax allows using an identifier as a hint value.
      Such values are stored here as ResolvedLiterals with string type.
              """,
      fields=[
          Field('qualifier', SCALAR_STRING, tag_id=2),
          Field('name', SCALAR_STRING, tag_id=3, ignorable=IGNORABLE),
          Field('value', 'ResolvedExpr', tag_id=4, ignorable=IGNORABLE)
      ])

  gen.AddNode(
      name='ResolvedWindowPartitioning',
      tag_id=54,
      parent='ResolvedArgument',
      comment="""
      Window partitioning specification for an analytic function call.

      PARTITION BY keys in <partition_by_list>.
              """,
      fields=[
          Field(
              'partition_by_list',
              'ResolvedColumnRef',
              tag_id=2,
              vector=True),
          Field(
              'hint_list',
              'ResolvedOption',
              tag_id=3,
              ignorable=IGNORABLE,
              is_constructor_arg=False,
              vector=True)
      ])

  gen.AddNode(
      name='ResolvedWindowOrdering',
      tag_id=55,
      parent='ResolvedArgument',
      comment="""
     Window ordering specification for an analytic function call.

     ORDER BY items in <order_by_list>. There should be exactly one ORDER
     BY item if this is a window ORDER BY for a RANGE-based window.
              """,
      fields=[
          Field(
              'order_by_item_list',
              'ResolvedOrderByItem',
              tag_id=2,
              vector=True),
          Field(
              'hint_list',
              'ResolvedOption',
              tag_id=3,
              ignorable=IGNORABLE,
              is_constructor_arg=False,
              vector=True)
      ])

  gen.AddNode(
      name='ResolvedWindowFrame',
      tag_id=56,
      parent='ResolvedArgument',
      comment="""
      Window framing specification for an analytic function call.

      ROW-based window frames compute the frame based on physical offsets
      from the current row.
      RANGE-based window frames compute the frame based on a logical
      range of rows around the current row based on the current row's
      ORDER BY key value.

      <start_expr> and <end_expr> cannot be NULL. If the window frame
      is one-sided in the input query, the resolver will generate an
      implicit ending boundary.
              """,
      fields=[
          Field('frame_unit', SCALAR_FRAME_UNIT, tag_id=2),
          Field('start_expr', 'ResolvedWindowFrameExpr', tag_id=3),
          Field('end_expr', 'ResolvedWindowFrameExpr', tag_id=4)
      ],
      use_custom_debug_string=True,
      extra_defs="""
  std::string GetFrameUnitString() const;
  static std::string FrameUnitToString(FrameUnit frame_unit);""")

  gen.AddNode(
      name='ResolvedAnalyticFunctionGroup',
      tag_id=57,
      parent='ResolvedArgument',
      comment="""
      This represents a group of analytic function calls that shares PARTITION
      BY and ORDER BY.

      <partition_by> can be NULL. <order_by> may be NULL depending on the
      functions in <analytic_function_list> and the window frame unit. See
      (broken link) for more details.

      All expressions in <analytic_function_list> have a
      ResolvedAggregateFunctionCall with a function in mode
      Function::AGGREGATE or Function::ANALYTIC.
              """,
      fields=[
          Field('partition_by', 'ResolvedWindowPartitioning', tag_id=2),
          Field('order_by', 'ResolvedWindowOrdering', tag_id=3),
          Field(
              'analytic_function_list',
              'ResolvedComputedColumn',
              tag_id=4,
              vector=True)
      ])

  gen.AddNode(
      name='ResolvedWindowFrameExpr',
      tag_id=58,
      parent='ResolvedArgument',
      comment="""
      Window frame boundary expression that determines the first/last row of
      the moving window for each tuple.

      <expression> cannot be NULL if the type is OFFSET_PRECEDING
      or OFFSET_FOLLOWING. It must be a constant expression. If this is a
      boundary for a ROW-based window, it must be integer type. Otherwise,
      it must be numeric type and must match exactly the type of the window
      ordering expression.  See (broken link) for more
      details.
              """,
      fields=[
          Field('boundary_type', SCALAR_BOUNDARY_TYPE, tag_id=2),
          Field('expression', 'ResolvedExpr', tag_id=3)
      ],
      use_custom_debug_string=True,
      extra_defs="""
  std::string GetBoundaryTypeString() const;
  static std::string BoundaryTypeToString(BoundaryType boundary_type);""")

  gen.AddNode(
      name='ResolvedDMLValue',
      tag_id=59,
      parent='ResolvedArgument',
      comment="""
      This represents a value inside an INSERT or UPDATE statement.

      The <value> is either an expression or a DMLDefault.

      For proto fields, NULL values mean the field should be cleared.
              """,
      fields=[Field('value', 'ResolvedExpr', tag_id=2)])

  gen.AddNode(
      name='ResolvedDMLDefault',
      tag_id=60,
      parent='ResolvedExpr',
      comment="""
      This is used to represent the value DEFAULT that shows up (in place of a
      value expression) in INSERT and UPDATE statements.
      For columns, engines should substitute the engine-defined default value
      for that column, or give an error.
      For proto fields, this always means to clear the field.
      This will never show up inside expressions other than ResolvedDMLValue.
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedAssertStmt',
      tag_id=98,
      parent='ResolvedStatement',
      comment="""
      This represents the ASSERT statement:
        ASSERT <expression> [AS <description>];

      <expression> is any expression that returns a bool.
      <description> is an optional string literal used to give a more
      descriptive error message in case the ASSERT fails.
              """,
      fields=[
          Field('expression', 'ResolvedExpr', tag_id=2),
          Field(
              'description',
              SCALAR_STRING,
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedAssertRowsModified',
      tag_id=61,
      parent='ResolvedArgument',
      comment="""
      This represents the ASSERT ROWS MODIFIED clause on a DML statement.
      The value must be a literal or (possibly casted) parameter int64.

      The statement should fail if the number of rows updated does not
      exactly match this number.
              """,
      fields=[Field('rows', 'ResolvedExpr', tag_id=2)])

  gen.AddNode(
      name='ResolvedInsertRow',
      tag_id=62,
      parent='ResolvedArgument',
      comment="""
      This represents one row in the VALUES clause of an INSERT.
              """,
      fields=[Field('value_list', 'ResolvedDMLValue', tag_id=2, vector=True)])

  gen.AddNode(
      name='ResolvedInsertStmt',
      tag_id=63,
      parent='ResolvedStatement',
      comment="""
      This represents an INSERT statement, or a nested INSERT inside an
      UPDATE statement.

      For top-level INSERT statements, <table_scan> gives the table to
      scan and creates ResolvedColumns for its columns.  Those columns can be
      referenced in <insert_column_list>.

      For nested INSERTS, there is no <table_scan> or <insert_column_list>.
      There is implicitly a single column to insert, and its type is the
      element type of the array being updated in the ResolvedUpdateItem
      containing this statement.

      For nested INSERTs, alternate modes are not supported and <insert_mode>
      will always be set to OR_ERROR.

      The rows to insert come from <row_list> or the result of <query>.
      Exactly one of these must be present.

      If <row_list> is present, the columns in the row_list match
      positionally with <insert_column_list>.

      If <query> is present, <query_output_column_list> must also be present.
      <query_output_column_list> is the list of output columns produced by
      <query> that correspond positionally with the target <insert_column_list>
      on the output table.  For nested INSERTs with no <insert_column_list>,
      <query_output_column_list> must have exactly one column.

      <query_parameter_list> is set for nested INSERTs where <query> is set and
      references non-target values (columns or field values) from the table. It
      is only set when FEATURE_V_1_2_CORRELATED_REFS_IN_NESTED_DML is enabled.
              """,
      fields=[
          Field(
              'table_scan',
              'ResolvedTableScan',
              tag_id=2,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'insert_mode',
              SCALAR_INSERT_MODE,
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Behavior on duplicate rows (normally defined to mean duplicate
              primary keys).
                      """),
          Field(
              'assert_rows_modified',
              'ResolvedAssertRowsModified',
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'insert_column_list',
              SCALAR_RESOLVED_COLUMN,
              tag_id=5,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'query_parameter_list',
              'ResolvedColumnRef',
              vector=True,
              ignorable=IGNORABLE_DEFAULT,
              tag_id=9),
          Field(
              'query',
              'ResolvedScan',
              tag_id=6,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'query_output_column_list',
              SCALAR_RESOLVED_COLUMN,
              tag_id=8,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'row_list',
              'ResolvedInsertRow',
              tag_id=7,
              vector=True,
              ignorable=IGNORABLE_DEFAULT)
      ],
      extra_defs="""
  std::string GetInsertModeString() const;
  static std::string InsertModeToString(InsertMode boundary_type);""")

  gen.AddNode(
      name='ResolvedDeleteStmt',
      tag_id=64,
      parent='ResolvedStatement',
      comment="""
      This represents a DELETE statement or a nested DELETE inside an
      UPDATE statement.

      For top-level DELETE statements, <table_scan> gives the table to
      scan and creates ResolvedColumns for its columns.  Those columns can
      be referenced inside the <where_expr>.

      For nested DELETEs, there is no <table_scan>.  The <where_expr> can
      only reference:
        (1) the element_column from the ResolvedUpdateItem containing this
            statement,
        (2) columns from the outer statements, and
        (3) (optionally) <array_offset_column>, which represents the 0-based
            offset of the array element being modified.

      <where_expr> is required.
              """,
      fields=[
          Field(
              'table_scan',
              'ResolvedTableScan',
              tag_id=2,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'assert_rows_modified',
              'ResolvedAssertRowsModified',
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'array_offset_column',
              'ResolvedColumnHolder',
              tag_id=5,
              ignorable=IGNORABLE_DEFAULT),
          Field('where_expr', 'ResolvedExpr', tag_id=4)
      ])

  gen.AddNode(
      name='ResolvedUpdateItem',
      tag_id=65,
      parent='ResolvedArgument',
      comment="""
      This represents one item inside the SET clause of an UPDATE.

      The entity being updated is specified by <target>.

      For a regular
        SET {target} = {expression} | DEFAULT
      clause (not including an array element update like SET a[OFFSET(0)] = 5),
      <target> and <set_value> will be present, and all other fields will be
      unset.

      For an array element update (e.g. SET a.b[<expr>].c = <value>),
        - <target> is set to the array,
        - <element_column> is a new ResolvedColumn that can be used inside the
          update items to refer to the array element.
        - <array_update_list> will have a node corresponding to the offset into
          that array and the modification to that array element.
      For example, for SET a.b[<expr>].c = <value>, we have
         ResolvedUpdateItem
         +-<target> = a.b
         +-<element_column> = <x>
         +-<array_update_list>
           +-ResolvedUpdateArrayItem
             +-<offset> = <expr>
             +-<update_item> = ResolvedUpdateItem
               +-<target> = <x>.c
               +-<set_value> = <value>

      The engine is required to fail the update if there are two elements of
      <array_update_list> corresponding to offset expressions that evaluate to
      the same value. These are considered to be conflicting updates.

      Multiple updates to the same array are always represented as multiple
      elements of <array_update_list> under a single ResolvedUpdateItem
      corresponding to that array. <array_update_list> will only have one
      element for modifications to an array-valued subfield of an array element.
      E.g., for SET a[<expr1>].b[<expr2>] = 5, a[<expr3>].b[<expr4>] = 6, we
      will have:
          ResolvedUpdateItem
          +-<target> = a
          +-<element_column> = x
          +-<array_update_list>
            +-ResolvedUpdateArrayItem
              +-<offset> = <expr1>
              +-ResolvedUpdateItem for <x>.b[<expr2>] = 5
            +-ResolvedUpdateArrayItem
              +-<offset> = <expr3>
              +-ResolvedUpdateItem for <x>.b[<expr4>] = 6
      The engine must give a runtime error if <expr1> and <expr3> evaluate to
      the same thing. Notably, it does not have to understand that the
      two ResolvedUpdateItems corresponding to "b" refer to the same array iff
      <expr1> and <expr3> evaluate to the same thing.

      TODO: Consider allowing the engine to execute an update like
      SET a[<expr1>].b = 1, a[<expr2>].c = 2 even if <expr1> == <expr2> since
      "b" and "c" do not overlap. Also consider allowing a more complex example
      like SET a[<expr1>].b[<expr2>] = ...,
      a[<expr3>].b[<expr4>].c[<expr5>] = ... even if <expr1> == <expr3>, as long
      as <expr2> != <expr4> in that case.

      For nested DML, <target> and <element_column> will both be set, and one or
      more of the nested statement lists will be non-empty. <target> must have
      ARRAY type, and <element_column> introduces a ResolvedColumn representing
      elements of that array. The nested statement lists will always be empty in
      a ResolvedUpdateItem child of a ResolvedUpdateArrayItem node.

      See (broken link) for more detail.
              """,
      fields=[
          Field(
              'target',
              'ResolvedExpr',
              tag_id=2,
              comment="""
              The target entity to be updated.

              This is an expression evaluated using the ResolvedColumns visible
              inside this statement.  This expression can contain only
              ResolvedColumnRefs, ResolvedGetProtoField and
              ResolvedGetStructField nodes.

              In a top-level UPDATE, the expression always starts with a
              ResolvedColumnRef referencing a column from the statement's
              TableScan.

              In a nested UPDATE, the expression always starts with a
              ResolvedColumnRef referencing the element_column from the
              ResolvedUpdateItem containing this scan.

              This node is also used to represent a modification of a single
              array element (when it occurs as a child of a
              ResolvedUpdateArrayItem node).  In that case, the expression
              starts with a ResolvedColumnRef referencing the <element_column>
              from its grandparent ResolvedUpdateItem. (E.g., for "SET a[<expr>]
              = 5", the grandparent ResolvedUpdateItem has <target> "a", the
              parent ResolvedUpdateArrayItem has offset <expr>, and this node
              has <set_value> 5 and target corresponding to the grandparent's
              <element_column> field.)

              For either a nested UPDATE or an array modification, there may be
              a path of field accesses after the initial ResolvedColumnRef,
              represented by a chain of GetField nodes.

              NOTE: We use the same GetField nodes as we do for queries, but
              they are not treated the same.  Here, they express a path inside
              an object that is being mutated, so they have reference semantics.
                      """),
          Field(
              'set_value',
              'ResolvedDMLValue',
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Set the target entity to this value.  The types must match.
              This can contain the same columns that can appear in the
              <where_expr> of the enclosing ResolvedUpdateStmt.

              This is mutually exclusive with all fields below, which are used
              for nested updates only.
                      """),
          Field(
              'element_column',
              'ResolvedColumnHolder',
              tag_id=4,
              comment="""
              The ResolvedColumn introduced to represent the elements of the
              array being updated.  This works similarly to
              ArrayScan::element_column.

              <target> must have array type, and this column has the array's
              element type.

              This column can be referenced inside the nested statements below.
                      """),
          Field(
              'array_update_list',
              'ResolvedUpdateArrayItem',
              tag_id=8,
              vector=True,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Array element modifications to apply. Each item runs on the value
              of <element_column> specified by ResolvedUpdateArrayItem.offset.
              This field is always empty if the analyzer option
              FEATURE_V_1_2_ARRAY_ELEMENTS_WITH_SET is disabled.

              The engine must fail if two elements in this list have offset
              expressions that evaluate to the same value.
              TODO: Consider generalizing this to allow
              SET a[<expr1>].b = ..., a[<expr2>].c = ...
                      """),
          Field(
              'delete_list',
              'ResolvedDeleteStmt',
              tag_id=5,
              vector=True,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Nested DELETE statements to apply.  Each delete runs on one value
              of <element_column> and may choose to delete that array element.

              DELETEs are applied before INSERTs or UPDATEs.

              It is legal for the same input element to match multiple DELETEs.
                      """),
          Field(
              'update_list',
              'ResolvedUpdateStmt',
              tag_id=6,
              vector=True,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Nested UPDATE statements to apply.  Each update runs on one value
              of <element_column> and may choose to update that array element.

              UPDATEs are applied after DELETEs and before INSERTs.

              It is an error if any element is matched by multiple UPDATEs.
                      """),
          Field(
              'insert_list',
              'ResolvedInsertStmt',
              tag_id=7,
              vector=True,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              Nested INSERT statements to apply.  Each insert will produce zero
              or more values for <element_column>.

              INSERTs are applied after DELETEs and UPDATEs.

              For nested UPDATEs, insert_mode will always be the default, and
              has no effect.
                      """)
      ])

  gen.AddNode(
      name='ResolvedUpdateArrayItem',
      tag_id=102,
      parent='ResolvedArgument',
      comment="""
      For an array element modification, this node represents the offset
      expression and the modification, but not the array. E.g., for
      SET a[<expr>] = 5, this node represents a modification of "= 5" to offset
      <expr> of the array defined by the parent node.
              """,
      fields=[
          Field(
              'offset',
              'ResolvedExpr',
              tag_id=2,
              comment="""
              The array offset to be modified.
                      """),
          Field(
              'update_item',
              'ResolvedUpdateItem',
              tag_id=3,
              comment="""
              The modification to perform to the array element.
                      """)
      ])

  gen.AddNode(
      name='ResolvedUpdateStmt',
      tag_id=66,
      parent='ResolvedStatement',
      comment="""
      This represents an UPDATE statement, or a nested UPDATE inside an
      UPDATE statement.

      For top-level UPDATE statements, <table_scan> gives the table to
      scan and creates ResolvedColumns for its columns.  Those columns can be
      referenced in the <update_item_list>. The top-level UPDATE statement may
      also have <from_scan>, the output of which is joined with
      the <table_scan> using expressions in the <where_expr>. The columns
      exposed in the <from_scan> are visible in the right side of the
      expressions in the <update_item_list> and in the <where_expr>.
      <array_offset_column> is never set for top-level UPDATE statements.

      Top-level UPDATE statements will also have <column_access_list> populated.
      For each column, this vector indicates if the column was read and/or
      written. The columns in this vector match those of
      <table_scan.column_list>. If a column was not encountered when producing
      the resolved AST, then the value at that index will be
      ResolvedStatement::NONE.

      For nested UPDATEs, there is no <table_scan>.  The <where_expr> can
      only reference:
        (1) the element_column from the ResolvedUpdateItem containing this
            statement,
        (2) columns from the outer statements, and
        (3) (optionally) <array_offset_column>, which represents the 0-based
            offset of the array element being modified.
      The left hand sides of the expressions in <update_item_list> can only
      reference (1). The right hand sides of those expressions can reference
      (1), (2), and (3).

      The updates in <update_item_list> will be non-overlapping.
      If there are multiple nested statements updating the same entity,
      they will be combined into one ResolvedUpdateItem.

      See (broken link) for more detail on nested DML.
              """,
      fields=[
          Field(
              'table_scan',
              'ResolvedTableScan',
              tag_id=2,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'column_access_list',
              SCALAR_OBJECT_ACCESS,
              tag_id=8,
              ignorable=IGNORABLE,
              vector=True,
              is_constructor_arg=False,
              java_to_string_method='toStringObjectAccess'),
          Field(
              'assert_rows_modified',
              'ResolvedAssertRowsModified',
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'array_offset_column',
              'ResolvedColumnHolder',
              tag_id=7,
              ignorable=IGNORABLE_DEFAULT),
          Field('where_expr', 'ResolvedExpr', tag_id=4),
          Field(
              'update_item_list',
              'ResolvedUpdateItem',
              tag_id=5,
              vector=True),
          Field(
              'from_scan',
              'ResolvedScan',
              tag_id=6,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedMergeWhen',
      tag_id=100,
      parent='ResolvedArgument',
      comment="""
      This is used by ResolvedMergeStmt to represent one WHEN ... THEN clause
      within MERGE statement.

      There are three types of clauses, which are MATCHED, NOT_MATCHED_BY_SOURCE
      and NOT_MATCHED_BY_TARGET. The <match_type> must have one of these values.

      The <match_expr> defines an optional expression to apply to the join
      result of <table_scan> and <from_scan> of the parent ResolvedMergeStmt.

      Each ResolvedMergeWhen must define exactly one of three operations,
        -- INSERT: <action_type> is ResolvedMergeWhen::INSERT.
                   Both <insert_column_list> and <insert_row> are non-empty.
                   The size of <insert_column_list> must be the same with the
                   value_list size of <insert_row>, and, the column data type
                   must match.
        -- UPDATE: <action_type> is ResolvedMergeWhen::UPDATE.
                   <update_item_list> is non-empty.
        -- DELETE: <action_type> is ResolvedMergeWhen::DELETE.
      The INSERT, UPDATE and DELETE operations are mutually exclusive.

      When <match_type> is MATCHED, <action_type> must be UPDATE or DELETE.
      When <match_type> is NOT_MATCHED_BY_TARGET, <action_type> must be INSERT.
      When <match_type> is NOT_MATCHED_BY_SOURCE, <action_type> must be UPDATE
      or DELETE.

      The column visibility within a ResolvedMergeWhen clause is defined as
      following,
        -- When <match_type> is MATCHED,
           -- All columns from <table_scan> and <from_scan> are allowed in
              <match_expr>.
           -- If <action_type> is UPDATE, only columns from <table_scan> are
              allowed on left side of expressions in <update_item_list>.
              All columns from <table_scan> and <from_scan> are allowed on right
              side of expressions in <update_item_list>.
        -- When <match_type> is NOT_MATCHED_BY_TARGET,
           -- Only columns from <from_scan> are allowed in <match_expr>.
           -- Only columns from <table_scan> are allowed in
              <insert_column_list>.
           -- Only columns from <from_scan> are allowed in <insert_row>.
        -- When <match_type> is NOT_MATCHED_BY_SOURCE,
           -- Only columns from <table_scan> are allowed in <match_expr>.
           -- If <action_type> is UPDATE, only columns from <table_scan> are
              allowed in <update_item_list>.
              """,
      fields=[
          Field('match_type', SCALAR_MERGE_MATCH_TYPE, tag_id=2),
          Field('match_expr', 'ResolvedExpr', tag_id=3),
          Field('action_type', SCALAR_MERGE_ACTION_TYPE, tag_id=4),
          Field(
              'insert_column_list',
              SCALAR_RESOLVED_COLUMN,
              tag_id=5,
              ignorable=IGNORABLE_DEFAULT,
              vector=True),
          Field(
              'insert_row',
              'ResolvedInsertRow',
              tag_id=6,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'update_item_list',
              'ResolvedUpdateItem',
              tag_id=7,
              ignorable=IGNORABLE_DEFAULT,
              vector=True)
      ])

  gen.AddNode(
      name='ResolvedMergeStmt',
      tag_id=101,
      parent='ResolvedStatement',
      comment="""
      This represents a MERGE statement.

      <table_scan> gives the target table to scan and creates ResolvedColumns
      for its columns.

      <column_access_list> indicates for each column, whether it was read and/or
      written. The columns in this vector match those of
      <table_scan.column_list>. If a column was not encountered when producing
      the resolved AST, then the value at that index will be
      ResolvedStatement::NONE(0).

      The output of <from_scan> is joined with <table_scan> using the join
      expression <merge_expr>.

      The order of elements in <when_clause_list> matters, as they are executed
      sequentially. At most one of the <when_clause_list> clause will be applied
      to each row from <table_scan>.

      <table_scan>, <from_scan>, <merge_expr> and <when_clause_list> are
      required. <when_clause_list> must be non-empty.

      See (broken link) for more detail on MERGE statement.
              """,
      fields=[
          Field('table_scan', 'ResolvedTableScan', tag_id=2),
          Field(
              'column_access_list',
              SCALAR_OBJECT_ACCESS,
              tag_id=6,
              ignorable=IGNORABLE,
              vector=True,
              is_constructor_arg=False,
              java_to_string_method='toStringObjectAccess'),
          Field('from_scan', 'ResolvedScan', tag_id=3),
          Field('merge_expr', 'ResolvedExpr', tag_id=4),
          Field(
              'when_clause_list',
              'ResolvedMergeWhen',
              tag_id=5,
              vector=True)
      ])

  gen.AddNode(
      name='ResolvedTruncateStmt',
      tag_id=133,
      parent='ResolvedStatement',
      comment="""
      This represents a TRUNCATE TABLE statement.

      Statement:
        TRUNCATE TABLE <table_name> [WHERE <boolean_expression>]

      <table_scan> is a TableScan for the target table, which is used during
                   resolving and validation. Consumers can use either the table
                   object inside it or name_path to reference the table.
      <where_expr> boolean expression that can reference columns in
                   ResolvedColumns (which the TableScan creates); the
                   <where_expr> should always correspond to entire partitions,
                   and is optional.
              """,
      fields=[
          Field(
              'table_scan',
              'ResolvedTableScan',
              tag_id=3),
          Field('where_expr', 'ResolvedExpr', tag_id=4),
      ])

  gen.AddNode(
      name='ResolvedPrivilege',
      tag_id=67,
      parent='ResolvedArgument',
      comment="""
      A grantable privilege.

      <action_type> is the type of privilege action, e.g. SELECT, INSERT, UPDATE
      or DELETE.
      <unit_list> is an optional list of units of the object (e.g. columns of a
      table) the privilege is restricted to. Privilege on the whole object
      should be granted/revoked if the list is empty.
              """,
      fields=[
          Field('action_type', SCALAR_STRING, tag_id=2),
          Field('unit_list', SCALAR_STRING, tag_id=3, vector=True)
      ])

  gen.AddNode(
      name='ResolvedGrantOrRevokeStmt',
      tag_id=68,
      parent='ResolvedStatement',
      is_abstract=True,
      comment="""
      Common superclass of GRANT/REVOKE statements.

      <privilege_list> is the list of privileges to be granted/revoked. ALL
      PRIVILEGES should be granted/fromed if it is empty.
      <object_type> is an optional string identifier, e.g., TABLE, VIEW.
      <name_path> is a vector of segments of the object identifier's pathname.
      <grantee_list> (DEPRECATED) is the list of grantees (strings).
      <grantee_expr_list> is the list of grantees, and may include parameters.

      Only one of <grantee_list> or <grantee_expr_list> will be populated,
      depending on whether or not the FEATURE_PARAMETERS_IN_GRANTEE_LIST
      is enabled.  The <grantee_list> is deprecated, and will be removed
      along with the corresponding FEATURE once all engines have migrated to
      use the <grantee_expr_list>.  Once <grantee_expr_list> is the only
      one, then it should be marked as NOT_IGNORABLE.
              """,
      fields=[
          Field(
              'privilege_list', 'ResolvedPrivilege', tag_id=2, vector=True),
          Field('object_type', SCALAR_STRING, tag_id=3),
          Field('name_path', SCALAR_STRING, tag_id=4, vector=True),
          Field('grantee_list', SCALAR_STRING, ignorable=IGNORABLE_DEFAULT,
                tag_id=5, vector=True,
                to_string_method='ToStringCommaSeparated',
                java_to_string_method='toStringCommaSeparated'),
          Field('grantee_expr_list', 'ResolvedExpr',
                ignorable=IGNORABLE_DEFAULT, tag_id=6, vector=True)
      ])

  gen.AddNode(
      name='ResolvedGrantStmt',
      tag_id=69,
      parent='ResolvedGrantOrRevokeStmt',
      comment="""
      A GRANT statement. It represents the action to grant a list of privileges
      on a specific object to/from list of grantees.
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedRevokeStmt',
      tag_id=70,
      parent='ResolvedGrantOrRevokeStmt',
      comment="""
      A REVOKE statement. It represents the action to revoke a list of
      privileges on a specific object to/from list of grantees.
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedAlterObjectStmt',
      tag_id=114,
      parent='ResolvedStatement',
      is_abstract=True,
      comment="""
      Common super class for statements:
        ALTER <object> [IF EXISTS] <name_path> <alter_action_list>

      <name_path> is a vector giving the identifier path in the table <name>.
      <alter_action_list> is a vector of actions to be done to the object.
      <is_if_exists> silently ignores the "name_path does not exist" error.
              """,
      fields=[
          Field('name_path', SCALAR_STRING, tag_id=2, vector=True),
          Field(
              'alter_action_list',
              'ResolvedAlterAction',
              tag_id=3,
              vector=True),
          Field(
              'is_if_exists',
              SCALAR_BOOL,
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT),
      ])

  gen.AddNode(
      name='ResolvedAlterDatabaseStmt',
      tag_id=134,
      parent='ResolvedAlterObjectStmt',
      comment="""
      This statement:
        ALTER DATABASE [IF EXISTS] <name_path> <alter_action_list>

      This statement could be used to change the database level options.
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedAlterMaterializedViewStmt',
      tag_id=127,
      parent='ResolvedAlterObjectStmt',
      comment="""
      This statement:
        ALTER MATERIALIZED VIEW [IF EXISTS] <name_path> <alter_action_list>
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedAlterTableStmt',
      tag_id=115,
      parent='ResolvedAlterObjectStmt',
      comment="""
      This statement:
        ALTER TABLE [IF EXISTS] <name_path> <alter_action_list>
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedAlterViewStmt',
      tag_id=118,
      parent='ResolvedAlterObjectStmt',
      comment="""
      This statement:
        ALTER VIEW [IF EXISTS] <name_path> <alter_action_list>
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedAlterAction',
      tag_id=116,
      parent='ResolvedArgument',
      is_abstract=True,
      comment="""
      A common super class for all actions in statement ALTER <object>
              """,
      fields=[])

  gen.AddNode(
      name='ResolvedSetOptionsAction',
      tag_id=117,
      parent='ResolvedAlterAction',
      comment="""
      SET OPTIONS action for ALTER <object> statement

      <option_list> has engine-specific directives that specify how to
                    alter the metadata for this object.
              """,
      fields=[
          Field('option_list', 'ResolvedOption', tag_id=2, vector=True),
      ])

  gen.AddNode(
      name='ResolvedAddColumnAction',
      tag_id=131,
      parent='ResolvedAlterAction',
      comment="""
      ADD COLUMN action for ALTER TABLE statement
              """,
      fields=[
          Field('is_if_not_exists', SCALAR_BOOL, tag_id=2),
          Field(
              'column_definition', 'ResolvedColumnDefinition', tag_id=3)
      ])

  gen.AddNode(
      name='ResolvedDropColumnAction',
      tag_id=132,
      parent='ResolvedAlterAction',
      comment="""
      DROP COLUMN action for ALTER TABLE statement

      <name> is the name of the column to drop.
      <column_reference> references the column to be dropped, if it exists.
             It might be missing if DROP IF EXISTS column does not exist.
              """,
      fields=[
          Field('is_if_exists', SCALAR_BOOL, tag_id=2),
          Field('name', SCALAR_STRING, tag_id=3),
          Field(
              'column_reference',
              'ResolvedColumnRef',
              tag_id=4,
              ignorable=IGNORABLE),
      ])

  gen.AddNode(
      name='ResolvedAlterTableSetOptionsStmt',
      tag_id=71,
      parent='ResolvedStatement',
      comment="""
      This statement:
        ALTER TABLE [IF EXISTS] <name> SET OPTIONS (...)

      NOTE: This is deprecated in favor of ResolvedAlterTableStmt.

      <name_path> is a vector giving the identifier path in the table <name>.
      <option_list> has engine-specific directives that specify how to
                    alter the metadata for this table.
      <is_if_exists> silently ignore the "name_path does not exist" error.
              """,
      fields=[
          Field('name_path', SCALAR_STRING, tag_id=2, vector=True),
          Field('option_list', 'ResolvedOption', tag_id=3, vector=True),
          Field(
              'is_if_exists',
              SCALAR_BOOL,
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedRenameStmt',
      tag_id=72,
      parent='ResolvedStatement',
      comment="""
      This statement: RENAME <object_type> <old_name_path> TO <new_name_path>;

      <object_type> is an string identifier,
                    e.g., "TABLE", "VIEW", "INDEX", "FUNCTION", "TYPE", etc.
      <old_name_path> is a vector giving the identifier path for the object to
                      be renamed.
      <new_name_path> is a vector giving the identifier path for the object to
                      be renamed to.
              """,
      fields=[
          Field('object_type', SCALAR_STRING, tag_id=2),
          Field('old_name_path', SCALAR_STRING, tag_id=3, vector=True),
          Field('new_name_path', SCALAR_STRING, tag_id=4, vector=True)
      ])

  gen.AddNode(
      name='ResolvedCreateRowAccessPolicyStmt',
      tag_id=73,
      parent='ResolvedStatement',
      comment="""
      This statement: CREATE [OR REPLACE] ROW ACCESS POLICY [IF NOT EXISTS]
                      [<name>] ON <target_name_path>
                      [GRANT TO (<grantee_list>)]
                      FILTER USING (<predicate>);

      <create_mode> indicates if this was CREATE, CREATE OR REPLACE, or
                    CREATE IF NOT EXISTS.
      <name> is the name of the row access policy to be created or an empty
             string.
      <target_name_path> is a vector giving the identifier path of the target
                         table.
      <table_scan> is a TableScan for the target table, which is used during
                   resolving and validation. Consumers can use either the table
                   object inside it or target_name_path to reference the table.
      <grantee_list> (DEPRECATED) is the list of user principals the policy
                     should apply to.
      <grantee_expr_list> is the list of user principals the policy should
                          apply to, and may include parameters.
      <predicate> is a boolean expression that selects the rows that are being
                  made visible.
      <predicate_str> is the string form of the predicate.

      Only one of <grantee_list> or <grantee_expr_list> will be populated,
      depending on whether or not the FEATURE_PARAMETERS_IN_GRANTEE_LIST
      is enabled.  The <grantee_list> is deprecated, and will be removed
      along with the corresponding FEATURE once all engines have migrated to
      use the <grantee_expr_list>.  Once <grantee_expr_list> is the only
      one, then it should be marked as NOT_IGNORABLE.
              """,
      fields=[
          Field(
              'create_mode',
              SCALAR_CREATE_MODE,
              tag_id=2,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'name', SCALAR_STRING, ignorable=IGNORABLE_DEFAULT, tag_id=3),
          Field(
              'target_name_path', SCALAR_STRING, tag_id=4, vector=True),
          Field('grantee_list', SCALAR_STRING, ignorable=IGNORABLE_DEFAULT,
                tag_id=5, vector=True,
                to_string_method='ToStringCommaSeparated',
                java_to_string_method='toStringCommaSeparated'),
          Field('grantee_expr_list', 'ResolvedExpr',
                ignorable=IGNORABLE_DEFAULT, tag_id=9, vector=True),
          Field(
              'table_scan',
              'ResolvedTableScan',
              tag_id=6,
              ignorable=IGNORABLE),
          Field(
              'predicate', 'ResolvedExpr', tag_id=7, ignorable=IGNORABLE),
          Field(
              'predicate_str', SCALAR_STRING, tag_id=8, ignorable=IGNORABLE)
      ],
      extra_defs="""
  typedef ResolvedCreateStatement::CreateMode CreateMode;""")

  gen.AddNode(
      name='ResolvedDropRowAccessPolicyStmt',
      tag_id=74,
      parent='ResolvedStatement',
      comment="""
      This statement:
          DROP ROW ACCESS POLICY <name> ON <target_name_path>; or
          DROP ALL ROW [ACCESS] POLICIES ON <target_name_path>;

      <is_drop_all> indicates that all policies should be dropped.
      <is_if_exists> silently ignore the "policy <name> does not exist" error.
                     This is not allowed if is_drop_all is true.
      <name> is the name of the row policy to be dropped or an empty string.
      <target_name_path> is a vector giving the identifier path of the target
                         table.
              """,
      fields=[
          Field(
              'is_drop_all',
              SCALAR_BOOL,
              ignorable=IGNORABLE_DEFAULT,
              tag_id=2),
          Field(
              'is_if_exists',
              SCALAR_BOOL,
              ignorable=IGNORABLE_DEFAULT,
              tag_id=3),
          Field(
              'name', SCALAR_STRING, ignorable=IGNORABLE_DEFAULT, tag_id=4),
          Field(
              'target_name_path', SCALAR_STRING, tag_id=5, vector=True)
      ])

  gen.AddNode(
      name='ResolvedGrantToAction',
      tag_id=135,
      parent='ResolvedAlterAction',
      comment="""
      GRANT TO action for ALTER ROW ACCESS POLICY statement

      <grantee_expr_list> is the list of grantees, and may include parameters.
              """,
      fields=[
          Field(
              'grantee_expr_list',
              'ResolvedExpr',
              ignorable=NOT_IGNORABLE,
              tag_id=2,
              vector=True),
      ])

  gen.AddNode(
      name='ResolvedFilterUsingAction',
      tag_id=136,
      parent='ResolvedAlterAction',
      comment="""
      FILTER USING action for ALTER ROW ACCESS POLICY statement

      <predicate> is a boolean expression that selects the rows that are being
                  made visible.
      <predicate_str> is the string form of the predicate.
              """,
      fields=[
          Field(
              'predicate', 'ResolvedExpr', tag_id=2, ignorable=IGNORABLE),
          Field(
              'predicate_str',
              SCALAR_STRING,
              tag_id=3,
              ignorable=NOT_IGNORABLE)
      ])

  gen.AddNode(
      name='ResolvedRevokeFromAction',
      tag_id=137,
      parent='ResolvedAlterAction',
      comment="""
      REVOKE FROM action for ALTER ROW ACCESS POLICY statement

      <revokee_expr_list> is the list of revokees, and may include parameters.
      <is_revoke_from_all> is a boolean indicating whether it was a REVOKE FROM
                           ALL statement.
              """,
      fields=[
          Field(
              'revokee_expr_list',
              'ResolvedExpr',
              ignorable=IGNORABLE_DEFAULT,
              tag_id=2,
              vector=True),
          Field(
              'is_revoke_from_all',
              SCALAR_BOOL,
              ignorable=IGNORABLE_DEFAULT,
              tag_id=3)
      ])

  gen.AddNode(
      name='ResolvedRenameToAction',
      tag_id=138,
      parent='ResolvedAlterAction',
      comment="""
      RENAME TO action for ALTER ROW ACCESS POLICY statement

      <new_name> is the new name of the row access policy.
              """,
      fields=[
          Field(
              'new_name', SCALAR_STRING, ignorable=NOT_IGNORABLE, tag_id=2),
      ])

  gen.AddNode(
      name='ResolvedAlterRowAccessPolicyStmt',
      tag_id=75,
      parent='ResolvedAlterObjectStmt',
      comment="""
      This statement:
          ALTER ROW ACCESS POLICY [IF EXISTS]
          <name> ON <name_path>
          <alter_action_list>

      <name> is the name of the row access policy to be altered, scoped to the
             table in the base <name_path>.
      <table_scan> is a TableScan for the target table, which is used during
                   resolving and validation. Consumers can use either the table
                   object inside it or base <name_path> to reference the table.
              """,
      fields=[
          Field('name', SCALAR_STRING, tag_id=2),
          Field(
              'table_scan',
              'ResolvedTableScan',
              tag_id=6,
              ignorable=IGNORABLE),
      ])

  gen.AddNode(
      name='ResolvedAlterAllRowAccessPoliciesStmt',
      tag_id=145,
      parent='ResolvedAlterObjectStmt',
      comment="""
      This statement:
          ALTER ALL ROW ACCESS POLICIES ON <name_path> <alter_action_list>

      <name_path> is a vector giving the identifier path in the table name.
      <alter_action_list> is a vector of actions to be done to the object. It
                          must have exactly one REVOKE FROM action with either
                          a non-empty grantee list or 'all'.
      <table_scan> is a TableScan for the target table, which is used during
                   resolving and validation. Consumers can use either the table
                   object inside it or base <name_path> to reference the table.
              """,
      fields=[
          Field(
              'table_scan',
              'ResolvedTableScan',
              tag_id=6,
              ignorable=IGNORABLE),
      ])

  gen.AddNode(
      name='ResolvedCreateConstantStmt',
      tag_id=99,
      parent='ResolvedCreateStatement',
      emit_default_constructor=False,
      comment="""
      This statement creates a user-defined named constant:
        CREATE [OR REPLACE] [TEMP | TEMPORARY | PUBLIC | PRIVATE] CONSTANT
          [IF NOT EXISTS] <name_path> = <expression>

        <name_path> is the identifier path of the named constants.
        <expr> is the expression that determines the type and the value of the
               named constant. Note that <expr> need not be constant. Its value
               is bound to the named constant which is then treated as
               immutable. <expr> can be evaluated at the time this statement is
               processed or later (lazy evaluation during query execution).
              """,
      fields=[Field('expr', 'ResolvedExpr', tag_id=2)])

  gen.AddNode(
      name='ResolvedCreateFunctionStmt',
      tag_id=76,
      parent='ResolvedCreateStatement',
      emit_default_constructor=False,
      comment="""
      This statement creates a user-defined function:
        CREATE [TEMP] FUNCTION [IF NOT EXISTS] <name_path> (<arg_list>)
          [RETURNS <return_type>] [<determinism_level>] [LANGUAGE <language>]
          [AS <code> | AS ( <function_expression> )] [OPTIONS (<option_list>)]

        <name_path> is the identifier path of the function.
        <has_explicit_return_type> is true iff RETURNS clause is present.
        <return_type> is the return type for the function, which can be any
               valid ZetaSQL type, including ARRAY or STRUCT. It is inferred
               from <function_expression> if not explicitly set.
               TODO: Deprecate and remove this. The return type is
               already specified by the <signature>.
        <argument_name_list> The names of the function arguments.
        <signature> is the FunctionSignature of the created function, with all
               options.  This can be used to create a Function to load into a
               Catalog for future queries.
        <is_aggregate> is true if this is an aggregate function.  All arguments
               are assumed to be aggregate input arguments that may vary for
               every row.
        <language> is the programming language used by the function. This field
               is set to 'SQL' for SQL functions and otherwise to the language
               name specified in the LANGUAGE clause.
        <code> is a string literal that contains the function definition.  Some
               engines may allow this argument to be omitted for certain types
               of external functions. This will always be set for SQL functions.
        <aggregate_expression_list> is a list of SQL aggregate functions to
               compute prior to computing the final <function_expression>.
               See below.
        <function_expression> is the resolved SQL expression invoked for the
               function. This will be unset for external language functions. For
               non-template SQL functions, this is a resolved representation of
               the expression in <code>.
        <option_list> has engine-specific directives for modifying functions.
        <sql_security> is the declared security mode for the function. Values
               include 'INVOKER', 'DEFINER'.
        <determinism_level> is the declared determinism level of the function.
               Values are 'DETERMINISTIC', 'NOT DETERMINISTIC', 'IMMUTABLE',
               'STABLE', 'VOLATILE'.

      Note that <function_expression> and <code> are both marked as IGNORABLE
      because an engine could look at either one (but might not look at both).
      An engine must look at one (and cannot ignore both) to be semantically
      valid, but there is currently no way to enforce that.

      For aggregate functions, <is_aggregate> will be true.
      Aggregate functions will only occur if LanguageOptions has
      FEATURE_CREATE_AGGREGATE_FUNCTION enabled.

      Arguments to aggregate functions must have
      <FunctionSignatureArgumentTypeOptions::is_not_aggregate> true or false.
      Non-aggregate arguments must be passed constant values only.

      For SQL aggregate functions, there will be both an
      <aggregate_expression_list>, with aggregate expressions to compute first,
      and then a final <function_expression> to compute on the results
      of the aggregates.  Each aggregate expression is a
      ResolvedAggregateFunctionCall, and may reference any input arguments.
      Each ResolvedComputedColumn in <aggregate_expression_list> gives the
      aggregate expression a column id.  The final <function_expression> can
      reference these created aggregate columns, and any input arguments
      with <argument_kind>=NOT_AGGREGATE.

      For example, with
        CREATE TEMP FUNCTION my_avg(x) = (SUM(x) / COUNT(x));
      we would have an <aggregate_expression_list> with
        agg1#1 := SUM(ResolvedArgumentRef(x))
        agg2#2 := COUNT(ResolvedArgumentRef(x))
      and a <function_expression>
        ResolvedColumnRef(agg1#1) / ResolvedColumnRef(agg2#2)

      For example, with
        CREATE FUNCTION scaled_avg(x,y NOT AGGREGATE) = (SUM(x) / COUNT(x) * y);
      we would have an <aggregate_expression_list> with
        agg1#1 := SUM(ResolvedArgumentRef(x))
        agg2#2 := COUNT(ResolvedArgumentRef(x))
      and a <function_expression>
        ResolvedColumnRef(agg1#1) / ResolvedColumnRef(agg2#2) * \
ResolvedArgumentRef(y)

      When resolving a query that calls an aggregate UDF, the query will
      have a ResolvedAggregateScan that invokes the UDF function.  The engine
      should remove the UDF aggregate function from the <aggregate_list>, and
      instead compute the additional aggregates from the
      UDF's <aggregate_expression_list>, and then add an additional Project
      to compute the final <function_expression>, which should produce the
      value for the original ResolvedAggregateScan's computed column for the
      UDF.  Some rewrites of the ResolvedColumn references inside the UDF will
      be required.  TODO If using ResolvedColumns makes this renaming
      too complicated, we could switch to use ResolvedArgumentRefs, or
      something new.
              """,
      fields=[
          Field(
              'has_explicit_return_type',
              SCALAR_BOOL,
              tag_id=13,
              ignorable=IGNORABLE),
          Field(
              'return_type', SCALAR_TYPE, tag_id=3, ignorable=IGNORABLE),
          Field(
              'argument_name_list',
              SCALAR_STRING,
              ignorable=IGNORABLE,
              vector=True,
              tag_id=11,
              to_string_method='ToStringCommaSeparated'),
          Field(
              'signature',
              SCALAR_FUNCTION_SIGNATURE,
              tag_id=10,
              ignorable=IGNORABLE,
              to_string_method='ToStringVerbose'),
          Field(
              'is_aggregate',
              SCALAR_BOOL,
              ignorable=IGNORABLE_DEFAULT,
              tag_id=8),
          Field(
              'language',
              SCALAR_STRING,
              ignorable=IGNORABLE_DEFAULT,
              tag_id=4),
          Field('code', SCALAR_STRING, ignorable=IGNORABLE, tag_id=5),
          Field(
              'aggregate_expression_list',
              'ResolvedComputedColumn',
              tag_id=9,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'function_expression',
              'ResolvedExpr',
              ignorable=IGNORABLE,
              tag_id=6),
          Field(
              'option_list',
              'ResolvedOption',
              ignorable=IGNORABLE_DEFAULT,
              tag_id=7,
              vector=True),
          Field(
              'sql_security',
              SCALAR_SQL_SECURITY,
              tag_id=12,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'determinism_level',
              SCALAR_DETERMINISM_LEVEL,
              tag_id=14,
              ignorable=IGNORABLE_DEFAULT)
      ],
      extra_defs="""
        static FunctionEnums::Volatility DeterminismLevelToVolatility(
          ResolvedCreateStatementEnums::DeterminismLevel);

        // Converts the function's determinism level into a volatility.
        // Functions with unspecified/non deterministic/volatile
        // specifiers are considered volatile, functions with deterministic
        // and immutable specifiers are considered immutable and functions
        // with the stable specifier are considered stable.
        FunctionEnums::Volatility volatility() const {
          return DeterminismLevelToVolatility(determinism_level());
        }
      """,
  )

  gen.AddNode(
      name='ResolvedArgumentDef',
      tag_id=77,
      parent='ResolvedArgument',
      comment="""
      This represents an argument definition, e.g. in a function's argument
      list.

      <name> is the name of the argument; optional for DROP FUNCTION statements.
      <type> is the type of the argument.
      <argument_kind> indicates what kind of argument this is, including scalar
              vs aggregate.  NOT_AGGREGATE means this is a non-aggregate
              argument in an aggregate function, which can only passed constant
              values only.

      NOTE: Statements that create functions now include a FunctionSignature
      directly, and an argument_name_list if applicable.  These completely
      describe the function signature, so the ResolvedArgumentDef list can
      be considered unnecessary and deprecated.
      TODO We could remove this node in the future.
              """,
      fields=[
          Field('name', SCALAR_STRING, tag_id=2, ignorable=IGNORABLE),
          Field('type', SCALAR_TYPE, tag_id=3),
          Field(
              'argument_kind',
              SCALAR_ARGUMENT_KIND,
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedArgumentRef',
      tag_id=78,
      parent='ResolvedExpr',
      comment="""
      This represents an argument reference, e.g. in a function's body.
      <name> is the name of the argument.
      <argument_kind> is the ArgumentKind from the ResolvedArgumentDef.
              For scalar functions, this is always SCALAR.
              For aggregate functions, it can be AGGREGATE or NOT_AGGREGATE.
              If NOT_AGGREGATE, then this is a non-aggregate argument
              to an aggregate function, which has one constant value
              for the entire function call (over all rows in all groups).
              (This is copied from the ResolvedArgumentDef for convenience.)
              """,
      fields=[
          Field('name', SCALAR_STRING, tag_id=2),
          Field(
              'argument_kind',
              SCALAR_ARGUMENT_KIND,
              ignorable=IGNORABLE,
              tag_id=3)
      ])

  gen.AddNode(
      name='ResolvedCreateTableFunctionStmt',
      tag_id=88,
      parent='ResolvedCreateStatement',
      emit_default_constructor=False,
      comment="""
      This statement creates a user-defined table-valued function:
        CREATE [TEMP] TABLE FUNCTION [IF NOT EXISTS]
          <name_path> (<argument_name_list>)
          [RETURNS <return_type>]
          [OPTIONS (<option_list>)]
          [LANGUAGE <language>]
          [AS <code> | AS ( <query> )]

        <argument_name_list> contains the names of the function arguments.
        <signature> is the FunctionSignature of the created function, with all
               options.  This can be used to create a Function to load into a
               Catalog for future queries.
        <option_list> has engine-specific directives for modifying functions.
        <language> is the programming language used by the function. This field
               is set to 'SQL' for SQL functions, to the language name specified
               in the LANGUAGE clause if present, and to 'UNDECLARED' if both
               the LANGUAGE clause and query are not present.
        <code> is an optional string literal that contains the function
               definition.  Some engines may allow this argument to be omitted
               for certain types of external functions.  This will always be set
               for SQL functions.
        <query> is the SQL query invoked for the function.  This will be unset
               for external language functions. For non-templated SQL functions,
               this is a resolved representation of the query in <code>.
        <output_column_list> is the list of resolved output
               columns returned by the table-valued function.
        <is_value_table> If true, this function returns a value table.
               Rather than producing rows with named columns, it produces
               rows with a single unnamed value type. <output_column_list> will
               have exactly one anonymous column (with no name).
               See (broken link).
        <sql_security> is the declared security mode for the function. Values
               include 'INVOKER', 'DEFINER'.

      ----------------------
      Table-Valued Functions
      ----------------------

      This is a statement to create a new table-valued function. Each
      table-valued function returns an entire table as output instead of a
      single scalar value. Table-valued functions can only be created if
      LanguageOptions has FEATURE_CREATE_TABLE_FUNCTION enabled.

      For SQL table-valued functions that include a defined SQL body, the
      <query> is non-NULL and contains the resolved SQL body.
      In this case, <output_column_list> contains a list of the
      output columns of the SQL body. The <query> uses
      ResolvedArgumentRefs to refer to scalar arguments and
      ResolvedRelationArgumentScans to refer to relation arguments.

      The table-valued function may include RETURNS TABLE<...> to explicitly
      specify a schema for the output table returned by the function. If the
      function declaration includes a SQL body, then the names and types of the
      output columns of the corresponding <query> will have been
      coerced to exactly match 1:1 with the names and types of the columns
      specified in the RETURNS TABLE<...> section.

      When resolving a query that calls a table-valued function, the query will
      have a ResolvedTVFScan that invokes the function.

      Value tables: If the function declaration includes a value-table
      parameter, this is written as an argument of type "TABLE" where the table
      contains a single anonymous column with a type but no name. In this case,
      calls to the function may pass a (regular or value) table with a single
      (named or unnamed) column for any of these parameters, and ZetaSQL
      accepts these arguments as long as the column type matches.

      Similarly, if the CREATE TABLE FUNCTION statement includes a "RETURNS
      TABLE" section with a single column with no name, then this defines a
      value-table return type. The function then returns a value table as long
      as the SQL body returns a single column whose type matches (independent of
      whether the SQL body result is a value table or not, and whether the
      returned column is named or unnamed).

      --------------------------------
      Templated Table-Valued Functions
      --------------------------------

      ZetaSQL supports table-valued function declarations with parameters of
      type ANY TABLE. This type indicates that any schema is valid for tables
      passed for this parameter. In this case:

      * the IsTemplated() method of the <signature> field returns true,
      * the <output_column_list> field is empty,
      * the <is_value_table> field is set to a default value of false (since
        ZetaSQL cannot analyze the function body in the presence of templated
        parameters, it is not possible to detect this property yet),

      TODO: Update this description once ZetaSQL supports more types
      of templated function parameters. Currently only ANY TABLE is supported.
              """,
      fields=[
          Field(
              'argument_name_list',
              SCALAR_STRING,
              ignorable=IGNORABLE,
              vector=True,
              tag_id=2,
              to_string_method='ToStringCommaSeparated'),
          Field(
              'signature',
              SCALAR_FUNCTION_SIGNATURE,
              tag_id=3,
              ignorable=IGNORABLE,
              to_string_method='ToStringVerbose'),
          Field(
              'option_list',
              'ResolvedOption',
              ignorable=IGNORABLE_DEFAULT,
              tag_id=4,
              vector=True),
          Field(
              'language',
              SCALAR_STRING,
              ignorable=IGNORABLE_DEFAULT,
              tag_id=5),
          Field('code', SCALAR_STRING, ignorable=IGNORABLE, tag_id=6),
          Field(
              'query',
              'ResolvedScan',
              ignorable=IGNORABLE_DEFAULT,
              tag_id=7),
          Field(
              'output_column_list',
              'ResolvedOutputColumn',
              tag_id=8,
              vector=True),
          Field(
              'is_value_table',
              SCALAR_BOOL,
              tag_id=9,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'sql_security',
              SCALAR_SQL_SECURITY,
              tag_id=10,
              ignorable=IGNORABLE_DEFAULT)
      ])

  gen.AddNode(
      name='ResolvedRelationArgumentScan',
      tag_id=89,
      parent='ResolvedScan',
      comment="""
      This represents a relation argument reference in a table-valued function's
      body. The 'column_list' of this ResolvedScan includes column names from
      the relation argument in the table-valued function signature.
              """,
      fields=[
          Field(
              'name',
              SCALAR_STRING,
              tag_id=2,
              comment="""
              This is the name of the relation argument for the table-valued
              function.  It is used to match this relation argument reference in
              a TVF SQL function body with one of possibly several relation
              arguments in the TVF call.
                      """),
          Field(
              'is_value_table',
              SCALAR_BOOL,
              tag_id=3,
              ignorable=IGNORABLE_DEFAULT,
              comment="""
              If true, the result of this query is a value table. Rather than
              producing rows with named columns, it produces rows with a single
              unnamed value type. See (broken link).
                      """)
      ])

  gen.AddNode(
      name='ResolvedArgumentList',
      tag_id=79,
      parent='ResolvedArgument',
      comment="""
      This statement: [ (<arg_list>) ];

      <arg_list> is an optional list of parameters.  If given, each parameter
                 may consist of a type, or a name and a type.

      NOTE: This can be considered deprecated in favor of the FunctionSignature
            stored directly in the statement.

      NOTE: ResolvedArgumentList is not related to the ResolvedArgument class,
            which just exists to organize node classes.
             """,
      fields=[
          Field(
              'arg_list',
              'ResolvedArgumentDef',
              tag_id=2,
              vector=True,
              ignorable=IGNORABLE)
      ])

  gen.AddNode(
      name='ResolvedFunctionSignatureHolder',
      tag_id=84,
      parent='ResolvedArgument',
      emit_default_constructor=False,
      comment="""
      This wrapper is used for an optional FunctionSignature.
              """,
      fields=[
          Field(
              'signature',
              SCALAR_FUNCTION_SIGNATURE,
              tag_id=2,
              to_string_method='ToStringVerbose')
      ])

  gen.AddNode(
      name='ResolvedDropFunctionStmt',
      tag_id=80,
      parent='ResolvedStatement',
      comment="""
      This statement: DROP FUNCTION [IF EXISTS] <name_path>
        [ (<arguments>) ];

      <is_if_exists> silently ignore the "name_path does not exist" error.
      <name_path> is the identifier path of the function to be dropped.
      <arguments> is an optional list of parameters.  If given, each parameter
                 may consist of a type, or a name and a type.  The name is
                 disregarded, and is allowed to permit copy-paste from CREATE
                 FUNCTION statements.
      <signature> is the signature of the dropped function.  Argument names and
                 argument options are ignored because only the types matter
                 for matching signatures in DROP FUNCTION.  The return type
                 in this signature will always be <void>, since return type
                 is ignored when matching signatures for DROP.
                 TODO <arguments> could be deprecated in favor of this.
              """,
      fields=[
          Field('is_if_exists', SCALAR_BOOL, tag_id=2),
          Field('name_path', SCALAR_STRING, tag_id=3, vector=True),
          Field(
              'arguments',
              'ResolvedArgumentList',
              tag_id=4,
              ignorable=IGNORABLE,
              comment="""
              NOTE: arguments for DROP FUNCTION statements are matched only on
              type; names for any arguments in ResolvedArgumentList will be set
              to the empty string irrespective of whether or not argument names
              were given in the DROP FUNCTION statement.
                      """),
          Field(
              'signature',
              'ResolvedFunctionSignatureHolder',
              tag_id=5,
              ignorable=IGNORABLE,
              comment="""
              NOTE: arguments for DROP FUNCTION statements are matched only on
              type; names are irrelevant, so no argument names are saved to use
              with this signature.  Additionally, the return type will always be
              <void>, since return types are ignored for DROP FUNCTION.
                      """)
      ])

  gen.AddNode(
      name='ResolvedCallStmt',
      tag_id=83,
      parent='ResolvedStatement',
      emit_default_constructor=False,
      comment="""
      This statement: CALL <procedure>;

      <procedure> Procedure to call.
      <signature> Resolved FunctionSignature for this procedure.
      <argument_list> Procedure arguments.
              """,
      fields=[
          Field('procedure', SCALAR_PROCEDURE, tag_id=2),
          Field(
              'signature',
              SCALAR_FUNCTION_SIGNATURE,
              tag_id=3,
              ignorable=IGNORABLE),
          Field('argument_list', 'ResolvedExpr', tag_id=4, vector=True)
      ])

  gen.AddNode(
      name='ResolvedImportStmt',
      tag_id=86,
      parent='ResolvedStatement',
      comment="""
      This statement: IMPORT <import_kind>
                                   [<name_path> [AS|INTO <alias_path>]
                                   |<file_path>]
                             [<option_list>];

      <import_kind> The type of the object, currently supports MODULE and PROTO.
      <name_path>   The identifier path of the object to import, e.g., foo.bar,
                    used in IMPORT MODULE statement.
      <file_path>   The file path of the object to import, e.g., "file.proto",
                    used in IMPORT PROTO statement.
      <alias_path>  The AS alias path for the object.
      <into_alias_path>  The INTO alias path for the object.
      <option_list> Engine-specific directives for the import.

      Either <name_path> or <file_path> will be populated but not both.
            <name_path> will be populated for IMPORT MODULE.
            <file_path> will be populated for IMPORT PROTO.

      At most one of <alias_path> or <into_alias_path> will be populated.
            <alias_path> may be populated for IMPORT MODULE.
            <into_alias_path> may be populated for IMPORT PROTO.

      IMPORT MODULE and IMPORT PROTO both support options.

      See (broken link) for more detail on IMPORT MODULE.
      See (broken link) for more detail on IMPORT PROTO.
              """,
      fields=[
          Field('import_kind', SCALAR_IMPORT_KIND, tag_id=2),
          Field(
              'name_path',
              SCALAR_STRING,
              tag_id=3,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'file_path',
              SCALAR_STRING,
              tag_id=4,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'alias_path',
              SCALAR_STRING,
              tag_id=5,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'into_alias_path',
              SCALAR_STRING,
              tag_id=7,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'option_list',
              'ResolvedOption',
              ignorable=IGNORABLE_DEFAULT,
              tag_id=6,
              vector=True)
      ],
      extra_defs="""
  std::string GetImportKindString() const;
  static std::string ImportKindToString(ImportKind kind);""")

  gen.AddNode(
      name='ResolvedModuleStmt',
      tag_id=87,
      parent='ResolvedStatement',
      comment="""
      This statement: MODULE <name_path> [<option_list>];

      <name_path> is the identifier path of the module.
      <option_list> Engine-specific directives for the module statement.

      See (broken link) for more detail on MODULEs.
              """,
      fields=[
          Field('name_path', SCALAR_STRING, tag_id=2, vector=True),
          Field(
              'option_list',
              'ResolvedOption',
              ignorable=IGNORABLE_DEFAULT,
              tag_id=3,
              vector=True)
      ])

  gen.AddNode(
      name='ResolvedAggregateHavingModifier',
      tag_id=85,
      parent='ResolvedArgument',
      comment="""
      This represents a HAVING MAX or HAVING MIN modifier in an aggregate
      expression. If an aggregate has arguments (x HAVING {MAX/MIN} y),
      the aggregate will be computed over only the x values in the rows with the
      maximal/minimal values of y.

      <kind> the MAX/MIN kind of this HAVING
      <having_expr> the HAVING expression (y in the above example)
              """,
      fields=[
          Field('kind', SCALAR_HAVING_MODIFIER_KIND, tag_id=2),
          Field('having_expr', 'ResolvedExpr', tag_id=3)
      ],
      extra_defs="""
  std::string GetHavingModifierKindString() const;
  static std::string HavingModifierKindToString(HavingModifierKind kind);""")

  gen.AddNode(
      name='ResolvedCreateMaterializedViewStmt',
      tag_id=119,
      parent='ResolvedCreateViewBase',
      comment="""
      This statement:
        CREATE MATERIALIZED VIEW <name> [PARTITION BY expr, ...]
        [CLUSTER BY expr, ...] [OPTIONS (...)] AS SELECT ...

      <column_definition_list> matches 1:1 with the <output_column_list> in
      ResolvedCreateViewBase and provides explicit definition for each
      ResolvedColumn produced by <query>. Output column names and types must
      match column definition names and types. If the table is a value table,
      <column_definition_list> must have exactly one column, with a generated
      name such as "$struct".

      Currently <column_definition_list> contains the same schema information
      (column names and types) as <output_definition_list>, but when/if we
      allow specifying column OPTIONS as part of CMV statement, this information
      will be available only in <column_definition_list>. Therefore, consumers
      are encouraged to read from <column_definition_list> rather than from
      <output_column_list> to determine the schema, if possible.

      <partition_by_list> specifies the partitioning expressions for the
                          materialized view.
      <cluster_by_list> specifies the clustering expressions for the
                        materialized view.
              """,
      fields=[
          Field(
              'column_definition_list',
              'ResolvedColumnDefinition',
              tag_id=2,
              vector=True,
              # This is ignorable for backwards compatibility.
              ignorable=IGNORABLE),
          Field(
              'partition_by_list',
              'ResolvedExpr',
              tag_id=3,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
          Field(
              'cluster_by_list',
              'ResolvedExpr',
              tag_id=4,
              vector=True,
              ignorable=IGNORABLE_DEFAULT),
      ])

  gen.AddNode(
      name='ResolvedCreateProcedureStmt',
      tag_id=125,
      parent='ResolvedCreateStatement',
      emit_default_constructor=False,
      comment="""
      This statement creates a user-defined procedure:
        CREATE [OR REPLACE] [TEMP] PROCEDURE [IF NOT EXISTS] <name_path>
        (<arg_list>) [OPTIONS (<option_list>)]
        BEGIN
        <procedure_body>
        END;

        <name_path> is the identifier path of the procedure.
        <argument_name_list> The names of the function arguments.
        <signature> is the FunctionSignature of the created procedure, with all
               options.  This can be used to create a procedure to load into a
               Catalog for future queries.
        <option_list> has engine-specific directives for modifying procedures.
        <procedure_body> is a string literal that contains the procedure body.
               It includes everything from the BEGIN keyword to the END keyword,
               inclusive.

               The resolver will perform some basic validation on the procedure
               body, for example, verifying that DECLARE statements are in the
               proper position, and that variables are not declared more than
               once, but any validation that requires the catalog (including
               generating resolved tree nodes for individual statements) is
               deferred until the procedure is actually called.  This deferral
               makes it possible to define a procedure which references a table
               or routine that does not yet exist, so long as the entity is
               created before the procedure is called.
              """,
      fields=[
          Field(
              'argument_name_list',
              SCALAR_STRING,
              ignorable=NOT_IGNORABLE,
              vector=True,
              tag_id=2,
              to_string_method='ToStringCommaSeparated'),
          Field(
              'signature',
              SCALAR_FUNCTION_SIGNATURE,
              tag_id=3,
              ignorable=NOT_IGNORABLE,
              to_string_method='ToStringVerbose'),
          Field(
              'option_list',
              'ResolvedOption',
              ignorable=IGNORABLE_DEFAULT,
              tag_id=4,
              vector=True),
          Field(
              'procedure_body',
              SCALAR_STRING,
              ignorable=NOT_IGNORABLE,
              tag_id=5),
      ])

  gen.AddNode(
      name='ResolvedExecuteImmediateArgument',
      tag_id=143,
      parent='ResolvedArgument',
      comment="""
      An argument for an EXECUTE IMMEDIATE's USING clause.

        <name> an optional name for this expression
        <expression> the expression's value
             """,
      fields=[
          Field('name', SCALAR_STRING, tag_id=2),
          Field('expression', 'ResolvedExpr', tag_id=3),
      ])

  gen.AddNode(
      name='ResolvedExecuteImmediateStmt',
      tag_id=140,
      parent='ResolvedStatement',
      comment="""
      An EXECUTE IMMEDIATE statement
          EXECUTE IMMEDIATE <sql> [<into_clause>] [<using_clause>]

          <sql> a string expression indicating a SQL statement to be dynamically
            executed
          <into_identifier_list> the identifiers whose values should be set.
            Identifiers should not be repeated in the list.
          <using_argument_list> a list of arguments to supply for dynamic SQL.
             The arguments should either be all named or all unnamed, and
             arguments should not be repeated in the list.
              """,
      fields=[
          Field(
              'sql', 'ResolvedExpr', ignorable=NOT_IGNORABLE, tag_id=2),
          Field(
              'into_identifier_list',
              SCALAR_STRING,
              ignorable=NOT_IGNORABLE,
              tag_id=3,
              vector=True,
              to_string_method='ToStringCommaSeparated',
              java_to_string_method='toStringCommaSeparated'),
          Field(
              'using_argument_list',
              'ResolvedExecuteImmediateArgument',
              ignorable=NOT_IGNORABLE,
              tag_id=4,
              vector=True),
      ])

  gen.AddNode(
      name='ResolvedAssignmentStmt',
      tag_id=142,
      parent='ResolvedStatement',
      comment="""
      An assignment of a value to another value.
      """,
      fields=[
          Field(
              'target',
              'ResolvedExpr',
              tag_id=2,
              comment='Target of the assignment.  Currently, this will be '
              'either ResolvedSystemVariable, or a chain of ResolveGetField '
              'operations around it.'),
          Field(
              'expr',
              'ResolvedExpr',
              tag_id=3,
              comment='Value to assign into the target.  This will always be '
              'the same type as the target.')
      ])

  gen.Generate(input_file_paths, output_file_paths)


if __name__ == '__main__':
  app.run(main)
