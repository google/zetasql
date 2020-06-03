/*
 * Copyright 2019 ZetaSQL Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.google.zetasql.resolvedast;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.ProtocolMessageEnum;
import com.google.zetasql.Connection;
import com.google.zetasql.Constant;
import com.google.zetasql.FunctionSignature;
import com.google.zetasql.ZetaSQLDescriptorPool.ZetaSQLFieldDescriptor;
import com.google.zetasql.ZetaSQLStrings;
import com.google.zetasql.Model;
import com.google.zetasql.Procedure;
import com.google.zetasql.ResolvedFunctionCallInfo;
import com.google.zetasql.TVFSignature;
import com.google.zetasql.Table;
import com.google.zetasql.TableValuedFunction;
import com.google.zetasql.Type;
import com.google.zetasql.TypeAnnotationProto.FieldFormat;
import com.google.zetasql.Value;
import com.google.zetasql.resolvedast.ResolvedFunctionCallBaseEnums.ErrorMode;
import com.google.zetasql.resolvedast.ResolvedInsertStmtEnums.InsertMode;
import com.google.zetasql.resolvedast.ResolvedNode.DebugStringField;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCast;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedComputedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedConstant;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExtendedCastInfo;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedFunctionCallBase;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedMakeProtoField;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedOption;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedOutputColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedSystemVariable;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWindowFrame;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWindowFrameExpr;
import com.google.zetasql.resolvedast.ResolvedStatementEnums.ObjectAccess;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper functions for generating debug strings for {@link ResolvedNode}s.
 */
class DebugStrings {

  // isDefaultValue functions for different node field types, similar to the C++ implementations in
  // zetasql/resolved_ast/resolved_ast.cc.template

  static boolean isDefaultValue(ProtocolMessageEnum e) {
    return e.getNumber() == 0;
  }

  static boolean isDefaultValue(List<?> l) {
    return l.isEmpty();
  }

  static boolean isDefaultValue(boolean b) {
    return !b;
  }

  static boolean isDefaultValue(String s) {
    return s.isEmpty();
  }

  @SuppressWarnings("unused")
  static boolean isDefaultValue(Type t) {
    return false;
  }

  static boolean isDefaultValue(Value v) {
    return !v.isValid();
  }

  static boolean isDefaultValue(FunctionSignature signature) {
    return signature.isDefaultValue();
  }

  static boolean isDefaultValue(ResolvedFunctionCallInfo call) {
    return call.isDefaultValue();
  }

  static boolean isDefaultValue(TableValuedFunction tvf) {
    return tvf.isDefaultValue();
  }

  static boolean isDefaultValue(TVFSignature signature) {
    return signature.isDefaultValue();
  }

  static boolean isDefaultValue(ResolvedColumn column) {
    return column.isDefaultValue();
  }

  // Used for positional parameters.
  static boolean isDefaultValue(long position) {
    return position == 0;
  }

  // toStringImpl functions for different node field types, similar to the C++ implementation in
  // zetasql/resolved_ast/resolved_ast.cc.template

  static String toStringImpl(String s) {
    return ZetaSQLStrings.toStringLiteral(s);
  }

  static String toStringImpl(boolean b) {
    return b ? "TRUE" : "FALSE";
  }

  static String toStringImpl(long i) {
    return Long.toString(i);
  }

  static String toStringImpl(InsertMode mode) {
    return mode.name().replace('_', ' ');
  }

  static String toStringImpl(Enum<?> value) {
    return value.name();
  }

  static String toStringImpl(Model model) {
    return model.getFullName();
  }

  static String toStringImpl(Connection connection) {
    return connection.getFullName();
  }

  static String toStringImpl(Table table) {
    return table.getFullName();
  }

  static String toStringImpl(Type type) {
    return type.debugString();
  }

  static String toStringImpl(ZetaSQLFieldDescriptor field) {
    FieldDescriptor descriptor = field.getDescriptor();
    return descriptor.isExtension() ? "[" + descriptor.getFullName() + "]" : descriptor.getName();
  }

  static String toStringImpl(ResolvedColumn column) {
    return column.debugString();
  }

  static String toStringImpl(Constant constant) {
    return constant.toString();
  }

  static String toStringImpl(FunctionSignature signature) {
    return signature.toString();
  }

  static String toStringImpl(ResolvedFunctionCallInfo call) {
    return call.toString();
  }

  static String toStringImpl(TableValuedFunction tvf) {
    return tvf.toString();
  }

  static String toStringImpl(TVFSignature signature) {
    return signature.toString();
  }

  static String toStringImpl(Procedure procedure) {
    return procedure.toString();
  }

  // To deal with type erasure, every toString function for repeated fields
  // must have a different signature or different name. For now, it's fine to
  // use ImmutableList<ResolvedColumn> here and List<String> below. This is
  // hacky but it works. A third function could use Iterable<T> or we could use
  // different names for each type.
  static String toStringImpl(ImmutableList<ResolvedColumn> columns) {
    return ResolvedColumn.toString(columns);
  }

  static String toStringImpl(Value value) {
    return value.shortDebugString();
  }

  static String toStringImpl(List<String> values, String separator) {
    StringBuilder sb = new StringBuilder();
    for (String value : values) {
      if (sb.length() > 0) {
        sb.append(separator);
      }
      sb.append(ZetaSQLStrings.toIdentifierLiteral(value));
    }
    return sb.toString();
  }

  // Most vector<string> fields are identifier paths so we format
  // the value that way by default.
  // For other vector<string> fields, we can override this with to_string_method.
  static String toStringImpl(List<String> values) {
    return toStringImpl(values, ".");
  }

  static String toStringPeriodSeparatedForFieldDescriptors(List<ZetaSQLFieldDescriptor> fields) {
    StringBuilder sb = new StringBuilder();
    for (ZetaSQLFieldDescriptor field : fields) {
      if (sb.length() > 0) {
        sb.append(".");
      }
      sb.append(toStringImpl(field));
    }
    return sb.toString();
  }

  static String toStringCommaSeparatedForInt(List<Long> values) {
    return "[" + Joiner.on(", ").join(values) + "]";
  }

  static String toStringVerbose(FunctionSignature signature) {
    return signature.debugString(/*functionName=*/"", /*verbose=*/true);
  }

  // Custom implementation for the list of enums. Named uniquely to avoid collisions with any other
  // method.
  static String toStringObjectAccess(ImmutableList<ObjectAccess> values) {
    StringBuilder sb = new StringBuilder();
    for (ObjectAccess value : values) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(value.name());
    }
    return sb.toString();
  }

  // This formats a list of identifiers (quoting if needed).
  static String toStringCommaSeparated(List<String> values) {
    return "[" + toStringImpl(values, ", ") + "]";
  }

  // Functions for classes in the generated code with customized debugStrings. These functions
  // should only be called from the generated code in ResolvedNodes.java. The implementations are
  // nearly identical to the C++ implementations in
  // ResolvedComputedColumn::CollectDebugStringFields

  /**
   * ResolvedComputedColumn gets formatted as "name := expr" with expr's children printed as its own
   * children.
   */
  static void collectDebugStringFields(ResolvedComputedColumn node, List<DebugStringField> fields) {
      node.getExpr().collectDebugStringFieldsWithNameFormat(fields);
  }

  static String getNameForDebugString(ResolvedComputedColumn node) {
    return node.getExpr().getNameForDebugStringWithNameFormat(node.getColumn().shortDebugString());
  }

  /**
   * ResolvedOutputColumn gets formatted as "column AS name [column.type]".
   */
  @SuppressWarnings("unused")
  static void collectDebugStringFields(ResolvedOutputColumn node, List<DebugStringField> fields) {
    Preconditions.checkState(fields.isEmpty());
  }

  static String getNameForDebugString(ResolvedOutputColumn node) {
    return node.getColumn().debugString() + " AS "
        + ZetaSQLStrings.toIdentifierLiteral(node.getName())
        + " [" + node.getColumn().getType().debugString() + "]";
  }

  /**
   * ResolvedConstant gets formatted as Constant(name(constant), type[, value]).
   */
  static void collectDebugStringFields(
      ResolvedConstant node, List<DebugStringField> fields) {
    Preconditions.checkArgument(fields.size() <= 1);

    // The base class, ResolvedExpr, has its implementation called first, so when we get here,
    // <fields> is already populated with the type.  Insert the name first, then the value last,
    // to match the behavior of the C++ implementation, so that tests which rely on the debug string
    // output can pass.
    fields.add(0, new DebugStringField("", node.getConstant().getFullName()));
    fields.add(new DebugStringField("value", node.getConstant().getValue().debugString()));
  }

  static String getNameForDebugString(ResolvedConstant node) {
    return "Constant";
  }

  /**
   * ResolvedFunctionCall gets formatted as "FunctionCall(name(arg_types) -> type)" with only
   * arguments printed as children.
   */
  static void collectDebugStringFields(
      ResolvedFunctionCallBase node, List<DebugStringField> fields) {
    Preconditions.checkArgument(fields.size() <= 1);

    fields.clear();
    if (!node.getArgumentList().isEmpty()) {
      // Use empty name to avoid printing "arguments=" with extra indentation.
      fields.add(new DebugStringField("", node.getArgumentList()));
    }
  }

  static String getNameForDebugString(ResolvedFunctionCallBase node) {
    return node.nodeKindString() + "("
        + (node.getErrorMode() == ErrorMode.SAFE_ERROR_MODE ? "{SAFE_ERROR_MODE} " : "")
        + (node.getFunction() != null ? node.getFunction().toString() : "<unknown>")
        + node.getSignature().toString() + ")";
  }

  /**
   * ResolvedCast gets formatted as "Cast(from_type -> to_type)" with only from_expr printed as
   * a child.
   */
  static void collectDebugStringFields(ResolvedCast node, List<DebugStringField> fields) {
    Preconditions.checkArgument(fields.size() <= 1);

   fields.clear();
   if (node.getExpr() != null) {
     // Use empty name to avoid printing "arguments=" with extra indentation.
     fields.add(new DebugStringField("", node.getExpr()));
   }
   if (node.getReturnNullOnError()) {
     fields.add(new DebugStringField("return_null_on_error", "TRUE"));
   }
    if (node.getExtendedCast() != null) {
      fields.add(new DebugStringField("extended_cast", node.getExtendedCast()));
    }
  }

  static String getNameForDebugString(ResolvedCast node) {
   return "Cast(" + node.getExpr().getType().debugString() + " -> "
       + node.getType().debugString() + ")";
  }

  /** ResolvedExtendedCastInfo gets formatted as "ResolvedExtendedCastInfo(function=name)". */
  static void collectDebugStringFields(
      ResolvedExtendedCastInfo node, List<DebugStringField> fields) {
    Preconditions.checkArgument(fields.size() <= 1);
  }

  static String getNameForDebugString(ResolvedExtendedCastInfo node) {
    return node.nodeKindString()
        + "(function="
        + (node.getFunction() != null ? node.getFunction().toString() : "<unknown>")
        + ")";
  }

  /**
   * ResolvedMakeProtoField gets formatted as "field[(format=TIMESTAMP_MILLIS)] := expr" with expr's
   * children printed as its own children. The required proto format is shown in parentheses when
   * present. expr is normally just a ResolvedColumnRef, but could be a cast expr.
   */
  static void collectDebugStringFields(ResolvedMakeProtoField node, List<DebugStringField> fields) {
    node.getExpr().collectDebugStringFieldsWithNameFormat(fields);
  }

  static String getNameForDebugString(ResolvedMakeProtoField node) {
    String name;

    FieldDescriptor descriptor = node.getFieldDescriptor().getDescriptor();
    if (descriptor.isExtension()) {
      name = "[" + descriptor.getFullName() + "]";
    } else {
      name = descriptor.getName();
    }

    // If the MakeProtoFieldNode has any modifiers present, add them
    // in parentheses on the field name.
    List<String> modifiers = new ArrayList<>();
    if (node.getFormat() != FieldFormat.Format.DEFAULT_FORMAT) {
      modifiers.add("format=" + node.getFormat().name());
    }

    if (!modifiers.isEmpty()) {
      name = name + "(" + Joiner.on(",").join(modifiers) + ")";
    }
    return node.getExpr().getNameForDebugStringWithNameFormat(name);
  }

  /** ResolvedOption gets formatted as "[qualifier.]name := value" */
  static void collectDebugStringFields(ResolvedOption node, List<DebugStringField> fields) {
    node.getValue().collectDebugStringFieldsWithNameFormat(fields);
  }

  static String getNameForDebugString(ResolvedOption node) {
    String prefix = node.getQualifier().isEmpty() ? ""
        : ZetaSQLStrings.toIdentifierLiteral(node.getQualifier()) + ".";

    return node.getValue().getNameForDebugStringWithNameFormat(
        prefix + ZetaSQLStrings.toIdentifierLiteral(node.getName()));
  }

  static void collectDebugStringFields(ResolvedWindowFrame node, List<DebugStringField> fields) {
    fields.add(new DebugStringField("start_expr", node.getStartExpr()));
    fields.add(new DebugStringField("end_expr", node.getEndExpr()));
  }

  static String getNameForDebugString(ResolvedWindowFrame node) {
    return node.nodeKindString() + "(frame_unit=" + node.getFrameUnit().name() + ")";
  }

  static void collectDebugStringFields(
      ResolvedWindowFrameExpr node, List<DebugStringField> fields) {
    if (node.getExpression() != null) {
      // Use empty name to avoid printing "expression=" with extra indentation.
      fields.add(new DebugStringField("", node.getExpression()));
    }
  }

  static String getNameForDebugString(ResolvedWindowFrameExpr node) {
    return node.nodeKindString()
        + "(boundary_type=" + node.getBoundaryType().name().replace('_', ' ') + ")";
  }

  static void collectDebugStringFields(ResolvedSystemVariable node, List<DebugStringField> fields) {
    fields.clear();
    ArrayList<String> pathParts = new ArrayList<>();
    for (String pathPart : node.getNamePath()) {
      pathParts.add(ZetaSQLStrings.toIdentifierLiteral(pathPart));
    }

    fields.add(new DebugStringField("", String.join(".", pathParts)));
    fields.add(new DebugStringField("type", node.getType().toString()));
  }

  static String getNameForDebugString(ResolvedSystemVariable node) {
    return node.nodeKindString();
  }
}
