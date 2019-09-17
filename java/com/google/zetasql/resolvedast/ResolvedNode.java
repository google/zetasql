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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import com.google.zetasql.AnyResolvedNodeProto;
import com.google.zetasql.FileDescriptorSetsBuilder;
import com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind;
import com.google.zetasql.ResolvedNodeProto;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This is the base class for the resolved AST. Subclasses are in the generated file
 * ResolvedNodes.java. ResolvedNodeKind enum is in the generated file resolved_node_kind.proto.
 * See gen_resolved_ast.py.
 *
 * <p> View code generated classes at (broken link).
 * HTML documentation for the class hierarchy is generated in resolved_ast.html.
 * A viewable copy is available at (broken link).
 *
 * <p> In this hierarchy, classes are either abstract or leaves.
 */
public abstract class ResolvedNode implements Serializable {

  @SuppressWarnings("unused")
  protected ResolvedNode(ResolvedNodeProto proto, DeserializationHelper helper) {
    // Nothing to deserialize for now.
  }

  /**
   * Deserializes {@code proto} into a sub class of {@link ResolvedNode}. The
   * {@link DeserializationHelper} is used to deserialize types which are passed by reference. This
   * should only be used by the ZetaSQL implementation.
   */
  public static ResolvedNode deserialize(AnyResolvedNodeProto proto, DeserializationHelper helper) {
    return ResolvedNodes.deserialize(proto, helper);
  }

  /**
   * Base case for recursive call in derived AST nodes.
   */
  public Message serialize(
      @SuppressWarnings("unused") FileDescriptorSetsBuilder fileDescriptorSetsBuilder) {
    return ResolvedNodeProto.newBuilder().build();
  }

  public abstract ResolvedNodeKind nodeKind();

  public abstract String nodeKindString();

  /** Accepts a visitor, which can process the node and optionally redispatch to children. */
  public abstract void accept(ResolvedNodes.Visitor visitor);

  /**
   * Accepts a visitor, dispatching to the children of this node.
   *
   * <p>Note: this function is made non-abstract to allow all generated ResolvedNode implementations
   * to perform a {@code super.acceptChildren} call, which ensures all children nodes are sent to
   * {@code visitor}.
   */
  protected void acceptChildren(ResolvedNodes.Visitor visitor) {}

  @Override
  public final String toString() {
    return debugString();
  }

  /** Class used to collect all fields that should be printed in debugString. */
  static class DebugStringField {

    // If name is non-empty, "<name>=" will be printed in front of the values.
    private final String name;

    // One of the two fields below will be filled in.
    // If nodes is non-empty, all nodes are non-NULL.
    private final String value; // Print this value directly.
    private final ImmutableList<? extends ResolvedNode> nodes; // Print debugString for these nodes.

    DebugStringField(String name, String value) {
      this.name = Preconditions.checkNotNull(name);
      this.value = Preconditions.checkNotNull(value);
      this.nodes = ImmutableList.of();
    }

    DebugStringField(String name, ImmutableList<? extends ResolvedNode> nodes) {
      this.name = Preconditions.checkNotNull(name);
      this.value = null;
      this.nodes = Preconditions.checkNotNull(nodes);
    }

    DebugStringField(String name, ResolvedNode node) {
      this.name = Preconditions.checkNotNull(name);
      this.value = null;
      this.nodes = ImmutableList.of(node);
    }
  }

  public final String debugString() {
    StringBuilder sb = new StringBuilder();
    debugStringImpl("", "", sb);
    return sb.toString();
  }

  /**
   * Print the tree recursively. The implementation is nearly identical to the C++ implementation in
   * ResolvedNode::DebugStringImpl. The outputs need to be identical so the implementations should
   * be kept similar to make changes to the output easier.
   *
   * @param prefix1 is the indentation to attach to child nodes.
   * @param prefix2 is the indentation to attach to the root of this tree.
   * @param sb is the output StringBuilder
   */
  private void debugStringImpl(String prefix1, String prefix2, StringBuilder sb) {
    List<DebugStringField> fields = new ArrayList<>();
    collectDebugStringFields(fields);

    // Use multiline DebugString format if any of the fields are ResolvedNodes.
    boolean multiline = false;
    for (DebugStringField field : fields) {
      if (!field.nodes.isEmpty()) {
        multiline = true;
        break;
      }
    }

    sb.append(prefix2).append(getNameForDebugString());
    if (fields.isEmpty()) {
      sb.append("\n");
    } else if (multiline) {
      sb.append("\n");
      for (DebugStringField field : fields) {
        boolean printFieldName = !field.name.isEmpty();
        boolean printOneLine = field.nodes.isEmpty();

        if (printFieldName) {
          sb.append(prefix1).append("+-").append(field.name).append("=");
          if (printOneLine) {
            sb.append(field.value);
          }
          sb.append("\n");
        } else if (printOneLine) {
          sb.append(prefix1).append("+-").append(field.value).append("\n");
        }

        if (!printOneLine) {
          for (ResolvedNode node : field.nodes) {
            Preconditions.checkState(node != null);
            String fieldNameIndent =
                printFieldName ? (field != fields.get(fields.size() - 1) ? "| " : "  ") : "";
            String fieldValueIndent = node != field.nodes.get(field.nodes.size() - 1) ? "| " : "  ";
            node.debugStringImpl(
                prefix1 + fieldNameIndent + fieldValueIndent, prefix1 + fieldNameIndent + "+-", sb);
          }
        }
      }
    } else {
      sb.append("(");
      for (DebugStringField field : fields) {
        if (field != fields.get(0)) {
          sb.append(", ");
        }
        if (field.name.isEmpty()) {
          sb.append(field.value);
        } else {
          sb.append(field.name).append("=").append(field.value);
        }
      }
      sb.append(")\n");
    }
  }

  /**
   * Add all fields that should be printed in debugString to {@code fields}. Implementations should
   * recursively call same method in the superclass.
   */
  @SuppressWarnings("unused")
  protected void collectDebugStringFields(List<DebugStringField> fields) {}

  /**
   * Get the name string displayed for this node in the debugString. Normally it's just
   * nodeKindString(), but it can be customized.
   */
  protected String getNameForDebugString() {
    return nodeKindString();
  }

  /**
   * Return true if the fields returned from collectDebugStringFields would contain any field with a
   * non-empty {@code nodes} list. These are the fields that would have child nodes in the
   * debugString tree.
   */
  private boolean hasDebugStringFieldsWithNodes() {
    List<DebugStringField> fields = new ArrayList<>();
    collectDebugStringFields(fields);
    for (DebugStringField field : fields) {
      if (!field.nodes.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /**
   * collectDebugStringFieldsWithNameFormat and getNameForDebugStringWithNameFormat are helpers used
   * in custom format methods to format nodes as an assignment, "name := node", with node's children
   * as children of this node.
   */
  protected final void collectDebugStringFieldsWithNameFormat(List<DebugStringField> fields) {
    Preconditions.checkArgument(fields.isEmpty());
    if (hasDebugStringFieldsWithNodes()) {
      fields.add(new DebugStringField("", this));
    } else {
      collectDebugStringFields(fields);
    }
  }

  protected final String getNameForDebugStringWithNameFormat(String name) {
    if (hasDebugStringFieldsWithNodes()) {
      return name + " :=";
    } else {
      return name + " := " + getNameForDebugString();
    }
  }
}
