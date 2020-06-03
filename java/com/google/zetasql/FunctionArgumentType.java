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

package com.google.zetasql;

import com.google.common.base.Preconditions;
import com.google.protobuf.TextFormat;
import com.google.zetasql.FunctionProtos.FunctionArgumentTypeOptionsProto;
import com.google.zetasql.FunctionProtos.FunctionArgumentTypeProto;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums.ArgumentCardinality;
import com.google.zetasql.ZetaSQLFunctions.SignatureArgumentKind;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A type for an argument or result value in a function signature.  Types
 * can be fixed or templated.  Arguments can be marked as repeated (denoting
 * it can occur zero or more times in a function invocation) or optional.
 * Result types cannot be marked as repeated or optional.  A
 * FunctionArgumentType is concrete if it is not templated and
 * numOccurrences indicates how many times the argument appears in a
 * concrete FunctionSignature.  FunctionArgumentTypeOptions can be used to
 * apply additional constraints on legal values for the argument.
 */
public final class FunctionArgumentType implements Serializable {

  private final SignatureArgumentKind kind;
  private final Type type;
  private final int numOccurrences;

  private transient FunctionArgumentTypeOptionsProto.Builder optionsBuilder =
      FunctionArgumentTypeOptionsProto.newBuilder();

  public FunctionArgumentType(
      SignatureArgumentKind kind, FunctionArgumentTypeOptionsProto options, int numOccurrences) {
    Preconditions.checkArgument(kind != SignatureArgumentKind.ARG_TYPE_FIXED);
    this.kind = kind;
    this.type = null;
    this.numOccurrences = numOccurrences;
    this.optionsBuilder.mergeFrom(options);
  }

  public FunctionArgumentType(Type type, FunctionArgumentTypeOptionsProto options,
                              int numOccurrences) {
    this.kind = SignatureArgumentKind.ARG_TYPE_FIXED;
    this.type = type;
    this.numOccurrences = numOccurrences;
    this.optionsBuilder.mergeFrom(options);
  }

  public FunctionArgumentType(
      SignatureArgumentKind kind, ArgumentCardinality cardinality, int numOccurrences) {
    Preconditions.checkArgument(kind != SignatureArgumentKind.ARG_TYPE_FIXED);
    this.kind = kind;
    this.type = null;
    this.numOccurrences = numOccurrences;
    this.optionsBuilder.setCardinality(cardinality);
  }

  public FunctionArgumentType(
      Type type, ArgumentCardinality cardinality, int numOccurrences) {
    this.kind = SignatureArgumentKind.ARG_TYPE_FIXED;
    this.type = type;
    this.numOccurrences = numOccurrences;
    this.optionsBuilder.setCardinality(cardinality);
  }

  public FunctionArgumentType(Type type, ArgumentCardinality cardinality) {
    this(type, cardinality, -1);
  }

  public FunctionArgumentType(SignatureArgumentKind kind) {
    this(kind, ArgumentCardinality.REQUIRED, -1);
  }

  public FunctionArgumentType(Type type) {
    this(type, ArgumentCardinality.REQUIRED, -1);
  }

  public boolean isConcrete() {
    return kind == SignatureArgumentKind.ARG_TYPE_FIXED && numOccurrences >= 0;
  }

  public int getNumOccurrences() {
    return numOccurrences;
  }

  public ArgumentCardinality getCardinality() {
    return optionsBuilder.getCardinality();
  }

  public boolean isRepeated() {
    return getCardinality() == ArgumentCardinality.REPEATED;
  }

  public boolean isRequired() {
    return getCardinality() == ArgumentCardinality.REQUIRED;
  }

  public boolean isOptional() {
    return getCardinality() == ArgumentCardinality.OPTIONAL;
  }

  /**
   * Returns Type of the argument when it's fixed, or null if it's templated.
   */
  @Nullable
  public Type getType() {
    return type;
  }

  public SignatureArgumentKind getKind() {
    return kind;
  }

  public String debugString(boolean verbose) {
    StringBuilder builder = new StringBuilder();
    if (isRepeated()) {
      builder.append("repeated");
    } else if (isOptional()) {
      builder.append("optional");
    }

    if (!isRequired()) {
      if (isConcrete()) {
        builder.append("(" + numOccurrences + ") ");
      } else {
        builder.append(" ");
      }
    }

    if (type != null) {
      builder.append(type.debugString());
    } else if (kind == SignatureArgumentKind.ARG_TYPE_ARBITRARY) {
      builder.append("ANY TYPE");
    } else {
      builder.append(signatureArgumentKindToString(kind));
    }

    if (verbose) {
      // We want to include the equivalent of the shortDebugString of the proto, but with the
      // cardinality, extra_relation_input_columns_allowed, and argument_name fields excluded.
      FunctionArgumentTypeOptionsProto.Builder tempBuilder =
          FunctionArgumentTypeOptionsProto.newBuilder(optionsBuilder.build());
      tempBuilder.clearCardinality();
      tempBuilder.clearExtraRelationInputColumnsAllowed();
      tempBuilder.clearArgumentName();
      tempBuilder.clearArgumentNameIsMandatory();

      String options = TextFormat.shortDebugString(tempBuilder);
      if (!options.isEmpty()) {
        builder.append(" {");
        builder.append(options);
        builder.append("}");
      }
    }

    if (optionsBuilder.hasArgumentName()) {
      builder.append(" ");
      builder.append(optionsBuilder.getArgumentName());
    }

    return builder.toString();
  }

  public String debugString() {
    return debugString(false);
  }

  @Override
  public String toString() {
    return debugString(false);
  }

  private static String signatureArgumentKindToString(SignatureArgumentKind kind) {
    switch (kind) {
      case ARG_ARRAY_TYPE_ANY_1:
        return "<array<T1>>";
      case ARG_ARRAY_TYPE_ANY_2:
        return "<array<T2>>";
      case ARG_ENUM_ANY:
        return "<enum>";
      case ARG_PROTO_ANY:
        return "<proto>";
      case ARG_STRUCT_ANY:
        return "<struct>";
      case ARG_TYPE_ANY_1:
        return "<T1>";
      case ARG_TYPE_ANY_2:
        return "<T2>";
      case ARG_TYPE_ARBITRARY:
        return "<arbitrary>";
      case ARG_TYPE_FIXED:
        return "FIXED";
      case ARG_TYPE_VOID:
        return "<void>";
      case __SignatureArgumentKind__switch_must_have_a_default__:
      default:
        return "UNKNOWN_ARG_KIND";
    }
  }

  public FunctionArgumentTypeProto serialize(FileDescriptorSetsBuilder fileDescriptorSetsBuilder) {
    FunctionArgumentTypeProto.Builder builder = FunctionArgumentTypeProto.newBuilder();
    builder.setKind(kind);
    builder.setNumOccurrences(numOccurrences);
    builder.setOptions(optionsBuilder);
    if (type != null) {
      type.serialize(builder.getTypeBuilder(), fileDescriptorSetsBuilder);
    }
    return builder.build();
  }

  public static FunctionArgumentType deserialize(
      FunctionArgumentTypeProto proto, List<ZetaSQLDescriptorPool> pools) {
    SignatureArgumentKind kind = proto.getKind();
    TypeFactory factory = TypeFactory.nonUniqueNames();

    if (kind == SignatureArgumentKind.ARG_TYPE_FIXED) {
      return new FunctionArgumentType(
          factory.deserialize(proto.getType(), pools), proto.getOptions(),
                              proto.getNumOccurrences());
    } else {
      return new FunctionArgumentType(kind, proto.getOptions(), proto.getNumOccurrences());
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    out.writeObject(optionsBuilder.build());
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    FunctionArgumentTypeOptionsProto options = (FunctionArgumentTypeOptionsProto) in.readObject();
    optionsBuilder = options.toBuilder();
  }
}
