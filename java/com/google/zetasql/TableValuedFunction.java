/*
 * Copyright 2019 Google LLC
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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.zetasql.FunctionProtos.TVFRelationColumnProto;
import com.google.zetasql.FunctionProtos.TVFRelationProto;
import com.google.zetasql.FunctionProtos.TableValuedFunctionOptionsProto;
import com.google.zetasql.FunctionProtos.TableValuedFunctionProto;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums.TableValuedFunctionType;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums.Volatility;
import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * This interface describes a table-valued function (TVF) available in a query engine.
 *
 * <p>More information in zetasql/public/table_valued_function.h
 */
public abstract class TableValuedFunction implements Serializable {

  private final ImmutableList<String> namePath;
  private final FunctionSignature signature;
  private final ImmutableList<TVFRelation.Column> columns;
  @Nullable private final String customContext;
  @Nullable private final Volatility volatility;
  private final TableValuedFunctionOptionsProto options;

  /**
   * Constructs a new TVF object with the given name and argument signature.
   *
   * <p>Each TVF may accept value or relation arguments. The signature specifies whether each
   * argument should be a value or a relation. For a value argument, the signature may specify a
   * concrete {@code Type} or a (possibly templated) {@code SignatureArgumentKind}. For relation
   * arguments, the signature should use {@code ARG_TYPE_RELATION}, and any relation will be
   * accepted as an argument.
   */
  protected TableValuedFunction(
      ImmutableList<String> namePath,
      FunctionSignature signature,
      ImmutableList<TVFRelation.Column> columns,
      @Nullable String customContext,
      @Nullable Volatility volatility,
      TableValuedFunctionOptionsProto options) {
    this.namePath = namePath;
    this.signature = signature;
    this.columns = columns;
    this.customContext = customContext;
    this.volatility = volatility;
    this.options = options;
  }

  /** Deserializes a table-valued function from a protocol buffer. */
  public static TableValuedFunction deserialize(
      TableValuedFunctionProto proto,
      ImmutableList<? extends DescriptorPool> pools,
      TypeFactory typeFactory) {
    switch (proto.getType()) {
      case FIXED_OUTPUT_SCHEMA_TVF:
        return FixedOutputSchemaTVF.deserialize(proto, pools, typeFactory);
      case FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_TVF:
        return ForwardInputSchemaToOutputSchemaTVF.deserialize(proto, pools, typeFactory);
      case TEMPLATED_SQL_TVF:
        return TemplatedSQLTVF.deserialize(proto, pools, typeFactory);
      case FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_WITH_APPENDED_COLUMNS:
        return ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF.deserialize(
            proto, pools, typeFactory);
      default:
        throw new IllegalArgumentException(
            "Serialization is not implemented yet for table-valued function: "
                + String.join(".", proto.getNamePathList()));
    }
  }

  /**
   * Serializes this table-valued function to a protocol buffer. Each TVF only supports a single
   * signature for now, which we represent with a FunctionSignatureProto within 'proto'.
   */
  public TableValuedFunctionProto serialize(FileDescriptorSetsBuilder fileDescriptorSetsBuilder) {
    TableValuedFunctionProto.Builder builder =
        TableValuedFunctionProto.newBuilder()
            .addAllNamePath(namePath)
            .setSignature(signature.serialize(fileDescriptorSetsBuilder))
            .setOptions(options);
    builder.setType(getType());
    if (getCustomContext() != null) {
      builder.setCustomContext(getCustomContext());
    }
    if (getVolatility() != null) {
      builder.setVolatility(getVolatility());
    }
    switch (getType()) {
      case FIXED_OUTPUT_SCHEMA_TVF:
      case FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_TVF:
        break;
      case FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_WITH_APPENDED_COLUMNS:
        TVFRelationProto.Builder relationBuilder = TVFRelationProto.newBuilder();
        for (TVFRelation.Column column : columns) {
          relationBuilder.addColumn(
              TVFRelationColumnProto.newBuilder()
                  .setName(column.getName())
                  .setType(column.getType().serialize(fileDescriptorSetsBuilder))
                  .build());
        }
        builder.setCustomContext(new String(relationBuilder.build().toByteArray(), UTF_8));
        break;
      case TEMPLATED_SQL_TVF:
        for (String name : ((TemplatedSQLTVF) this).argumentNames) {
          builder.addArgumentName(name);
        }
        builder.setParseResumeLocation(((TemplatedSQLTVF) this).parseResumeLocation.serialize());
        break;
      default:
        throw new IllegalArgumentException(
            "Serialization is not implemented yet for table-valued function: " + getFullName());
    }
    return builder.build();
  }

  abstract FunctionEnums.TableValuedFunctionType getType();

  public String getName() {
    return namePath.get(namePath.size() - 1);
  }

  public ImmutableList<String> getNamePath() {
    return namePath;
  }

  public FunctionSignature getFunctionSignature() {
    return signature;
  }

  public String getFullName() {
    return getFullName(true);
  }

  public String getFullName(boolean includeGroup) {
    return Joiner.on('.').join(namePath);
  }

  @Nullable
  public String getCustomContext() {
    return customContext;
  }

  @Nullable
  public Volatility getVolatility() {
    return volatility;
  }

  @Override
  public final String toString() {
    return toDebugString(true);
  }

  public String toDebugString(boolean verbose) {
    return getName() + "(" + getFunctionSignature().debugString("", verbose) + ")";
  }

  public boolean isDefaultValue() {
    return true;
  }

  /** A TVF that always returns a relation with the same fixed output schema. */
  public static class FixedOutputSchemaTVF extends TableValuedFunction {
    private final TVFRelation outputSchema;

    public FixedOutputSchemaTVF(
        ImmutableList<String> namePath,
        FunctionSignature signature,
        TVFRelation outputSchema) {
      this(namePath, signature, outputSchema, TableValuedFunctionOptionsProto.getDefaultInstance());
    }

    public FixedOutputSchemaTVF(
        ImmutableList<String> namePath,
        FunctionSignature signature,
        TVFRelation outputSchema,
        TableValuedFunctionOptionsProto options) {
      super(namePath, signature, ImmutableList.of(), null, null, options);
      this.outputSchema = outputSchema;
    }

    public TVFRelation getOutputSchema() {
      return outputSchema;
    }

    @Override
    public FunctionEnums.TableValuedFunctionType getType() {
      return FunctionEnums.TableValuedFunctionType.FIXED_OUTPUT_SCHEMA_TVF;
    }

    /** Deserializes this table-valued function from a protocol buffer. */
    public static FixedOutputSchemaTVF deserialize(
        TableValuedFunctionProto proto,
        ImmutableList<? extends DescriptorPool> pools,
        TypeFactory typeFactory) {
      ImmutableList<String> namePath = ImmutableList.copyOf(proto.getNamePathList());
      FunctionSignature signature = FunctionSignature.deserialize(proto.getSignature(), pools);
      boolean hasRelationInputSchema =
          proto.getSignature().getReturnType().getOptions().hasRelationInputSchema();
      Preconditions.checkArgument(hasRelationInputSchema, proto);
      Preconditions.checkArgument(
          proto.getType() == FunctionEnums.TableValuedFunctionType.FIXED_OUTPUT_SCHEMA_TVF, proto);
      return new FixedOutputSchemaTVF(
          namePath,
          signature,
          TVFRelation.deserialize(
              proto.getSignature().getReturnType().getOptions().getRelationInputSchema(),
              pools,
              typeFactory),
          proto.getOptions());
    }
  }

  /**
   * This represents a TVF that accepts a relation for its first argument. The TVF returns a
   * relation with the same output schema as this input relation.
   */
  public static class ForwardInputSchemaToOutputSchemaTVF extends TableValuedFunction {

    public ForwardInputSchemaToOutputSchemaTVF(
        ImmutableList<String> namePath, FunctionSignature signature) {
      this(namePath, signature, null, null);
    }

    public ForwardInputSchemaToOutputSchemaTVF(
        ImmutableList<String> namePath,
        FunctionSignature signature,
        TableValuedFunctionOptionsProto options) {
      this(namePath, signature, null, null, options);
    }

    public ForwardInputSchemaToOutputSchemaTVF(
        ImmutableList<String> namePath,
        FunctionSignature signature,
        @Nullable String customContext,
        @Nullable Volatility volatility) {
      this(namePath, signature, customContext, volatility,
           TableValuedFunctionOptionsProto.getDefaultInstance());
    }

    public ForwardInputSchemaToOutputSchemaTVF(
        ImmutableList<String> namePath,
        FunctionSignature signature,
        @Nullable String customContext,
        @Nullable Volatility volatility,
        TableValuedFunctionOptionsProto options) {
      super(namePath, signature, ImmutableList.of(), customContext, volatility, options);
    }

    @Override
    public FunctionEnums.TableValuedFunctionType getType() {
      return FunctionEnums.TableValuedFunctionType.FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_TVF;
    }

    /** Deserializes this table-valued function from a protocol buffer. */
    public static ForwardInputSchemaToOutputSchemaTVF deserialize(
        TableValuedFunctionProto proto,
        ImmutableList<? extends DescriptorPool> pools,
        TypeFactory typeFactory) {
      Preconditions.checkArgument(
          proto.getType()
              == FunctionEnums.TableValuedFunctionType.FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_TVF,
          proto);
      String customContext = proto.hasCustomContext() ? proto.getCustomContext() : null;
      Volatility volatility = proto.hasVolatility() ? proto.getVolatility() : null;
      ImmutableList<String> namePath = ImmutableList.copyOf(proto.getNamePathList());
      FunctionSignature signature = FunctionSignature.deserialize(proto.getSignature(), pools);
      return new ForwardInputSchemaToOutputSchemaTVF(
          namePath, signature, customContext, volatility, proto.getOptions());
    }
  }

  /**
   * This represents a templated function with a SQL body.
   *
   * <p>The purpose of this class is to help support statements of the form "{@code CREATE FUNCTION
   * <name>(<arguments>) AS <query>}", where the <arguments> may have templated types like "{@code
   * ANY TYPE}". In this case, ZetaSQL cannot resolve the function expression right away and must
   * defer this work until later when the function is called with concrete argument types. A TVF
   * that always returns a relation with the same fixed output schema.
   */
  public static class TemplatedSQLTVF extends TableValuedFunction {
    private final ImmutableList<String> argumentNames;
    private final ParseResumeLocation parseResumeLocation;

    public TemplatedSQLTVF(
        ImmutableList<String> namePath,
        FunctionSignature signature,
        ImmutableList<String> argumentNames,
        ParseResumeLocation parseResumeLocation) {
      this(namePath, signature, argumentNames, parseResumeLocation,
           TableValuedFunctionOptionsProto.getDefaultInstance());
    }

    public TemplatedSQLTVF(
        ImmutableList<String> namePath,
        FunctionSignature signature,
        ImmutableList<String> argumentNames,
        ParseResumeLocation parseResumeLocation,
        TableValuedFunctionOptionsProto options) {
      super(namePath, signature, ImmutableList.of(), null, null, options);
      this.argumentNames = argumentNames;
      this.parseResumeLocation = parseResumeLocation;
    }

    public ImmutableList<String> getArgumentNames() {
      return argumentNames;
    }

    public String getSqlBody() {
      return this.parseResumeLocation
          .getInput()
          .substring(this.parseResumeLocation.getBytePosition());
    }

    @Override
    public FunctionEnums.TableValuedFunctionType getType() {
      return FunctionEnums.TableValuedFunctionType.TEMPLATED_SQL_TVF;
    }

    /** Deserializes this table-valued function from a protocol buffer. */
    public static TemplatedSQLTVF deserialize(
        TableValuedFunctionProto proto,
        ImmutableList<? extends DescriptorPool> pools,
        TypeFactory typeFactory) {
      Preconditions.checkArgument(
          proto.getType() == FunctionEnums.TableValuedFunctionType.TEMPLATED_SQL_TVF, proto);
      ImmutableList<String> namePath = ImmutableList.copyOf(proto.getNamePathList());
      FunctionSignature signature = FunctionSignature.deserialize(proto.getSignature(), pools);
      ImmutableList<String> args = ImmutableList.copyOf(proto.getArgumentNameList());
      return new TemplatedSQLTVF(
          namePath, signature, args, new ParseResumeLocation(proto.getParseResumeLocation()),
          proto.getOptions());
    }
  }

  /**
   * This represents a TVF that accepts a relation for its first argument. The TVF returns a
   * relation with a schema that is constructed by copying the schema of input relation and
   * appending extra columns to the schema.
   */
  public static class ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF
      extends TableValuedFunction {
    public ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
        ImmutableList<String> namePath,
        FunctionSignature signature,
        ImmutableList<TVFRelation.Column> columns,
        @Nullable String customContext,
        @Nullable Volatility volatility) {
      this(namePath, signature, columns, customContext, volatility,
           TableValuedFunctionOptionsProto.getDefaultInstance());
    }

    public ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
        ImmutableList<String> namePath,
        FunctionSignature signature,
        ImmutableList<TVFRelation.Column> columns,
        @Nullable String customContext,
        @Nullable Volatility volatility,
        TableValuedFunctionOptionsProto options) {
      super(namePath, signature, columns, customContext, volatility, options);
    }

    public static ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF deserialize(
        TableValuedFunctionProto proto,
        ImmutableList<? extends DescriptorPool> pools,
        TypeFactory typeFactory) {
      Preconditions.checkArgument(
          proto.getType()
              == TableValuedFunctionType
                  .FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_WITH_APPENDED_COLUMNS,
          proto);
      ImmutableList<String> namePath = ImmutableList.copyOf(proto.getNamePathList());
      FunctionSignature signature = FunctionSignature.deserialize(proto.getSignature(), pools);
      ImmutableList.Builder<TVFRelation.Column> builder = ImmutableList.builder();
      if (proto.hasCustomContext()) {
        try {
          TVFRelationProto relationProto =
              TVFRelationProto.parseFrom(proto.getCustomContextBytes());
          for (TVFRelationColumnProto columnProto : relationProto.getColumnList()) {
            Type type = typeFactory.deserialize(columnProto.getType(), pools);
            builder.add(
                TVFRelation.Column.create(
                    columnProto.getName(), type, columnProto.getIsPseudoColumn()));
          }
        } catch (InvalidProtocolBufferException e) {
          throw new IllegalArgumentException(
              "Failed to deserialize TVFRelationProto from custom_context in "
                  + proto.getNamePath(proto.getNamePathCount() - 1),
              e);
        }
      }
      String customContext = proto.hasCustomContext() ? proto.getCustomContext() : null;
      Volatility volatility = proto.getVolatility();
      return new ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
          namePath, signature, builder.build(), customContext, volatility, proto.getOptions());
    }

    @Override
    TableValuedFunctionType getType() {
      return TableValuedFunctionType.FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_WITH_APPENDED_COLUMNS;
    }
  }
}
