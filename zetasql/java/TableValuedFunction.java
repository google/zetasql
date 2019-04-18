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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.zetasql.FunctionProtos.TableValuedFunctionProto;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums;
import java.io.Serializable;

/**
 * This interface describes a table-valued function (TVF) available in a query engine.
 *
 * <p>More information in zetasql/public/table_valued_function.h
 */
public abstract class TableValuedFunction implements Serializable {

  private final ImmutableList<String> namePath;
  private final FunctionSignature signature;

  /**
   * Constructs a new TVF object with the given name and argument signature.
   *
   * <p>Each TVF may accept value or relation arguments. The signature specifies whether each
   * argument should be a value or a relation. For a value argument, the signature may specify a
   * concrete Type or a (possibly templated) SignatureArgumentKind. For relation arguments, the
   * signature should use ARG_TYPE_RELATION, and any relation will be accepted as an argument.
   */
  public TableValuedFunction(ImmutableList<String> namePath, FunctionSignature signature) {
    this.namePath = namePath;
    this.signature = signature;
  }

  /**
   * Deserializes a table-valued function from a protocol buffer.
   */
  public static TableValuedFunction deserialize(
      TableValuedFunctionProto proto,
      ImmutableList<ZetaSQLDescriptorPool> pools,
      TypeFactory typeFactory) {
    switch (proto.getType()) {
      case FIXED_OUTPUT_SCHEMA_TVF:
        return FixedOutputSchemaTVF.deserialize(proto, pools, typeFactory);
      case FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_TVF:
        return ForwardInputSchemaToOutputSchemaTVF.deserialize(proto, pools, typeFactory);
      case TEMPLATED_SQL_TVF:
        return TemplatedSQLTVF.deserialize(proto, pools, typeFactory);
      default:
        StringBuilder builder = new StringBuilder();
        for (String name : proto.getNamePathList()) {
          builder.append(name);
        }
        throw new IllegalArgumentException(
            "Serialization is not implemented yet for table-valued function: "
            + builder.toString());
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
            .setSignature(signature.serialize(fileDescriptorSetsBuilder));
    builder.setType(getType());
    switch (getType()) {
      case FIXED_OUTPUT_SCHEMA_TVF:
        break;
      case FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_TVF:
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

  public String getFullName() {
    return getFullName(true);
  }

  public String getFullName(boolean includeGroup) {
    return Joiner.on('.').join(namePath);
  }

  @Override
  public String toString() {
    return "TableValuedFunction";
  }

  public boolean isDefaultValue() {
    return true;
  }

  /** A TVF that always returns a relation with the same fixed output schema. */
  public static class FixedOutputSchemaTVF extends TableValuedFunction {
    private final TVFRelation outputSchema;

    public FixedOutputSchemaTVF(
        ImmutableList<String> namePath, FunctionSignature signature, TVFRelation outputSchema) {
      super(namePath, signature);
      this.outputSchema = outputSchema;
    }

    public TVFRelation getOutputSchema() {
      return outputSchema;
    }

    @Override
    public FunctionEnums.TableValuedFunctionType getType() {
      return FunctionEnums.TableValuedFunctionType.FIXED_OUTPUT_SCHEMA_TVF;
    }

    /**
     * Deserializes this table-valued function from a protocol buffer.
     */
    public static FixedOutputSchemaTVF deserialize(
        TableValuedFunctionProto proto,
        ImmutableList<ZetaSQLDescriptorPool> pools,
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
              typeFactory));
    }
  }

  /**
   * This represents a TVF that accepts a relation for its first argument. The TVF returns a
   * relation with the same output schema as this input relation.
   */
  public static class ForwardInputSchemaToOutputSchemaTVF extends TableValuedFunction {

    public ForwardInputSchemaToOutputSchemaTVF(
        ImmutableList<String> namePath, FunctionSignature signature) {
      super(namePath, signature);
    }

    @Override
    public FunctionEnums.TableValuedFunctionType getType() {
      return FunctionEnums.TableValuedFunctionType.FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_TVF;
    }

    /**
     * Deserializes this table-valued function from a protocol buffer.
     */
    public static ForwardInputSchemaToOutputSchemaTVF deserialize(
        TableValuedFunctionProto proto,
        ImmutableList<ZetaSQLDescriptorPool> pools,
        TypeFactory typeFactory) {
      Preconditions.checkArgument(proto.getType()
              == FunctionEnums.TableValuedFunctionType.FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_TVF,
          proto);
      ImmutableList<String> namePath = ImmutableList.copyOf(proto.getNamePathList());
      FunctionSignature signature = FunctionSignature.deserialize(proto.getSignature(), pools);
      return new ForwardInputSchemaToOutputSchemaTVF(namePath, signature);
    }
  }

  /**
   * This represents a templated function with a SQL body.
   *
   * <p>The purpose of this class is to help support statements of the form
   * "CREATE FUNCTION <name>(<arguments>) AS <query>", where the <arguments>
   * may have templated types like "ANY TYPE". In this case, ZetaSQL cannot
   * resolve the function expression right away and must defer this work until
   * later when the function is called with concrete argument types.
   * A TVF that always returns a relation with the same fixed output schema.
   */
  public static class TemplatedSQLTVF extends TableValuedFunction {
    private final ImmutableList<String> argumentNames;
    private final ParseResumeLocation parseResumeLocation;

    public TemplatedSQLTVF(
        ImmutableList<String> namePath, FunctionSignature signature,
        ImmutableList<String> argumentNames, ParseResumeLocation parseResumeLocation) {
      super(namePath, signature);
      this.argumentNames = argumentNames;
      this.parseResumeLocation = parseResumeLocation;
    }

    public ImmutableList<String> getArgumentNames() {
      return argumentNames;
    }
    public String getSqlBody() {
      return this.parseResumeLocation.getInput().substring(
          this.parseResumeLocation.getBytePosition());
    }

    @Override
    public FunctionEnums.TableValuedFunctionType getType() {
      return FunctionEnums.TableValuedFunctionType.TEMPLATED_SQL_TVF;
    }

    /**
     * Deserializes this table-valued function from a protocol buffer.
     */
    public static TemplatedSQLTVF deserialize(
        TableValuedFunctionProto proto,
        ImmutableList<ZetaSQLDescriptorPool> pools,
        TypeFactory typeFactory) {
      Preconditions.checkArgument(
          proto.getType() == FunctionEnums.TableValuedFunctionType.TEMPLATED_SQL_TVF, proto);
      ImmutableList<String> namePath = ImmutableList.copyOf(proto.getNamePathList());
      FunctionSignature signature = FunctionSignature.deserialize(proto.getSignature(), pools);
      ImmutableList.Builder<String> builder = new ImmutableList.Builder<String>();
      for (String name : proto.getArgumentNameList()) {
        builder.add(name);
      }
      return new TemplatedSQLTVF(
          namePath, signature, builder.build(),
          new ParseResumeLocation(proto.getParseResumeLocation()));
    }
  }
}
