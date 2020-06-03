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
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.zetasql.ZetaSQLType.TypeProto;
import com.google.zetasql.LocalService.EvaluateRequest;
import com.google.zetasql.LocalService.EvaluateResponse;
import com.google.zetasql.LocalService.PrepareRequest;
import com.google.zetasql.LocalService.PrepareResponse;
import com.google.zetasql.LocalService.UnprepareRequest;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * ZetaSQL expression evaluation using Service RPC.
 *
 * <p>When evaluating an expression, callers can provide
 *
 * <ul>
 *   <li>A set of expression columns - column names usable in the expression.
 *   <li>A set of parameters - parameters usable in the expression as \@param.
 * </ul>
 *
 * <p>Columns / Parameters are passed as String:Value Maps.
 *
 * <p>A prepared expression will create server side state, which should be released by calling
 * close() when the expression is no longer used. Note that the close() method is called in
 * finalize(), but you should not rely on that because the Java garbage collector has no idea how
 * much memory is used on C++ side and GC may happen later than necessary.
 *
 * <p>Read the unit tests for examples.
 *
 * <p>This class is not thread-safe. External synchronization is needed when it is shared by
 * multiple threads.
 */
public class PreparedExpression implements AutoCloseable {
  private final String sql;
  private boolean prepared = false;
  private boolean closed = false;
  private Type outputType;
  private long preparedId;
  private FileDescriptorSetsBuilder fileDescriptorSetsBuilder;
  private TypeFactory factory = TypeFactory.nonUniqueNames();
  private AnalyzerOptions options;

  public PreparedExpression(String sql) {
    this.sql = sql;
  }

  /**
   * Prepare the expression with given options. Throwing SqlException if there's an error (not
   * necessarily network/server failure). See unit tests for examples how options can be set.
   *
   * @param options
   */
  public void prepare(AnalyzerOptions options) {
    Preconditions.checkState(!prepared);
    Preconditions.checkState(!closed);
    this.options = options;
    PrepareRequest.Builder request = PrepareRequest.newBuilder();
    request.setSql(sql);
    fileDescriptorSetsBuilder = new FileDescriptorSetsBuilder();
    request.setOptions(options.serialize(fileDescriptorSetsBuilder));
    for (FileDescriptorSet fileDescriptorSet : fileDescriptorSetsBuilder.build()) {
      request.addFileDescriptorSet(fileDescriptorSet);
    }

    PrepareResponse resp;
    try {
      resp = Client.getStub().prepare(request.build());
    } catch (StatusRuntimeException e) {
      throw new SqlException(e);
    }

    preparedId = resp.getPreparedExpressionId();

    outputType =
        factory.deserialize(resp.getOutputType(), fileDescriptorSetsBuilder.getDescriptorPools());

    prepared = true;
  }

  public Type getOutputType() {
    Preconditions.checkState(prepared);
    Preconditions.checkState(!closed);
    return outputType;
  }

  /**
   * Evaluate the sql expression via Service RPC.
   *
   * @return The evaluation result.
   */
  public Value execute() {
    return execute(Collections.<String, Value>emptyMap(), Collections.<String, Value>emptyMap());
  }

  /**
   * Evaluate the sql expression via Service RPC.
   *
   * @param columns Map of column name:value pairs used in the sql expression.
   * @param parameters Map of parameter name:value pairs.
   * @return The evaluation result.
   */
  public Value execute(Map<String, Value> columns, Map<String, Value> parameters) {
    Preconditions.checkNotNull(columns);
    Preconditions.checkNotNull(parameters);
    Preconditions.checkState(!closed);
    EvaluateRequest.Builder request = EvaluateRequest.newBuilder();
    if (prepared) {
      request.setPreparedExpressionId(preparedId);
      validateParameters(columns, options.getExpressionColumns(), "column");
      validateParameters(parameters, options.getQueryParameters(), "query");
    } else {
      request.setSql(sql);
      fileDescriptorSetsBuilder = new FileDescriptorSetsBuilder();
      options = new AnalyzerOptions();
      for (Entry<String, Value> column : columns.entrySet()) {
        options.addExpressionColumn(column.getKey(), column.getValue().getType());
      }
      for (Entry<String, Value> param : parameters.entrySet()) {
        options.addQueryParameter(param.getKey(), param.getValue().getType());
      }
    }

    for (Entry<String, Value> entry : columns.entrySet()) {
      request.addColumns(
          serializeParameter(entry.getKey(), entry.getValue(), fileDescriptorSetsBuilder));
    }

    for (Entry<String, Value> entry : parameters.entrySet()) {
      request.addParams(
          serializeParameter(entry.getKey(), entry.getValue(), fileDescriptorSetsBuilder));
    }

    if (!prepared) {
      request.setOptions(options.serialize(fileDescriptorSetsBuilder));
      for (FileDescriptorSet fileDescriptorSet : fileDescriptorSetsBuilder.build()) {
        request.addFileDescriptorSet(fileDescriptorSet);
      }
    }

    List<ZetaSQLDescriptorPool> pools = fileDescriptorSetsBuilder.getDescriptorPools();

    EvaluateResponse resp;
    try {
      resp = Client.getStub().evaluate(request.build());
    } catch (StatusRuntimeException e) {
      throw new SqlException(e);
    }

    Type type = factory.deserialize(resp.getType(), pools);
    if (!prepared) {
      outputType = type;
      preparedId = resp.getPreparedExpressionId();
      prepared = true;
    }
    return Value.deserialize(type, resp.getValue());
  }

  private void validateParameters(
      Map<String, Value> parameters, Map<String, Type> expected, String kind) {
    for (String name : parameters.keySet()) {
      if (!expected.containsKey(name)) {
        throw new SqlException("Unexpected " + kind + " parameter '" + name + "'");
      }
      Type type = expected.get(name);
      if (!type.equals(parameters.get(name).getType())) {
        throw new SqlException(
            "Expected " + kind + " parameter '" + name + "' to be of type " + type);
      }
    }

    if (parameters.size() < expected.size()) {
      throw new SqlException("Incomplete " + kind + " parameters");
    }
  }

  /**
   * Release the server side state for this prepared expression.
   */
  @Override
  public void close() {
    if (prepared && !closed) {
      try {
        Client.getStub()
            .unprepare(UnprepareRequest.newBuilder().setPreparedExpressionId(preparedId).build());
      } catch (StatusRuntimeException e) {
        // ignore
      }
      prepared = false;
      closed = true;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    close();
  }

  private EvaluateRequest.Parameter serializeParameter(
      String name, Value value, FileDescriptorSetsBuilder fileDescriptorSetsBuilder) {
    Type type = value.getType();

    TypeProto.Builder typeProtoBuilder = TypeProto.newBuilder();
    type.serialize(typeProtoBuilder, fileDescriptorSetsBuilder);

    return EvaluateRequest.Parameter.newBuilder()
        .setName(name)
        .setValue(value.serialize())
        .setType(typeProtoBuilder.build())
        .build();
  }
}
