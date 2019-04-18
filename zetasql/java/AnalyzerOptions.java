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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.zetasql.ZetaSQLOptions.ErrorMessageMode;
import com.google.zetasql.ZetaSQLOptions.ParameterMode;
import com.google.zetasql.ZetaSQLOptions.StatementContext;
import com.google.zetasql.ZetaSQLOptionsProto.AnalyzerOptionsProto;
import com.google.zetasql.ZetaSQLOptionsProto.AnalyzerOptionsProto.QueryParameterProto;
import com.google.zetasql.ZetaSQLType.TypeProto;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * AnalyzerOptions contains options that affect analyzer behavior, other than LanguageOptions that
 * control the language accepted.
 */
public class AnalyzerOptions implements Serializable {
  private final Map<String, Type> queryParameters = new HashMap<>();
  private final List<Type> positionalQueryParameters = new ArrayList<>();
  private final Map<String, Type> expressionColumns = new HashMap<>();
  private Entry<String, Type> inScopeExpressionColumn;
  private final Map<String, Type> ddlPseudoColumns = new HashMap<>();
  private transient AnalyzerOptionsProto.Builder builder = AnalyzerOptionsProto.newBuilder();
  private LanguageOptions languageOptions = new LanguageOptions();
  private AllowedHintsAndOptions allowedHintsAndOptions = new AllowedHintsAndOptions();

  public AnalyzerOptions() {
    builder.setDefaultTimezone("America/Los_Angeles");
    builder.setRecordParseLocations(false);
    builder.setPruneUnusedColumns(false);
    builder.setAllowUndeclaredParameters(false);
    builder.setParameterMode(ParameterMode.PARAMETER_NAMED);
    builder.setErrorMessageMode(ErrorMessageMode.ERROR_MESSAGE_ONE_LINE);
    builder.setStatementContext(StatementContext.CONTEXT_DEFAULT);
  }

  public AnalyzerOptionsProto serialize(FileDescriptorSetsBuilder fileDescriptorSetsBuilder) {
    builder.clearInScopeExpressionColumn();
    if (inScopeExpressionColumn != null) {
      TypeProto.Builder typeBuilder = TypeProto.newBuilder();
      inScopeExpressionColumn.getValue().serialize(typeBuilder, fileDescriptorSetsBuilder);
      String name = inScopeExpressionColumn.getKey();
      builder.getInScopeExpressionColumnBuilder().setName(name).setType(typeBuilder.build());
    }

    builder.clearQueryParameters();
    for (Entry<String, Type> param : queryParameters.entrySet()) {
      TypeProto.Builder typeProtoBuilder = TypeProto.newBuilder();
      param.getValue().serialize(typeProtoBuilder, fileDescriptorSetsBuilder);
      builder.addQueryParametersBuilder().setName(param.getKey()).setType(typeProtoBuilder.build());
    }

    builder.clearPositionalQueryParameters();
    for (Type type : positionalQueryParameters) {
      TypeProto.Builder typeProtoBuilder = TypeProto.newBuilder();
      type.serialize(typeProtoBuilder, fileDescriptorSetsBuilder);
      builder.addPositionalQueryParameters(typeProtoBuilder.build());
    }

    builder.clearExpressionColumns();
    for (Entry<String, Type> column : expressionColumns.entrySet()) {
      if (column.getKey().equals(getInScopeExpressionColumnName())) {
        continue;
      }
      TypeProto.Builder typeProtoBuilder = TypeProto.newBuilder();
      column.getValue().serialize(typeProtoBuilder, fileDescriptorSetsBuilder);
      builder.addExpressionColumnsBuilder().setName(column.getKey()).setType(
          typeProtoBuilder.build());
    }

    builder.clearDdlPseudoColumns();
    for (Entry<String, Type> column : ddlPseudoColumns.entrySet()) {
      TypeProto.Builder typeProtoBuilder = TypeProto.newBuilder();
      column.getValue().serialize(typeProtoBuilder, fileDescriptorSetsBuilder);
      builder
          .addDdlPseudoColumnsBuilder()
          .setName(column.getKey())
          .setType(typeProtoBuilder.build());
    }

    builder.setLanguageOptions(languageOptions.serialize());
    builder.clearAllowedHintsAndOptions();
    builder.setAllowedHintsAndOptions(allowedHintsAndOptions.serialize(fileDescriptorSetsBuilder));
    return builder.build();
  }

  public void setLanguageOptions(LanguageOptions languageOptions) {
    this.languageOptions = languageOptions;
  }

  public LanguageOptions getLanguageOptions() {
    return languageOptions;
  }

  public void addQueryParameter(String name, Type type) {
    queryParameters.put(name, type);
  }

  public void addPositionalQueryParameter(Type type) {
    positionalQueryParameters.add(type);
  }

  public void addExpressionColumn(String name, Type type) {
    expressionColumns.put(name, type);
  }

  public Map<String, Type> getExpressionColumns() {
    return ImmutableMap.copyOf(expressionColumns);
  }
  
  public void addDdlPseudoColumn(String name, Type type) {
    ddlPseudoColumns.put(name, type);
  }
  
  public Map<String, Type> getDdlPseudoColumns() {
    return ImmutableMap.copyOf(ddlPseudoColumns);
  }

  public Map<String, Type> getQueryParameters() {
    return ImmutableMap.copyOf(queryParameters);
  }

  public List<Type> getPositionalQueryParameters() {
    return ImmutableList.copyOf(positionalQueryParameters);
  }

  public void setInScopeExpressionColumn(String name, Type type) {
    Preconditions.checkState(inScopeExpressionColumn == null);
    inScopeExpressionColumn = new SimpleImmutableEntry(name, type);
    addExpressionColumn(name, type);
  }

  public boolean hasInScopeExpressionColumn() {
    return inScopeExpressionColumn != null;
  }

  public String getInScopeExpressionColumnName() {
    return inScopeExpressionColumn == null ? null : inScopeExpressionColumn.getKey();
  }

  public Type getInScopeExpressionColumnType() {
    return inScopeExpressionColumn == null ? null : inScopeExpressionColumn.getValue();
  }

  public void setDefaultTimezone(String timezone) {
    builder.setDefaultTimezone(timezone);
  }

  public String getDefaultTimezone() {
    return builder.getDefaultTimezone();
  }

  public void setErrorMessageMode(ErrorMessageMode mode) {
    builder.setErrorMessageMode(mode);
  }

  public ErrorMessageMode getErrorMessageMode() {
    return builder.getErrorMessageMode();
  }

  public void setStatementContext(StatementContext context) {
    builder.setStatementContext(context);
  }

  public StatementContext getStatementContext() {
    return builder.getStatementContext();
  }

  public void setRecordParseLocations(boolean value) {
    builder.setRecordParseLocations(value);
  }

  public boolean getRecordParseLocations() {
    return builder.getRecordParseLocations();
  }

  public void setPruneUnusedColumns(boolean value) {
    builder.setPruneUnusedColumns(value);
  }

  public boolean getPruneUnusedColumns() {
    return builder.getPruneUnusedColumns();
  }

  public void setAllowedHintsAndOptions(AllowedHintsAndOptions allowedHintsAndOptions) {
    this.allowedHintsAndOptions = Preconditions.checkNotNull(allowedHintsAndOptions);
  }

  public AllowedHintsAndOptions getAllowedHintsAndOptions() {
    return allowedHintsAndOptions;
  }

  public void setAllowUndeclaredParameters(boolean value) {
    builder.setAllowUndeclaredParameters(value);
  }

  public boolean getAllowUndeclaredParameters() {
    return builder.getAllowUndeclaredParameters();
  }

  public void setParameterMode(ParameterMode parameterMode) {
    builder.setParameterMode(parameterMode);
  }

  public ParameterMode getParameterMode() {
    return builder.getParameterMode();
  }

  static AnalyzerOptions deserialize(
      AnalyzerOptionsProto proto, List<ZetaSQLDescriptorPool> pools, TypeFactory factory) {
    AnalyzerOptions options = new AnalyzerOptions();
    options.setLanguageOptions(new LanguageOptions(proto.getLanguageOptions()));
    options.setDefaultTimezone(proto.getDefaultTimezone());
    options.setErrorMessageMode(proto.getErrorMessageMode());
    options.setStatementContext(proto.getStatementContext());
    options.setPruneUnusedColumns(proto.getPruneUnusedColumns());
    options.setRecordParseLocations(proto.getRecordParseLocations());
    options.setAllowUndeclaredParameters(proto.getAllowUndeclaredParameters());
    options.setParameterMode(proto.getParameterMode());

    if (proto.hasInScopeExpressionColumn()) {
      options.setInScopeExpressionColumn(
          proto.getInScopeExpressionColumn().getName(),
          factory.deserialize(proto.getInScopeExpressionColumn().getType(), pools));
    }

    for (QueryParameterProto param : proto.getQueryParametersList()) {
      options.addQueryParameter(param.getName(), factory.deserialize(param.getType(), pools));
    }

    for (TypeProto type : proto.getPositionalQueryParametersList()) {
      options.addPositionalQueryParameter(factory.deserialize(type, pools));
    }

    for (QueryParameterProto column : proto.getExpressionColumnsList()) {
      if (!column.getName().equals(options.getInScopeExpressionColumnName())) {
        options.addExpressionColumn(column.getName(), factory.deserialize(column.getType(), pools));
      }
    }

    for (QueryParameterProto column : proto.getDdlPseudoColumnsList()) {
      options.addDdlPseudoColumn(column.getName(), factory.deserialize(column.getType(), pools));
    }

    if (proto.hasAllowedHintsAndOptions()) {
      options.setAllowedHintsAndOptions(
          AllowedHintsAndOptions.deserialize(proto.getAllowedHintsAndOptions(), pools, factory));
    }

    return options;
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    builder.build().toByteString().writeTo(out);
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.builder = AnalyzerOptionsProto.newBuilder().mergeFrom(ByteString.readFrom(in));
  }
}
