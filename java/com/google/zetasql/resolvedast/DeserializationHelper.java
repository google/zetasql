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

package com.google.zetasql.resolvedast;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.zetasql.Column;
import com.google.zetasql.ColumnRefProto;
import com.google.zetasql.Connection;
import com.google.zetasql.ConnectionRefProto;
import com.google.zetasql.Constant;
import com.google.zetasql.ConstantRefProto;
import com.google.zetasql.DescriptorPool;
import com.google.zetasql.Function;
import com.google.zetasql.FunctionRefProto;
import com.google.zetasql.GraphElementLabel;
import com.google.zetasql.GraphElementLabelRefProto;
import com.google.zetasql.GraphElementTable;
import com.google.zetasql.GraphElementTableRefProto;
import com.google.zetasql.GraphPropertyDeclaration;
import com.google.zetasql.GraphPropertyDeclarationRefProto;
import com.google.zetasql.Model;
import com.google.zetasql.ModelRefProto;
import com.google.zetasql.NotFoundException;
import com.google.zetasql.Procedure;
import com.google.zetasql.ProcedureRefProto;
import com.google.zetasql.PropertyGraph;
import com.google.zetasql.PropertyGraphRefProto;
import com.google.zetasql.Sequence;
import com.google.zetasql.SequenceRefProto;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.Table;
import com.google.zetasql.TableRefProto;
import com.google.zetasql.TableValuedFunction;
import com.google.zetasql.TableValuedFunctionRefProto;
import com.google.zetasql.TypeFactory;
import javax.annotation.Nullable;

/** Deserializes objects in the ResolvedAST which require catalog lookup */
public final class DeserializationHelper extends AbstractDeserializationHelper {

  // A reference to a SimpleCatalog is necessary to deserialize scalar functions and tables. These
  // should only be encountered in ASTs compiled from queries on catalogs.
  private final SimpleCatalog catalog;

  public DeserializationHelper(
      TypeFactory typeFactory,
      ImmutableList<? extends DescriptorPool> pools,
      SimpleCatalog catalog) {
    super(typeFactory, pools);
    this.catalog = checkNotNull(catalog);
  }

  @Override
  Constant deserialize(ConstantRefProto proto) {
    Constant constant;
    try {
      constant = catalog.findConstant(Splitter.on(".").splitToList(proto.getName()));
    } catch (NotFoundException e) {
      constant = null;
    }
    return checkNotNull(constant);
  }

  @Override
  Function deserialize(FunctionRefProto proto) {
    return checkNotNull(catalog.getFunctionByFullName(proto.getName()));
  }

  @Override
  TableValuedFunction deserialize(TableValuedFunctionRefProto proto) {
    return checkNotNull(catalog.getTvfByFullName(proto.getName()));
  }

  @Override
  Procedure deserialize(ProcedureRefProto proto) {
    ImmutableList.Builder<String> namePath = new ImmutableList.Builder<>();
    namePath.addAll(Splitter.on('.').split(proto.getName()));
    Procedure procedure;
    try {
      procedure = catalog.findProcedure(namePath.build());
    } catch (NotFoundException e) {
      procedure = null;
    }
    return checkNotNull(procedure);
  }

  @Override
  Column deserialize(ColumnRefProto proto) {
    ImmutableList.Builder<String> namePath = new ImmutableList.Builder<>();
    namePath.addAll(Splitter.on('.').split(proto.getTableRef().getFullName()));
    Table table;
    Column column;
    try {
      table = catalog.findTable(namePath.build());
      column = table.findColumnByName(proto.getName());
    } catch (NotFoundException e) {
      column = null;
    }
    return checkNotNull(column);
  }

  @Override
  Model deserialize(ModelRefProto proto) {
    return checkNotNull(
        catalog.getModelById(proto.getSerializationId()),
        "Could not find model '%s' in catalog.",
        proto.getName());
  }

  @Override
  Connection deserialize(ConnectionRefProto proto) {
    return checkNotNull(
        catalog.getConnectionByFullName(proto.getFullName()),
        "Could not find connection '%s' in catalog.",
        proto.getName());
  }

  @Override
  Sequence deserialize(SequenceRefProto proto) {
    return checkNotNull(
        catalog.getSequenceByFullName(proto.getFullName()),
        "Could not find sequence '%s' in catalog.",
        proto.getName());
  }

  @Override
  @Nullable
  Table deserialize(TableRefProto proto) {
    if (proto.hasSerializationId()) {
      return checkNotNull(
          catalog.getTableById(proto.getSerializationId()),
          "Could not find table '%s' in catalog.",
          proto.getName());
    }
    return null;
  }

  @Override
  PropertyGraph deserialize(PropertyGraphRefProto proto) {
    PropertyGraph propertyGraph;
    ImmutableList<String> path = ImmutableList.copyOf(Splitter.on('.').split(proto.getFullName()));
    try {
      propertyGraph = catalog.findPropertyGraph(path);
    } catch (NotFoundException e) {
      propertyGraph = null;
    }
    return checkNotNull(
        propertyGraph, "Could not find PropertyGraph '%s' in catalog.", proto.getFullName());
  }

  @Override
  GraphPropertyDeclaration deserialize(GraphPropertyDeclarationRefProto proto) {
    PropertyGraph propertyGraph = deserialize(proto.getPropertyGraph());
    GraphPropertyDeclaration propertyDeclaration =
        propertyGraph.findPropertyDeclarationByName(proto.getName());

    return checkNotNull(
        propertyDeclaration,
        "Could not find PropertyDeclaration '%s' in PropertyGraph '%s'.",
        proto.getName(),
        proto.getPropertyGraph().getFullName());
  }

  @Override
  GraphElementLabel deserialize(GraphElementLabelRefProto proto) {
    PropertyGraph propertyGraph = deserialize(proto.getPropertyGraph());
    GraphElementLabel graphElementLabel = propertyGraph.findLabelByName(proto.getName());

    return checkNotNull(
        graphElementLabel,
        "Could not find Graph Element Label '%s' in PropertyGraph '%s'.",
        proto.getName(),
        proto.getPropertyGraph().getFullName());
  }

  @Override
  GraphElementTable deserialize(GraphElementTableRefProto proto) {
    PropertyGraph propertyGraph = deserialize(proto.getPropertyGraph());
    GraphElementTable graphElementTable = propertyGraph.findElementTableByName(proto.getName());

    return checkNotNull(
        graphElementTable,
        "Could not find Graph Element Table'%s' in PropertyGraph '%s'.",
        proto.getName(),
        proto.getPropertyGraph().getFullName());
  }

}
