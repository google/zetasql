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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.zetasql.FunctionProtos.TVFRelationColumnProto;
import com.google.zetasql.FunctionProtos.TVFRelationProto;
import java.io.Serializable;
import java.util.List;

/**
 * This represents a relation passed as an input argument to a TVF, or returned from a TVF. It
 * either contains a list of columns, where each column contains a name and a type, or the relation
 * may be a value table. For the value table case, there should be exactly one column, with an empty
 * name.
 *
 * <p>TODO: Give this class a better name that suggests it is the schema of a table-valued
 * argument or return value. The word 'relation' implies that it might contain an entire table,
 * which is untrue.
 */
public class TVFRelation implements Serializable {
  private final ImmutableList<Column> columns;
  private final boolean isValueTable;

  private TVFRelation(Iterable<Column> columns, boolean isValueTable) {
    this.columns = ImmutableList.copyOf(columns);
    this.isValueTable = isValueTable;
  }

  /** Creates a new TVFRelation with a fixed list of column names and types. */
  public static TVFRelation createColumnBased(List<Column> columns) {
    return new TVFRelation(columns, /* isValueTable= */ false);
  }

  /** Creates a new value-table TVFRelation with a single column of 'type' with no name. */
  public static TVFRelation createValueTableBased(Type type) {
    return new TVFRelation(
        ImmutableList.of(Column.create(/* name= */ "", type)), /* isValueTable= */ true);
  }

  public ImmutableList<Column> getColumns() {
    return columns;
  }

  public boolean isValueTable() {
    return isValueTable;
  }

  public TVFRelationProto serialize() {
    TVFRelationProto.Builder protoBuilder = TVFRelationProto.newBuilder();
    for (Column col : columns) {
      protoBuilder
          .addColumn(
              TVFRelationColumnProto.newBuilder()
                  .setName(col.getName())
                  .setType(col.getType().serialize()))
          .build();
    }
    protoBuilder.setIsValueTable(isValueTable);
    return protoBuilder.build();
  }

  public static TVFRelation deserialize(
      TVFRelationProto proto,
      ImmutableList<ZetaSQLDescriptorPool> pools,
      TypeFactory typeFactory) {
    if (proto.getIsValueTable()) {
      Type type = typeFactory.deserialize(proto.getColumn(0).getType(), pools);
      return createValueTableBased(type);
    } else {
      ImmutableList.Builder<Column> columns = ImmutableList.builder();
      for (TVFRelationColumnProto columnProto : proto.getColumnList()) {
        Type type = typeFactory.deserialize(columnProto.getType(), pools);
        columns.add(Column.create(columnProto.getName(), type));
      }
      return createColumnBased(columns.build());
    }
  }

  /** A column of a {@link TVFRelation}. */
  @AutoValue
  public abstract static class Column implements Serializable {

    public static Column create(String name, Type type) {
      return new AutoValue_TVFRelation_Column(name, type);
    }

    public abstract String getName();

    public abstract Type getType();
  }
}
