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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.zetasql.Constant;
import com.google.zetasql.ConstantRefProto;
import com.google.zetasql.FieldDescriptorRefProto;
import com.google.zetasql.Function;
import com.google.zetasql.FunctionProtos.FunctionSignatureProto;
import com.google.zetasql.FunctionProtos.ResolvedFunctionCallInfoProto;
import com.google.zetasql.FunctionProtos.TVFSignatureProto;
import com.google.zetasql.FunctionRefProto;
import com.google.zetasql.FunctionSignature;
import com.google.zetasql.ZetaSQLDescriptorPool;
import com.google.zetasql.ZetaSQLDescriptorPool.ZetaSQLDescriptor;
import com.google.zetasql.ZetaSQLDescriptorPool.ZetaSQLFieldDescriptor;
import com.google.zetasql.ZetaSQLType.ProtoTypeProto;
import com.google.zetasql.ZetaSQLType.TypeProto;
import com.google.zetasql.Model;
import com.google.zetasql.ModelRefProto;
import com.google.zetasql.NotFoundException;
import com.google.zetasql.Procedure;
import com.google.zetasql.ProcedureRefProto;
import com.google.zetasql.ResolvedColumnProto;
import com.google.zetasql.ResolvedFunctionCallInfo;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.TVFSignature;
import com.google.zetasql.Table;
import com.google.zetasql.TableRefProto;
import com.google.zetasql.TableValuedFunction;
import com.google.zetasql.TableValuedFunctionRefProto;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.Value;
import com.google.zetasql.ValueWithTypeProto;

/**
 * Deserializes objects in the ResolvedAST which require some context. These
 * objects are passed using reference semantics, so deserializing them is mostly looking them up by
 * id.
 */
public final class DeserializationHelper {
  private final TypeFactory typeFactory;
  private final ImmutableList<ZetaSQLDescriptorPool> pools;
  // A reference to a SimpleCatalog is necessary to deserialize scalar functions and tables. These
  // should only be encountered in ASTs compiled from queries on catalogs.
  private final SimpleCatalog catalog;

  public DeserializationHelper(
      TypeFactory typeFactory,
      ImmutableList<ZetaSQLDescriptorPool> pools,
      SimpleCatalog catalog) {
    this.typeFactory = checkNotNull(typeFactory);
    this.pools = checkNotNull(pools);
    this.catalog = checkNotNull(catalog);
  }

  ZetaSQLFieldDescriptor deserialize(FieldDescriptorRefProto proto) {
    ProtoTypeProto containingMessageProto = proto.getContainingProto();
    ZetaSQLDescriptor containingMessage =
        checkNotNull(
            checkNotNull(pools.get(containingMessageProto.getFileDescriptorSetIndex()))
                .findMessageTypeByName(containingMessageProto.getProtoName()),
            "Couldn't find pool for descriptor %s",
            containingMessageProto.getProtoName());
    return checkNotNull(containingMessage.findFieldByNumber(proto.getNumber()));
  }

  Constant deserialize(ConstantRefProto proto) {
    Constant constant;
    try {
      constant = catalog.findConstant(Splitter.on(".").splitToList(proto.getName()));
    } catch (NotFoundException e) {
      constant = null;
    }
    return checkNotNull(constant);
  }

  Function deserialize(FunctionRefProto proto) {
    return checkNotNull(catalog.getFunctionByFullName(proto.getName()));
  }

  FunctionSignature deserialize(FunctionSignatureProto proto) {
    return FunctionSignature.deserialize(proto, pools);
  }

  TableValuedFunction deserialize(TableValuedFunctionRefProto proto) {
    return checkNotNull(catalog.getTVFByName(proto.getName()));
  }

  ResolvedFunctionCallInfo deserialize(ResolvedFunctionCallInfoProto proto) {
    return ResolvedFunctionCallInfo.deserialize(proto, pools);
  }

  TVFSignature deserialize(TVFSignatureProto proto) {
    return TVFSignature.deserialize(proto, pools);
  }

  Procedure deserialize(ProcedureRefProto proto) {
    ImmutableList.Builder<String> namePath = new ImmutableList.Builder<>();
    for (String item : Splitter.on('.').split(proto.getName())) {
      namePath.add(item);
    }
    Procedure procedure;
    try {
      procedure = catalog.findProcedure(namePath.build());
    } catch (NotFoundException e) {
      procedure = null;
    }
    return checkNotNull(procedure);
  }

  ResolvedColumn deserialize(ResolvedColumnProto proto) {
    return new ResolvedColumn(
        proto.getColumnId(),
        proto.getTableName(),
        proto.getName(),
        deserialize(proto.getType()));
  }

  Model deserialize(ModelRefProto proto) {
    return checkNotNull(
        catalog.getModelById(proto.getSerializationId()),
        "Could not find model '%s' in catalog.",
        proto.getName());
  }

  Table deserialize(TableRefProto proto) {
    return checkNotNull(
        catalog.getTableById(proto.getSerializationId()),
        "Could not find table '%s' in catalog.",
        proto.getName());
  }

  Type deserialize(TypeProto proto) {
    return typeFactory.deserialize(proto, pools);
  }

  Value deserialize(ValueWithTypeProto proto) {
    if (!(proto.hasType() && proto.hasValue())) {
      return new Value(); // Invalid value.
    }
    return Value.deserialize(deserialize(proto.getType()), proto.getValue());
  }
}
