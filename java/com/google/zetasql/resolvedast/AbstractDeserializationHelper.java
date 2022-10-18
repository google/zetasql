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

import com.google.common.collect.ImmutableList;
import com.google.zetasql.Column;
import com.google.zetasql.ColumnRefProto;
import com.google.zetasql.Connection;
import com.google.zetasql.ConnectionRefProto;
import com.google.zetasql.Constant;
import com.google.zetasql.ConstantRefProto;
import com.google.zetasql.DescriptorPool;
import com.google.zetasql.DescriptorPool.ZetaSQLDescriptor;
import com.google.zetasql.DescriptorPool.ZetaSQLFieldDescriptor;
import com.google.zetasql.DescriptorPool.ZetaSQLOneofDescriptor;
import com.google.zetasql.FieldDescriptorRefProto;
import com.google.zetasql.Function;
import com.google.zetasql.FunctionProtos.FunctionSignatureProto;
import com.google.zetasql.FunctionProtos.ResolvedFunctionCallInfoProto;
import com.google.zetasql.FunctionProtos.TVFSignatureProto;
import com.google.zetasql.FunctionRefProto;
import com.google.zetasql.FunctionSignature;
import com.google.zetasql.ZetaSQLAnnotation.AnnotationMapProto;
import com.google.zetasql.ZetaSQLType.ProtoTypeProto;
import com.google.zetasql.ZetaSQLType.TypeProto;
import com.google.zetasql.ZetaSQLTypeParameters.TypeParametersProto;
import com.google.zetasql.Model;
import com.google.zetasql.ModelRefProto;
import com.google.zetasql.OneofDescriptorRefProto;
import com.google.zetasql.Procedure;
import com.google.zetasql.ProcedureRefProto;
import com.google.zetasql.ResolvedCollationProto;
import com.google.zetasql.ResolvedColumnProto;
import com.google.zetasql.ResolvedFunctionCallInfo;
import com.google.zetasql.TVFSignature;
import com.google.zetasql.Table;
import com.google.zetasql.TableRefProto;
import com.google.zetasql.TableValuedFunction;
import com.google.zetasql.TableValuedFunctionRefProto;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.TypeParameters;
import com.google.zetasql.Value;
import com.google.zetasql.ValueWithTypeProto;
import javax.annotation.Nullable;

/** Deserializes objects in the ResolvedAST which require some context. */
public abstract class AbstractDeserializationHelper {
  private final TypeFactory typeFactory;
  private final ImmutableList<? extends DescriptorPool> pools;

  public AbstractDeserializationHelper(
      TypeFactory typeFactory, ImmutableList<? extends DescriptorPool> pools) {
    this.typeFactory = checkNotNull(typeFactory);
    this.pools = checkNotNull(pools);
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

  abstract Constant deserialize(ConstantRefProto proto);

  abstract Function deserialize(FunctionRefProto proto);

  FunctionSignature deserialize(FunctionSignatureProto proto) {
    return FunctionSignature.deserialize(proto, pools);
  }

  abstract TableValuedFunction deserialize(TableValuedFunctionRefProto proto);

  ResolvedFunctionCallInfo deserialize(ResolvedFunctionCallInfoProto proto) {
    return ResolvedFunctionCallInfo.deserialize(proto, pools);
  }

  TVFSignature deserialize(TVFSignatureProto proto) {
    return TVFSignature.deserialize(proto, pools);
  }

  abstract Procedure deserialize(ProcedureRefProto proto);

  abstract Column deserialize(ColumnRefProto proto);

  ResolvedColumn deserialize(ResolvedColumnProto proto) {
    return new ResolvedColumn(
        proto.getColumnId(),
        proto.getTableName(),
        proto.getName(),
        deserialize(proto.getType()));
  }

  abstract Model deserialize(ModelRefProto proto);

  abstract Connection deserialize(ConnectionRefProto proto);

  @Nullable
  abstract Table deserialize(TableRefProto proto);

  Type deserialize(TypeProto proto) {
    return typeFactory.deserialize(proto, pools);
  }

  Value deserialize(ValueWithTypeProto proto) {
    if (!(proto.hasType() && proto.hasValue())) {
      return new Value(); // Invalid value.
    }
    return Value.deserialize(deserialize(proto.getType()), proto.getValue());
  }

  @Nullable
  AnnotationMap deserialize(AnnotationMapProto proto) {
    // TODO: use TypeFactory to create AnnotatedType.
    return null;
  }

  ResolvedCollation deserialize(ResolvedCollationProto proto) {
    return ResolvedCollation.deserialize(proto);
  }

  TypeParameters deserialize(TypeParametersProto proto) {
    return TypeParameters.deserialize(proto);
  }
}
