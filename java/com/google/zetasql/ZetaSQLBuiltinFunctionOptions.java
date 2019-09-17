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

import com.google.common.collect.ImmutableSet;
import com.google.zetasql.ZetaSQLFunction.FunctionSignatureId;
import com.google.zetasql.ZetaSQLOptionsProto.ZetaSQLBuiltinFunctionOptionsProto;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Controls whether builtin FunctionSignatureIds and their matching FunctionSignatures are included
 * or excluded for an implementation.
 *
 * <p>LanguageOptions are applied to determine set of candidate functions before inclusion/exclusion
 * lists are applied.
 */
public class ZetaSQLBuiltinFunctionOptions implements Serializable {
  private final ZetaSQLBuiltinFunctionOptionsProto.Builder builder =
      ZetaSQLBuiltinFunctionOptionsProto.newBuilder();
  private final LanguageOptions languageOptions;
  private final Set<FunctionSignatureId> includeFunctionIds = new HashSet<>();
  private final Set<FunctionSignatureId> excludeFunctionIds = new HashSet<>();

  /**
   * Default constructor.  If LanguageOptions are not passed in, default to
   * including all possible functions.
   */
  public ZetaSQLBuiltinFunctionOptions() {
    this(new LanguageOptions().enableMaximumLanguageFeatures());
  }

  public ZetaSQLBuiltinFunctionOptions(LanguageOptions languageOptions) {
    this.languageOptions = languageOptions;
  }

  ZetaSQLBuiltinFunctionOptions(ZetaSQLBuiltinFunctionOptionsProto proto) {
    this(new LanguageOptions(proto.getLanguageOptions()));
    builder.mergeFrom(proto);
  }

  ZetaSQLBuiltinFunctionOptionsProto serialize() {
    builder.setLanguageOptions(languageOptions.serialize());
    return builder.build();
  }

  public LanguageOptions getLanguageOptions() {
    return languageOptions;
  }

  public void includeFunctionSignatureId(FunctionSignatureId id) {
    if (!includeFunctionIds.contains(id)) {
      includeFunctionIds.add(id);
      builder.addIncludeFunctionIds(id);
    }
  }

  public void excludeFunctionSignatureId(FunctionSignatureId id) {
    if (!excludeFunctionIds.contains(id)) {
      excludeFunctionIds.add(id);
      builder.addExcludeFunctionIds(id);
    }
  }

  public ImmutableSet<FunctionSignatureId> getIncludeFunctionSignatureIds() {
    return ImmutableSet.copyOf(includeFunctionIds);
  }

  public ImmutableSet<FunctionSignatureId> getExcludeFunctionSignatureIds() {
    return ImmutableSet.copyOf(excludeFunctionIds);
  }
}
