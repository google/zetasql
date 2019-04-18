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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import com.google.zetasql.ZetaSQLOptionsProto.AllowedHintsAndOptionsProto;
import com.google.zetasql.ZetaSQLOptionsProto.AllowedHintsAndOptionsProto.HintProto;
import com.google.zetasql.ZetaSQLOptionsProto.AllowedHintsAndOptionsProto.OptionProto;
import com.google.zetasql.ZetaSQLType.TypeProto;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;

/**
 * This class specifies a set of allowed hints and options, and their expected types.
 *
 * <p>Each hint or option has an expected Type, which can be NULL. If the expected type is NULL,
 * then any type is allowed. If a type is specified, the resolved value for the hint will always
 * have the expected type, and the analyzer will give an error if coercion is not possible.
 *
 * <p>Hint, option and qualifier names are all case insensitive. The resolved AST will contain the
 * original case as written by the user.
 *
 * <p>The {@code disallow_unknown_options} and {@code disallow_unknown_hints_with_qualifiers} fields
 * can be set to indicate that errors should be given on unknown options or hints (with specific
 * qualifiers). Unknown hints with other qualifiers do not cause errors.
 */
public class AllowedHintsAndOptions implements Serializable {

  // If true, give an error for an unknown option.
  boolean disallowUnknownOptions = false;
  // Keys are lower-case qualifiers, values are the original qualifiers.
  private final Map<String, String> disallowUnknownHintsWithQualifiers = new HashMap<>();
  // Table containing declared hints. Table keys are in lower-case. The key is
  // (qualifier, hint). Unqualified hints are declared using an empty qualifier.
  private final Table<String, String, Hint> hints = HashBasedTable.create();
  // Map containing declared options. Map keys are in lower-case.
  private final Map<String, Type> options = new HashMap<>();

  public AllowedHintsAndOptions() {}

  /**
   * This is recommended constructor to use for normal settings.
   *
   * <p>All supported hints and options should be added with the Add methods.
   * <br>Unknown options will be errors.
   * <br>Unknown hints without qualifiers, or with {@code qualifier}, will be
   * errors.
   * <br>Unkonwn hints with other qualifiers will be allowed (because these are
   * typically interpreted as hints intended for other engines).
   */
  public AllowedHintsAndOptions(String qualifier) {
    disallowUnknownOptions = true;
    disallowUnknownHintsWithQualifier(qualifier);
    disallowUnknownHintsWithQualifier("");
  }

  /**
   * Serialize this AllowedHintsAndOptions into protobuf, with
   * FileDescriptors emitted to the builder as needed.
   */
  AllowedHintsAndOptionsProto serialize(
      FileDescriptorSetsBuilder fileDescriptorSetsBuilder) {
    AllowedHintsAndOptionsProto.Builder builder = AllowedHintsAndOptionsProto.newBuilder();
    builder.setDisallowUnknownOptions(disallowUnknownOptions);
    builder.addAllDisallowUnknownHintsWithQualifier(disallowUnknownHintsWithQualifiers.values());
    for (Hint hint : hints.values()) {
      if (hint.getQualifier().isEmpty() && !hint.getAllowUnqualified()) {
        continue;
      }
      if (hint.getType() != null) {
        TypeProto.Builder typeBuilder = TypeProto.newBuilder();
        hint.getType().serialize(typeBuilder, fileDescriptorSetsBuilder);
        builder.addHintBuilder()
            .setQualifier(hint.getQualifier()).setName(hint.getName())
            .setType(typeBuilder).setAllowUnqualified(hint.getAllowUnqualified());
      } else {
        builder.addHintBuilder()
            .setQualifier(hint.getQualifier()).setName(hint.getName())
            .setAllowUnqualified(hint.getAllowUnqualified());
      }
    }
    for (Entry<String, Type> option : options.entrySet()) {
      if (option.getValue() != null) {
        TypeProto.Builder typeBuilder = TypeProto.newBuilder();
        option.getValue().serialize(typeBuilder, fileDescriptorSetsBuilder);
        builder.addOptionBuilder().setName(option.getKey()).setType(typeBuilder);
      } else {
        builder.addOptionBuilder().setName(option.getKey());
      }
    }
    return builder.build();
  }

  /**
   * Deserialize an AllowedHintsAndOptions from proto. Types will be created
   * using given type factory and descriptor pools.
   */
  static AllowedHintsAndOptions deserialize(
      AllowedHintsAndOptionsProto proto, List<ZetaSQLDescriptorPool> pools, TypeFactory factory) {
    AllowedHintsAndOptions allowed = new AllowedHintsAndOptions();
    for (String qualifier : proto.getDisallowUnknownHintsWithQualifierList()) {
      Preconditions.checkArgument(
          !allowed.disallowUnknownHintsWithQualifiers.containsKey(qualifier.toLowerCase()));
      allowed.disallowUnknownHintsWithQualifier(qualifier);
    }
    allowed.setDisallowUnknownOptions(proto.getDisallowUnknownOptions());
    for (HintProto hint : proto.getHintList()) {
      if (hint.hasType()) {
        allowed.addHint(hint.getQualifier(), hint.getName(),
            factory.deserialize(hint.getType(), pools), hint.getAllowUnqualified());
      } else {
        allowed.addHint(hint.getQualifier(), hint.getName(), null, hint.getAllowUnqualified());
      }
    }
    for (OptionProto option : proto.getOptionList()) {
      if (option.hasType()) {
        allowed.addOption(option.getName(), factory.deserialize(option.getType(), pools));
      } else {
        allowed.addOption(option.getName(), null);
      }
    }
    return allowed;
  }

  /**
   * Add an option.
   * @param name
   * @param type may be NULL to indicate that all Types are allowed.
   */
  public void addOption(String name, @Nullable Type type) {
    Preconditions.checkNotNull(name);
    Preconditions.checkArgument(!name.isEmpty());
    Preconditions.checkArgument(!options.containsKey(name.toLowerCase()));
    options.put(name.toLowerCase(), type);
  }

  /**
   * Add a hint.
   * @param qualifier may be empty to add this hint only unqualified, but hints
   * for some engine should normally allow the engine name as a qualifier.
   * @param name
   * @param type may be NULL to indicate that all Types are allowed.
   * @param allowUnqualified if true, this hint is allowed both unqualified and
   * qualified with {@code qualifier}.
   */
  public void addHint(
      String qualifier, String name, @Nullable Type type, boolean allowUnqualified) {
    Preconditions.checkNotNull(name);
    Preconditions.checkArgument(!name.isEmpty());
    Preconditions.checkNotNull(qualifier);
    Preconditions.checkArgument(!qualifier.isEmpty() || allowUnqualified,
        "Cannot have hint with no qualifier and !allowUnqualified");
    Preconditions.checkArgument(
        !hints.contains(qualifier.toLowerCase(), name.toLowerCase()));
    hints.put(qualifier.toLowerCase(), name.toLowerCase(),
        new Hint(qualifier, name, type, allowUnqualified));
    if (allowUnqualified && !qualifier.isEmpty()) {
      Preconditions.checkArgument(!hints.contains("", name.toLowerCase()));
      hints.put("", name.toLowerCase(), new Hint("", name, type,
          false  /* This is to mark this hint is not explicitly added and won't be serialized */));
    }
  }

  public void setDisallowUnknownOptions(boolean disallowUnknownOptions) {
    this.disallowUnknownOptions = disallowUnknownOptions;
  }

  public boolean getDisallowUnknownOptions() {
    return disallowUnknownOptions;
  }

  public Hint getHint(String qualifier, String name) {
    return hints.get(qualifier.toLowerCase(), name.toLowerCase());
  }

  public ImmutableList<Hint> getHintList() {
    return ImmutableList.copyOf(hints.values());
  }

  public Type getOptionType(String name) {
    return options.get(name.toLowerCase());
  }

  public ImmutableList<String> getOptionNameList() {
    return ImmutableList.copyOf(options.keySet());
  }

  public void disallowUnknownHintsWithQualifier(String qualifier) {
    Preconditions.checkNotNull(qualifier);
    Preconditions.checkArgument(
        !disallowUnknownHintsWithQualifiers.containsKey(qualifier.toLowerCase()));
    disallowUnknownHintsWithQualifiers.put(qualifier.toLowerCase(), qualifier);
  }

  public ImmutableList<String> getDisallowUnknownHintsWithQualifiers() {
    return ImmutableList.copyOf(disallowUnknownHintsWithQualifiers.values());
  }

  /** This class specifies all information of a hint. */
  public static class Hint implements Serializable {
    private String qualifier;
    private String name;
    private Type type;
    private boolean allowUnqualified;

    Hint(String qualifier, String name, @Nullable Type type, boolean allowUnqualified) {
      this.qualifier = qualifier;
      this.name = name;
      this.type = type;
      this.allowUnqualified = allowUnqualified;
    }

    public String getQualifier() {
      return qualifier;
    }

    public String getName() {
      return name;
    }

    @Nullable
    public Type getType() {
      return type;
    }

    /**
     * AllowUnqualified is only used when adding new hints and serializing to
     * proto to recreate hints in C++ side. This should only be used inside the
     * class and should not be used by users.
     */
    private boolean getAllowUnqualified() {
      return allowUnqualified;
    }
  }
}
