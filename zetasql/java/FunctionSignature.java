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
import com.google.zetasql.FunctionProtos.FunctionArgumentTypeProto;
import com.google.zetasql.FunctionProtos.FunctionSignatureOptionsProto;
import com.google.zetasql.FunctionProtos.FunctionSignatureProto;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * FunctionSignature identifies the argument Types and other properties
 * per overload of a Function.  A FunctionSignature is concrete if it
 * identifies the exact number and fixed Types of its arguments and results.
 * A FunctionSignature can be abstract, specifying templated types and
 * identifying arguments as repeated or optional.  Optional arguments must
 * appear at the end of the argument list.
 *
 * <p>If multiple arguments are repeated, they must be consecutive and are
 * treated as if they repeat together.  To illustrate, consider the expression:
 *      'CASE WHEN {@code <bool_expr_1>} THEN {@code <expr_1>}
 *       WHEN {@code <bool_expr_2>} THEN {@code <expr_2>}
 *       ...
 *       ELSE {@code <expr_n>} END'.
 *
 * <p>This expression has the following signature {@code <arguments>}:
 *   arg1: {@code <bool>} repeated - WHEN
 *   arg2: {@code <any_type_1>} repeated - THEN
 *   arg3: {@code <any_type_1>} optional - ELSE
 *   result: {@code <any_type_1>}
 *
 * <p>The WHEN and THEN arguments (arg1 and arg2) repeat together and must
 * occur at least once, and the ELSE is optional.  The THEN, ELSE, and
 * RESULT types can be any type, but must be the same type.
 *
 * <p>In order to avoid potential ambiguity, the number of optional arguments
 * must be less than the number of repeated arguments.
 *
 * <p>The FunctionSignature also includes {@code <options>} for specifying
 * additional signature matching requirements, if any.
 */
public final class FunctionSignature implements Serializable {

  private final ImmutableList<FunctionArgumentType> arguments;
  private final FunctionArgumentType resultType;
  private final long contextId;
  private final FunctionSignatureOptionsProto options;
  private final boolean isConcrete;
  private final ImmutableList<FunctionArgumentType> concreteArguments;

  public FunctionSignature(
      FunctionArgumentType resultType, List<FunctionArgumentType> arguments, long contextId) {
    this(resultType, arguments, contextId, FunctionSignatureOptionsProto.getDefaultInstance());
  }

  public FunctionSignature(
      FunctionArgumentType resultType, List<FunctionArgumentType> arguments, long contextId,
      FunctionSignatureOptionsProto options) {
    this.arguments = ImmutableList.copyOf(arguments);
    this.resultType = resultType;
    this.contextId = contextId;
    this.options = options;
    isConcrete = computeIsConcrete();
    concreteArguments = ImmutableList.copyOf(computeConcreteArgumentTypes());
  }

  public FunctionSignatureProto serialize(FileDescriptorSetsBuilder fileDescriptorSetsBuilder) {
    FunctionSignatureProto.Builder builder = FunctionSignatureProto.newBuilder();
    builder.setOptions(options);
    builder.setReturnType(resultType.serialize(fileDescriptorSetsBuilder));
    builder.setContextId(contextId);
    for (FunctionArgumentType argument : arguments) {
      builder.addArgument(argument.serialize(fileDescriptorSetsBuilder));
    }
    return builder.build();
  }

  public static FunctionSignature deserialize(
      FunctionSignatureProto proto, ImmutableList<ZetaSQLDescriptorPool> pools) {
    List<FunctionArgumentType> arguments = new ArrayList<>();
    for (FunctionArgumentTypeProto argument : proto.getArgumentList()) {
      arguments.add(FunctionArgumentType.deserialize(argument, pools));
    }
    FunctionSignature signature = new FunctionSignature(
        FunctionArgumentType.deserialize(proto.getReturnType(), pools),
        arguments, proto.getContextId(), proto.getOptions());
    return signature;
  }

  private List<FunctionArgumentType> computeConcreteArgumentTypes() {
    if (!isConcrete) {
      return new ArrayList<>();
    }

    // Count number of concrete args, and find the range of repeateds.
    int firstRepeatedIndex = -1;
    int lastRepeatedIndex = -1;
    for (int idx = 0; idx < arguments.size(); ++idx) {
      FunctionArgumentType argument = arguments.get(idx);
      if (argument.isRepeated()) {
        lastRepeatedIndex = idx;
        if (firstRepeatedIndex == -1) {
          firstRepeatedIndex = idx;
        }
      }
    }

    ArrayList<FunctionArgumentType> result = new ArrayList<>();

    if (firstRepeatedIndex == -1) {
      // If we have no repeateds, just loop through and copy present args.
      for (int idx = 0; idx < arguments.size(); ++idx) {
        FunctionArgumentType argument = arguments.get(idx);
        if (argument.getNumOccurrences() == 1) {
          result.add(argument);
        }
      }
    } else {
      // Add arguments that come before repeated arguments.
      for (int idx = 0; idx < firstRepeatedIndex; ++idx) {
        FunctionArgumentType argument = arguments.get(idx);
        if (argument.getNumOccurrences() == 1) {
          result.add(argument);
        }
      }

      // Add concrete repetitions of all repeated arguments.
      int numRepeatedOccurrences = arguments.get(firstRepeatedIndex).getNumOccurrences();
      for (int c = 0; c < numRepeatedOccurrences; ++c) {
        for (int idx = firstRepeatedIndex; idx <= lastRepeatedIndex; ++idx) {
          result.add(arguments.get(idx));
        }
      }

      // Add any arguments that come after the repeated arguments.
      for (int idx = lastRepeatedIndex + 1; idx < arguments.size(); ++idx) {
        FunctionArgumentType argument = arguments.get(idx);
        if (argument.getNumOccurrences() == 1) {
          result.add(argument);
        }
      }
    }

    return result;
  }


  public ImmutableList<FunctionArgumentType> getFunctionArgumentList() {
    return arguments;
  }

  /**
   * @throws IllegalStateException if the signature is not concrete.
   */
  public int getConcreteArgumentsCount() {
    Preconditions.checkState(isConcrete);
    return concreteArguments.size();
  }

  public Type getConcreteArgumentType(int index) {
    Preconditions.checkState(isConcrete);
    return concreteArguments.get(index).getType();
  }

  public FunctionArgumentType getResultType() {
    return resultType;
  }

  public long getContextId() {
    return contextId;
  }

  public boolean isConcrete() {
    return isConcrete;
  }

  public boolean isDeprecated() {
    return options.getIsDeprecated();
  }

  public FunctionSignatureOptionsProto getOptions() {
    return options;
  }

  private boolean computeIsConcrete() {
    for (FunctionArgumentType argument : arguments) {
      if (argument.getNumOccurrences() > 0 && !argument.isConcrete()) {
        return false;
      }
    }
    return resultType.isConcrete();
  }

  public String debugString(String functionName, boolean verbose) {
    StringBuilder result = new StringBuilder(functionName);
    result.append('(');
    if (verbose) {
      boolean first = true;
      for (FunctionArgumentType argument : arguments) {
        if (!first) {
          result.append(", ");
        }
        first = false;

        result.append(argument.debugString(verbose));
      }
    } else {
      Joiner.on(", ").appendTo(result, arguments);
    }
    result.append(") -> ").append(resultType.debugString(verbose));

    if (verbose) {
      int numWarnings = options.getAdditionalDeprecationWarningCount();
      if (numWarnings > 0) {
        result.append(" (").append(numWarnings).append(" deprecation warning");
        if (numWarnings > 1) {
          result.append("s");
        }
        result.append(")");
      }
    }

    return result.toString();
  }

  public String debugString(String functionName) {
    return debugString(functionName, false);
  }

  @Override
  public String toString() {
    return debugString("");
  }

  public static String signaturesToString(List<FunctionSignature> signatures) {
    return "  " + Joiner.on("\n  ").join(signatures);
  }

  public boolean isDefaultValue() {
    return false;
  }
}
