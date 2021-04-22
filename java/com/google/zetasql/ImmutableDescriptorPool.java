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

package com.google.zetasql;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toMap;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ExtensionRegistry.ExtensionInfo;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;

/** An immutable implementation of {@link DescriptorPool}. */
public final class ImmutableDescriptorPool implements DescriptorPool {
  // Notice, insertion order _must_ match dependency order. ImmutableMap preserves insertion
  // order.
  private final ImmutableMap<String, FileDescriptor> fileDescriptorsByNameInDependencyOrder;
  private final ImmutableMap<String, EnumDescriptor> enumsByName;
  private final ImmutableMap<String, Descriptor> messagesByName;
  private final ExtensionRegistry extensionRegistry;

  private ImmutableDescriptorPool(
      ImmutableMap<String, FileDescriptor> fileDescriptorsByNameInDependencyOrder,
      ImmutableMap<String, EnumDescriptor> enumsByName,
      ImmutableMap<String, Descriptor> messagesByName,
      ExtensionRegistry extensionRegistry) {
    this.fileDescriptorsByNameInDependencyOrder = fileDescriptorsByNameInDependencyOrder;
    this.enumsByName = enumsByName;
    this.messagesByName = messagesByName;
    this.extensionRegistry = extensionRegistry;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return builder().importFileDescriptors(fileDescriptorsByNameInDependencyOrder.values());
  }

  /**
   * Builder class for {@link ImmutableDescriptorPool}.
   *
   * <p>This class is not thread-safe.
   */
  public static class Builder {
    private final Map<String, FileDescriptor> fileDescriptors = new LinkedHashMap<>();

    /** Add {@param fileDescriptors} as well as, transitively all of its dependencies. */
    public Builder importFileDescriptor(FileDescriptor fileDescriptor) {
      FileDescriptor existing = fileDescriptors.get(fileDescriptor.getFullName());
      checkArgument(
          existing == null || existing == fileDescriptor,
          "duplicate FileDescriptor with name %s",
          fileDescriptor.getFullName());
      if (existing == null) {
        // Import dependencies first.
        importFileDescriptors(fileDescriptor.getDependencies());
        fileDescriptors.put(fileDescriptor.getFullName(), fileDescriptor);
      }
      return this;
    }

    /** Add {@code fileDescriptors} as well as, transitively all of their dependencies. */
    public Builder importFileDescriptors(Collection<FileDescriptor> fileDescriptors) {
      for (FileDescriptor descriptor : fileDescriptors) {
        importFileDescriptor(descriptor);
      }
      return this;
    }

    public ImmutableDescriptorPool build() {
      Map<String, FileDescriptor> fileDescriptorsByName = new LinkedHashMap<>();
      Map<String, EnumDescriptor> enumsByName = new LinkedHashMap<>();
      Map<String, Descriptor> messagesByName = new LinkedHashMap<>();
      ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();
      for (FileDescriptor file : fileDescriptors.values()) {
        checkArgument(!fileDescriptorsByName.containsKey(file.getFullName()));
        fileDescriptorsByName.put(file.getFullName(), file);
        updateDescriptorMapsFromFileDescriptor(
            file, enumsByName, messagesByName, extensionRegistry);
      }

      return new ImmutableDescriptorPool(
          ImmutableMap.copyOf(fileDescriptorsByName),
          ImmutableMap.copyOf(enumsByName),
          ImmutableMap.copyOf(messagesByName),
          extensionRegistry.getUnmodifiable());
    }
  }

  static void updateDescriptorMapsFromFileDescriptor(
      FileDescriptor file,
      Map<String, EnumDescriptor> enumsByName,
      Map<String, Descriptor> messagesByName,
      ExtensionRegistry extensionRegistry) {
    for (EnumDescriptor enumDescriptor : file.getEnumTypes()) {
      addEnum(enumDescriptor, enumsByName);
    }
    for (Descriptor message : file.getMessageTypes()) {
      addMessage(message, enumsByName, messagesByName, extensionRegistry);
    }
    for (FieldDescriptor extension : file.getExtensions()) {
      if (extension.getJavaType() == JavaType.MESSAGE) {
        // We must handle MESSAGE type extensions differently since the Extension Registry requires
        // a default instance. The ExtensionRegistry also maintains two versions of the registry
        // (one for mutable extensions and one immutable extensions). It inspects the default
        // instance passed to determine which copy this message is added to. It is also possible to
        // call this again with the same dynamic message .mutableCopy() in order to provide a
        // mutable entry in the registry. It doesn't matter at the moment, since the
        // ZetaSQLDescriptorPool is only using this for structure navigation within protos and not
        // for actually instantiating them.
        extensionRegistry.add(
            extension, DynamicMessage.getDefaultInstance(extension.getMessageType()));
      } else {
        extensionRegistry.add(extension);
      }
    }
  }

  private static void addMessage(
      Descriptor descriptor,
      Map<String, EnumDescriptor> enumsByName,
      Map<String, Descriptor> messagesByName,
      ExtensionRegistry extensionRegistry) {
    messagesByName.put(descriptor.getFullName(), descriptor);
    for (Descriptor nestedDescriptor : descriptor.getNestedTypes()) {
      addMessage(nestedDescriptor, enumsByName, messagesByName, extensionRegistry);
    }
    for (EnumDescriptor nestedEnum : descriptor.getEnumTypes()) {
      addEnum(nestedEnum, enumsByName);
    }
    for (FieldDescriptor extension : descriptor.getExtensions()) {
      if (extension.getJavaType() == JavaType.MESSAGE) {
        extensionRegistry.add(extension, DynamicMessage.getDefaultInstance(descriptor));
      } else {
        extensionRegistry.add(extension);
      }
    }
  }

  private static void addEnum(
      EnumDescriptor enumDescriptor, Map<String, EnumDescriptor> enumsByName) {
    enumsByName.put(enumDescriptor.getFullName(), enumDescriptor);
  }

  /**
   * Returns the {@code ZetaSQLEnumDescriptor} of the enum with given {@code name} or null if not
   * found.
   */
  @Override
  @Nullable
  public ZetaSQLEnumDescriptor findEnumTypeByName(String name) {
    if (enumsByName.containsKey(name)) {
      return ImmutableZetaSQLEnumDescriptor.create(this, enumsByName.get(name));
    }
    return null;
  }
  /**
   * Returns the {@code ZetaSQLDescriptor} of the message with given {@code name} or null if not
   * found.
   */
  @Override
  @Nullable
  public ZetaSQLDescriptor findMessageTypeByName(String name) {
    if (messagesByName.containsKey(name)) {
      return ImmutableZetaSQLDescriptor.create(this, messagesByName.get(name));
    }
    return null;
  }

  /**
   * Returns the {@code ZetaSQLFileDescriptor} of the file with given {@code name} or null if not
   * found.
   */
  @Override
  @Nullable
  public ZetaSQLFileDescriptor findFileByName(String name) {
    if (fileDescriptorsByNameInDependencyOrder.containsKey(name)) {
      return ImmutableZetaSQLFileDescriptor.create(
          this, fileDescriptorsByNameInDependencyOrder.get(name));
    }
    return null;
  }

  /**
   * Returns the {@code ZetaSQLFieldDescriptor} on the given {@code descriptor} or null if not
   * found. Note, this method should resolve any extensions, and should not call or make use of the
   * convenience method {@link ZetaSQLFieldDescriptor.findFieldByNumber} to resolve this.
   *
   * <p>That method may make use of this one, however.
   */
  @Nullable
  @Override
  public ZetaSQLFieldDescriptor findFieldByNumber(
      DescriptorPool.ZetaSQLDescriptor descriptor, int number) {
    FieldDescriptor field = descriptor.getDescriptor().findFieldByNumber(number);
    if (field == null) {
      ExtensionInfo info =
          extensionRegistry.findImmutableExtensionByNumber(descriptor.getDescriptor(), number);
      if (info != null) {
        field = info.descriptor;
      }
    }
    return ImmutableZetaSQLFieldDescriptor.create(this, field);
  }

  @Override
  public ImmutableList<FileDescriptor> getAllFileDescriptorsInDependencyOrder() {
    return ImmutableList.copyOf(fileDescriptorsByNameInDependencyOrder.values());
  }

  static FileDescriptor resolveFileDescriptor(
      String filename,
      Map<String, FileDescriptorProto> fileDescriptorProtosByName,
      Map<String, FileDescriptor> resolvedFileDescriptorsByName) {
    FileDescriptor cached = resolvedFileDescriptorsByName.get(filename);
    if (cached != null) {
      return cached;
    }
    FileDescriptorProto proto = fileDescriptorProtosByName.get(filename);
    if (proto == null) {
      throw new IllegalArgumentException(
          "Inconsistent descriptor pool.  Missing definition for: " + filename);
    }
    FileDescriptor[] dependencies = new FileDescriptor[proto.getDependencyCount()];
    for (int i = 0; i < dependencies.length; i++) {
      String dependency = proto.getDependency(i);
      if (filename.equals(dependency)) {
        throw new IllegalArgumentException(
            "Invalid proto dependencies, recursion detected."
                + " "
                + proto.getName()
                + " depends on "
                + dependency
                + ".");
      }
      dependencies[i] =
          resolveFileDescriptor(
              dependency, fileDescriptorProtosByName, resolvedFileDescriptorsByName);
    }
    try {
      FileDescriptor descriptor = FileDescriptor.buildFrom(proto, dependencies);
      resolvedFileDescriptorsByName.put(filename, descriptor);
      return descriptor;
    } catch (DescriptorValidationException e) {
      throw new IllegalArgumentException("Invalid descriptor: " + proto.getName() + ". " + e);
    }
  }

  static ImmutableList<FileDescriptor> resolveFileDescriptors(
      Collection<FileDescriptorProto> fileDescriptorProtos) {
    Map<String, FileDescriptorProto> fileDescriptorProtosByName =
        fileDescriptorProtos.stream().collect(toMap(FileDescriptorProto::getName, p -> p));
    Map<String, FileDescriptor> resolvedFileDescriptorsByName = new LinkedHashMap<>();
    for (String filename : fileDescriptorProtosByName.keySet()) {
      resolveFileDescriptor(filename, fileDescriptorProtosByName, resolvedFileDescriptorsByName);
    }
    return ImmutableList.copyOf(resolvedFileDescriptorsByName.values());
  }

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private static final class SerializationProxy implements Serializable {
    private final ImmutableList<FileDescriptorProto> fileDescriptorProtos;

    SerializationProxy(ImmutableDescriptorPool concrete) {
      fileDescriptorProtos =
          concrete.fileDescriptorsByNameInDependencyOrder.values().stream()
              .map(FileDescriptor::toProto)
              .collect(toImmutableList());
    }

    private Object readResolve() {
      ImmutableList<FileDescriptor> fileDescriptors = resolveFileDescriptors(fileDescriptorProtos);
      return builder().importFileDescriptors(fileDescriptors).build();
    }
  }

  /**
   * Wrapped {@code FileDescriptor} with the {@code ZetaSQLDescriptorPool} from which it was
   * created.
   */
  @AutoValue
  abstract static class ImmutableZetaSQLFileDescriptor
      implements DescriptorPool.ZetaSQLFileDescriptor {
    /**
     * Creates an {@code ImmutableZetaSQLFileDescriptor}, note, this doesn't add it to the given
     * pool.
     */
    public static ImmutableZetaSQLFileDescriptor create(
        DescriptorPool pool, FileDescriptor descriptor) {
      return new AutoValue_ImmutableDescriptorPool_ImmutableZetaSQLFileDescriptor(
          pool, descriptor);
    }

    @Override
    public abstract DescriptorPool getDescriptorPool();

    @Override
    public abstract FileDescriptor getDescriptor();
  }

  /**
   * Wrapped {@code EnumDescriptor} with the {@code ZetaSQLDescriptorPool} from which it was
   * created.
   */
  @AutoValue
  abstract static class ImmutableZetaSQLEnumDescriptor
      implements DescriptorPool.ZetaSQLEnumDescriptor {
    /**
     * Creates an {@code ImmutableZetaSQLEnumDescriptor}, note, this doesn't add it to the given
     * pool.
     */
    public static ImmutableZetaSQLEnumDescriptor create(
        DescriptorPool pool, EnumDescriptor descriptor) {
      return new AutoValue_ImmutableDescriptorPool_ImmutableZetaSQLEnumDescriptor(
          pool, descriptor);
    }

    @Override
    public abstract DescriptorPool getDescriptorPool();

    @Override
    public abstract EnumDescriptor getDescriptor();
  }

  /**
   * Wrapped {@code Descriptor} with the {@code ZetaSQLDescriptorPool} from which it was created.
   */
  @AutoValue
  abstract static class ImmutableZetaSQLDescriptor implements DescriptorPool.ZetaSQLDescriptor {
    /**
     * Creates an {@code ImmutableZetaSQLDescriptor}, note, this doesn't add it to the given pool.
     */
    public static ImmutableZetaSQLDescriptor create(DescriptorPool pool, Descriptor descriptor) {
      return new AutoValue_ImmutableDescriptorPool_ImmutableZetaSQLDescriptor(pool, descriptor);
    }

    @Override
    public ImmutableZetaSQLOneofDescriptor findOneofByIndex(int index) {
      OneofDescriptor oneof = getDescriptor().getOneofs().get(index);
      return ImmutableZetaSQLOneofDescriptor.create(getDescriptorPool(), oneof);
    }

    @Override
    public abstract DescriptorPool getDescriptorPool();

    @Override
    public abstract Descriptor getDescriptor();
  }

  /**
   * Wrapped {@code FieldDescriptor} with the {@code ZetaSQLDescriptorPool} from which it was
   * created.
   */
  @AutoValue
  abstract static class ImmutableZetaSQLFieldDescriptor
      implements DescriptorPool.ZetaSQLFieldDescriptor {
    /**
     * Creates an {@code ImmutableZetaSQLFieldDescriptor}, note, this doesn't add it to the given
     * pool.
     */
    public static ImmutableZetaSQLFieldDescriptor create(
        DescriptorPool pool, FieldDescriptor descriptor) {
      return new AutoValue_ImmutableDescriptorPool_ImmutableZetaSQLFieldDescriptor(
          pool, descriptor);
    }

    @Override
    public abstract DescriptorPool getDescriptorPool();

    @Override
    public abstract FieldDescriptor getDescriptor();
  }

  /**
   * Wrapped {@code OneofDescriptor} with the {@code ZetaSQLDescriptorPool} from which it was
   * created.
   */
  @AutoValue
  abstract static class ImmutableZetaSQLOneofDescriptor
      implements DescriptorPool.ZetaSQLOneofDescriptor {
    /**
     * Creates an {@code ImmutableZetaSQLOneofDescriptor}, note, this doesn't add it to the given
     * pool.
     */
    public static ImmutableZetaSQLOneofDescriptor create(
        DescriptorPool pool, OneofDescriptor descriptor) {
      return new AutoValue_ImmutableDescriptorPool_ImmutableZetaSQLOneofDescriptor(
          pool, descriptor);
    }

    @Override
    public abstract DescriptorPool getDescriptorPool();

    @Override
    public abstract OneofDescriptor getDescriptor();
  }
}
