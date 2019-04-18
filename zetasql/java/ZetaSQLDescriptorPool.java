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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.GenericDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ExtensionRegistry.ExtensionInfo;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Class mimicking the C++ version of DescriptorPool. The Java DescriptorPool is a private class,
 * and it has different semantics from the C++ version. We need one that follows the C++ semantics
 * to make sure serialization of types that uses descriptors from more than 1 pool have the same
 * behavior.
 *
 * <p>The existing com.google.protobuf.contrib.descriptor_pool.DescriptorPool is incomplete and
 * doesn't meet our requirements, for details,
 *
 * @see "(broken link)"
 *
 * This class is not thread safe.
 */
public class ZetaSQLDescriptorPool implements Serializable {
  // Global pool for generated descriptors i.e. those compiled into the Java
  // program, rather than loaded from protobuf. Generated descriptors will be
  // imported into this pool when they are used to create types.
  private static final GeneratedDescriptorPool generatedDescriptorPool =
      new GeneratedDescriptorPool();

  /**
   * Imports the given generated {@code descriptor} to the global {@code generatedDescriptorPool};
   */
  static void importIntoGeneratedPool(GenericDescriptor descriptor) {
    generatedDescriptorPool.importFileDescriptor(descriptor);
  }

  static ZetaSQLDescriptorPool getGeneratedPool() {
    return generatedDescriptorPool;
  }

  transient Map<String, FileDescriptor> fileDescriptorsByName = new LinkedHashMap<>();
  private transient Map<String, EnumDescriptor> enumsByName = new LinkedHashMap<>();
  private transient Map<String, Descriptor> messagesByName = new LinkedHashMap<>();
  private transient ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();

  /**
   * Imports the {@code fileDescriptorSet} into the descriptor pool.
   *
   * @throws IllegalArgumentException if {@code fileDescriptorSet} is not self-contained, i.e.
   *     contains a FileDescriptor whose dependencies are not within the set.
   */
  public void importFileDescriptorSet(FileDescriptorSet fileDescriptorSet) {
    importFileDescriptors(fileDescriptorSet.getFileList());
  }

  /**
   * Imports a collection of {@code fileDescriptors} into the descriptor pool.
   *
   * @throws IllegalArgumentException if @{fileDescriptors} are not self-contained, i.e. have
   * dependencies that are not within the collection.
   */
  public void importFileDescriptors(Collection<FileDescriptorProto> fileDescriptors) {
    importFileDescriptorsInternal(fileDescriptors);
  }

  /**
   * Internal version to be called directly instead of the overridden {@link #importFileDescriptors}
   * in {@link GeneratedDescriptorPool}.
   */
  protected final void importFileDescriptorsInternal(
      Collection<FileDescriptorProto> fileDescriptors) {
    Map<String, FileDescriptorProto> filenameToFileDescriptorProtoMap = new LinkedHashMap<>();
    for (FileDescriptorProto fileDescriptorProto : fileDescriptors) {
      filenameToFileDescriptorProtoMap.put(fileDescriptorProto.getName(), fileDescriptorProto);
    }
    for (String filename : filenameToFileDescriptorProtoMap.keySet()) {
      resolve(filename, filenameToFileDescriptorProtoMap);
    }
  }

  /**
   * Add the {@code FileDescriptor} of given {@code filename} into the pool. Dependencies will be
   * added first if any. All {@code FileDescriptorProto}s of the specified file and its
   * dependencies should be in the {@code filenameToFileDescriptorProtoMap}, indexed by
   * {@code filename}.
   *
   * @return the {@code FileDescriptor} of the specified {@code filename}
   * @throws IllegalArgumentException if {@code filenameToFileDescriptorProtoMap} is incomplete/has
   * recursion/contains invalid descriptor
   */
  private FileDescriptor resolve(
      String filename, Map<String, FileDescriptorProto> filenameToFileDescriptorProtoMap) {
    FileDescriptor cached = fileDescriptorsByName.get(filename);
    if (cached != null) {
      return cached;
    }
    FileDescriptorProto proto = filenameToFileDescriptorProtoMap.get(filename);
    if (proto == null) {
      throw new IllegalArgumentException(
          "Inconsistent descriptor pool. " + "Missing definition for: " + filename);
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
      dependencies[i] = resolve(dependency, filenameToFileDescriptorProtoMap);
    }
    FileDescriptor descriptor;
    try {
      descriptor = FileDescriptor.buildFrom(proto, dependencies);
    } catch (DescriptorValidationException e) {
      throw new IllegalArgumentException(
          "Invalid descriptor: " + proto.getName() + ". " + e.toString());
    }
    addFileDescriptor(descriptor);
    return descriptor;
  }

  /**
   * Adds a newly parsed {@code file} and its message/enum descriptors into the named maps for
   * future lookup.
   */
  protected void addFileDescriptor(FileDescriptor file) {
    if (fileDescriptorsByName.containsKey(file.getFullName())) {
      return;
    }

    fileDescriptorsByName.put(file.getFullName(), file);
    for (EnumDescriptor enumDescriptor : file.getEnumTypes()) {
      addEnum(enumDescriptor);
    }
    for (Descriptor message : file.getMessageTypes()) {
      addMessage(message);
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

  private void addMessage(Descriptor descriptor) {
    messagesByName.put(descriptor.getFullName(), descriptor);
    for (Descriptor nestedDescriptor : descriptor.getNestedTypes()) {
      addMessage(nestedDescriptor);
    }
    for (EnumDescriptor nestedEnum : descriptor.getEnumTypes()) {
      addEnum(nestedEnum);
    }
    for (FieldDescriptor extension : descriptor.getExtensions()) {
      if (extension.getJavaType() == JavaType.MESSAGE) {
        extensionRegistry.add(extension, DynamicMessage.getDefaultInstance(descriptor));
      } else {
        extensionRegistry.add(extension);
      }
    }
  }

  private void addEnum(EnumDescriptor enumDescriptor) {
    enumsByName.put(enumDescriptor.getFullName(), enumDescriptor);
  }

  /**
   * Returns the {@code ZetaSQLEnumDescriptor} of the enum with given {@code name} or null if not
   * found.
   */
  @Nullable
  public ZetaSQLEnumDescriptor findEnumTypeByName(String name) {
    if (enumsByName.containsKey(name)) {
      return new ZetaSQLEnumDescriptor(enumsByName.get(name), this);
    }
    return null;
  }

  /**
   * Returns the {@code ZetaSQLDescriptor} of the message with given {@code name} or null if not
   * found.
   */
  @Nullable
  public ZetaSQLDescriptor findMessageTypeByName(String name) {
    if (messagesByName.containsKey(name)) {
      return new ZetaSQLDescriptor(messagesByName.get(name), this);
    }
    return null;
  }

  /**
   * Returns the {@code ZetaSQLFileDescriptor} of the file with given {@code name} or null if not
   * found.
   */
  @Nullable
  public ZetaSQLFileDescriptor findFileByName(String name) {
    if (fileDescriptorsByName.containsKey(name)) {
      return new ZetaSQLFileDescriptor(fileDescriptorsByName.get(name), this);
    }
    return null;
  }

  ImmutableList<FileDescriptor> getAllFileDescriptors() {
    return ImmutableList.copyOf(fileDescriptorsByName.values());
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    out.writeInt(fileDescriptorsByName.size());
    for (FileDescriptor descriptor : fileDescriptorsByName.values()) {
      out.writeObject(descriptor.toProto());
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    fileDescriptorsByName = new LinkedHashMap<>();
    enumsByName = new LinkedHashMap<>();
    messagesByName = new LinkedHashMap<>();
    extensionRegistry = ExtensionRegistry.newInstance();
    int fileDescriptorsSize = in.readInt();
    List<FileDescriptorProto> descriptorProtos = new ArrayList<>();
    for (int i = 0; i < fileDescriptorsSize; i++) {
      FileDescriptorProto descriptor = (FileDescriptorProto) in.readObject();
      descriptorProtos.add(descriptor);
    }
    importFileDescriptorsInternal(descriptorProtos);
  }

  private abstract static class ZetaSQLGenericDescriptor<T extends GenericDescriptor>
      implements Serializable {
    private final ZetaSQLDescriptorPool pool;
    protected transient T descriptor;

    private ZetaSQLGenericDescriptor(T descriptor, ZetaSQLDescriptorPool pool) {
      this.descriptor = checkNotNull(descriptor);
      this.pool = checkNotNull(pool);
    }

    public final ZetaSQLDescriptorPool getZetaSQLDescriptorPool() {
      return pool;
    }

    public final T getDescriptor() {
      return descriptor;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ZetaSQLDescriptor) {
        @SuppressWarnings("unchecked")
        ZetaSQLDescriptor that = (ZetaSQLDescriptor) o;
        return Objects.equals(this.getDescriptor(), that.getDescriptor())
            && Objects.equals(this.getZetaSQLDescriptorPool(), that.getZetaSQLDescriptorPool());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getDescriptor(), getZetaSQLDescriptorPool());
    }
  }

  /**
   * Wrapped {@code FileDescriptor} with the {@code ZetaSQLDescriptorPool} from which it was
   * created.
   */
  public static class ZetaSQLFileDescriptor extends ZetaSQLGenericDescriptor<FileDescriptor> {
    private ZetaSQLFileDescriptor(FileDescriptor descriptor, ZetaSQLDescriptorPool pool) {
      super(descriptor, pool);
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
      out.defaultWriteObject();
      out.writeObject(descriptor.toProto());
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      FileDescriptorProto proto = (FileDescriptorProto) in.readObject();
      this.descriptor =
          getZetaSQLDescriptorPool().findFileByName(proto.getName()).getDescriptor();
    }
  }

  /**
   * Wrapped {@code EnumDescriptor} with the {@code ZetaSQLDescriptorPool} from which it was
   * created.
   */
  public static class ZetaSQLEnumDescriptor extends ZetaSQLGenericDescriptor<EnumDescriptor> {
    private ZetaSQLEnumDescriptor(EnumDescriptor descriptor, ZetaSQLDescriptorPool pool) {
      super(descriptor, pool);
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
      out.defaultWriteObject();
      out.writeObject(descriptor.toProto());
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      EnumDescriptorProto proto = (EnumDescriptorProto) in.readObject();
      this.descriptor =
          getZetaSQLDescriptorPool().findEnumTypeByName(proto.getName()).getDescriptor();
    }
  }

  /**
   * Wrapped {@code Descriptor} with the {@code ZetaSQLDescriptorPool} from which it was created.
   */
  public static class ZetaSQLDescriptor extends ZetaSQLGenericDescriptor<Descriptor> {
    private ZetaSQLDescriptor(Descriptor descriptor, ZetaSQLDescriptorPool pool) {
      super(descriptor, pool);
    }

    public ZetaSQLFieldDescriptor findFieldByNumber(int number) {
      FieldDescriptor field = getDescriptor().findFieldByNumber(number);
      if (field == null) {
        ExtensionInfo info =
            getZetaSQLDescriptorPool()
                .extensionRegistry
                .findExtensionByNumber(getDescriptor(), number);
        if (info != null) {
          field = info.descriptor;
        }
      }
      return new ZetaSQLFieldDescriptor(field, getZetaSQLDescriptorPool());
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
      out.defaultWriteObject();
      out.writeObject(descriptor.toProto());
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      DescriptorProto proto = (DescriptorProto) in.readObject();
      this.descriptor =
          getZetaSQLDescriptorPool().findMessageTypeByName(proto.getName()).getDescriptor();
    }
  }

  /**
   * Wrapped {@link FieldDescriptor} with the {@link ZetaSQLDescriptorPool} from which it was
   * created.
   */
  public static class ZetaSQLFieldDescriptor extends ZetaSQLGenericDescriptor<FieldDescriptor> {
    private ZetaSQLFieldDescriptor(FieldDescriptor descriptor, ZetaSQLDescriptorPool pool) {
      super(descriptor, pool);
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
      out.defaultWriteObject();
      out.writeUTF(descriptor.getContainingType().getFullName());
      out.writeInt(descriptor.getNumber());
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      String fullName = in.readUTF();
      int index = in.readInt();
      this.descriptor =
          getZetaSQLDescriptorPool()
              .findMessageTypeByName(fullName)
              .findFieldByNumber(index)
              .getDescriptor();
    }
  }

  /**
   * Pool for generated descriptors, i.e. those generated from .proto files and compiled into the
   * Java program. This is supposed to be shared by globally and is thread safe. Descriptors are
   * imported lazily because there's no efficient way to do it in a static intializer. Imported
   * descriptors are kept forever, which is fine since the number of generated descriptors is
   * fixed when the program is compiled.
   */
  private static class GeneratedDescriptorPool extends ZetaSQLDescriptorPool {
    /**
     * Imports the given generated {@code descriptor} and its dependencies.
     */
    synchronized void importFileDescriptor(GenericDescriptor descriptor) {
      FileDescriptor file = descriptor.getFile();
      if (fileDescriptorsByName.containsKey(file.getFullName())) {
        return;
      }

      for (FileDescriptor dependency : file.getDependencies()) {
        importFileDescriptor(dependency);
      }
      addFileDescriptor(file);
    }

    @Override
    public void importFileDescriptors(Collection<FileDescriptorProto> fileDescriptors) {
      throw new IllegalStateException(
          "Importing FileDescriptor from proto to the generated pool not supported.");
    }

    @Override
    public synchronized ZetaSQLEnumDescriptor findEnumTypeByName(String name) {
      return super.findEnumTypeByName(name);
    }

    @Override
    public synchronized ZetaSQLDescriptor findMessageTypeByName(String name) {
      return super.findMessageTypeByName(name);
    }

    @Override
    public synchronized ZetaSQLFileDescriptor findFileByName(String name) {
      return super.findFileByName(name);
    }

    @Override
    synchronized ImmutableList<FileDescriptor> getAllFileDescriptors() {
      return super.getAllFileDescriptors();
    }
  }
}
