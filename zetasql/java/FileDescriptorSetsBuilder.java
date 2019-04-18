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
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * Builds List of FileDescriptorSet which will be passed along with serialized Types in ZetaSQL
 * RPCs. This is mostly for internal use only, but made public so that it can be used by the
 * resolvedast package.
 */
public class FileDescriptorSetsBuilder implements Serializable {
  // We are not using a Map<ZetaSQLDescriptorPool, FileDescriptorSetsBuilder>
  // because we need the integer index explicitly.
  private final Map<ZetaSQLDescriptorPool, Integer> descriptorPoolIndex = new HashMap<>();
  private final List<FileDescriptorSetBuilder> fileDescriptorSetBuilders = new ArrayList<>();
  // Whether build() is called.
  private boolean isBuilt = false;

  /**
   * Build the List of FileDescriptorSet to be passed together with serialized
   * Types in a ZetaSQL RPC request. To deserialize types in the
   * response, a list of ZetaSQLDescriptorPool in the same order must be used,
   * which can be retrieved using {@link #getDescriptorPools}.
   *
   * @throws IllegalStateException if called more than once.
   */
  public ImmutableList<FileDescriptorSet> build() {
    Preconditions.checkState(!isBuilt);
    ImmutableList.Builder<FileDescriptorSet> fileDescriptorSets = ImmutableList.builder();
    for (FileDescriptorSetBuilder fileDescriptorSetBuilder : fileDescriptorSetBuilders) {
      fileDescriptorSets.add(fileDescriptorSetBuilder.fileDescriptorSet.build());
    }
    isBuilt = true;
    return fileDescriptorSets.build();
  }

  ImmutableList<ZetaSQLDescriptorPool> getDescriptorPools() {
    Map<Integer, ZetaSQLDescriptorPool> pools = new TreeMap<>();
    // Sort keys of descriptorPoolIndex map by value.
    for (Entry<ZetaSQLDescriptorPool, Integer> entry : descriptorPoolIndex.entrySet()) {
      pools.put(entry.getValue(), entry.getKey());
    }
    return ImmutableList.copyOf(pools.values());
  }

  public int getFileDescriptorSetCount() {
    return fileDescriptorSetBuilders.size();
  }

  int addAllFileDescriptors(ZetaSQLDescriptorPool pool) {
    for (FileDescriptor descriptor : pool.getAllFileDescriptors()) {
      addFileDescriptor(descriptor, pool);
    }

    return descriptorPoolIndex.get(pool);
  }

  /**
   * Add the given {@code fileDescriptor} and its dependencies into the
   * {@code FileDescriptorSet} associated with the given {@code pool}.
   *
   * @return Index of the FileDescriptorSet the {@code fileDescriptor} goes
   *     into.
   * @throws IllegalStateException if a new fileDescriptor is added after
   *     build() is called.
   */
  int addFileDescriptor(FileDescriptor fileDescriptor, ZetaSQLDescriptorPool pool) {
    Preconditions.checkState(!isBuilt);
    Integer index = descriptorPoolIndex.get(pool);
    if (index == null) {
      // When we see a new ZetaSQLDescriptorPool, we need to create a new
      // FileDescriptorSet to accommodate the FileDescriptors belonging to it.
      // Otherwise FileDescriptors with the same filename but from different
      // pools will collide when the FileDescriptorSet is deserialized back to
      // ZetaSQLDescriptorPool.
      fileDescriptorSetBuilders.add(new FileDescriptorSetBuilder());
      // The array index grows with the number of ZetaSQLDescriptorPools
      // we ever seen. It is set in ProtoTypeProto and EnumTypeProto fields
      // to index the List<FileDescriptorSet> built by this class.
      index = fileDescriptorSetBuilders.size() - 1;
      descriptorPoolIndex.put(pool, index);
    }
    fileDescriptorSetBuilders.get(index).add(fileDescriptor);
    return index;
  }
  
  void importDescriptorPoolIndex(FileDescriptorSetsBuilder other) {
    Preconditions.checkState(descriptorPoolIndex.isEmpty());
    Preconditions.checkState(fileDescriptorSetBuilders.isEmpty());
    for (Entry<ZetaSQLDescriptorPool, Integer> entry : other.descriptorPoolIndex.entrySet()) {
      descriptorPoolIndex.put(entry.getKey(), entry.getValue());
      fileDescriptorSetBuilders.add(new FileDescriptorSetBuilder());
    }
  }

  /** Merge another FileDescriptorSetsBuilder into this one, and return the difference. */
  FileDescriptorSetsBuilder mergeDiff(FileDescriptorSetsBuilder other) {
    FileDescriptorSetsBuilder result = new FileDescriptorSetsBuilder();
    result.importDescriptorPoolIndex(other);
    for (int i = fileDescriptorSetBuilders.size();
        i < other.fileDescriptorSetBuilders.size(); ++i) {
      FileDescriptorSetBuilder otherFileDescriptorSet =
          other.fileDescriptorSetBuilders.get(i);
      fileDescriptorSetBuilders.add(otherFileDescriptorSet);
      result.fileDescriptorSetBuilders.set(i, otherFileDescriptorSet);
    }
    for (Entry<ZetaSQLDescriptorPool, Integer> entry : other.descriptorPoolIndex.entrySet()) {
      ZetaSQLDescriptorPool pool = entry.getKey();
      int index = entry.getValue();
      if (descriptorPoolIndex.containsKey(entry.getKey())) {
        Preconditions.checkState(index == descriptorPoolIndex.get(entry.getKey()));
        FileDescriptorSetBuilder thisFileDescriptorSet = fileDescriptorSetBuilders.get(index);
        Preconditions.checkState(thisFileDescriptorSet != null);
        FileDescriptorSetBuilder otherFileDescriptorSet =
            other.fileDescriptorSetBuilders.get(index);
        Preconditions.checkState(otherFileDescriptorSet != null);
        result.fileDescriptorSetBuilders.set(
            index, thisFileDescriptorSet.mergeDiff(otherFileDescriptorSet));
      } else {
        descriptorPoolIndex.put(pool, index);
        FileDescriptorSetBuilder otherFileDescriptorSet =
            other.fileDescriptorSetBuilders.get(index);
        Preconditions.checkState(otherFileDescriptorSet != null);
        Preconditions.checkState(fileDescriptorSetBuilders.get(index) == otherFileDescriptorSet);
      }
    }
    return result;
  }
  
  private Integer getFileDescriptorIndex(
      FileDescriptor fileDescriptor, ZetaSQLDescriptorPool pool) {
    Integer index = descriptorPoolIndex.get(pool);
    if (index != null) {
      FileDescriptorSetBuilder fileDescriptorSetBuilder = fileDescriptorSetBuilders.get(index);
      if (!fileDescriptorSetBuilder.contains(fileDescriptor)) {
        return null;
      }
    }
    return index;
  }

  /**
   * Returns the index of FileDescriptorSet containing the given
   * {@code fileDescriptor}, adding it if necessary.
   */
  public int getOrAddFileDescriptorIndex(
      FileDescriptor fileDescriptor, ZetaSQLDescriptorPool pool) {
    Integer index = getFileDescriptorIndex(fileDescriptor, pool);
    if (index == null) {
      return addFileDescriptor(fileDescriptor, pool);
    } else {
      return index;
    }
  }

  /** Dedupping builder for FileDescriptorSet proto. */
  private static class FileDescriptorSetBuilder implements Serializable {
    // The actual builder for the FileDescriptorSet proto.
    private transient FileDescriptorSet.Builder fileDescriptorSet = FileDescriptorSet.newBuilder();

    // Set container for deduplication.
    private final Set<FileDescriptorProto> fileDescriptors = new HashSet<>();

    /**
     * Add the given {@code fileDescriptor} and its dependencies to the
     * {@code FileDescriptorSet}, skipping duplicates.
     */
    private void add(FileDescriptor fileDescriptor) {
      if (fileDescriptors.contains(fileDescriptor.toProto())) {
        return;
      }
      for (FileDescriptor dependency : fileDescriptor.getDependencies()) {
        add(dependency);
      }
      fileDescriptorSet.addFile(fileDescriptor.toProto());
      fileDescriptors.add(fileDescriptor.toProto());
    }

    boolean contains(FileDescriptor fileDescriptor) {
      return fileDescriptors.contains(fileDescriptor.toProto());
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
      out.defaultWriteObject();
      fileDescriptorSet.build().toByteString().writeTo(out);
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      this.fileDescriptorSet = FileDescriptorSet.newBuilder().mergeFrom(ByteString.readFrom(in));
    }
    
    /** Merge another FileDescriptorSetBuilder into this one, and return the difference. */
    private FileDescriptorSetBuilder mergeDiff(FileDescriptorSetBuilder otherFileDescriptorSet) {
      FileDescriptorSetBuilder result = new FileDescriptorSetBuilder();
      for (FileDescriptorProto fileDescriptor : otherFileDescriptorSet.fileDescriptors) {
        if (!fileDescriptors.contains(fileDescriptor)) {
          fileDescriptors.add(fileDescriptor);
          result.fileDescriptors.add(fileDescriptor);
        }
      }
      return result;
    }
  }
}
