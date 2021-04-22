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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * Class mimicking the C++ version of DescriptorPool. It defines a mapping from fullname (package +
 * name) to various "ZetaSQL*Descriptor". Each of these is a wrapper around a
 * com.google.protobuf.Descriptor object with a reference back to this object.
 *
 * <p>
 *
 * @see "(broken link)"
 *     <p>This class is not thread safe.
 */
public interface DescriptorPool extends Serializable {

  /**
   * Returns the {@code ZetaSQLEnumDescriptor} of the enum with given {@code name} or null if not
   * found.
   */
  @Nullable
  ZetaSQLEnumDescriptor findEnumTypeByName(String name);

  /**
   * Returns the {@code ZetaSQLDescriptor} of the message with given {@code name} or null if not
   * found.
   */
  @Nullable
  ZetaSQLDescriptor findMessageTypeByName(String name);

  /**
   * Returns the {@code ZetaSQLFileDescriptor} of the file with given {@code name} or null if not
   * found.
   */
  @Nullable
  ZetaSQLFileDescriptor findFileByName(String name);

  /**
   * Returns the {@code ZetaSQLFieldDescriptor} on the given {@code descriptor} or null if not
   * found. Note, this method should resolve any extensions, and should not call or make use of the
   * convenience method {@link ZetaSQLFieldDescriptor.findFieldByNumber} to resolve this.
   *
   * <p>That method may make use of this one, however.
   */
  @Nullable
  ZetaSQLFieldDescriptor findFieldByNumber(ZetaSQLDescriptor descriptor, int number);

  /**
   * Wrapped {@code FileDescriptor} with the {@code ImmutableZetaSQLDescriptorPool} from which it
   * was created.
   */
  public interface ZetaSQLFileDescriptor {
    /** @deprecated use {@link #getDescriptorPool()} */
    @Deprecated
    default DescriptorPool getZetaSQLDescriptorPool() {
      return getDescriptorPool();
    }

    DescriptorPool getDescriptorPool();

    FileDescriptor getDescriptor();
  }

  /**
   * Wrapped {@code EnumDescriptor} with the {@code ImmutableZetaSQLDescriptorPool} from which it
   * was created.
   */
  public interface ZetaSQLEnumDescriptor {
    /** @deprecated use {@link #getDescriptorPool()} */
    @Deprecated
    default DescriptorPool getZetaSQLDescriptorPool() {
      return getDescriptorPool();
    }

    DescriptorPool getDescriptorPool();

    EnumDescriptor getDescriptor();
  }

  /**
   * Wrapped {@code Descriptor} with the {@code ImmutableZetaSQLDescriptorPool} from which it was
   * created.
   */
  public interface ZetaSQLDescriptor {

    /** @deprecated use {@link #getDescriptorPool()} */
    @Deprecated
    default DescriptorPool getZetaSQLDescriptorPool() {
      return getDescriptorPool();
    }

    DescriptorPool getDescriptorPool();

    Descriptor getDescriptor();

    default ZetaSQLFieldDescriptor findFieldByNumber(int number) {
      return getDescriptorPool().findFieldByNumber(this, number);
    }

    ZetaSQLOneofDescriptor findOneofByIndex(int index);
  }

  /**
   * Wrapped {@code FieldDescriptor} with the {@code ZetaSQLDescriptorPool} from which it was
   * created.
   */
  public interface ZetaSQLFieldDescriptor {
    /** @deprecated use {@link #getDescriptorPool()} */
    @Deprecated
    default DescriptorPool getZetaSQLDescriptorPool() {
      return getDescriptorPool();
    }

    DescriptorPool getDescriptorPool();

    FieldDescriptor getDescriptor();
  }

  /** Wrapped {@link OneofDescriptor} with the {@link DescriptorPool} from which it was created. */
  public interface ZetaSQLOneofDescriptor {
    /** @deprecated use {@link #getDescriptorPool()} */
    @Deprecated
    default DescriptorPool getZetaSQLDescriptorPool() {
      return getDescriptorPool();
    }

    DescriptorPool getDescriptorPool();

    OneofDescriptor getDescriptor();
  }

  /**
   * Return all the file descriptors in this descriptor pool in proto file dependency order.
   *
   * <p>Additionally, this should return all transitive dependencies explicitly.
   *
   * <p>This generally means descriptor.proto will be first, and application specific protos will be
   * last.
   */
  ImmutableList<FileDescriptor> getAllFileDescriptorsInDependencyOrder();
}
