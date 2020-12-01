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
   * Wrapped {@code FileDescriptor} with the {@code ImmutableZetaSQLDescriptorPool} from which it
   * was created.
   */
  public interface ZetaSQLFileDescriptor extends Serializable {
    DescriptorPool getZetaSQLDescriptorPool();

    DescriptorPool getDescriptorPool();

    FileDescriptor getDescriptor();
  }

  /**
   * Wrapped {@code EnumDescriptor} with the {@code ImmutableZetaSQLDescriptorPool} from which it
   * was created.
   */
  public interface ZetaSQLEnumDescriptor extends Serializable {
    DescriptorPool getZetaSQLDescriptorPool();

    DescriptorPool getDescriptorPool();

    EnumDescriptor getDescriptor();
  }

  /**
   * Wrapped {@code Descriptor} with the {@code ImmutableZetaSQLDescriptorPool} from which it was
   * created.
   */
  public interface ZetaSQLDescriptor extends Serializable {
    DescriptorPool getZetaSQLDescriptorPool();

    DescriptorPool getDescriptorPool();

    Descriptor getDescriptor();

    ZetaSQLFieldDescriptor findFieldByNumber(int number);
  }

  /**
   * Wrapped {@code FieldDescriptor} with the {@code ZetaSQLDescriptorPool} from which it was
   * created.
   */
  public interface ZetaSQLFieldDescriptor extends Serializable {
    DescriptorPool getZetaSQLDescriptorPool();

    DescriptorPool getDescriptorPool();

    FieldDescriptor getDescriptor();
  }

  ImmutableList<FileDescriptor> getAllFileDescriptors();
}
