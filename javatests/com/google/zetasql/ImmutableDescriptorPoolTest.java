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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.SerializableTester;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Timestamp;
import com.google.zetasql.ImmutableDescriptorPool.ImmutableZetaSQLDescriptor;
import com.google.zetasql.ImmutableDescriptorPool.ImmutableZetaSQLEnumDescriptor;
import com.google.zetasql.ImmutableDescriptorPool.ImmutableZetaSQLFieldDescriptor;
import com.google.zetasql.ImmutableDescriptorPool.ImmutableZetaSQLFileDescriptor;
import com.google.zetasql.ImmutableDescriptorPool.ImmutableZetaSQLOneofDescriptor;
import com.google.zetasqltest.TestSchemaProto.AnotherTestEnum;
import com.google.zetasqltest.TestSchemaProto.KitchenSinkPB;
import com.google.zetasqltest.TestSchemaProto.MessageWithMapField;
import com.google.zetasqltest.TestSchemaProto.TestEnum;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)

public class ImmutableDescriptorPoolTest {
  @Test
  public void testImmutableDescriptorPoolSerializableEmpty() {
    ImmutableDescriptorPool emptyPool = ImmutableDescriptorPool.builder().build();
    assertThat(emptyPool.getAllFileDescriptorsInDependencyOrder()).isEmpty();

    ImmutableDescriptorPool reserializedPool = SerializableTester.reserialize(emptyPool);
    assertThat(reserializedPool.getAllFileDescriptorsInDependencyOrder()).isEmpty();
  }

  private static Set<String> getFilenames(DescriptorPool pool) {
    return pool.getAllFileDescriptorsInDependencyOrder().stream()
        .map(FileDescriptor::getFullName)
        .collect(Collectors.toSet());
  }

  static void assertFileDescriptorsAreDependencyOrdered(DescriptorPool pool) {
    Set<String> fileNames = new LinkedHashSet<>();
    for (FileDescriptor file : pool.getAllFileDescriptorsInDependencyOrder()) {
      fileNames.add(file.getFullName());
      for (FileDescriptor dependency : file.getDependencies()) {
        assertThat(fileNames).contains(dependency.getFullName());
      }
    }
  }

  @Test
  public void testImmutableDescriptorPoolSerializable() {
    ImmutableDescriptorPool pool =
        ImmutableDescriptorPool.builder()
            .importFileDescriptor(TestEnum.getDescriptor().getFile())
            .build();
    assertFileDescriptorsAreDependencyOrdered(pool);
    EnumDescriptor testEnumDescriptor =
        pool.findEnumTypeByName("zetasql_test__.TestEnum").getDescriptor();
    assertThat(testEnumDescriptor).isEqualTo(TestEnum.getDescriptor());

    ImmutableSet<String> expectedDescriptorNames =
        ImmutableSet.of(
            "zetasql/public/proto/wire_format_annotation.proto",
            "google/protobuf/descriptor.proto",
            "zetasql/public/proto/type_annotation.proto",
            "zetasql/testdata/test_schema.proto");
    assertThat(getFilenames(pool)).containsExactlyElementsIn(expectedDescriptorNames);

    ImmutableDescriptorPool reserializedPool = SerializableTester.reserialize(pool);
    assertFileDescriptorsAreDependencyOrdered(reserializedPool);
    assertThat(getFilenames(reserializedPool)).containsExactlyElementsIn(expectedDescriptorNames);

    EnumDescriptor reserializedTestEnumDescriptor =
        reserializedPool.findEnumTypeByName("zetasql_test__.TestEnum").getDescriptor();

    // We expect that the descriptor will not be equal to the input.
    assertThat(reserializedTestEnumDescriptor).isNotEqualTo(testEnumDescriptor);
    // However, the proto should match
    assertThat(reserializedTestEnumDescriptor.toProto()).isEqualTo(testEnumDescriptor.toProto());
  }

  @Test
  public void testImmutableZetaSQLFileDescriptorHashEquals() {
    ImmutableDescriptorPool pool = ImmutableDescriptorPool.builder().build();
    ImmutableDescriptorPool pool2 = ImmutableDescriptorPool.builder().build();

    FileDescriptor descriptor1 = TestEnum.getDescriptor().getFile();
    FileDescriptor descriptor2 = Timestamp.getDescriptor().getFile();
    new EqualsTester()
        .addEqualityGroup(
            ImmutableZetaSQLFileDescriptor.create(pool, descriptor1),
            ImmutableZetaSQLFileDescriptor.create(pool, descriptor1))
        .addEqualityGroup(ImmutableZetaSQLFileDescriptor.create(pool, descriptor2))
        .addEqualityGroup(ImmutableZetaSQLFileDescriptor.create(pool2, descriptor1))
        .testEquals();
  }

  @Test
  public void testImmutableZetaSQLEnumDescriptorHashEquals() {
    ImmutableDescriptorPool pool = ImmutableDescriptorPool.builder().build();
    ImmutableDescriptorPool pool2 = ImmutableDescriptorPool.builder().build();

    EnumDescriptor descriptor1 = TestEnum.getDescriptor();
    EnumDescriptor descriptor2 = AnotherTestEnum.getDescriptor();
    new EqualsTester()
        .addEqualityGroup(
            ImmutableZetaSQLEnumDescriptor.create(pool, descriptor1),
            ImmutableZetaSQLEnumDescriptor.create(pool, descriptor1))
        .addEqualityGroup(ImmutableZetaSQLEnumDescriptor.create(pool, descriptor2))
        .addEqualityGroup(ImmutableZetaSQLEnumDescriptor.create(pool2, descriptor1))
        .testEquals();
  }

  @Test
  public void testImmutableZetaSQLDescriptorHashEquals() {
    ImmutableDescriptorPool pool = ImmutableDescriptorPool.builder().build();
    ImmutableDescriptorPool pool2 = ImmutableDescriptorPool.builder().build();

    Descriptor descriptor1 = KitchenSinkPB.getDescriptor();
    Descriptor descriptor2 = MessageWithMapField.getDescriptor();
    new EqualsTester()
        .addEqualityGroup(
            ImmutableZetaSQLDescriptor.create(pool, descriptor1),
            ImmutableZetaSQLDescriptor.create(pool, descriptor1))
        .addEqualityGroup(ImmutableZetaSQLDescriptor.create(pool, descriptor2))
        .addEqualityGroup(ImmutableZetaSQLDescriptor.create(pool2, descriptor1))
        .testEquals();
  }

  @Test
  public void testImmutableZetaSQLFieldDescriptorHashEquals() {
    ImmutableDescriptorPool pool = ImmutableDescriptorPool.builder().build();
    ImmutableDescriptorPool pool2 = ImmutableDescriptorPool.builder().build();

    FieldDescriptor descriptor1 = KitchenSinkPB.getDescriptor().findFieldByName("int64_key_1");
    FieldDescriptor descriptor2 = KitchenSinkPB.getDescriptor().findFieldByName("int64_key_2");
    new EqualsTester()
        .addEqualityGroup(
            ImmutableZetaSQLFieldDescriptor.create(pool, descriptor1),
            ImmutableZetaSQLFieldDescriptor.create(pool, descriptor1))
        .addEqualityGroup(ImmutableZetaSQLFieldDescriptor.create(pool, descriptor2))
        .addEqualityGroup(ImmutableZetaSQLFieldDescriptor.create(pool2, descriptor1))
        .testEquals();
  }

  @Test
  public void testImmutableZetaSQLOneofDescriptorHashEquals() {
    ImmutableDescriptorPool pool = ImmutableDescriptorPool.builder().build();
    ImmutableDescriptorPool pool2 = ImmutableDescriptorPool.builder().build();

    // int32_one_of and string_one_of are both members of one_of_field
    OneofDescriptor descriptor1 =
        KitchenSinkPB.getDescriptor().findFieldByName("int32_one_of").getContainingOneof();
    OneofDescriptor descriptor1a =
        KitchenSinkPB.getDescriptor().findFieldByName("string_one_of").getContainingOneof();
    // int64_one_of is a member of one_of_field2
    OneofDescriptor descriptor2 =
        KitchenSinkPB.getDescriptor().findFieldByName("int64_one_of").getContainingOneof();
    new EqualsTester()
        .addEqualityGroup(
            ImmutableZetaSQLOneofDescriptor.create(pool, descriptor1),
            ImmutableZetaSQLOneofDescriptor.create(pool, descriptor1),
            ImmutableZetaSQLOneofDescriptor.create(pool, descriptor1a))
        .addEqualityGroup(ImmutableZetaSQLOneofDescriptor.create(pool, descriptor2))
        .addEqualityGroup(ImmutableZetaSQLOneofDescriptor.create(pool2, descriptor1))
        .testEquals();
  }
}
