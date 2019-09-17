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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.fail;

import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import com.google.zetasql.ZetaSQLOptionsProto.AllowedHintsAndOptionsProto;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.ZetaSQLType.TypeProto;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AllowedHintsAndOptionsTest {

  @Test
  public void testAddOption() {
    AllowedHintsAndOptions allowed = new AllowedHintsAndOptions("qual");
    allowed.addOption("option1", TypeFactory.createSimpleType(TypeKind.TYPE_BYTES));
    allowed.addOption("Option2", null);
    try {
      allowed.addOption(null, null);
      fail();
    } catch (NullPointerException expected) {
    }
    try {
      allowed.addOption("", null);
      fail();
    } catch (IllegalArgumentException expected) {
    }
    try {
      allowed.addOption("OPTION1", null);
      fail();
    } catch (IllegalArgumentException expected) {
    }
    try {
      allowed.addOption("option2", null);
      fail();
    } catch (IllegalArgumentException expected) {
    }

    assertThat(allowed.getOptionType("OPTION1")).isNotNull();
    assertThat(allowed.getOptionNameList()).hasSize(2);
  }

  @Test
  public void testAddHint() {
    AllowedHintsAndOptions allowed = new AllowedHintsAndOptions("qual");
    allowed.addHint("test_qual", "hint1", TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE), true);
    allowed.addHint("test_qual", "HINT2", null, false);
    allowed.addHint("", "hint2", null, true);
    allowed.addHint("", "hint3", null, true);
    assertThat(allowed.getHintList()).hasSize(5);
    try {
      allowed.addHint(null, "hint", null, true);
      fail();
    } catch (NullPointerException expected) {
    }
    try {
      allowed.addHint("qual", null, null, true);
      fail();
    } catch (NullPointerException expected) {
    }
    try {
      allowed.addHint("qual", "", null, true);
      fail();
    } catch (IllegalArgumentException expected) {
    }
    try {
      allowed.addHint("", "hint", null, false);
      fail();
    } catch (IllegalArgumentException expected) {
    }
    try {
      allowed.addHint("TEST_qual", "Hint1", null, true);
      fail();
    } catch (IllegalArgumentException expected) {
    }
    try {
      allowed.addHint("TEST_qual", "Hint3", null, true);
      fail();
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testSerializeAndDeserialize() throws ParseException {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    AllowedHintsAndOptions allowed = new AllowedHintsAndOptions("Qual");
    allowed.addHint("test_qual", "hint1", factory.createProtoType(TypeProto.class), true);
    allowed.addOption("option1", null);
    FileDescriptorSetsBuilder builder = new FileDescriptorSetsBuilder();
    AllowedHintsAndOptions allowed2 =
        AllowedHintsAndOptions.deserialize(
            allowed.serialize(builder), builder.getDescriptorPools(), factory);
    assertThat(allowed2.getDisallowUnknownOptions()).isEqualTo(allowed.getDisallowUnknownOptions());
    assertThat(allowed2.getDisallowUnknownHintsWithQualifiers())
        .containsExactlyElementsIn(allowed.getDisallowUnknownHintsWithQualifiers());
    assertThat(allowed2.getHintList()).hasSize(allowed.getHintList().size());
    assertThat(allowed2.getHint("test_qual", "hint1").getType())
        .isEqualTo(allowed.getHint("test_qual", "hint1").getType());
    assertThat(allowed2.getHint("", "hint1").getType())
        .isEqualTo(allowed.getHint("", "hint1").getType());
    assertThat(allowed2.getOptionNameList()).containsExactlyElementsIn(allowed.getOptionNameList());

    String typeProtoPath = "zetasql/public/type.proto";
    AllowedHintsAndOptionsProto.Builder protoBuilder = AllowedHintsAndOptionsProto.newBuilder();
    TextFormat.merge(
        "disallow_unknown_options: true\n"
            + "disallow_unknown_hints_with_qualifier: \"qual\"\n"
            + "disallow_unknown_hints_with_qualifier: \"\"\n"
            + "hint {\n"
            + "  qualifier: \"test_qual\"\n"
            + "  name: \"hint1\"\n"
            + "  type {\n"
            + "    type_kind: TYPE_PROTO\n"
            + "    proto_type {\n"
            + "      proto_name: \"zetasql.TypeProto\"\n"
            + "      proto_file_name: \""
            + typeProtoPath
            + "\"\n"
            + "      file_descriptor_set_index: 0\n"
            + "    }\n"
            + "  }\n"
            + "  allow_unqualified: true\n"
            + "}\n"
            + "option {\n"
            + "  name: \"option1\"\n"
            + "}",
        protoBuilder);
    AllowedHintsAndOptionsProto proto =
        AllowedHintsAndOptions.deserialize(
                protoBuilder.build(), builder.getDescriptorPools(), factory)
            .serialize(builder);
    assertThat(proto.getDisallowUnknownHintsWithQualifierList()).hasSize(2);
    assertThat(proto.getHint(0)).isEqualTo(protoBuilder.build().getHint(0));
    assertThat(proto.getOption(0)).isEqualTo(protoBuilder.build().getOption(0));
    assertThat(proto.getDisallowUnknownOptions()).isTrue();
  }

  @Test
  public void testGets() {
    AllowedHintsAndOptions allowed = new AllowedHintsAndOptions("qual");
    allowed.addOption("option1", TypeFactory.createSimpleType(TypeKind.TYPE_BYTES));
    allowed.addOption("Option2", null);
    allowed.addHint("test_qual", "hint1", TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE), true);
    allowed.addHint("test_qual", "HINT2", null, false);
    allowed.setDisallowUnknownOptions(false);
    allowed.disallowUnknownHintsWithQualifier("qual2");

    assertThat(allowed.getDisallowUnknownOptions()).isFalse();

    assertThat(allowed.getDisallowUnknownHintsWithQualifiers()).hasSize(3);
    assertThat(allowed.getDisallowUnknownHintsWithQualifiers()).contains("qual");
    assertThat(allowed.getDisallowUnknownHintsWithQualifiers()).contains("qual2");
    assertThat(allowed.getDisallowUnknownHintsWithQualifiers()).contains("");

    assertThat(allowed.getHint("Test_Qual", "HINT1").getType().isDouble()).isTrue();
    assertThat(allowed.getHint("", "HINT1").getType().isDouble()).isTrue();
    assertThat(allowed.getHint("Test_Qual", "HINT2").getType()).isNull();
    assertThat(allowed.getHint("", "hint2")).isNull();
    assertThat(allowed.getHint("qual", "nohint")).isNull();
    assertThat(allowed.getHintList()).hasSize(3);

    assertThat(allowed.getOptionNameList()).hasSize(2);
    assertThat(allowed.getOptionType("OPTION1").isBytes()).isTrue();
    assertThat(allowed.getOptionType("Option2")).isNull();
    assertThat(allowed.getOptionType("noOption")).isNull();
  }

  @Test
  public void testClassAndProtoSize() {
    assertWithMessage(
            "The number of fields of AllowedHintsAndOptionsProto has changed, please also update "
                + "the serialization code accordingly.")
        .that(AllowedHintsAndOptionsProto.getDescriptor().getFields())
        .hasSize(4);
    assertWithMessage(
            "The number of fields in AllowedHintsAndOptions class has changed, please also update "
                + "the proto and serialization code accordingly.")
        .that(TestUtil.getNonStaticFieldCount(AllowedHintsAndOptions.class))
        .isEqualTo(4);
  }
}
