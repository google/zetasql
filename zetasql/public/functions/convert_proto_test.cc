//
// Copyright 2019 ZetaSQL Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "zetasql/public/functions/convert_proto.h"

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::HasSubstr;
using ::testing::_;
using ::zetasql_base::testing::StatusIs;

namespace zetasql {
namespace functions {
namespace {

TEST(ConvertProtoTest, BasicPrinting) {
  zetasql_test::KitchenSinkPB proto;
  zetasql_base::Status error;
  std::string out;

  EXPECT_TRUE(ProtoToString(&proto, &out, &error));
  EXPECT_EQ(out, "");
  ZETASQL_EXPECT_OK(error);

  proto.set_int64_val(1984);

  EXPECT_TRUE(ProtoToString(&proto, &out, &error));
  EXPECT_EQ(out, "int64_val: 1984");
  ZETASQL_EXPECT_OK(error);

  out.clear();
  proto.set_string_val("spam");

  EXPECT_TRUE(ProtoToString(&proto, &out, &error));
  EXPECT_EQ(out, "int64_val: 1984 string_val: \"spam\"");
  ZETASQL_EXPECT_OK(error);
}

TEST(ConvertProtoTest, BasicParsing) {
  zetasql_test::KitchenSinkPB proto;
  zetasql_base::Status error;

  EXPECT_TRUE(StringToProto("int64_key_1: 1 int64_key_2: 2", &proto, &error));
  EXPECT_EQ(proto.int64_key_1(), 1);
  EXPECT_EQ(proto.int64_key_2(), 2);
  ZETASQL_EXPECT_OK(error);

  EXPECT_TRUE(StringToProto(
      "int64_key_1: 1 int64_key_2: 2 string_val: \"spam\"", &proto, &error));
  EXPECT_EQ(proto.int64_key_1(), 1);
  EXPECT_EQ(proto.int64_key_2(), 2);
  EXPECT_EQ(proto.string_val(), "spam");
  ZETASQL_EXPECT_OK(error);
}

TEST(ConvertProtoTest, ParsingWithoutRequiredField) {
  zetasql_test::KitchenSinkPB proto;
  zetasql_base::Status error;

  EXPECT_FALSE(StringToProto("", &proto, &error));
  EXPECT_THAT(
      error,
      StatusIs(
          _, HasSubstr("Error parsing proto: Message missing required fields: "
                       "int64_key_1, int64_key_2 [0:1]")));
}

TEST(ConvertProtoTest, ParsingWithUnknownField) {
  zetasql_test::KitchenSinkPB proto;
  zetasql_base::Status error;

  EXPECT_FALSE(
      StringToProto("int64_key_1: 1 int64_key_2: 2 123: 4", &proto, &error));
  EXPECT_THAT(
      error,
      StatusIs(
          _, HasSubstr(
                 "Error parsing proto: Expected identifier, got: 123 [1:31]")));
}

TEST(ConvertProtoTest, ParsingWithExtensions) {
  zetasql_test::KitchenSinkPB proto;
  zetasql_base::Status error;
  std::string out;

  EXPECT_TRUE(
      StringToProto("int64_key_1: 1 int64_key_2: 2 "
                    "[zetasql_test.KitchenSinkExtension.int_extension]: 1234",
                    &proto, &error));
  EXPECT_EQ(proto.GetExtension(
    zetasql_test::KitchenSinkExtension::int_extension), 1234);
  ZETASQL_EXPECT_OK(error);
}

TEST(ConvertProtoTest, ParsingWithUnknownExtension) {
  zetasql_test::KitchenSinkPB proto;
  zetasql_base::Status error;
  std::string out;

  EXPECT_FALSE(
      StringToProto("int64_key_1: 1 int64_key_2: 2 "
                    "[zetasql_test.UnknownExtension.int_extension]: 1234",
                    &proto, &error));
  EXPECT_THAT(
      error,
      StatusIs(_, HasSubstr("Error parsing proto: Extension \"zetasql_test."
                            "UnknownExtension.int_extension\" is not defined or"
                            " is not an extension of \"zetasql_test."
                            "KitchenSinkPB\".")));
}

}  // namespace
}  // namespace functions
}  // namespace zetasql
