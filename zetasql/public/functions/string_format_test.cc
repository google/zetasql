//
// Copyright 2019 Google LLC
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

#include "zetasql/public/functions/string_format.h"

#include <limits>
#include <memory>

#include "zetasql/base/logging.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace functions {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

void TestBadPattern(absl::string_view pattern,
                    const std::vector<Value>& values) {
  std::string output;
  bool is_null;
  absl::Status status = StringFormatUtf8(
      pattern, values, ProductMode::PRODUCT_INTERNAL, &output, &is_null);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kOutOfRange,
                               HasSubstr("Format string")));
  std::vector<const Type*> types;
  for (const auto& value : values) {
    types.push_back(value.type());
  }
  status = CheckStringFormatUtf8ArgumentTypes(pattern, types,
                                              ProductMode::PRODUCT_INTERNAL);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kOutOfRange,
                               HasSubstr("Format string")));
  EXPECT_THAT(output, testing::IsEmpty());
}

void TestBadValue(absl::string_view pattern, Value value) {
  std::string output;
  bool is_null;
  const absl::Status status = StringFormatUtf8(
      pattern, {value}, ProductMode::PRODUCT_INTERNAL, &output, &is_null);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kOutOfRange,
                               HasSubstr("Invalid value")));
  EXPECT_THAT(output, testing::IsEmpty());
}

TEST(StringFormatTest, TestBadUtf8Patterns) {
  // Single invalid byte - this should be the first of a multi-byte sequence.
  TestBadPattern("\xc1", {});
  TestBadPattern("a\xc1", {});
  TestBadPattern("a\xc1z", {});
  TestBadPattern("%\xc1z", {});
  TestBadPattern("\xc1%", {});
  TestBadPattern("%\xc1%", {});
  TestBadPattern("%\xc1s", {values::String("")});
  TestBadPattern("%\xc1s", {values::String("")});
  TestBadPattern("%\xc1.s", {values::String("")});
  TestBadPattern("%.\xc1s", {values::String("")});

  // Disallowed byte. This has the correct number of bytes (4), but this
  // particular pattern is disallowed (as it would encode to 'zero' which should
  // simply be '\0'.
  TestBadPattern("\xf0\x80\x80\x80", {});
  TestBadPattern("a\xf0\x80\x80\x80", {});
  TestBadPattern("a\xf0\x80\x80\x80z", {});
  TestBadPattern("%\xf0\x80\x80\x80z", {});
  TestBadPattern("\xf0\x80\x80\x80%", {});
  TestBadPattern("%\xf0\x80\x80\x80%", {});
  TestBadPattern("%\xf0\x80\x80\x80s", {values::String("")});
  TestBadPattern("%\xf0\x80\x80\x80s", {values::String("")});
}

TEST(StringFormatTest, TestBadUtf8Values) {
  Value bad_value1 = Value::String("\xc1");
  Value long_bad_value1 = Value::String("1234566789\xc1xyz");
  Value bad_value2 = Value::String("\xf0\x80\x80\x80");

  TestBadValue("%s", bad_value1);
  TestBadValue("%.2s", bad_value1);
  TestBadValue("%t", bad_value1);
  TestBadValue("%T", bad_value1);

  TestBadValue("%s", bad_value2);
  TestBadValue("%.2s", bad_value2);
  TestBadValue("%t", bad_value2);
  TestBadValue("%T", bad_value2);

  TestBadValue("%s", long_bad_value1);
  // This particular test could be deleted in the future if we get a little
  // more clever about processing strings.  We don't actually need to examine
  // the end of this string to return a correct result.
  TestBadValue("%.2s", long_bad_value1);
  TestBadValue("%t", long_bad_value1);
  TestBadValue("%T", long_bad_value1);

  TypeFactory type_factory;

  const ArrayType* array_type = types::StringArrayType();
  const Value bad_array_value = values::StringArray({"\xc1"});
  TestBadValue("%t", bad_array_value);
  TestBadValue("%T", bad_array_value);

  const StructType* struct_type;
  ZETASQL_ASSERT_OK(
      type_factory.MakeStructType({{"f1", types::StringType()}}, &struct_type));
  const Value bad_struct_value = Value::Struct(struct_type, {bad_value1});
  TestBadValue("%t", bad_struct_value);
  TestBadValue("%T", bad_struct_value);

  const StructType* struct_of_array_type;
  ZETASQL_ASSERT_OK(
      type_factory.MakeStructType({{"a1", array_type}}, &struct_of_array_type));
  const Value bad_struct_of_array_value =
      Value::Struct(struct_of_array_type, {bad_array_value});
  TestBadValue("%t", bad_struct_of_array_value);
  TestBadValue("%T", bad_struct_of_array_value);

  const ArrayType* array_of_struct_type;
  ZETASQL_ASSERT_OK(type_factory.MakeArrayType(struct_type, &array_of_struct_type));
  const Value bad_array_of_struct_value =
      Value::Array(array_of_struct_type, {bad_struct_value});
  TestBadValue("%t", bad_array_of_struct_value);
  TestBadValue("%T", bad_array_of_struct_value);

  zetasql_test__::KitchenSinkPB proto;
  proto.set_string_val("abc\xc1xyz");

  const ProtoType* proto_type;
  ZETASQL_ASSERT_OK(type_factory.MakeProtoType(proto.GetDescriptor(), &proto_type));
  const Value bad_proto_value =
      Value::Proto(proto_type, SerializePartialToCord(proto));
  TestBadValue("%p", bad_proto_value);
  TestBadValue("%P", bad_proto_value);
  TestBadValue("%t", bad_proto_value);
  TestBadValue("%T", bad_proto_value);
}

TEST(StringFormatTest, TestBadJsonValue) {
  Value bad_json = Value::UnvalidatedJsonString(R"({"a": 12)");

  TestBadValue("%t", bad_json);
  TestBadValue("%T", bad_json);
  TestBadValue("%p", bad_json);
  TestBadValue("%P", bad_json);
}

}  // namespace functions
}  // namespace zetasql
