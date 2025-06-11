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

#include "zetasql/parser/parse_tree_errors.h"

#include <functional>
#include <memory>
#include <string>

#include "zetasql/common/status_payload_utils.h"
#include "zetasql/common/testing/proto_matchers.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"

using ::zetasql::testing::EqualsProto;
using testing::HasSubstr;
using zetasql_base::testing::StatusIs;

namespace zetasql {

static absl::Status NoError() { return absl::OkStatus(); }

static absl::Status ErrorWithoutLocation() {
  return MakeSqlError() << "No location";
}

static absl::Status ErrorWithLocation() {
  return MakeSqlErrorAtPoint(ParseLocationPoint::FromByteOffset(10))
      << "With location";
}

static absl::Status ErrorNonSQL() {
  return absl::NotFoundError("Non-SQL error");
}

static absl::Status RetCheckError() { ZETASQL_RET_CHECK_FAIL() << "ret_check_error"; }

TEST(GetErrorLocationPoint, Basic) {
  FakeASTNode ast_location;
  ZETASQL_ASSERT_OK(ast_location.InitFields());
  ParseLocationPoint expected =
      ParseLocationPoint::FromByteOffset("fake_filename", 7);

  EXPECT_THAT(
      GetErrorLocationPoint(&ast_location, false).ToInternalErrorLocation(),
      EqualsProto(expected.ToInternalErrorLocation()));
}

TEST(GetErrorLocationPoint, AtEnd) {
  FakeASTNode ast_location;
  ZETASQL_ASSERT_OK(ast_location.InitFields());
  ParseLocationPoint expected =
      ParseLocationPoint::FromByteOffset("fake_filename", 10);

  EXPECT_THAT(
      GetErrorLocationPoint(&ast_location, /*include_leftmost_child=*/false,
                            /*use_end_location=*/true)
          .ToInternalErrorLocation(),
      EqualsProto(expected.ToInternalErrorLocation()));
}

TEST(GetErrorLocationPoint, LeftMost_ButNoChildren) {
  FakeASTNode ast_location;
  ZETASQL_ASSERT_OK(ast_location.InitFields());
  ParseLocationPoint expected =
      ParseLocationPoint::FromByteOffset("fake_filename", 7);

  EXPECT_THAT(
      GetErrorLocationPoint(&ast_location, true).ToInternalErrorLocation(),
      EqualsProto(expected.ToInternalErrorLocation()));
}

TEST(GetErrorLocationPoint, LeftMost) {
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_EXPECT_OK(ParseExpression("( a, b)", ParserOptions(), &parser_output));
  const ASTExpression *expr = parser_output->expression();
  EXPECT_EQ(0, GetErrorLocationPoint(expr, true).GetByteOffset());
  // "a"
  EXPECT_EQ(2, GetErrorLocationPoint(expr->child(0), true).GetByteOffset());
  // "b"
  EXPECT_EQ(5, GetErrorLocationPoint(expr->child(1), true).GetByteOffset());
}

TEST(Errors, LocationOverride) {
  // Dummy query that can be used to resolve all lines and columns in
  // InternalErrorLocations in this test.
  const std::string query = "1\n2\n3\n42345\n";

  auto ReturnWithLocationOverride =
      [](std::function<absl::Status()> make_error) -> absl::Status {
    FakeASTNode ast_location;
    ZETASQL_RETURN_IF_ERROR(ast_location.InitFields());
    ZETASQL_RETURN_IF_ERROR(make_error()).With(LocationOverride(&ast_location));
    return absl::OkStatus();
  };

  absl::Status test_status = ConvertInternalErrorLocationToExternal(
      ReturnWithLocationOverride(RetCheckError), query);
  EXPECT_THAT(test_status, StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(
      FormatError(test_status),
      HasSubstr("ret_check_error [zetasql.ErrorLocation] { line: 4 "
                "column: 2 filename: \"fake_filename\" "
                "input_start_line_offset: 0 input_start_column_offset: 0 }"));

  ZETASQL_EXPECT_OK(ConvertInternalErrorLocationToExternal(
      ReturnWithLocationOverride(&NoError), query));

  test_status = ConvertInternalErrorLocationToExternal(
      ReturnWithLocationOverride(&ErrorWithoutLocation), query);
  EXPECT_THAT(test_status, StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(FormatError(test_status),
              HasSubstr("No location [at fake_filename:4:2]"));

  test_status = ConvertInternalErrorLocationToExternal(
      ReturnWithLocationOverride(&ErrorWithLocation), query);
  EXPECT_THAT(test_status, StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(FormatError(test_status),
              HasSubstr("With location [at fake_filename:4:2]"));

  // LocationOverride only overrides location, never error code. We took the
  // philosophy that any overriding of specific error cods should be explicit
  // at the place where they need to be overridden.
  test_status = ConvertInternalErrorLocationToExternal(
      ReturnWithLocationOverride(&ErrorNonSQL), query);
  EXPECT_THAT(test_status, StatusIs(absl::StatusCode::kNotFound));
  EXPECT_THAT(
      FormatError(test_status),
      HasSubstr("generic::not_found: Non-SQL error [zetasql.ErrorLocation] { "
                "line: 4 column: 2 filename: \"fake_filename\" "
                "input_start_line_offset: 0 input_start_column_offset: 0 }"));
}

static absl::Status TestOnNode(const ASTNode* node) {
  ZETASQL_RETURN_IF_ERROR(MakeSqlErrorIfPresent(node)) << "Node not supported here";
  return absl::OkStatus();
}

TEST(Errors, MakeSqlErrorIfPresent) {
  const ASTNode* node = nullptr;
  ZETASQL_EXPECT_OK(TestOnNode(node));

  FakeASTNode ast_location;
  node = &ast_location;
  EXPECT_THAT(TestOnNode(node), StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(TestOnNode(node).ToString(),
              HasSubstr("Node not supported here"));
}

}  // namespace zetasql
