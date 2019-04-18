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

#include "zetasql/parser/parse_tree_errors.h"

#include "zetasql/common/status_payload_utils.h"
#include "zetasql/common/testing/proto_matchers.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status_builder.h"

using ::zetasql::testing::EqualsProto;
using testing::HasSubstr;
using testing::IsEmpty;
using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

namespace zetasql {

static zetasql_base::Status NoError() { return ::zetasql_base::OkStatus(); }

static zetasql_base::Status ErrorWithoutLocation() {
  return MakeSqlError() << "No location";
}

static zetasql_base::Status ErrorWithLocation() {
  return MakeSqlErrorAtPoint(ParseLocationPoint::FromByteOffset(10))
      << "With location";
}

static zetasql_base::Status ErrorNonSQL() {
  return ::zetasql_base::NotFoundErrorBuilder(ZETASQL_LOC) << "Non-SQL error";
}

static zetasql_base::Status ReturnIfTest1() {
  FakeASTNode ast_location;
  ast_location.InitFields();
  RETURN_SQL_ERROR_AT_IF_ERROR(&ast_location, NoError());
  RETURN_SQL_ERROR_AT_IF_ERROR(&ast_location, ErrorWithoutLocation());
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status ReturnIfTest2() {
  FakeASTNode ast_location;
  ast_location.InitFields();
  RETURN_SQL_ERROR_AT_IF_ERROR(&ast_location, NoError());
  RETURN_SQL_ERROR_AT_IF_ERROR(&ast_location, ErrorWithLocation());
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status ReturnIfTest3() {
  FakeASTNode ast_location;
  ast_location.InitFields();
  RETURN_SQL_ERROR_AT_IF_ERROR(&ast_location, NoError());
  RETURN_SQL_ERROR_AT_IF_ERROR(&ast_location, ErrorNonSQL());
  return ::zetasql_base::OkStatus();
}

TEST(GetErrorLocationPoint, Basic) {
  FakeASTNode ast_location;
  ast_location.InitFields();
  ParseLocationPoint expected =
      ParseLocationPoint::FromByteOffset("fake_filename", 7);

  EXPECT_THAT(
      GetErrorLocationPoint(&ast_location, false).ToInternalErrorLocation(),
      EqualsProto(expected.ToInternalErrorLocation()));
}

TEST(GetErrorLocationPoint, LeftMost_ButNoChildren) {
  FakeASTNode ast_location;
  ast_location.InitFields();
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

TEST(Errors, ReturnIf) {
  // Dummy query that can be used to resolve all lines and columns in
  // InternalErrorLocations in this test.
  const std::string query = "1\n2\n3\n42345\n";

  EXPECT_EQ("OK", FormatError(NoError()));
  EXPECT_EQ("No location", FormatError(ErrorWithoutLocation()));
  EXPECT_EQ("With location [at 4:5]",
            FormatError(ConvertInternalErrorLocationToExternal(
                ErrorWithLocation(), query)));
  EXPECT_EQ("generic::not_found: Non-SQL error", FormatError(ErrorNonSQL()));

  // Now call via ReturnIfTest and see how the errors get modified to
  // override the location.  The added location is 4,2 (the fake location of
  // FakeASTNode)
  EXPECT_EQ("No location [at fake_filename:4:2]",
            FormatError(ConvertInternalErrorLocationToExternal(ReturnIfTest1(),
                                                               query)));
  EXPECT_EQ("With location [at fake_filename:4:2]",
            FormatError(ConvertInternalErrorLocationToExternal(ReturnIfTest2(),
                                                               query)));
  // This also converts the error to a SQL error, dropping the old error code.
  EXPECT_EQ("Non-SQL error [at fake_filename:4:2]",
            FormatError(ConvertInternalErrorLocationToExternal(ReturnIfTest3(),
                                                               query)));
}

}  // namespace zetasql
