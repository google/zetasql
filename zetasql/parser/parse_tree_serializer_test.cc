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

#include "zetasql/parser/parse_tree_serializer.h"

#include <iostream>
#include <string>

#include "zetasql/base/path.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/functional/bind_front.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "file_based_test_driver/file_based_test_driver.h"
#include "file_based_test_driver/run_test_case_result.h"
#include "file_based_test_driver/test_case_options.h"

ABSL_FLAG(std::string, test_file, "parse_tree_serializer.test",
          "name of test data file.");

namespace zetasql {
class ParseTreeSerializerTest : public ::testing::Test {
 public:
  ParseTreeSerializerTest() {}
  void RunTest(absl::string_view test_case_input,
               file_based_test_driver::RunTestCaseResult* test_result) {
    // Read an SQL statement from the .test file. Parse it and extract the
    // parsed ASTStatement. Serialize() the ASTStatement and compare the
    // proto to the expected value.
    std::string sql = std::string(test_case_input);
    auto language_options = absl::make_unique<LanguageOptions>();
    ParserOptions parser_options =
        ParserOptions(/*id_string_pool=*/nullptr,
                      /*arena=*/nullptr, language_options.get());
    std::unique_ptr<ParserOutput> parser_output;
    absl::Status status = ParseStatement(sql, parser_options, &parser_output);
    ASSERT_TRUE(status.ok());

    const zetasql::ASTStatement* statement =
        status.ok() ? parser_output->statement() : nullptr;

    zetasql::AnyASTStatementProto proto;
    status = ParseTreeSerializer::Serialize(statement, &proto);
    test_result->AddTestOutput(proto.DebugString());

    ParserOptions deserialize_parser_options =
        ParserOptions(/*id_string_pool=*/nullptr,
                      /*arena=*/nullptr, /* language_options=*/nullptr);

    absl::StatusOr<std::unique_ptr<ParserOutput>> deserialized_parser_output =
        ParseTreeSerializer::Deserialize(proto,
                                         deserialize_parser_options);
    ZETASQL_EXPECT_OK(deserialized_parser_output);
    const zetasql::ASTStatement* deserialized_statement =
        deserialized_parser_output.value()->statement();

    // This is only a partially reliable test for node equality
    // because not all attributes of a node are necessarily represented in
    // the DebugString. Still need to generate a deep equals() method for
    // better comparison.
    EXPECT_EQ(statement->DebugString(), deserialized_statement->DebugString());
    EXPECT_EQ(Unparse(statement), Unparse(deserialized_statement));
  }
};

TEST_F(ParseTreeSerializerTest, SerializeStatements) {
  EXPECT_TRUE(file_based_test_driver::RunTestCasesFromFiles(
      absl::GetFlag(FLAGS_test_file),
      absl::bind_front(&ParseTreeSerializerTest::RunTest, this)));
}

}  // namespace zetasql
