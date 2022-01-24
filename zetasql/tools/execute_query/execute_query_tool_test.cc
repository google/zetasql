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

#include "zetasql/tools/execute_query/execute_query_tool.h"

#include <string>

#include "zetasql/base/path.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/map_field.h"
#include "google/protobuf/text_format.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "zetasql/tools/execute_query/execute_query_proto_writer.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/flags/reflection.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/strip.h"

namespace zetasql {
namespace {

using zetasql_test__::EmptyMessage;
using zetasql_test__::KitchenSinkPB;
using testing::HasSubstr;
using testing::IsEmpty;
using testing::Not;
using testing::NotNull;
using zetasql_base::testing::StatusIs;

using ToolMode = ExecuteQueryConfig::ToolMode;
using SqlMode = ExecuteQueryConfig::SqlMode;

absl::Status ExecuteQuery(absl::string_view sql, ExecuteQueryConfig& config,
                          std::ostream& out_stream) {
  ExecuteQueryStreamWriter writer{out_stream};
  return ExecuteQuery(sql, config, writer);
}

TEST(SetToolModeFromFlags, ToolMode) {
  auto CheckFlag = [](absl::string_view name, ToolMode expected_mode) {
    absl::SetFlag(&FLAGS_mode, name);
    ExecuteQueryConfig config;
    ZETASQL_EXPECT_OK(SetToolModeFromFlags(config));
    EXPECT_EQ(config.tool_mode(), expected_mode);
  };
  CheckFlag("parse", ToolMode::kParse);
  CheckFlag("resolve", ToolMode::kResolve);
  CheckFlag("explain", ToolMode::kExplain);
  CheckFlag("execute", ToolMode::kExecute);
}

TEST(SetToolModeFromFlags, BadToolMode) {
  absl::SetFlag(&FLAGS_mode, "bad-mode");
  ExecuteQueryConfig config;
  EXPECT_THAT(SetToolModeFromFlags(config),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(SetSqlModeFromFlags, SqlMode) {
  auto CheckFlag = [](absl::string_view name, SqlMode expected_mode) {
    absl::SetFlag(&FLAGS_sql_mode, name);
    ExecuteQueryConfig config;
    ZETASQL_EXPECT_OK(SetSqlModeFromFlags(config));
    EXPECT_EQ(config.sql_mode(), expected_mode);
  };
  CheckFlag("query", SqlMode::kQuery);
  CheckFlag("expression", SqlMode::kExpression);
}

TEST(SetSqlModeFromFlags, BadSqlMode) {
  absl::SetFlag(&FLAGS_sql_mode, "bad-mode");
  ExecuteQueryConfig config;
  EXPECT_THAT(SetSqlModeFromFlags(config),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(MakeWriterFromFlagsTest, Empty) {
  absl::FlagSaver fs;

  absl::SetFlag(&FLAGS_output_mode, "");
  std::ostringstream output;
  EXPECT_THAT(MakeWriterFromFlags(ExecuteQueryConfig{}, output),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Must specify --output_mode"));
  EXPECT_THAT(output.str(), IsEmpty());
}

TEST(MakeWriterFromFlagsTest, Unknown) {
  absl::FlagSaver fs;

  absl::SetFlag(&FLAGS_output_mode, "foobar");
  std::ostringstream output;
  EXPECT_THAT(MakeWriterFromFlags(ExecuteQueryConfig{}, output),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Unknown output mode foobar"));
  EXPECT_THAT(output.str(), IsEmpty());
}

TEST(SetEvaluatorOptionsFromFlagsTest, Options) {
  absl::FlagSaver fs;
  EvaluatorOptions default_options;

  ExecuteQueryConfig config;
  ZETASQL_EXPECT_OK(SetEvaluatorOptionsFromFlags(config));
  EXPECT_EQ(config.evaluator_options().max_value_byte_size,
            default_options.max_value_byte_size);
  EXPECT_EQ(config.evaluator_options().max_intermediate_byte_size,
            default_options.max_intermediate_byte_size);

  absl::SetFlag(&FLAGS_evaluator_max_value_byte_size, 5);
  absl::SetFlag(&FLAGS_evaluator_max_intermediate_byte_size, 6);
  ZETASQL_EXPECT_OK(SetEvaluatorOptionsFromFlags(config));
  EXPECT_EQ(config.evaluator_options().max_value_byte_size, 5);
  EXPECT_EQ(config.evaluator_options().max_intermediate_byte_size, 6);
}

void RunWriter(const absl::string_view mode, std::ostream& output) {
  absl::FlagSaver fs;

  absl::SetFlag(&FLAGS_output_mode, mode);

  ExecuteQueryConfig config;
  config.set_tool_mode(ToolMode::kExecute);
  config.SetDescriptorPool(google::protobuf::DescriptorPool::generated_pool());

  ZETASQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<ExecuteQueryWriter> writer,
                       MakeWriterFromFlags(config, output));

  EXPECT_THAT(writer, NotNull());

  const std::vector<SimpleTable::NameAndType> columns{
      {"key", types::Int32Type()},
      {"name", types::StringType()},
      {"tag", types::StringArrayType()},
  };
  SimpleTable table{"DataTable", columns};
  table.SetContents({
      {values::Int32(0), values::String(""),
       values::EmptyArray(types::StringArrayType())},
      {values::Int32(100), values::String("foo"),
       values::StringArray({"aaa", "zzz"})},
      {values::Int32(200), values::String("bar"), values::StringArray({"bbb"})},
  });

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       table.CreateEvaluatorTableIterator({0, 1, 2}));

  ZETASQL_EXPECT_OK(writer->executed(*MakeResolvedLiteral(), std::move(iter)));
}

TEST(MakeWriterFromFlagsTest, Box) {
  std::ostringstream output;
  RunWriter("box", output);
  EXPECT_EQ(output.str(), R"(+-----+------+------------+
| key | name | tag        |
+-----+------+------------+
| 0   |      | []         |
| 100 | foo  | [aaa, zzz] |
| 200 | bar  | [bbb]      |
+-----+------+------------+

)");
}

TEST(MakeWriterFromFlagsTest, Json) {
  // ExecuteQueryWriteJson has better tests
  std::ostringstream output;
  RunWriter("json", output);
  ZETASQL_EXPECT_OK(JSONValue::ParseJSONString(output.str()));
}

TEST(MakeWriterFromFlagsTest, Textproto) {
  // ExecuteQueryWriteTextproto has better tests
  std::ostringstream output;
  RunWriter("textproto", output);
}

TEST(SetDescriptorPoolFromFlags, DescriptorPool) {
  absl::FlagSaver fs;

  absl::SetFlag(&FLAGS_descriptor_pool, "none");
  ExecuteQueryConfig config;
  ZETASQL_EXPECT_OK(SetDescriptorPoolFromFlags(config));

  const Type* type = nullptr;
  ZETASQL_EXPECT_OK(
      config.mutable_catalog().GetType("zetasql_test__.KitchenSinkPB", &type));
  EXPECT_EQ(type, nullptr);
}

static std::string TestDataDir() {
  return zetasql_base::JoinPath(
      getenv("TEST_SRCDIR"),
      "com_google_zetasql/zetasql/tools/execute_query/testdata");
}

static void VerifyDataMatches(
    const Table& table, const std::vector<std::vector<Value>>& expected_table) {
  std::vector<int> all_columns;
  for (int i = 0; i < table.NumColumns(); ++i) {
    all_columns.push_back(i);
  }

  absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>> iterator_or =
      table.CreateEvaluatorTableIterator(all_columns);
  ZETASQL_EXPECT_OK(iterator_or);
  EvaluatorTableIterator* iter = iterator_or->get();

  for (int row = 0; row < expected_table.size(); ++row) {
    const std::vector<Value>& expected_row = expected_table[row];
    EXPECT_TRUE(iter->NextRow());
    EXPECT_EQ(iter->NumColumns(), expected_row.size());
    for (int col = 0; col < expected_row.size(); ++col) {
      EXPECT_EQ(iter->GetValue(col), expected_row[col]);
    }
  }
  EXPECT_FALSE(iter->NextRow()) << "Unexpected Extra rows";
}

static std::string CsvFilePath() {
  return zetasql_base::JoinPath(TestDataDir(), "test.csv");
}

TEST(MakeTableFromCsvFile, NoFile) {
  const std::string missing_file_path =
      zetasql_base::JoinPath(TestDataDir(), "nothing_here.csv");
  EXPECT_THAT(MakeTableFromCsvFile("ignored", missing_file_path),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST(MakeTableFromCsvFile, Read) {
  absl::StatusOr<std::unique_ptr<const Table>> table_or =
      MakeTableFromCsvFile("great-table-name", CsvFilePath());
  ZETASQL_EXPECT_OK(table_or);
  const Table& table = **table_or;
  EXPECT_EQ(table.Name(), "great-table-name");
  EXPECT_EQ(table.NumColumns(), 3);
  const Column* col1 = table.GetColumn(0);
  const Column* col2 = table.GetColumn(1);
  const Column* col3 = table.GetColumn(2);
  EXPECT_EQ(col1->Name(), "col1");
  EXPECT_TRUE(col1->GetType()->IsString());

  EXPECT_EQ(col2->Name(), "col2");
  EXPECT_TRUE(col2->GetType()->IsString());

  EXPECT_EQ(col3->Name(), "col3");
  EXPECT_TRUE(col3->GetType()->IsString());

  VerifyDataMatches(table, {{Value::String("hello"), Value::String("45"),
                             Value::String("123.456")},
                            {Value::String("goodbye"), Value::String("90"),
                             Value::String("867.5309")}});
}

static std::string TextProtoFilePath() {
  return zetasql_base::JoinPath(TestDataDir(), "KitchenSinkPB.textproto");
}

TEST(AddTablesFromFlags, BadFlags) {
  auto ExpectTableSpecIsInvalid = [](absl::string_view table_spec) {
    ExecuteQueryConfig config;
    absl::SetFlag(&FLAGS_table_spec, table_spec);
    EXPECT_FALSE(AddTablesFromFlags(config).ok());
  };

  ExpectTableSpecIsInvalid("===");
  ExpectTableSpecIsInvalid("BadTable=bad_format:ff");
  ExpectTableSpecIsInvalid("BadTable=csv:");  // empty path
  ExpectTableSpecIsInvalid("BadTable=csv:too:many_args");

  // SSTable
  ExpectTableSpecIsInvalid("BadTable=sstable::");  // empty path
  ExpectTableSpecIsInvalid("BadTable=sstable:too:many:args");

  // BinProto
  ExpectTableSpecIsInvalid("BadTable=binproto:missing_proto");
  ExpectTableSpecIsInvalid("BadTable=binproto:::extra");

  // TextProto
  ExpectTableSpecIsInvalid("BadTable=textproto:missing_proto");
  ExpectTableSpecIsInvalid("BadTable=textproto:::extra");
}

TEST(AddTablesFromFlags, GoodFlags) {
  ExecuteQueryConfig config;
  config.mutable_catalog().SetDescriptorPool(
      google::protobuf::DescriptorPool::generated_pool());

  absl::SetFlag(&FLAGS_table_spec,
                absl::StrCat(
                    "TextProtoTable=textproto:zetasql_test__.KitchenSinkPB:",
                    TextProtoFilePath(),
                    ","
                    "CsvTable=csv:",
                    CsvFilePath()));
  ZETASQL_EXPECT_OK(AddTablesFromFlags(config));

  absl::flat_hash_set<const Table*> tables;
  ZETASQL_EXPECT_OK(config.catalog().GetTables(&tables));
  EXPECT_EQ(tables.size(),  //
            2);

  const Table* csv_table = nullptr;
  ZETASQL_EXPECT_OK(config.mutable_catalog().GetTable("CsvTable", &csv_table));
  EXPECT_NE(csv_table, nullptr);
  EXPECT_EQ(csv_table->NumColumns(), 3);

  const Table* textproto_table = nullptr;
  ZETASQL_EXPECT_OK(
      config.mutable_catalog().GetTable("TextProtoTable", &textproto_table));
  EXPECT_NE(textproto_table, nullptr);
  EXPECT_EQ(textproto_table->NumColumns(), 1);
}

TEST(ExecuteQuery, ReadCsvTableFileEndToEnd) {
  ExecuteQueryConfig config;
  config.mutable_catalog().SetDescriptorPool(
      google::protobuf::DescriptorPool::generated_pool());

  absl::SetFlag(&FLAGS_table_spec,
                absl::StrCat("CsvTable=csv:", CsvFilePath()));
  ZETASQL_EXPECT_OK(AddTablesFromFlags(config));
  std::ostringstream output;
  ZETASQL_EXPECT_OK(
      ExecuteQuery("SELECT col1 FROM CsvTable ORDER BY col1", config, output));
  EXPECT_EQ(output.str(), R"(+---------+
| col1    |
+---------+
| goodbye |
| hello   |
+---------+

)");
}

TEST(ExecuteQuery, ParseQuery) {
  ExecuteQueryConfig config;
  config.set_tool_mode(ToolMode::kParse);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("select 1", config, output));
  EXPECT_EQ(output.str(), R"(QueryStatement [0-8]
  Query [0-8]
    Select [0-8]
      SelectList [7-8]
        SelectColumn [7-8]
          IntLiteral(1) [7-8]

)");
}

TEST(ExecuteQuery, ResolveQuery) {
  ExecuteQueryConfig config;
  config.set_tool_mode(ToolMode::kResolve);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("select 1", config, output));
  EXPECT_EQ(output.str(), R"(QueryStmt
+-output_column_list=
| +-$query.$col1#1 AS `$col1` [INT64]
+-query=
  +-ProjectScan
    +-column_list=[$query.$col1#1]
    +-expr_list=
    | +-$col1#1 := Literal(type=INT64, value=1)
    +-input_scan=
      +-SingleRowScan

)");
}

TEST(ExecuteQuery, ExplainQuery) {
  ExecuteQueryConfig config;
  config.set_tool_mode(ToolMode::kExplain);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("select 1", config, output));
  EXPECT_EQ(output.str(), R"(RootOp(
+-input: ComputeOp(
  +-map: {
  | +-$col1 := ConstExpr(1)},
  +-input: EnumerateOp(ConstExpr(1))))
)");
}

TEST(ExecuteQuery, ExecuteQuery) {
  ExecuteQueryConfig config;
  config.set_tool_mode(ToolMode::kExecute);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("select 1", config, output));
  EXPECT_EQ(output.str(), R"(+---+
|   |
+---+
| 1 |
+---+

)");
}

TEST(ExecuteQuery, ParseExpression) {
  ExecuteQueryConfig config;
  config.set_tool_mode(ToolMode::kParse);
  config.set_sql_mode(SqlMode::kExpression);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("1", config, output));
  EXPECT_EQ(output.str(), R"(IntLiteral(1) [0-1]

)");
}

TEST(ExecuteQuery, ResolveExpression) {
  ExecuteQueryConfig config;
  config.set_tool_mode(ToolMode::kResolve);
  config.set_sql_mode(SqlMode::kExpression);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("1", config, output));
  EXPECT_EQ(output.str(), R"(Literal(type=INT64, value=1)

)");
}

TEST(ExecuteQuery, ExplainExpression) {
  ExecuteQueryConfig config;
  config.set_tool_mode(ToolMode::kExplain);
  config.set_sql_mode(SqlMode::kExpression);

  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("1", config, output));
  EXPECT_EQ(output.str(), R"(RootExpr(ConstExpr(1))
)");
}

TEST(ExecuteQuery, ExecuteExpression) {
  ExecuteQueryConfig config;
  config.set_tool_mode(ToolMode::kExecute);
  config.set_sql_mode(SqlMode::kExpression);

  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("1", config, output));
  EXPECT_EQ(output.str(), "1");
}

TEST(ExecuteQuery, ExecuteError) {
  ExecuteQueryConfig config;
  config.set_tool_mode(ToolMode::kExecute);
  std::ostringstream output;
  EXPECT_THAT(ExecuteQuery("select a", config, output),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ExecuteQuery, RespectEvaluatorOptions) {
  ExecuteQueryConfig config;
  config.set_tool_mode(ToolMode::kExecute);
  config.mutable_catalog().AddZetaSQLFunctions(
      config.analyzer_options().language());

  {
    std::ostringstream output;
    ZETASQL_EXPECT_OK(ExecuteQuery("select CURRENT_DATE()", config, output));
    EXPECT_THAT(output.str(), Not(HasSubstr("1066")));
  }
  {
    absl::Time fake_time = absl::FromCivil(
        absl::CivilSecond(1066, 10, 14, 1, 1, 1), absl::UTCTimeZone());
    std::ostringstream output;

    zetasql_base::SimulatedClock fake_clock(fake_time);
    config.mutable_evaluator_options().clock = &fake_clock;
    ZETASQL_EXPECT_OK(ExecuteQuery("select CURRENT_DATE()", config, output));
    EXPECT_THAT(output.str(), HasSubstr("1066"));
  }
}

TEST(ExecuteQuery, RespectEvaluatorOptionsExpressions) {
  ExecuteQueryConfig config;
  config.set_tool_mode(ToolMode::kExecute);
  config.set_sql_mode(SqlMode::kExpression);
  config.mutable_catalog().AddZetaSQLFunctions(
      config.analyzer_options().language());

  {
    std::ostringstream output;
    ZETASQL_EXPECT_OK(ExecuteQuery("CURRENT_DATE()", config, output));
    EXPECT_THAT(output.str(), Not(HasSubstr("1066")));
  }
  {
    absl::Time fake_time = absl::FromCivil(
        absl::CivilSecond(1066, 10, 14, 1, 1, 1), absl::UTCTimeZone());
    std::ostringstream output;

    zetasql_base::SimulatedClock fake_clock(fake_time);
    config.mutable_evaluator_options().clock = &fake_clock;
    ZETASQL_EXPECT_OK(ExecuteQuery("CURRENT_DATE()", config, output));
    EXPECT_THAT(output.str(), HasSubstr("1066"));
  }
}

TEST(ExecuteQuery, ExamineResolvedASTCallback) {
  ExecuteQueryConfig config;
  config.set_tool_mode(ToolMode::kExecute);
  config.set_examine_resolved_ast_callback(
      [](const ResolvedNode*) -> absl::Status {
        return absl::Status(absl::StatusCode::kFailedPrecondition, "");
      });

  std::ostringstream output;
  EXPECT_THAT(ExecuteQuery("select 1", config, output),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  EXPECT_THAT(output.str(), IsEmpty());
}

}  // namespace
}  // namespace zetasql
