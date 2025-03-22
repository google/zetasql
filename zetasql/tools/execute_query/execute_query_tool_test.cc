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

#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include "zetasql/base/path.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/map_field.h"
#include "google/protobuf/text_format.h"
#include "zetasql/common/options_utils.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "zetasql/tools/execute_query/execute_query_proto_writer.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/commandlineflag.h"
#include "absl/flags/flag.h"
#include "absl/flags/reflection.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/types/span.h"
#include "file_based_test_driver/file_based_test_driver.h"
#include "file_based_test_driver/test_case_options.h"

namespace zetasql {
namespace {

using zetasql_test__::EmptyMessage;
using zetasql_test__::KitchenSinkPB;
using testing::HasSubstr;
using testing::IsEmpty;
using testing::MatchesRegex;
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

TEST(ExecuteQueryDefaults, AllNonDevRewritesEnabledByDefault) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  ZETASQL_ASSERT_OK(SetAnalyzerOptionsFromFlags(config));
  EXPECT_EQ(config.analyzer_options().enabled_rewrites(),
            internal::GetRewrites(
                /*include_in_development=*/false,
                /*include_default_disabled=*/true));
}

TEST(ExecuteQueryDefaults, FoldLiteralCastEnabledByDefault) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  ZETASQL_ASSERT_OK(SetAnalyzerOptionsFromFlags(config));
  EXPECT_TRUE(config.analyzer_options().fold_literal_cast());
}

TEST(SetToolModeFromFlags, SingleToolMode) {
  absl::FlagSaver fs;
  auto CheckFlag = [](std::string name, ToolMode expected_mode) {
    absl::SetFlag(&FLAGS_mode, std::vector<std::string>{name});
    ExecuteQueryConfig config;
    ZETASQL_EXPECT_OK(SetToolModeFromFlags(config));
    EXPECT_TRUE(config.has_tool_mode(expected_mode));
  };
  CheckFlag("parse", ToolMode::kParse);
  CheckFlag("unparse", ToolMode::kUnparse);
  CheckFlag("resolve", ToolMode::kResolve);
  CheckFlag("unanalyze", ToolMode::kUnAnalyze);
  CheckFlag("explain", ToolMode::kExplain);
  CheckFlag("execute", ToolMode::kExecute);
}

TEST(SetToolModeFromFlags, MultipleToolModes) {
  absl::FlagSaver fs;
  auto CheckFlag = [](const std::vector<std::string>& names,
                      const absl::flat_hash_set<ToolMode>& expected_modes,
                      const absl::flat_hash_set<ToolMode>& unexpected_modes) {
    absl::SetFlag(&FLAGS_mode, names);
    ExecuteQueryConfig config;
    ZETASQL_EXPECT_OK(SetToolModeFromFlags(config));
    for (const ToolMode mode : expected_modes) {
      EXPECT_TRUE(config.has_tool_mode(mode));
    }
    for (const ToolMode mode : unexpected_modes) {
      EXPECT_FALSE(config.has_tool_mode(mode));
    }
  };
  CheckFlag({"parse", "unparse"}, {ToolMode::kParse, ToolMode::kUnparse},
            {ToolMode::kResolve, ToolMode::kUnAnalyze, ToolMode::kExplain,
             ToolMode::kExecute});
  CheckFlag({"unparse", "resolve"}, {ToolMode::kUnparse, ToolMode::kResolve},
            {ToolMode::kParse, ToolMode::kUnAnalyze, ToolMode::kExplain,
             ToolMode::kExecute});
  CheckFlag({"resolve", "unanalyze"},
            {ToolMode::kResolve, ToolMode::kUnAnalyze},
            {ToolMode::kParse, ToolMode::kUnparse, ToolMode::kExplain,
             ToolMode::kExecute});
  CheckFlag({"unanalyze", "explain"},
            {ToolMode::kUnAnalyze, ToolMode::kExplain},
            {ToolMode::kParse, ToolMode::kUnparse, ToolMode::kResolve,
             ToolMode::kExecute});
  CheckFlag({"explain", "execute"}, {ToolMode::kExplain, ToolMode::kExecute},
            {ToolMode::kParse, ToolMode::kUnparse, ToolMode::kResolve,
             ToolMode::kUnAnalyze});
  CheckFlag({"execute", "parse"}, {ToolMode::kExecute, ToolMode::kParse},
            {ToolMode::kUnparse, ToolMode::kResolve, ToolMode::kUnAnalyze,
             ToolMode::kExplain});
}

TEST(SetToolModeFromFlags, BadToolMode) {
  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_mode, {"bad-mode"});
  ExecuteQueryConfig config;
  EXPECT_THAT(SetToolModeFromFlags(config),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(SetSqlModeFromFlags, SqlMode) {
  absl::FlagSaver fs;
  auto CheckFlag = [](absl::string_view name, SqlMode expected_mode) {
    absl::SetFlag(&FLAGS_sql_mode, name);
    ExecuteQueryConfig config;
    ZETASQL_EXPECT_OK(SetSqlModeFromFlags(config));
    EXPECT_EQ(config.sql_mode(), expected_mode);
  };
  CheckFlag("query", SqlMode::kQuery);
  CheckFlag("expression", SqlMode::kExpression);
  CheckFlag("script", SqlMode::kScript);
}

TEST(SetSqlModeFromFlags, BadSqlMode) {
  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_sql_mode, "bad-mode");
  ExecuteQueryConfig config;
  EXPECT_THAT(SetSqlModeFromFlags(config),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ExecuteQueryConfigTest, ParseSqlMode) {
  ExecuteQueryConfig config;
  EXPECT_EQ(config.parse_sql_mode("query"), SqlMode::kQuery);
  EXPECT_EQ(config.parse_sql_mode("expression"), SqlMode::kExpression);
  EXPECT_EQ(config.parse_sql_mode("script"), SqlMode::kScript);
  // Invalid mode received
  EXPECT_EQ(config.parse_sql_mode("some_invalid_mode"), std::nullopt);
}

TEST(ExecuteQueryConfigTest, SqlModeName) {
  ExecuteQueryConfig config;
  EXPECT_EQ(config.sql_mode_name(SqlMode::kQuery), "query");
  EXPECT_EQ(config.sql_mode_name(SqlMode::kExpression), "expression");
  EXPECT_EQ(config.sql_mode_name(SqlMode::kScript), "script");
}

TEST(SetLanguageOptionsFromFlags, BadProductMode) {
  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_product_mode, "bad-mode");
  ExecuteQueryConfig config;
  EXPECT_THAT(SetLanguageOptionsFromFlags(config),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(SetLanguageOptionsFromFlags, ProductMode) {
  absl::FlagSaver fs;
  auto CheckFlag = [](absl::string_view name, ProductMode expected_mode) {
    absl::SetFlag(&FLAGS_product_mode, name);
    ExecuteQueryConfig config;
    ZETASQL_EXPECT_OK(SetLanguageOptionsFromFlags(config));
    EXPECT_EQ(config.analyzer_options().language().product_mode(),
              expected_mode);
  };
  CheckFlag("internal", PRODUCT_INTERNAL);
  CheckFlag("external", PRODUCT_EXTERNAL);
}

TEST(SetLanguageOptionsFromFlags, NameResolutionMode) {
  absl::FlagSaver fs;
  auto CheckFlag = [](bool flag_value, NameResolutionMode expected_mode) {
    absl::SetFlag(&FLAGS_strict_name_resolution_mode, flag_value);
    ExecuteQueryConfig config;
    ZETASQL_EXPECT_OK(SetLanguageOptionsFromFlags(config));
    EXPECT_EQ(config.analyzer_options().language().name_resolution_mode(),
              expected_mode);
  };
  CheckFlag(false, NAME_RESOLUTION_DEFAULT);
  CheckFlag(true, NAME_RESOLUTION_STRICT);
}

TEST(SetAnalyzerOptionsFromFlags, EnabledAstRewrites) {
  absl::FlagSaver fs;
  auto CheckFlag = [](absl::string_view str,
                      absl::Span<const ResolvedASTRewrite> expected_enabled,
                      absl::Span<const ResolvedASTRewrite> expected_disabled) {
    absl::CommandLineFlag* flag =
        absl::FindCommandLineFlag("enabled_ast_rewrites");
    std::string error;
    EXPECT_TRUE(flag->ParseFrom(str, &error));
    ExecuteQueryConfig config;
    ZETASQL_EXPECT_OK(SetAnalyzerOptionsFromFlags(config));
    for (const ResolvedASTRewrite rewrite : expected_enabled) {
      EXPECT_TRUE(config.analyzer_options().rewrite_enabled(rewrite))
          << ResolvedASTRewrite_Name(rewrite)
          << " should be enabled when flag is '" << str << "'";
    }
    for (const ResolvedASTRewrite rewrite : expected_disabled) {
      EXPECT_FALSE(config.analyzer_options().rewrite_enabled(rewrite))
          << ResolvedASTRewrite_Name(rewrite)
          << " should be disabled when flag is '" << str << "'";
    }
  };
  // We don't need to exhaustively test this, just ensure we are invoking the
  // the 'parser' for this format.
  absl::btree_set<ResolvedASTRewrite> default_rewrites =
      AnalyzerOptions::DefaultRewrites();

  CheckFlag("DEFAULTS",
            std::vector<ResolvedASTRewrite>{default_rewrites.begin(),
                                            default_rewrites.end()},
            {REWRITE_ANONYMIZATION});
  CheckFlag("DEFAULTS,-PIVOT", {REWRITE_FLATTEN},
            {REWRITE_PIVOT, REWRITE_ANONYMIZATION});
  CheckFlag("DEFAULTS,+ANONYMIZATION",
            {REWRITE_FLATTEN, REWRITE_PIVOT, REWRITE_ANONYMIZATION},
            {REWRITE_INVALID_DO_NOT_USE});
}

TEST(SetFoldLiteralCastFromFlags, FoldLiteralCast) {
  absl::FlagSaver fs;
  auto CheckFlag = [](bool flag_value, bool expected_value) {
    absl::SetFlag(&FLAGS_fold_literal_cast, flag_value);
    ExecuteQueryConfig config;
    ZETASQL_EXPECT_OK(SetAnalyzerOptionsFromFlags(config));
    EXPECT_EQ(config.analyzer_options().fold_literal_cast(), expected_value);
  };
  CheckFlag(false, false);
  CheckFlag(true, true);
}

TEST(SetLanguageOptionsFromFlags, EnabledLanguageFeatures) {
  absl::FlagSaver fs;
  auto CheckFlag = [](absl::string_view str,
                      absl::Span<const LanguageFeature> expected_enabled,
                      absl::Span<const LanguageFeature> expected_disabled) {
    absl::CommandLineFlag* flag =
        absl::FindCommandLineFlag("enabled_language_features");
    std::string error;
    EXPECT_TRUE(flag->ParseFrom(str, &error));
    ExecuteQueryConfig config;
    ZETASQL_EXPECT_OK(SetLanguageOptionsFromFlags(config));
    for (const LanguageFeature value : expected_enabled) {
      EXPECT_TRUE(
          config.analyzer_options().language().LanguageFeatureEnabled(value))
          << LanguageFeature_Name(value) << " should be enabled when flag is '"
          << str << "'";
    }
    for (const LanguageFeature value : expected_disabled) {
      EXPECT_FALSE(
          config.analyzer_options().language().LanguageFeatureEnabled(value))
          << LanguageFeature_Name(value) << " should be disabled when flag is '"
          << str << "'";
    }
  };
  // We don't need to exhaustively test this, just ensure we are invoking the
  // the 'parser' for this format.
  CheckFlag("DEV", {FEATURE_TEST_IDEALLY_ENABLED_BUT_IN_DEVELOPMENT},
            {FEATURE_TEST_IDEALLY_DISABLED});
  CheckFlag("DEV,-TEST_IDEALLY_ENABLED_BUT_IN_DEVELOPMENT", {},
            {FEATURE_TEST_IDEALLY_ENABLED_BUT_IN_DEVELOPMENT,
             FEATURE_TEST_IDEALLY_DISABLED});
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

TEST(SetQueryParameterValuesFromFlagsTest, NoOp) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  ZETASQL_EXPECT_OK(SetQueryParametersFromFlags(config));
  EXPECT_THAT(config.analyzer_options().query_parameters(), IsEmpty());
  EXPECT_THAT(config.query_parameter_values(), IsEmpty());
}

TEST(SetQueryParameterValuesFromFlagsTest, BadFlag) {
  absl::FlagSaver fs;

  absl::SetFlag(&FLAGS_parameters, "p1=1;p2=pizza");

  ExecuteQueryConfig config;
  EXPECT_THAT(SetQueryParametersFromFlags(config),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Unrecognized name: pizza [at 1:1]"));
}

TEST(SetQueryParameterValuesFromFlagsTest, Options) {
  absl::FlagSaver fs;

  absl::SetFlag(&FLAGS_parameters, "p1=1;p2=(select x from (select 'a' as x))");

  ExecuteQueryConfig config;
  ZETASQL_EXPECT_OK(SetQueryParametersFromFlags(config));
  EXPECT_THAT(config.analyzer_options().query_parameters(),
              testing::Contains(std::make_pair("p1", types::Int64Type())));
  EXPECT_THAT(config.analyzer_options().query_parameters(),
              testing::Contains(std::make_pair("p2", types::StringType())));

  EXPECT_THAT(config.query_parameter_values(),
              testing::Contains(std::make_pair("p1", values::Int64(1))));
  EXPECT_THAT(config.query_parameter_values(),
              testing::Contains(std::make_pair("p2", values::String("a"))));
}

void RunWriter(const absl::string_view mode, std::ostream& output) {
  absl::FlagSaver fs;

  absl::SetFlag(&FLAGS_output_mode, mode);

  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kExecute);
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
  google::protobuf::TextFormat::Parser parser;
  parser.AllowUnknownField(true);
  EmptyMessage msg;
  EXPECT_TRUE(parser.ParseFromString(output.str(), &msg));
}

TEST(SetDescriptorPoolFromFlags, DescriptorPool) {
  absl::FlagSaver fs;

  absl::SetFlag(&FLAGS_descriptor_pool, "none");
  ExecuteQueryConfig config;
  ZETASQL_EXPECT_OK(SetDescriptorPoolFromFlags(config));

  const Type* type = nullptr;
  EXPECT_THAT(
      config.catalog()->FindType({"zetasql_test__.KitchenSinkPB"}, &type),
      StatusIs(absl::StatusCode::kNotFound));
  EXPECT_EQ(type, nullptr);
}

static std::string TestDataDir() {
  return zetasql_base::JoinPath(
      absl::NullSafeStringView(getenv("TEST_SRCDIR")),
      "com_google_zetasql/zetasql/tools/execute_query/testdata");
}

static void VerifyDataMatches(
    const Table& table, absl::Span<const std::vector<Value>> expected_table) {
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
  absl::FlagSaver fs;
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
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.SetDescriptorPool(google::protobuf::DescriptorPool::generated_pool());

  absl::SetFlag(&FLAGS_table_spec,
                absl::StrCat(
                    "TextProtoTable=textproto:zetasql_test__.KitchenSinkPB:",
                    TextProtoFilePath(),
                    ","
                    "CsvTable=csv:",
                    CsvFilePath()));
  ZETASQL_EXPECT_OK(AddTablesFromFlags(config));

  absl::flat_hash_set<const Table*> tables;
  ZETASQL_EXPECT_OK(config.wrapper_catalog()->GetTables(&tables));
  EXPECT_EQ(tables.size(),  //
            2);

  const Table* csv_table = nullptr;
  ZETASQL_EXPECT_OK(config.wrapper_catalog()->GetTable("CsvTable", &csv_table));
  EXPECT_NE(csv_table, nullptr);
  EXPECT_EQ(csv_table->NumColumns(), 3);

  const Table* textproto_table = nullptr;
  ZETASQL_EXPECT_OK(
      config.wrapper_catalog()->GetTable("TextProtoTable", &textproto_table));
  EXPECT_NE(textproto_table, nullptr);
  EXPECT_EQ(textproto_table->NumColumns(), 1);
}

TEST(ExecuteQuery, ReadCsvTableFileEndToEnd) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.SetDescriptorPool(google::protobuf::DescriptorPool::generated_pool());

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
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kParse);
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

TEST(ExecuteQuery, UnparseQuery) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kUnparse);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("select 1", config, output));
  EXPECT_EQ(output.str(), R"(SELECT
  1

)");
}

TEST(ExecuteQuery, ResolveQuery) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kResolve);
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

TEST(ExecuteQuery, UnAnalyzeQuery) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kUnAnalyze);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("select 1", config, output));
  EXPECT_EQ(output.str(), R"(SELECT
  1 AS a_1;

)");
}

TEST(ExecuteQuery, ExplainQuery) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kExplain);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("select 1", config, output));
  EXPECT_EQ(output.str(), R"(ComputeOp(
+-map: {
| +-$col1 := ConstExpr(1)},
+-input: EnumerateOp(ConstExpr(1)))

)");
}

TEST(ExecuteQuery, ExecuteQuery) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kExecute);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("select 1", config, output));
  EXPECT_EQ(output.str(), R"(+---+
|   |
+---+
| 1 |
+---+

)");
}

// This tests invoking ExecuteQuery multiple times with the same Config,
// carrying state (DDL definitions) from previous calls to the next.
// This simulates how --interactive mode calls ExecuteQuery.
TEST(ExecuteQuery, ExecuteQueryStateful) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  ZETASQL_ASSERT_OK(InitializeExecuteQueryConfig(config));
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kExecute);
  config.mutable_analyzer_options().set_error_message_mode(
      ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET);

  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("create function f() AS (10*2)", config, output));
  EXPECT_EQ(output.str(), "Function registered.\n");

  output.str("");
  ZETASQL_EXPECT_OK(ExecuteQuery("create function g() AS (f()*3)", config, output));
  EXPECT_EQ(output.str(), "Function registered.\n");

  output.str("");
  ZETASQL_EXPECT_OK(ExecuteQuery("select f() f, g() g", config, output));
  EXPECT_EQ(output.str(),
            R"(+----+----+
| f  | g  |
+----+----+
| 20 | 60 |
+----+----+

)");
}

TEST(ExecuteQuery, ParseExpression) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kParse);
  config.set_sql_mode(SqlMode::kExpression);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("1", config, output));
  EXPECT_EQ(output.str(), R"(IntLiteral(1) [0-1]

)");
}

TEST(ExecuteQuery, ResolveExpression) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kResolve);
  config.set_sql_mode(SqlMode::kExpression);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("1", config, output));
  EXPECT_EQ(output.str(), R"(Literal(type=INT64, value=1)

)");
}

TEST(ExecuteQuery, ExplainExpression) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kExplain);
  config.set_sql_mode(SqlMode::kExpression);

  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("1", config, output));
  EXPECT_EQ(output.str(), R"(ConstExpr(1)
)");
}

TEST(ExecuteQuery, ExecuteExpression) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kExecute);
  config.set_sql_mode(SqlMode::kExpression);

  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("1", config, output));
  EXPECT_EQ(output.str(), "1");
}

TEST(ExecuteQuery, ExecuteError) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kExecute);
  std::ostringstream output;
  EXPECT_THAT(ExecuteQuery("select a", config, output),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ExecuteQuery, RespectEvaluatorOptions) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kExecute);
  config.builtins_catalog()->AddBuiltinFunctions(
      BuiltinFunctionOptions(config.analyzer_options().language()));

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
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kExecute);
  config.set_sql_mode(SqlMode::kExpression);
  config.builtins_catalog()->AddBuiltinFunctions(
      BuiltinFunctionOptions(config.analyzer_options().language()));

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

TEST(ExecuteQuery, RespectQueryParameters) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kExecute);
  config.builtins_catalog()->AddBuiltinFunctions(
      BuiltinFunctionOptions(config.analyzer_options().language()));
  ZETASQL_ASSERT_OK(config.mutable_analyzer_options().AddQueryParameter(
      "p1", types::Int64Type()));
  config.mutable_query_parameter_values()["p1"] = values::Int64(5);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("select @p1", config, output));
  EXPECT_EQ(output.str(), R"(+---+
|   |
+---+
| 5 |
+---+

)");
}

TEST(ExecuteQuery, RespectQueryParametersExpression) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kExecute);
  config.set_sql_mode(SqlMode::kExpression);
  config.builtins_catalog()->AddBuiltinFunctions(
      BuiltinFunctionOptions(config.analyzer_options().language()));
  ZETASQL_ASSERT_OK(config.mutable_analyzer_options().AddQueryParameter(
      "p1", types::Int64Type()));
  config.mutable_query_parameter_values()["p1"] = values::Int64(5);
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("@p1", config, output));
  EXPECT_EQ(output.str(), "5");
}

TEST(ExecuteQuery, ExamineResolvedASTCallback) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kExecute);
  config.set_examine_resolved_ast_callback(
      [](const ResolvedNode*) -> absl::Status {
        return absl::Status(absl::StatusCode::kFailedPrecondition, "");
      });

  std::ostringstream output;
  EXPECT_THAT(ExecuteQuery("select 1", config, output),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  EXPECT_THAT(output.str(), IsEmpty());
}

TEST(ExecuteQuery, InitializeConfigReservesAllReservablesKeywords) {
  ExecuteQueryConfig config;
  ZETASQL_ASSERT_OK(InitializeExecuteQueryConfig(config));
  EXPECT_TRUE(config.analyzer_options().language().IsReservedKeyword(
      "MATCH_RECOGNIZE"));
  EXPECT_TRUE(
      config.analyzer_options().language().IsReservedKeyword("QUALIFY"));
}

TEST(SetLanguageOptionsFromFlags, SelectedCatalog_None) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  ZETASQL_ASSERT_OK(InitializeExecuteQueryConfig(config));
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kResolve);

  // Referencing built-in functions works.
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("select sqrt(1)", config, output));

  // There are no tables to reference.
  EXPECT_THAT(ExecuteQuery("select sqrt(1) from TestTable", config, output),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       MatchesRegex(".*Table not found.*")));
}

TEST(SetLanguageOptionsFromFlags, SelectedCatalog_Sample) {
  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_catalog, "sample");

  ExecuteQueryConfig config;
  ZETASQL_ASSERT_OK(InitializeExecuteQueryConfig(config));
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kResolve);

  // Built-ins work, and tables from SampleCatalog work.
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQuery("select sqrt(1) from TestTable", config, output));
}

// Regression test for a case where the query defines a function that refers to
// a builtin function in a different catalog. ExecuteQueryConfig has different
// catalogs for builtins and newly defined objects. This case was triggering a
// ZETASQL_RET_CHECK previously.
TEST(ExecuteQuery, ResolveFunction_DifferentCatalogs) {
  absl::FlagSaver fs;
  ExecuteQueryConfig config;
  config.clear_tool_modes();
  config.add_tool_mode(ToolMode::kResolve);
  config.mutable_analyzer_options()
      .mutable_language()
      ->SetSupportedStatementKinds({zetasql::RESOLVED_CREATE_FUNCTION_STMT});
  ZETASQL_ASSERT_OK(config.builtins_catalog()->AddBuiltinFunctionsAndTypes(
      BuiltinFunctionOptions(config.analyzer_options().language())));
  std::ostringstream output;

  // Here, the IF() function is provided by the builtins catalog.
  const auto& query = R"(
  CREATE TEMP FUNCTION NewFunction(id STRING) RETURNS INT64
  AS (
    IF(LENGTH(id) > 0, 1, 0)
  );)";
  ZETASQL_EXPECT_OK(ExecuteQuery(query, config, output));
}

static absl::Status RunFileBasedTestImpl(
    absl::string_view test_case_input,
    file_based_test_driver::RunTestCaseResult* test_result,
    std::ostringstream* output) {
  file_based_test_driver::TestCaseOptions test_case_options;
  // `mode` and `catalog` options correspond to flags of the same name.
  test_case_options.RegisterString("mode", "execute");
  test_case_options.RegisterString("sql_mode", "query");
  test_case_options.RegisterString("catalog", "sample");
  test_case_options.RegisterString("enabled_language_features", "");
  test_case_options.RegisterInt64("max_statements_to_execute", -1);

  std::string test_case = std::string(test_case_input);
  ZETASQL_RETURN_IF_ERROR(test_case_options.ParseTestCaseOptions(&test_case));

  absl::SetFlag(&FLAGS_mode,
                absl::StrSplit(test_case_options.GetString("mode"), ','));
  absl::SetFlag(&FLAGS_sql_mode, test_case_options.GetString("sql_mode"));
  absl::SetFlag(&FLAGS_catalog, test_case_options.GetString("catalog"));
  if (test_case_options.GetInt64("max_statements_to_execute") != -1) {
    absl::SetFlag(&FLAGS_max_statements_to_execute,
                  test_case_options.GetInt64("max_statements_to_execute"));
  }
  if (!test_case_options.GetString("enabled_language_features").empty()) {
    absl::CommandLineFlag* flag =
        absl::FindCommandLineFlag("enabled_language_features");
    std::string error;
    EXPECT_TRUE(flag->ParseFrom(
        test_case_options.GetString("enabled_language_features"), &error))
        << error;
  }

  ExecuteQueryConfig config;
  ZETASQL_RETURN_IF_ERROR(InitializeExecuteQueryConfig(config));
  config.mutable_analyzer_options().set_error_message_mode(
      ERROR_MESSAGE_MULTI_LINE_WITH_CARET);

  return ExecuteQuery(test_case, config, *output);
}

// Wrapper around RunFileBasedTestImpl that turns returned errors into
// test output.
static void RunFileBasedTest(
    absl::string_view test_case_input,
    file_based_test_driver::RunTestCaseResult* test_result) {
  std::ostringstream output;
  absl::Status status =
      RunFileBasedTestImpl(test_case_input, test_result, &output);
  if (!status.ok()) {
    status.ErasePayload(kErrorMessageModeUrl);

    // Show both the output so far plus the error.
    output << "ERROR: " << status.ToString() << std::endl;
  }
  // String-replace is a hack because some catalog->FindX errors have a trailing
  // space when there's no catalog name and then the linter blocks the CL.
  test_result->AddTestOutput(
      absl::StrReplaceAll(output.str(), {{" \n", "<space removed>\n"}}));
}

TEST(ExecuteQuery, FileBasedTest) {
  absl::FlagSaver fs;
  const std::string pattern =
      zetasql_base::JoinPath(::testing::SrcDir(),
                     "com_google_zetasql/zetasql/tools/execute_query/"
                     "testdata/execute_query_tool.test");

  EXPECT_TRUE(file_based_test_driver::RunTestCasesFromFiles(pattern,
                                                            &RunFileBasedTest));
}

}  // namespace
}  // namespace zetasql
