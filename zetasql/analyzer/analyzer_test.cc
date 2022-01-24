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

#include "zetasql/public/analyzer.h"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/compiler/importer.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/base/testing/status_matchers.h"  
#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/proto/options.pb.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/literal_remover.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/sql_formatter.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/enum_type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/sql_builder.h"
#include "zetasql/testdata/sample_catalog.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/status.h"

namespace zetasql {

using testing::_;
using testing::HasSubstr;
using testing::IsNull;
using testing::Not;
using zetasql_base::testing::StatusIs;

class AnalyzerOptionsTest : public ::testing::Test {
 public:
  AnalyzerOptionsTest() {
    options_.mutable_language()->SetSupportsAllStatementKinds();
    sample_catalog_ = absl::make_unique<SampleCatalog>(options_.language());
  }
  AnalyzerOptionsTest(const AnalyzerOptionsTest&) = delete;
  AnalyzerOptionsTest& operator=(const AnalyzerOptionsTest&) = delete;
  ~AnalyzerOptionsTest() override {}

  SimpleCatalog* catalog() {
    return sample_catalog_->catalog();
  }

  AnalyzerOptions options_;
  TypeFactory type_factory_;

  void ValidateQueryParam(const std::string& name, const Type* type) {
    const std::string& key = absl::AsciiStrToLower(name);
    const QueryParametersMap& query_parameters = options_.query_parameters();
    EXPECT_TRUE(zetasql_base::ContainsKey(query_parameters, key));
    EXPECT_EQ(query_parameters.find(key)->second, type);
  }

 private:
  std::unique_ptr<SampleCatalog> sample_catalog_;
};

TEST_F(AnalyzerOptionsTest, AddSystemVariable) {
  // Simple cases
  EXPECT_EQ(options_.system_variables().size(), 0);
  ZETASQL_EXPECT_OK(options_.AddSystemVariable({"bytes"}, type_factory_.get_bytes()));
  ZETASQL_EXPECT_OK(options_.AddSystemVariable({"bool"}, type_factory_.get_bool()));
  ZETASQL_EXPECT_OK(
      options_.AddSystemVariable({"foo", "bar"}, type_factory_.get_string()));
  ZETASQL_EXPECT_OK(options_.AddSystemVariable({"foo.bar", "baz"},
                                       type_factory_.get_string()));

  // Null type
  EXPECT_THAT(
      options_.AddSystemVariable({"zzz"}, nullptr),
      StatusIs(
          _, HasSubstr("Type associated with system variable cannot be NULL")));

  // Unsupported type
  AnalyzerOptions external_mode(options_);
  external_mode.mutable_language()->set_product_mode(
      ProductMode::PRODUCT_EXTERNAL);
  EXPECT_THAT(
      external_mode.AddSystemVariable({"zzz"}, type_factory_.get_int32()),
      StatusIs(_,
               HasSubstr("System variable zzz has unsupported type: INT32")));

  // Duplicate variable
  EXPECT_THAT(
      options_.AddSystemVariable({"foo", "bar"}, type_factory_.get_int64()),
      StatusIs(_, HasSubstr("Duplicate system variable foo.bar")));

  // Duplicate variable (case insensitive)
  EXPECT_THAT(
      options_.AddSystemVariable({"FOO", "BaR"}, type_factory_.get_int64()),
      StatusIs(_, HasSubstr("Duplicate system variable FOO.BaR")));

  // Empty name path
  EXPECT_THAT(
      options_.AddSystemVariable({}, type_factory_.get_int64()),
      StatusIs(_, HasSubstr("System variable cannot have empty name path")));

  // name path with empty element
  EXPECT_THAT(
      options_.AddSystemVariable({""}, type_factory_.get_int64()),
      StatusIs(
          _,
          HasSubstr("System variable cannot have empty string as path part")));
}

TEST_F(AnalyzerOptionsTest, AddQueryParameter) {
  EXPECT_EQ(options_.query_parameters().size(), 0);
  ZETASQL_EXPECT_OK(
      options_.AddQueryParameter("bytes", type_factory_.get_bytes()));
  ZETASQL_EXPECT_OK(options_.AddQueryParameter("bool", type_factory_.get_bool()));
  ZETASQL_EXPECT_OK(options_.AddQueryParameter("int32", type_factory_.get_int32()));
  ZETASQL_EXPECT_OK(options_.AddQueryParameter("string", type_factory_.get_string()));
  ZETASQL_EXPECT_OK(
      options_.AddQueryParameter("MiXeDcAsE", type_factory_.get_string()));

  const QueryParametersMap& query_parameters = options_.query_parameters();
  EXPECT_EQ(query_parameters.size(), 5);
  ValidateQueryParam("bool", type_factory_.get_bool());
  ValidateQueryParam("bytes", type_factory_.get_bytes());
  ValidateQueryParam("int32", type_factory_.get_int32());
  ValidateQueryParam("string", type_factory_.get_string());
  // Verifies case insensitivity of parameter names.
  ValidateQueryParam("mixedcase", type_factory_.get_string());
  ValidateQueryParam("MIXEDCASE", type_factory_.get_string());
  ValidateQueryParam("mixedCASE", type_factory_.get_string());

  // Invalid cases.
  EXPECT_THAT(
      options_.AddQueryParameter("random", nullptr),
      StatusIs(
          _, HasSubstr("Type associated with query parameter cannot be NULL")));
  EXPECT_THAT(
      options_.AddPositionalQueryParameter(nullptr),
      StatusIs(
          _, HasSubstr("Type associated with query parameter cannot be NULL")));
  EXPECT_THAT(options_.AddQueryParameter("", type_factory_.get_bool()),
              StatusIs(_, HasSubstr("Query parameter cannot have empty name")));
  EXPECT_THAT(options_.AddQueryParameter("string", type_factory_.get_bool()),
              StatusIs(_, HasSubstr("Duplicate parameter name")));
}

TEST_F(AnalyzerOptionsTest, AddQueryParameterCivilTimeTypes) {
  EXPECT_EQ(options_.query_parameters().size(), 0);
  const StructType* time_struct;
  ZETASQL_CHECK_OK(type_factory_.MakeStructType(
      {{"time_value", type_factory_.get_time()}}, &time_struct));
  const ArrayType* time_array;
  ZETASQL_CHECK_OK(type_factory_.MakeArrayType(type_factory_.get_time(), &time_array));
  const ProtoType* proto_with_time;
  ZETASQL_CHECK_OK(type_factory_.MakeProtoType(
      zetasql_test__::CivilTimeTypesSinkPB::descriptor(), &proto_with_time));

  options_.mutable_language()->DisableAllLanguageFeatures();
  EXPECT_FALSE(
      options_.AddQueryParameter("time_1", type_factory_.get_time()).ok());
  EXPECT_FALSE(
      options_.AddPositionalQueryParameter(type_factory_.get_time()).ok());
  EXPECT_FALSE(options_.AddQueryParameter("time_struct_1", time_struct).ok());
  EXPECT_FALSE(options_.AddQueryParameter("time_array_1", time_array).ok());
  // Adding a proto as parameter should be fine, even if there are some field in
  // it can be recognized as time or datetime.
  ZETASQL_EXPECT_OK(options_.AddQueryParameter("proto_with_time_1", proto_with_time));

  options_.mutable_language()->EnableLanguageFeature(FEATURE_V_1_2_CIVIL_TIME);
  ZETASQL_EXPECT_OK(options_.AddQueryParameter("time_2", type_factory_.get_time()));
  ZETASQL_EXPECT_OK(options_.AddQueryParameter("time_struct_2", time_struct));
  ZETASQL_EXPECT_OK(options_.AddQueryParameter("time_array_2", time_array));
  ZETASQL_EXPECT_OK(options_.AddQueryParameter("proto_with_time_2", proto_with_time));
}

TEST_F(AnalyzerOptionsTest, ParserASTOwnershipTests) {
  const std::string sql("SELECT 1;");

  std::unique_ptr<ParserOutput> parser_output;
  ParserOptions parser_options = options_.GetParserOptions();
  ZETASQL_ASSERT_OK(ParseStatement(sql, parser_options, &parser_output));

  // Analyze statement from ParserOutput without taking ownership of
  // <parser_output>.
  std::unique_ptr<const AnalyzerOutput> analyzer_output_unowned_parser_output;
  ZETASQL_EXPECT_OK(AnalyzeStatementFromParserOutputUnowned(
      &parser_output, options_, sql, catalog(), &type_factory_,
      &analyzer_output_unowned_parser_output));
  // <parser_output> retains ownership.
  ASSERT_NE(nullptr, parser_output);

  // Analyze statement from ParserOutput and take ownership of <parser_output>.
  std::unique_ptr<const AnalyzerOutput> analyzer_output_owned_parser_output;
  ZETASQL_EXPECT_OK(AnalyzeStatementFromParserOutputOwnedOnSuccess(
      &parser_output, options_, sql, catalog(), &type_factory_,
      &analyzer_output_owned_parser_output));

  // Both analyses result in the same resolved AST.
  EXPECT_EQ(analyzer_output_unowned_parser_output->resolved_statement()
              ->DebugString(),
            analyzer_output_owned_parser_output->resolved_statement()
              ->DebugString());

  // <parser_output> ownership has been released.
  EXPECT_EQ(nullptr, parser_output);

  // Test that for failed resolution, ownership of the <parser_output>
  // is retained.
  const std::string bad_sql("SELECT 1 + 'a';");

  // Re-use same ParserOutput.
  ZETASQL_ASSERT_OK(ParseStatement(bad_sql, parser_options, &parser_output));

  // Re-use same AnalyzerOutput.
  EXPECT_FALSE(AnalyzeStatementFromParserOutputOwnedOnSuccess(
      &parser_output, options_, bad_sql, catalog(), &type_factory_,
      &analyzer_output_owned_parser_output).ok());

  // <parser_output> retains ownership.
  EXPECT_NE(nullptr, parser_output);
}

TEST_F(AnalyzerOptionsTest, QueryParameterModes) {
  std::unique_ptr<const AnalyzerOutput> output;
  {
    // Named mode with positional parameter.
    AnalyzerOptions options = options_;
    options.set_parameter_mode(PARAMETER_NAMED);
    ZETASQL_EXPECT_OK(options.AddPositionalQueryParameter(types::Int64Type()));

    EXPECT_THAT(AnalyzeStatement("SELECT 1;", options, nullptr, &type_factory_,
                                 &output),
                StatusIs(_, HasSubstr("Positional parameters cannot be "
                                      "provided in named parameter mode")));
  }

  {
    // Positional mode with named parameter.
    AnalyzerOptions options = options_;
    options.set_parameter_mode(PARAMETER_POSITIONAL);
    ZETASQL_EXPECT_OK(options.AddQueryParameter("test_param", types::Int64Type()));

    EXPECT_THAT(AnalyzeStatement("SELECT 1;", options, nullptr, &type_factory_,
                                 &output),
                StatusIs(_, HasSubstr("Named parameters cannot be provided "
                                      "in positional parameter mode")));
  }

  {
    // Adding a positional parameter after setting allow_undeclared_parameters.
    AnalyzerOptions options = options_;
    options.set_allow_undeclared_parameters(true);
    options.set_parameter_mode(PARAMETER_POSITIONAL);
    EXPECT_THAT(options.AddPositionalQueryParameter(types::Int64Type()),
                StatusIs(_, HasSubstr("Positional query parameters cannot "
                                      "be provided when undeclared "
                                      "parameters are allowed")));
  }

  {
    // Adding a positional parameter prior to setting
    // allow_undeclared_parameters.
    AnalyzerOptions options = options_;
    options.set_parameter_mode(PARAMETER_POSITIONAL);
    ZETASQL_EXPECT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
    options.set_allow_undeclared_parameters(true);

    EXPECT_THAT(
        AnalyzeStatement("SELECT 1;", options, nullptr, &type_factory_,
                         &output),
        StatusIs(
            _,
            HasSubstr(
                "When undeclared parameters are allowed, no positional query "
                "parameters can be provided")));
  }
}

TEST_F(AnalyzerOptionsTest, AddExpressionColumn) {
  EXPECT_EQ(0, options_.expression_columns().size());
  ZETASQL_EXPECT_OK(
      options_.AddExpressionColumn("bytes", type_factory_.get_bytes()));
  ZETASQL_EXPECT_OK(options_.AddExpressionColumn("bool", type_factory_.get_bool()));
  ZETASQL_EXPECT_OK(
      options_.AddExpressionColumn("MiXeDcAsE", type_factory_.get_string()));

  const QueryParametersMap& expression_columns = options_.expression_columns();
  EXPECT_EQ(3, expression_columns.size());
  EXPECT_EQ("", options_.in_scope_expression_column_name());
  EXPECT_EQ(nullptr, options_.in_scope_expression_column_type());

  // Invalid cases.
  EXPECT_THAT(
      options_.AddExpressionColumn("random", nullptr),
      StatusIs(
          _,
          HasSubstr("Type associated with expression column cannot be NULL")));
  EXPECT_THAT(
      options_.AddExpressionColumn("", type_factory_.get_bool()),
      StatusIs(_, HasSubstr("Expression column cannot have empty name")));
  EXPECT_THAT(
      options_.AddExpressionColumn("MIXEDcase", type_factory_.get_bool()),
      StatusIs(_, HasSubstr("Duplicate expression column name")));

  EXPECT_THAT(options_.SetInScopeExpressionColumn("MIXEDcase",
                                                  type_factory_.get_bool()),
              StatusIs(_, HasSubstr("Duplicate expression column name")));
  EXPECT_EQ("", options_.in_scope_expression_column_name());
  EXPECT_EQ(nullptr, options_.in_scope_expression_column_type());

  ZETASQL_EXPECT_OK(options_.SetInScopeExpressionColumn("InScopeName",
                                                type_factory_.get_bool()));
  EXPECT_EQ(4, expression_columns.size());
  EXPECT_EQ("inscopename", options_.in_scope_expression_column_name());
  EXPECT_EQ("BOOL", options_.in_scope_expression_column_type()->DebugString());
}

TEST_F(AnalyzerOptionsTest, SetInScopeExpressionColumn_Unnamed) {
  EXPECT_TRUE(options_.expression_columns().empty());
  EXPECT_EQ("", options_.in_scope_expression_column_name());
  EXPECT_EQ(nullptr, options_.in_scope_expression_column_type());

  EXPECT_THAT(options_.SetInScopeExpressionColumn("col3", nullptr),
              StatusIs(_, HasSubstr("cannot be NULL")));

  ZETASQL_EXPECT_OK(options_.SetInScopeExpressionColumn("", types::Int32Type()));

  EXPECT_EQ(1, options_.expression_columns().size());
  EXPECT_EQ("", options_.in_scope_expression_column_name());
  EXPECT_EQ("INT32", options_.in_scope_expression_column_type()->DebugString());

  EXPECT_THAT(
      options_.SetInScopeExpressionColumn("xxx", types::BoolType()),
      StatusIs(_, HasSubstr("Cannot call SetInScopeExpressionColumn twice")));

  ZETASQL_EXPECT_OK(options_.AddExpressionColumn("col1", types::BoolType()));
  EXPECT_EQ(2, options_.expression_columns().size());
  EXPECT_EQ("", options_.in_scope_expression_column_name());
  EXPECT_EQ("INT32", options_.in_scope_expression_column_type()->DebugString());
}

TEST_F(AnalyzerOptionsTest, SetInScopeExpressionColumn_Named) {
  ZETASQL_EXPECT_OK(options_.SetInScopeExpressionColumn("value", types::Int32Type()));

  EXPECT_EQ(1, options_.expression_columns().size());
  EXPECT_EQ("value", options_.in_scope_expression_column_name());
  EXPECT_EQ("INT32", options_.in_scope_expression_column_type()->DebugString());

  // When we insert a named in-scope expression column, it also shows up in
  // expression_columns().
  EXPECT_EQ("value", options_.expression_columns().begin()->first);
  EXPECT_EQ("INT32",
            options_.expression_columns().begin()->second->DebugString());

  ZETASQL_EXPECT_OK(options_.AddExpressionColumn("col1", types::BoolType()));
  EXPECT_EQ(2, options_.expression_columns().size());
}

TEST_F(AnalyzerOptionsTest, SetDdlPseudoColumns) {
  EXPECT_EQ(nullptr, options_.ddl_pseudo_columns_callback());

  std::string table_name;
  std::vector<std::string> option_names;
  options_.SetDdlPseudoColumnsCallback(
      [this, &table_name, &option_names](
          const std::vector<std::string>& table_name_parts,
          const std::vector<const ResolvedOption*>& options,
          std::vector<std::pair<std::string, const Type*>>* pseudo_columns) {
        // Just record the table name and options.
        table_name = absl::StrJoin(table_name_parts, ".");
        option_names.clear();
        option_names.reserve(options.size());
        for (const ResolvedOption* option : options) {
          option_names.push_back(option->name());
          // Each option name can be referenced as a pseudo-column.
          pseudo_columns->push_back(
              std::make_pair(option->name(), type_factory_.get_int64()));
        }
        return absl::OkStatus();
      });
  EXPECT_NE(nullptr, options_.ddl_pseudo_columns_callback());

  std::unique_ptr<const AnalyzerOutput> output;
  LanguageOptions language_options;
  language_options.SetSupportsAllStatementKinds();
  language_options.EnableLanguageFeature(FEATURE_CREATE_TABLE_PARTITION_BY);
  options_.set_language(language_options);

  // Each test case contains the statement to analyze and the expected table and
  // options that the DDL pseudo-column callback will receive.
  struct TestCase {
    std::string statement;
    std::string expected_table;
    std::vector<std::string> expected_options;
  };
  const std::vector<TestCase> kTestCases = {
      // No PARTITION BY, hence the callback is not invoked.
      {"CREATE TABLE T (x INT64);", "", {}},
      // The PARTITION BY callback receives table name "T".
      {"CREATE TABLE T (x INT64) PARTITION BY x;", "T", {}},
      // Different table name.
      {"CREATE TABLE T.X.Y (x INT64) PARTITION BY x;", "T.X.Y", {}},
      // OPTIONS are foo and bar.
      {"CREATE TABLE T (x INT64) PARTITION BY x OPTIONS (foo=true, bar=1);",
       "T",
       {"foo", "bar"}},
      // The callback sets the pseudo-columns to the names of the options.
      {"CREATE TABLE T (x INT64) PARTITION BY foo, bar "
       "OPTIONS (foo=true, bar=1);",
       "T",
       {"foo", "bar"}},
  };
  for (const TestCase& test_case : kTestCases) {
    SCOPED_TRACE(test_case.statement);
    table_name.clear();
    option_names.clear();
    ZETASQL_ASSERT_OK(AnalyzeStatement(test_case.statement, options_, catalog(),
                               &type_factory_, &output));
    EXPECT_EQ(test_case.expected_table, table_name);
    EXPECT_THAT(option_names,
                ::testing::ElementsAreArray(test_case.expected_options));
  }

  options_.SetDdlPseudoColumns(
      {{"_partition_timestamp", type_factory_.get_timestamp()},
       {"_partition_date", type_factory_.get_date()}});
  std::vector<std::pair<std::string, const Type*>> pseudo_columns;
  ZETASQL_ASSERT_OK(options_.ddl_pseudo_columns_callback()({}, {}, &pseudo_columns));
  EXPECT_EQ(2, pseudo_columns.size());
  ZETASQL_ASSERT_OK(
      AnalyzeStatement("CREATE TABLE T (x INT64, y STRING) "
                       "PARTITION BY _partition_date, _partition_timestamp;",
                       options_, catalog(), &type_factory_, &output));
}

TEST_F(AnalyzerOptionsTest, ErrorMessageFormat) {
  std::unique_ptr<const AnalyzerOutput> output;

  const std::string query = "select *\nfrom BadTable";
  const std::string expr = "1 +\n2 + BadCol +\n3";

  EXPECT_EQ(ErrorMessageMode::ERROR_MESSAGE_ONE_LINE,
            options_.error_message_mode());

  EXPECT_EQ(
      "generic::invalid_argument: Table not found: BadTable; Did you mean "
      "abTable? [at 2:6]",
      zetasql::internal::StatusToString(AnalyzeStatement(
          query, options_, catalog(), &type_factory_, &output)));
  EXPECT_EQ("generic::invalid_argument: Unrecognized name: BadCol [at 2:5]",
            zetasql::internal::StatusToString(AnalyzeExpression(
                expr, options_, catalog(), &type_factory_, &output)));

  options_.set_error_message_mode(ErrorMessageMode::ERROR_MESSAGE_WITH_PAYLOAD);
  EXPECT_EQ(
      "generic::invalid_argument: Table not found: BadTable; Did you mean "
      "abTable? "
      "[zetasql.ErrorLocation] { line: 2 column: 6 }",
      zetasql::internal::StatusToString(AnalyzeStatement(
          query, options_, catalog(), &type_factory_, &output)));
  EXPECT_EQ(
      "generic::invalid_argument: Unrecognized name: BadCol "
      "[zetasql.ErrorLocation] { line: 2 column: 5 }",
      zetasql::internal::StatusToString(AnalyzeExpression(
          expr, options_, catalog(), &type_factory_, &output)));

  options_.set_error_message_mode(
      ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET);
  EXPECT_EQ(
      "generic::invalid_argument: Table not found: BadTable; Did you mean "
      "abTable? [at 2:6]\n"
      "from BadTable\n"
      "     ^",
      zetasql::internal::StatusToString(AnalyzeStatement(
          query, options_, catalog(), &type_factory_, &output)));
  EXPECT_EQ(
      "generic::invalid_argument: Unrecognized name: BadCol [at 2:5]\n"
      "2 + BadCol +\n"
      "    ^",
      zetasql::internal::StatusToString(AnalyzeExpression(
          expr, options_, catalog(), &type_factory_, &output)));
}

TEST_F(AnalyzerOptionsTest, NestedCatalogTypesErrorMessageFormat) {
  std::unique_ptr<const AnalyzerOutput> output;

  SimpleCatalog leaf_catalog_1("leaf_catalog_1");
  SimpleCatalog leaf_catalog_2("leaf_catalog_2");
  catalog()->AddCatalog(&leaf_catalog_1);
  catalog()->AddCatalog(&leaf_catalog_2);

  const ProtoType* proto_type_with_catalog = nullptr;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(
      TypeProto::descriptor(), &proto_type_with_catalog, {"catalog_name"}));
  leaf_catalog_1.AddType("zetasql.TypeProto", proto_type_with_catalog);

  const ProtoType* proto_type_without_catalog = nullptr;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(TypeProto::descriptor(),
                                        &proto_type_without_catalog));
  leaf_catalog_2.AddType("zetasql.TypeProto", proto_type_without_catalog);

  // The catalog name is shown.
  EXPECT_EQ(
      "generic::invalid_argument: Invalid cast from INT64 to "
      "catalog_name.zetasql.TypeProto [at 1:6]",
      zetasql::internal::StatusToString(
          AnalyzeExpression("CAST(1 AS leaf_catalog_1.zetasql.TypeProto)",
                            options_, catalog(), &type_factory_, &output)));

  // The catalog name is not shown.
  EXPECT_EQ(
      "generic::invalid_argument: Invalid cast from INT64 to "
      "zetasql.TypeProto [at 1:6]",
      zetasql::internal::StatusToString(
          AnalyzeExpression("CAST(1 AS leaf_catalog_2.zetasql.TypeProto)",
                            options_, catalog(), &type_factory_, &output)));
}

// Some of these were previously dchecking because of bug 20010119.
TEST_F(AnalyzerOptionsTest, EofErrorMessageTrailingNewlinesAndWhitespace) {
  std::unique_ptr<const AnalyzerOutput> output;

  options_.set_error_message_mode(
      ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET);

  // Trailing newlines are ignored for unexpected end of statement errors.
  for (const std::string& newline : {"\n", "\r\n", "\n\r", "\r"}) {
    EXPECT_EQ(
        "generic::invalid_argument: Syntax error: Unexpected end of "
        "statement [at 1:1]\n"
        "\n"
        "^",
        zetasql::internal::StatusToString(AnalyzeStatement(
            newline, options_, catalog(), &type_factory_, &output)));
  }

  // All of these instances of trailing whitespace are ignored for unexpected
  // end of statement errors. We're not testing all possible whitespace, but
  // one multibyte whitespace character is included to verify that we're using
  // the generic whitespace rule for this, and not some ASCII-only hack.
  for (const std::string& whitespace :
       {" ", "   ", "\342\200\200" /* EN QUAD */}) {
    for (const std::string& newline : {"\n", "\r\n", "\n\r", "\r"}) {
      for (const std::string& more_whitespace :
           {"", " ", "   ", "\342\200\200" /* EN QUAD */}) {
        EXPECT_EQ(
            absl::StrCat(
                "generic::invalid_argument: Syntax error: Unexpected end of "
                "statement [at 1:7]\n"
                "SELECT",
                whitespace,
                "\n"
                "      ^"),
            zetasql::internal::StatusToString(AnalyzeStatement(
                absl::StrCat("SELECT", whitespace, newline, more_whitespace),
                options_, catalog(), &type_factory_, &output)));
      }
    }
  }
}

// Basic functionality of literal replacement.
TEST_F(AnalyzerOptionsTest, LiteralReplacement) {
  std::unique_ptr<const AnalyzerOutput> output;
  options_.set_record_parse_locations(true);

  std::string sql = "   \tSELECT 1, 'Yes', 'a'='a'AND'b'='b'";
  ZETASQL_EXPECT_OK(AnalyzeStatement(sql, options_, catalog(),
                             &type_factory_, &output));

  std::string new_sql;
  LiteralReplacementMap literal_map;
  GeneratedParameterMap generated_parameters;
  absl::node_hash_set<std::string> option_names_to_ignore;
  ZETASQL_ASSERT_OK(ReplaceLiteralsByParameters(
      sql, option_names_to_ignore, options_, output.get(),
      &literal_map, &generated_parameters, &new_sql));
  EXPECT_EQ(
      "   \tSELECT @_p0_INT64, @_p1_STRING, @_p2_STRING=@_p3_STRING "
      "AND@_p4_STRING=@_p5_STRING",
      new_sql);
  ASSERT_EQ(6, literal_map.size());
  for (const auto& pair : literal_map) {
    if (pair.second == "_p0_INT64") {
      EXPECT_EQ(pair.first->value(), Value::Int64(1));
    } else if (pair.second == "_p1_STRING") {
      EXPECT_EQ(pair.first->value(), Value::String("Yes"));
    } else {
      EXPECT_THAT(pair.second,
                  testing::AnyOf("_p0_INT64", "_p1_STRING", "_p2_STRING",
                                 "_p3_STRING", "_p4_STRING", "_p5_STRING"));
    }
  }
  ASSERT_EQ(6, generated_parameters.size());
  EXPECT_EQ(generated_parameters["_p0_INT64"], Value::Int64(1));
  EXPECT_EQ(generated_parameters["_p1_STRING"], Value::String("Yes"));
}

// Test 'option_names_to_ignore' which is a set of option names to ignore during
// literal removal.
TEST_F(AnalyzerOptionsTest, LiteralReplacementIgnoreAllowlistedOptions) {
  std::unique_ptr<const AnalyzerOutput> output;
  options_.set_record_parse_locations(true);

  std::string sql = "   \t@{ hint1=1, hint2=true, hint3='foo' }SELECT 1, 'Yes'";
  ZETASQL_EXPECT_OK(AnalyzeStatement(sql, options_, catalog(),
                             &type_factory_, &output));

  std::string new_sql;
  LiteralReplacementMap literal_map;
  GeneratedParameterMap generated_parameters;
  ZETASQL_ASSERT_OK(ReplaceLiteralsByParameters(
      sql, /* ignore hint1 and hint3 but remove hint2 */ {"hint1", "hint3"},
      options_, output.get(), &literal_map, &generated_parameters, &new_sql));
  EXPECT_EQ(
      "   \t@{ hint1=1, hint2=@_p0_BOOL, hint3='foo' }SELECT @_p1_INT64, "
      "@_p2_STRING",
      new_sql);
  ASSERT_EQ(3, literal_map.size());
  for (const auto& pair : literal_map) {
    if (pair.second == "_p0_BOOL") {
      EXPECT_EQ(pair.first->value(), Value::Bool(true));
    } else if (pair.second == "_p1_INT64") {
      EXPECT_EQ(pair.first->value(), Value::Int64(1));
    } else if (pair.second == "_p2_STRING") {
      EXPECT_EQ(pair.first->value(), Value::String("Yes"));
    }
  }
  ASSERT_EQ(3, generated_parameters.size());
  EXPECT_EQ(generated_parameters["_p0_BOOL"], Value::Bool(true));
  EXPECT_EQ(generated_parameters["_p1_INT64"], Value::Int64(1));
  EXPECT_EQ(generated_parameters["_p2_STRING"], Value::String("Yes"));
}

TEST_F(AnalyzerOptionsTest, DeprecationWarnings) {
  // Create some deprecated functions as a stable way to generate deprecation
  // errors.  (Real deprecated features in the language won't stick around.)
  FunctionOptions function_options;
  function_options.set_is_deprecated(true);

  SimpleCatalog catalog("catalog");
  catalog.AddZetaSQLFunctions();
  catalog.AddOwnedFunction(new Function(
      "depr1", "test_group", Function::SCALAR,
      {{ARG_TYPE_ANY_1, {ARG_TYPE_ANY_1}, 1}}, function_options));
  catalog.AddOwnedFunction(new Function(
      "depr2", "test_group", Function::SCALAR,
      {{ARG_TYPE_ANY_1, {ARG_TYPE_ANY_1}, 2}}, function_options));

  // Use the same base expression in a query or in an expression, with a
  // leading newline so the error locations come out the same.
  const std::string base_expr = "DEPR1(5) + DEPR1(6) + DEPR2(7)";
  const std::string expr = absl::StrCat("\n", base_expr);
  const std::string sql = absl::StrCat("SELECT\n", base_expr);
  const std::string multi_sql = absl::StrCat("SELECT 1; SELECT\n", base_expr);

  std::unique_ptr<const AnalyzerOutput> output;

  EXPECT_FALSE(options_.language().error_on_deprecated_syntax());

  // Test with multiple input types (0=Statement, 1=NextStatement, 2=Expression)
  // and with error_on_deprecated_syntax on or off.
  for (int input_type = 0; input_type <= 2; ++input_type) {
    for (int with_errors = 0; with_errors <= 1; ++with_errors) {
      options_.mutable_language()->set_error_on_deprecated_syntax(
          with_errors == 1);

      absl::Status status;
      if (input_type == 0) {
        status = AnalyzeStatement(sql, options_, &catalog, &type_factory_,
                                  &output);
      } else if (input_type == 1) {
        // Parse two queries from one string.  The first query has no warnings.
        bool at_end;
        ParseResumeLocation location =
            ParseResumeLocation::FromStringView(multi_sql);
        ZETASQL_EXPECT_OK(AnalyzeNextStatement(
            &location, options_, &catalog, &type_factory_, &output, &at_end));
        ASSERT_FALSE(at_end);
        EXPECT_EQ(0, output->deprecation_warnings().size());

        status = AnalyzeNextStatement(
            &location, options_, &catalog, &type_factory_, &output, &at_end);
        EXPECT_TRUE(at_end);
      } else if (input_type == 2) {
        status = AnalyzeExpression(expr, options_, &catalog, &type_factory_,
                                   &output);
      }

      if (with_errors == 1) {
        EXPECT_EQ(
            "generic::invalid_argument: Function TEST_GROUP:DEPR1 is "
            "deprecated [at 2:1] [zetasql.DeprecationWarning] "
            "{ kind: DEPRECATED_FUNCTION }",
            zetasql::internal::StatusToString(status));
      } else {
        ZETASQL_EXPECT_OK(status);
        ASSERT_EQ(2, output->deprecation_warnings().size());
        EXPECT_EQ(
            "generic::invalid_argument: Function TEST_GROUP:DEPR1 is "
            "deprecated [at 2:1] [zetasql.DeprecationWarning] "
            "{ kind: DEPRECATED_FUNCTION }",
            zetasql::internal::StatusToString(
                output->deprecation_warnings()[0]));
        EXPECT_EQ(
            "generic::invalid_argument: Function TEST_GROUP:DEPR2 is "
            "deprecated [at 2:23] [zetasql.DeprecationWarning] "
            "{ kind: DEPRECATED_FUNCTION }",
            zetasql::internal::StatusToString(
                output->deprecation_warnings()[1]));
      }
    }
  }
}

TEST_F(AnalyzerOptionsTest, ResolvedASTRewrites) {
  // Should be on by default.
  EXPECT_TRUE(options_.rewrite_enabled(REWRITE_FLATTEN));
  options_.enable_rewrite(REWRITE_FLATTEN, /*enable=*/false);
  EXPECT_FALSE(options_.rewrite_enabled(REWRITE_FLATTEN));
  options_.enable_rewrite(REWRITE_FLATTEN);
  EXPECT_TRUE(options_.rewrite_enabled(REWRITE_FLATTEN));

  absl::btree_set<ResolvedASTRewrite> rewrites;
  rewrites.insert(REWRITE_INVALID_DO_NOT_USE);
  options_.set_enabled_rewrites(rewrites);
  EXPECT_FALSE(options_.rewrite_enabled(REWRITE_FLATTEN));
  EXPECT_TRUE(options_.rewrite_enabled(REWRITE_INVALID_DO_NOT_USE));
}

// Need to implement this to catch importing errors.
class MultiFileErrorCollector
    : public google::protobuf::compiler::MultiFileErrorCollector {
 public:
  MultiFileErrorCollector() {}
  MultiFileErrorCollector(const MultiFileErrorCollector&) = delete;
  MultiFileErrorCollector& operator=(const MultiFileErrorCollector&) = delete;
  void AddError(const std::string& filename, int line, int column,
                const std::string& message) override {
    absl::StrAppend(&error_, "Line ", line, " Column ", column, " :", message,
                    "\n");
  }
  const std::string& GetError() const { return error_; }

 private:
  std::string error_;
};

TEST_F(AnalyzerOptionsTest, Deserialize) {
  TypeFactory factory;

  const std::vector<std::string> test_files{
      // Order matters for these imports.
      "google/protobuf/descriptor.proto",
      "zetasql/public/proto/type_annotation.proto",
      "zetasql/testdata/test_schema.proto",
      "zetasql/testdata/external_extension.proto",
  };
  std::unique_ptr<google::protobuf::compiler::DiskSourceTree> source_tree =
      CreateProtoSourceTree();
  MultiFileErrorCollector error_collector;

  std::unique_ptr<google::protobuf::compiler::Importer> proto_importer(
      new google::protobuf::compiler::Importer(source_tree.get(), &error_collector));

  for (const std::string& test_file : test_files) {
    ZETASQL_CHECK(proto_importer->Import(test_file) != nullptr)
        << "Error importing " << test_file << ": "
        << error_collector.GetError();
  }
  std::unique_ptr<google::protobuf::DescriptorPool> external_pool(
      new google::protobuf::DescriptorPool(proto_importer->pool()));

  const EnumType* generated_enum_type;
  ZETASQL_CHECK_OK(factory.MakeEnumType(TypeKind_descriptor(), &generated_enum_type));

  const ProtoType* generated_proto_type;
  ZETASQL_CHECK_OK(factory.MakeProtoType(
      TypeProto::descriptor(), &generated_proto_type));

  const EnumType* external_enum_type;
  ZETASQL_CHECK_OK(factory.MakeEnumType(
      external_pool->FindEnumTypeByName("zetasql_test__.TestEnum"),
      &external_enum_type));

  const ProtoType* external_proto_type;
  ZETASQL_CHECK_OK(factory.MakeProtoType(
      external_pool->FindMessageTypeByName("zetasql_test__.KitchenSinkPB"),
      &external_proto_type));

  std::vector<const google::protobuf::DescriptorPool*> pools = {
    external_pool.get(), google::protobuf::DescriptorPool::generated_pool()
  };

  AnalyzerOptionsProto proto;
  proto.set_error_message_mode(ERROR_MESSAGE_WITH_PAYLOAD);
  proto.set_prune_unused_columns(true);
  proto.set_allow_undeclared_parameters(true);
  proto.set_parameter_mode(PARAMETER_POSITIONAL);
  proto.mutable_language_options()->set_product_mode(PRODUCT_INTERNAL);
  proto.set_default_timezone("Asia/Shanghai");
  proto.set_preserve_column_aliases(false);
  proto.set_parse_location_record_type(PARSE_LOCATION_RECORD_FULL_NODE_SCOPE);

  auto* param = proto.add_query_parameters();
  param->set_name("q1");
  ZETASQL_CHECK_OK(types::Int64Type()->SerializeToSelfContainedProto(
      param->mutable_type()));

  param = proto.add_query_parameters();
  param->set_name("q2");
  ZETASQL_CHECK_OK(external_enum_type->SerializeToProtoAndFileDescriptors(
      param->mutable_type()));
  param->mutable_type()->mutable_enum_type()->set_file_descriptor_set_index(0);

  param = proto.add_query_parameters();
  param->set_name("q3");
  ZETASQL_CHECK_OK(external_proto_type->SerializeToProtoAndFileDescriptors(
      param->mutable_type()));
  param->mutable_type()->mutable_proto_type()->set_file_descriptor_set_index(0);

  param = proto.add_query_parameters();
  param->set_name("q4");
  ZETASQL_CHECK_OK(generated_enum_type->SerializeToProtoAndFileDescriptors(
      param->mutable_type()));
  param->mutable_type()->mutable_enum_type()->set_file_descriptor_set_index(1);

  param = proto.add_query_parameters();
  param->set_name("q5");
  ZETASQL_CHECK_OK(generated_proto_type->SerializeToProtoAndFileDescriptors(
      param->mutable_type()));
  param->mutable_type()->mutable_proto_type()->set_file_descriptor_set_index(1);

  ZETASQL_ASSERT_OK(types::Int64Type()->SerializeToSelfContainedProto(
      proto.add_positional_query_parameters()));

  ZETASQL_ASSERT_OK(generated_proto_type->SerializeToSelfContainedProto(
      proto.add_positional_query_parameters()));
  proto.mutable_positional_query_parameters(1)
      ->mutable_proto_type()
      ->set_file_descriptor_set_index(1);

  auto* column = proto.add_expression_columns();
  column->set_name("c1");
  ZETASQL_CHECK_OK(external_enum_type->SerializeToProtoAndFileDescriptors(
      column->mutable_type()));
  column->mutable_type()->mutable_enum_type()->set_file_descriptor_set_index(0);

  column = proto.add_expression_columns();
  column->set_name("c2");
  ZETASQL_CHECK_OK(types::Int64Type()->SerializeToSelfContainedProto(
      column->mutable_type()));

  column = proto.add_expression_columns();
  column->set_name("c3");
  ZETASQL_CHECK_OK(external_proto_type->SerializeToProtoAndFileDescriptors(
      column->mutable_type()));
  column->mutable_type()->mutable_proto_type()
      ->set_file_descriptor_set_index(0);

  column = proto.add_expression_columns();
  column->set_name("c4");
  ZETASQL_CHECK_OK(generated_enum_type->SerializeToProtoAndFileDescriptors(
      column->mutable_type()));
  column->mutable_type()->mutable_enum_type()->set_file_descriptor_set_index(1);

  column = proto.add_expression_columns();
  column->set_name("c5");
  ZETASQL_CHECK_OK(generated_proto_type->SerializeToProtoAndFileDescriptors(
      column->mutable_type()));
  column->mutable_type()->mutable_proto_type()
      ->set_file_descriptor_set_index(1);

  auto* in_scope_column = proto.mutable_in_scope_expression_column();
  in_scope_column->set_name("isc");
  ZETASQL_CHECK_OK(generated_proto_type->SerializeToProtoAndFileDescriptors(
      in_scope_column->mutable_type()));
  in_scope_column->mutable_type()->mutable_proto_type()
      ->set_file_descriptor_set_index(1);

  auto* allowed = proto.mutable_allowed_hints_and_options();
  allowed->add_disallow_unknown_hints_with_qualifier("qual");
  allowed->add_disallow_unknown_hints_with_qualifier("");
  allowed->set_disallow_unknown_options(true);
  auto* hint1 = allowed->add_hint();
  hint1->set_qualifier("test_qual");
  hint1->set_name("hint1");
  ZETASQL_CHECK_OK(generated_proto_type->SerializeToProtoAndFileDescriptors(
      hint1->mutable_type()));
  hint1->mutable_type()->mutable_proto_type()->set_file_descriptor_set_index(1);
  hint1->set_allow_unqualified(true);
  auto* hint2 = allowed->add_hint();
  hint2->set_qualifier("test_qual");
  hint2->set_name("untyped_hint");
  hint2->set_allow_unqualified(true);
  auto* option1 = allowed->add_option();
  option1->set_name("untyped_option");
  auto* option2 = allowed->add_option();
  option2->set_name("option2");
  ZETASQL_CHECK_OK(generated_enum_type->SerializeToProtoAndFileDescriptors(
      option2->mutable_type()));
  option2->mutable_type()->mutable_enum_type()
      ->set_file_descriptor_set_index(1);

  AnalyzerOptions options;
  ZETASQL_CHECK_OK(AnalyzerOptions::Deserialize(proto, pools, &factory, &options));

  ASSERT_TRUE(options.prune_unused_columns());
  ASSERT_EQ(PARSE_LOCATION_RECORD_FULL_NODE_SCOPE,
            options.parse_location_record_type());
  ASSERT_TRUE(options.allow_undeclared_parameters());
  ASSERT_EQ(ERROR_MESSAGE_WITH_PAYLOAD, options.error_message_mode());
  ASSERT_EQ(PRODUCT_INTERNAL, options.language().product_mode());
  ASSERT_EQ(PARAMETER_POSITIONAL, options.parameter_mode());
  ASSERT_FALSE(options.preserve_column_aliases());

  ASSERT_EQ(5, options.query_parameters().size());
  ASSERT_TRUE(types::Int64Type()->Equals(options.query_parameters().at("q1")));
  ASSERT_TRUE(external_enum_type->Equals(options.query_parameters().at("q2")));
  ASSERT_TRUE(external_proto_type->Equals(options.query_parameters().at("q3")));
  ASSERT_TRUE(generated_enum_type->Equals(options.query_parameters().at("q4")));
  ASSERT_TRUE(generated_proto_type->Equals(
      options.query_parameters().at("q5")));

  ASSERT_EQ(2, options.positional_query_parameters().size());
  ASSERT_TRUE(
      types::Int64Type()->Equals(options.positional_query_parameters()[0]));
  ASSERT_TRUE(
      generated_proto_type->Equals(options.positional_query_parameters()[1]));

  ASSERT_EQ(6, options.expression_columns().size());
  ASSERT_TRUE(external_enum_type->Equals(
      options.expression_columns().at("c1")));
  ASSERT_TRUE(types::Int64Type()->Equals(
      options.expression_columns().at("c2")));
  ASSERT_TRUE(external_proto_type->Equals(
      options.expression_columns().at("c3")));
  ASSERT_TRUE(generated_enum_type->Equals(
      options.expression_columns().at("c4")));
  ASSERT_TRUE(generated_proto_type->Equals(
      options.expression_columns().at("c5")));
  ASSERT_TRUE(generated_proto_type->Equals(
      options.expression_columns().at("isc")));

  ASSERT_TRUE(options.has_in_scope_expression_column());
  ASSERT_EQ("isc", options.in_scope_expression_column_name());
  ASSERT_TRUE(generated_proto_type->Equals(
      options.in_scope_expression_column_type()));
  ASSERT_EQ("Asia/Shanghai", options.default_time_zone().name());

  ASSERT_TRUE(options.allowed_hints_and_options()
              .disallow_unknown_hints_with_qualifiers.find("qual") !=
                  options.allowed_hints_and_options()
                  .disallow_unknown_hints_with_qualifiers.end());
  ASSERT_TRUE(options.allowed_hints_and_options()
              .disallow_unknown_hints_with_qualifiers.find("") !=
                  options.allowed_hints_and_options()
                  .disallow_unknown_hints_with_qualifiers.end());
  ASSERT_TRUE(options.allowed_hints_and_options().disallow_unknown_options);
  ASSERT_TRUE(generated_proto_type->Equals(
      options.allowed_hints_and_options()
          .hints_lower.find(std::make_pair("test_qual", "hint1"))
          ->second));
  ASSERT_TRUE(generated_proto_type->Equals(
      options.allowed_hints_and_options()
          .hints_lower.find(std::make_pair("", "hint1"))
          ->second));
  ASSERT_THAT(options.allowed_hints_and_options()
                  .hints_lower.find(std::make_pair("test_qual", "untyped_hint"))
                  ->second,
              IsNull());
  ASSERT_TRUE(generated_enum_type->Equals(options.allowed_hints_and_options()
              .options_lower.find("option2")->second));
  ASSERT_THAT(options.allowed_hints_and_options()
              .options_lower.find("untyped_option")->second, IsNull());
}

TEST_F(AnalyzerOptionsTest, ClassAndProtoSize) {
  EXPECT_EQ(232, sizeof(AnalyzerOptions) - sizeof(LanguageOptions) -
                     sizeof(AllowedHintsAndOptions) -
                     sizeof(Catalog::FindOptions) - sizeof(SystemVariablesMap) -
                     2 * sizeof(QueryParametersMap) - 1 * sizeof(std::string) -
                     sizeof(absl::btree_set<ResolvedASTRewrite>))
      << "The size of AnalyzerOptions class has changed, please also update "
      << "the proto and serialization code if you added/removed fields in it.";
  EXPECT_EQ(19, AnalyzerOptionsProto::descriptor()->field_count())
      << "The number of fields in AnalyzerOptionsProto has changed, please "
      << "also update the serialization code accordingly.";
}

TEST_F(AnalyzerOptionsTest, AllowedHintsAndOptionsSerializeAndDeserialize) {
  TypeFactory factory;

  const std::vector<std::string> test_files{
      // Order matters for these imports.
      "google/protobuf/descriptor.proto",
      "zetasql/public/proto/type_annotation.proto",
      "zetasql/testdata/test_schema.proto",
      "zetasql/testdata/external_extension.proto",
  };
  std::unique_ptr<google::protobuf::compiler::DiskSourceTree> source_tree =
      CreateProtoSourceTree();
  MultiFileErrorCollector error_collector;

  std::unique_ptr<google::protobuf::compiler::Importer> proto_importer(
      new google::protobuf::compiler::Importer(source_tree.get(), &error_collector));

  for (const std::string& test_file : test_files) {
    ZETASQL_CHECK(proto_importer->Import(test_file) != nullptr)
        << "Error importing " << test_file << ": "
        << error_collector.GetError();
  }
  std::unique_ptr<google::protobuf::DescriptorPool> external_pool(
      new google::protobuf::DescriptorPool(proto_importer->pool()));

  const EnumType* generated_enum_type;
  ZETASQL_CHECK_OK(factory.MakeEnumType(TypeKind_descriptor(), &generated_enum_type));

  const ProtoType* generated_proto_type;
  ZETASQL_CHECK_OK(factory.MakeProtoType(
      TypeProto::descriptor(), &generated_proto_type));

  std::vector<const google::protobuf::DescriptorPool*> pools = {
      google::protobuf::DescriptorPool::generated_pool(), external_pool.get(),
  };

  std::map<const google::protobuf::DescriptorPool*,
  std::unique_ptr<Type::FileDescriptorEntry> > file_descriptor_set_map;
  AllowedHintsAndOptionsProto proto;
  AllowedHintsAndOptions allowed("Qual");
  allowed.AddHint("test_qual", "hint1", generated_proto_type, true);
  allowed.AddHint("test_qual", "hint2", generated_proto_type, false);
  allowed.AddHint("", "hint2", generated_proto_type, true);
  allowed.AddHint("test_qual", "hint3", nullptr, false);
  allowed.AddHint("", "hint3", nullptr, true);
  allowed.AddHint("test_qual", "hint4", generated_enum_type, false);
  allowed.AddHint("", "hint4", nullptr, true);
  allowed.AddHint("test_qual", "hint5", nullptr, false);
  allowed.AddHint("", "hint5", generated_enum_type, true);
  allowed.AddHint("test_qual", "hint6", generated_proto_type, false);
  allowed.AddHint("", "untyped_qual", nullptr, true);
  allowed.AddOption("option1", generated_enum_type);
  allowed.AddOption("untyped_option", nullptr);
  ZETASQL_CHECK_OK(allowed.Serialize(&file_descriptor_set_map, &proto));
  EXPECT_EQ(2, proto.disallow_unknown_hints_with_qualifier_size());
  EXPECT_TRUE(proto.disallow_unknown_options());
  EXPECT_EQ(9, proto.hint_size());
  EXPECT_EQ(2, proto.option_size());
  AllowedHintsAndOptions generated_allowed;
  ZETASQL_CHECK_OK(AllowedHintsAndOptions::Deserialize(
      proto, pools, &factory, &generated_allowed));
  EXPECT_EQ(2, generated_allowed.disallow_unknown_hints_with_qualifiers.size());
  EXPECT_TRUE(generated_allowed.disallow_unknown_options);
  EXPECT_EQ(12, generated_allowed.hints_lower.size());
  EXPECT_EQ(2, generated_allowed.options_lower.size());
}

TEST(AllowedHintsAndOptionsTest, ClassAndProtoSize) {
  EXPECT_EQ(8, sizeof(AllowedHintsAndOptions) -
                   sizeof(std::set<std::string>) -
                   2 * sizeof(absl::flat_hash_map<std::string, std::string>))
      << "The size of AllowedHintsAndOptions class has changed, please also "
      << "update the proto and serialization code if you added/removed fields "
      << "in it.";
  EXPECT_EQ(4, AllowedHintsAndOptionsProto::descriptor()->field_count())
      << "The number of fields in AllowedHintsAndOptionsProto has changed, "
      << "please also update the serialization code accordingly.";
}

// Defines a triple for testing the SupportedStatement feature in ZetaSQL.
struct SupportedStatementTestInput {
  SupportedStatementTestInput(const std::string& stmt, bool success,
                              const std::set<ResolvedNodeKind>& statement_kinds)
      : statement(stmt),
        expect_success(success),
        supported_statement_kinds(statement_kinds) {}
  // The statement to attempt to parse and analyze.
  const std::string statement;
  // The expected outcome of the parse and analyze.
  const bool expect_success;
  // The ResolvedNodeKinds to allow. The empty set is all accepting.
  const std::set<ResolvedNodeKind> supported_statement_kinds;
};

// Test ZetaSQL's ability to reject statements not in the allowlist
// contained in the AnalyzerOptions.
TEST(AnalyzerSupportedStatementsTest, SupportedStatementTest) {
  TypeFactory type_factory;
  std::unique_ptr<const AnalyzerOutput> output;
  const std::string query = "SELECT 1;";
  const std::string explain_query = "EXPLAIN SELECT 1;";
  // The statement is resolved first before looking at the hint.  If we have
  // an invalid hint on an unsupported statement, we'll get the unsupported
  // statement error.
  const std::string hinted_query = "@{key=value} SELECT 1;";
  const std::string bad_hinted_query = "@{key=@xxx} SELECT 1;";

  std::vector<SupportedStatementTestInput> test_input = {
      SupportedStatementTestInput(query, true, {}),
      SupportedStatementTestInput(query, true, {RESOLVED_QUERY_STMT}),
      SupportedStatementTestInput(query, false, {RESOLVED_EXPLAIN_STMT}),
      SupportedStatementTestInput(query, true,
                                  {RESOLVED_QUERY_STMT, RESOLVED_EXPLAIN_STMT}),
      SupportedStatementTestInput(explain_query, true, {}),
      SupportedStatementTestInput(explain_query, false, {RESOLVED_QUERY_STMT}),
      SupportedStatementTestInput(explain_query, false,
                                  {RESOLVED_EXPLAIN_STMT}),
      SupportedStatementTestInput(explain_query, true,
                                  {RESOLVED_QUERY_STMT,
                                   RESOLVED_EXPLAIN_STMT}),
      SupportedStatementTestInput(hinted_query, true, {RESOLVED_QUERY_STMT}),
      SupportedStatementTestInput(hinted_query, false, {RESOLVED_EXPLAIN_STMT}),
      SupportedStatementTestInput(bad_hinted_query, false,
                                  {RESOLVED_EXPLAIN_STMT}),
  };

  for (const SupportedStatementTestInput& input : test_input) {
    AnalyzerOptions options;
    options.mutable_language()->SetSupportedStatementKinds(
        input.supported_statement_kinds);
    SampleCatalog catalog(options.language());
    const absl::Status status = AnalyzeStatement(
        input.statement, options, catalog.catalog(), &type_factory, &output);
    if (input.expect_success) {
      ZETASQL_EXPECT_OK(status);
    } else {
      EXPECT_THAT(status, StatusIs(_, HasSubstr("Statement not supported:")));
    }
  }
}

// Verifies that an unsupported ALTER TABLE statement reports an informative
// error message rather than a syntax error (b/31389932).
TEST(AnalyzerSupportedStatementsTest, AlterTableNotSupported) {
  AnalyzerOptions options;
  options.mutable_language()->SetSupportedStatementKinds({RESOLVED_QUERY_STMT});
  SampleCatalog catalog(options.language());
  TypeFactory type_factory;
  std::unique_ptr<const AnalyzerOutput> output;
  const std::string sql = "ALTER TABLE t ADD COLUMN c INT64";
  auto status = AnalyzeStatement(
      sql, options, catalog.catalog(), &type_factory, &output);
  EXPECT_THAT(status, StatusIs(_, HasSubstr("Statement not supported")));
}

// Defines a test for the LanguageFeatures option in ZetaSQL.  The
// <expected_error_string> is only compared if it is non-empty and
// <expect_success> is false.
struct SupportedFeatureTestInput {
  SupportedFeatureTestInput(const std::string& stmt,
                            const LanguageOptions::LanguageFeatureSet& features,
                            bool expect_success_in,
                            const std::string& error_string = "")
      : statement(stmt),
        supported_features(features),
        expect_success(expect_success_in),
        expected_error_string(error_string) {}

  // The statement to parse and analyze.
  const std::string statement;
  // The LanguageFeatures to allowlist.
  const LanguageOptions::LanguageFeatureSet supported_features;
  // The expected outcome of the parse and analyze.
  const bool expect_success;
  // The expected error string, only relevant if <expect_success> is false.
  const std::string expected_error_string;

  std::string FeaturesToString() const {
    return LanguageOptions::ToString(supported_features);
  }
};

// Test ZetaSQL's ability to reject features not in the allowlist
// contained in the AnalyzerOptions.
TEST(AnalyzerSupportedFeaturesTest, SupportedFeaturesTest) {
  // Simple getter/setter tests.
  AnalyzerOptions options;
  EXPECT_FALSE(
      options.language().LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));
  options.mutable_language()->EnableLanguageFeature(FEATURE_ANALYTIC_FUNCTIONS);
  EXPECT_TRUE(
      options.language().LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));
  options.mutable_language()->DisableAllLanguageFeatures();
  EXPECT_FALSE(
      options.language().LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));

  // Setting an already set feature is not an error.
  options.mutable_language()->EnableLanguageFeature(FEATURE_ANALYTIC_FUNCTIONS);
  EXPECT_TRUE(
      options.language().LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));
  options.mutable_language()->EnableMaximumLanguageFeatures();
  EXPECT_TRUE(
      options.language().LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));

  // Disable all and enable all.
  options.mutable_language()->DisableAllLanguageFeatures();
  EXPECT_FALSE(
      options.language().LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));
  options.mutable_language()->EnableMaximumLanguageFeatures();
  EXPECT_TRUE(
      options.language().LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));

  // Query tests.
  TypeFactory type_factory;
  std::unique_ptr<const AnalyzerOutput> output;
  const std::string aggregate_query = "SELECT sum(key) from KeyValue;";
  const std::string analytic_query = "SELECT sum(key) over () from KeyValue;";

  std::vector<SupportedFeatureTestInput> test_input = {
      SupportedFeatureTestInput(aggregate_query, {}, true /* expect_success */),
      SupportedFeatureTestInput(aggregate_query, {FEATURE_ANALYTIC_FUNCTIONS},
                                true),
      // This tests the allowlist feature capability prior to
      // analysis/resolution being implemented for analytic functions.
      SupportedFeatureTestInput(analytic_query, {}, false,
                                "Analytic functions not supported"),
      SupportedFeatureTestInput(analytic_query, {FEATURE_ANALYTIC_FUNCTIONS},
                                true /* expect_success */),
  };

  for (const SupportedFeatureTestInput& input : test_input) {
    AnalyzerOptions options;
    options.mutable_language()->SetEnabledLanguageFeatures(
        input.supported_features);
    ZETASQL_LOG(INFO) << "Supported features: " << input.FeaturesToString();
    SampleCatalog catalog(options.language());
    const absl::Status status = AnalyzeStatement(
        input.statement, options, catalog.catalog(), &type_factory, &output);
    if (input.expect_success) {
      ZETASQL_EXPECT_OK(status)
          << "Query: " << input.statement
          << "\nSupported features: " << input.FeaturesToString();
      EXPECT_GT(output->max_column_id(), 0);
    } else {
      EXPECT_THAT(status, StatusIs(_, HasSubstr(input.expected_error_string)))
          << "Query: " << input.statement
          << "\nSupported features: " << input.FeaturesToString();
    }
  }
}

TEST(AnalyzerSupportedFeaturesTest, EnableFeaturesForVersion) {
  // Tests below show that SetLanguageVersion sets the features to exactly
  // the features for that version, and clears any others (like
  // FEATURE_ANALYTIC_FUNCTIONS).
  AnalyzerOptions options;
  options.mutable_language()->EnableLanguageFeature(FEATURE_ANALYTIC_FUNCTIONS);
  options.mutable_language()->EnableLanguageFeature(
      FEATURE_V_1_1_ORDER_BY_COLLATE);

  options.mutable_language()->SetLanguageVersion(VERSION_1_0);
  EXPECT_FALSE(
      options.language().LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));
  EXPECT_FALSE(options.language().LanguageFeatureEnabled(
      FEATURE_V_1_1_ORDER_BY_COLLATE));

  options.mutable_language()->EnableLanguageFeature(FEATURE_ANALYTIC_FUNCTIONS);
  options.mutable_language()->SetLanguageVersion(VERSION_1_1);
  EXPECT_FALSE(
      options.language().LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));
  EXPECT_TRUE(options.language().LanguageFeatureEnabled(
      FEATURE_V_1_1_ORDER_BY_COLLATE));

  options.mutable_language()->EnableLanguageFeature(FEATURE_ANALYTIC_FUNCTIONS);
  EXPECT_TRUE(
      options.language().LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));
  EXPECT_TRUE(options.language().LanguageFeatureEnabled(
      FEATURE_V_1_1_ORDER_BY_COLLATE));

  options.mutable_language()->DisableAllLanguageFeatures();
  EXPECT_FALSE(
      options.language().LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));
  EXPECT_FALSE(options.language().LanguageFeatureEnabled(
      FEATURE_V_1_1_ORDER_BY_COLLATE));

  // VERSION_CURRENT clears the features and adds back in the maximal set
  // of versioned features, but not the cross-version features.
  options.mutable_language()->EnableLanguageFeature(FEATURE_ANALYTIC_FUNCTIONS);
  options.mutable_language()->SetLanguageVersion(VERSION_CURRENT);
  EXPECT_FALSE(
      options.language().LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));
  EXPECT_TRUE(options.language().LanguageFeatureEnabled(
      FEATURE_V_1_1_ORDER_BY_COLLATE));
}

// Test resolving a proto extension where the extension comes from
// a different DescriptorPool.  We have to load up a second DescriptorPool
// manually to test this.
TEST(AnalyzerTest, ExternalExtension) {
  const std::vector<std::string> test_files{
      // Order matters for these imports.
      "google/protobuf/descriptor.proto",
      "zetasql/public/proto/type_annotation.proto",
      "zetasql/testdata/test_schema.proto",
      "zetasql/testdata/external_extension.proto",
  };

  std::unique_ptr<google::protobuf::compiler::DiskSourceTree> source_tree =
      CreateProtoSourceTree();

  MultiFileErrorCollector error_collector;
  std::unique_ptr<google::protobuf::compiler::Importer> proto_importer(
      new google::protobuf::compiler::Importer(source_tree.get(), &error_collector));

  for (const std::string& test_file : test_files) {
    ZETASQL_CHECK(proto_importer->Import(test_file) != nullptr)
        << "Error importing " << test_file << ": "
        << error_collector.GetError();
  }
  std::unique_ptr<google::protobuf::DescriptorPool> external_pool(
      new google::protobuf::DescriptorPool(proto_importer->pool()));
  const std::string external_extension_name =
      "zetasql_test__.ExternalExtension";
  const google::protobuf::Descriptor* external_extension =
      external_pool->FindMessageTypeByName(external_extension_name);
  ASSERT_NE(external_extension, nullptr);

  const std::string query1 =
      "SELECT "
      "t.KitchenSink.test_extra_pb.(zetasql_test__.ExternalExtension.field) "
      "as ext_field FROM TestTable t";

  TypeFactory type_factory;
  AnalyzerOptions options;
  SampleCatalog sample_catalog(options.language());
  SimpleCatalog* catalog = sample_catalog.catalog();

  std::unique_ptr<const AnalyzerOutput> output1;

  // Check that the extension type name does not exist in the default catalog.
  const Type* found_type;
  ZETASQL_EXPECT_OK(catalog->FindType({"zetasql_test__.KitchenSinkPB"}, &found_type));
  EXPECT_FALSE(catalog->FindType({external_extension_name}, &found_type).ok());

  // Analyze the queries and we'll get an error that the extension is not found.
  EXPECT_THAT(
      AnalyzeStatement(query1, options, catalog, &type_factory, &output1),
      StatusIs(
          _,
          HasSubstr(
              "Extension zetasql_test__.ExternalExtension.field not found")));

  // Now add the extension type into the Catalog.
  // (Modifying the SampleCatalog is a hack to save code.)
  const ProtoType* extension_type;
  ZETASQL_ASSERT_OK(type_factory.MakeProtoType(external_extension, &extension_type));
  static_cast<SimpleCatalog*>(catalog)->AddType(
      external_extension_name, extension_type);

  ZETASQL_EXPECT_OK(catalog->FindType({external_extension_name}, &found_type));

  // Re-analyze the queries and now it will succeed.
  // This checks that we can create and validate a ResolvedGetProtoField that
  // points at a FieldDescriptor from a different DescriptorPool.
  ZETASQL_ASSERT_OK(AnalyzeStatement(query1, options, catalog, &type_factory,
                             &output1));
}

static void ExpectStatementHasAnonymization(
    const std::string& sql, bool expect_anonymization = true,
    bool expect_analyzer_success = true) {
  AnalyzerOptions options;
  options.mutable_language()->SetSupportedStatementKinds(
      {RESOLVED_QUERY_STMT, RESOLVED_CREATE_TABLE_AS_SELECT_STMT});
  options.mutable_language()->EnableLanguageFeature(FEATURE_ANONYMIZATION);
  options.enable_rewrite(REWRITE_ANONYMIZATION, /*enable=*/false);
  SampleCatalog catalog(options.language());
  TypeFactory type_factory;
  std::unique_ptr<const AnalyzerOutput> output;
  absl::Status status = AnalyzeStatement(
      sql, options, catalog.catalog(), &type_factory, &output);
  if (expect_analyzer_success) {
    ZETASQL_EXPECT_OK(status);
    EXPECT_EQ(
        output->analyzer_output_properties().IsRelevant(REWRITE_ANONYMIZATION),
        expect_anonymization);
  } else {
    // Note that if the analyzer failed, then there is no AnalyzerOutput.
    EXPECT_FALSE(status.ok());
  }
}

TEST(AnalyzerTest, TestStatementHasAnonymization) {
  ExpectStatementHasAnonymization(
      "SELECT * FROM KeyValue", false);
  ExpectStatementHasAnonymization(
      "SELECT WITH ANONYMIZATION key FROM KeyValue GROUP BY key");
  ExpectStatementHasAnonymization("SELECT ANON_COUNT(*) FROM KeyValue");
  ExpectStatementHasAnonymization("SELECT ANON_SUM(key) FROM KeyValue");
  ExpectStatementHasAnonymization("SELECT ANON_AVG(key) FROM KeyValue");
  ExpectStatementHasAnonymization("SELECT ANON_COUNT(value) FROM KeyValue");
  ExpectStatementHasAnonymization(
      "SELECT * FROM (SELECT ANON_COUNT(*) FROM KeyValue)");
  ExpectStatementHasAnonymization(
      "SELECT (SELECT ANON_COUNT(*) FROM KeyValue) FROM KeyValue");
  ExpectStatementHasAnonymization(
      absl::StrCat("SELECT * FROM KeyValue ",
                   "WHERE key IN (SELECT ANON_COUNT(*) FROM KeyValue)"));
  // DDL has anonymization
  ExpectStatementHasAnonymization(
      "CREATE TABLE foo AS SELECT ANON_COUNT(*) AS a FROM KeyValue");

  // Resolution fails, has_anonymization is false even though we found it
  // in the first part of the query (before finding the error).
  ExpectStatementHasAnonymization(
      absl::StrCat("SELECT ANON_COUNT(*) FROM KeyValue ",
                   "UNION ALL ",
                   "SELECT 'string_literal'"), false, false);
}

static void ExpectExpressionHasAnonymization(const std::string& sql,
                                             bool expect_anonymization = true) {
  AnalyzerOptions options;
  options.mutable_language()->EnableLanguageFeature(FEATURE_ANONYMIZATION);
  SampleCatalog catalog(options.language());
  TypeFactory type_factory;
  std::unique_ptr<const AnalyzerOutput> output;
  absl::Status status = AnalyzeExpression(
      sql, options, catalog.catalog(), &type_factory, &output);
  ZETASQL_ASSERT_OK(status);
  EXPECT_EQ(
      output->analyzer_output_properties().IsRelevant(REWRITE_ANONYMIZATION),
      expect_anonymization);
}

TEST(AnalyzerTest, TestExpressionHasAnonymization) {
  // Note that AnalyzeExpression only works for scalar expressions, not
  // aggregate expressions.
  ExpectExpressionHasAnonymization("concat('a', 'b')", false);
  ExpectExpressionHasAnonymization("5 IN (SELECT ANON_COUNT(*) FROM KeyValue)");
}

TEST(AnalyzerTest, AstRewriting) {
  AnalyzerOptions options;
  options.mutable_language()->EnableLanguageFeature(
      FEATURE_V_1_3_UNNEST_AND_FLATTEN_ARRAYS);
  SampleCatalog catalog(options.language());
  TypeFactory type_factory;
  std::unique_ptr<const AnalyzerOutput> output;

  for (const bool should_rewrite : { true, false }) {
    options.enable_rewrite(REWRITE_FLATTEN, should_rewrite);
    ZETASQL_ASSERT_OK(AnalyzeStatement("SELECT FLATTEN([STRUCT([] AS X)].X)", options,
                               catalog.catalog(), &type_factory, &output));
    if (should_rewrite) {
      // If rewrites are enabled the Flatten should be rewritten away.
      EXPECT_THAT(output->resolved_statement()->DebugString(),
                  Not(HasSubstr("FlattenedArg")));
    } else {
      // Otherwise it should remain.
      EXPECT_THAT(output->resolved_statement()->DebugString(),
                  HasSubstr("FlattenedArg"));
    }
  }
}

// Test that the language_options setters and getters on AnalyzerOptions work
// correctly and don't overwrite the options outside LanguageOptions.
TEST(AnalyzerTest, LanguageOptions) {
  AnalyzerOptions analyzer_options;
  EXPECT_FALSE(
      analyzer_options.language().SupportsStatementKind(RESOLVED_DELETE_STMT));
  EXPECT_EQ(ErrorMessageMode::ERROR_MESSAGE_ONE_LINE,
            analyzer_options.error_message_mode());
  analyzer_options.set_error_message_mode(
      ErrorMessageMode::ERROR_MESSAGE_WITH_PAYLOAD);
  EXPECT_EQ(ErrorMessageMode::ERROR_MESSAGE_WITH_PAYLOAD,
            analyzer_options.error_message_mode());

  LanguageOptions language_options;
  EXPECT_FALSE(language_options.SupportsStatementKind(RESOLVED_DELETE_STMT));
  language_options.SetSupportsAllStatementKinds();
  EXPECT_TRUE(language_options.SupportsStatementKind(RESOLVED_DELETE_STMT));

  analyzer_options.set_language(language_options);
  EXPECT_TRUE(
      analyzer_options.language().SupportsStatementKind(RESOLVED_DELETE_STMT));
  EXPECT_TRUE(
      analyzer_options.language().SupportsStatementKind(RESOLVED_DELETE_STMT));
  EXPECT_EQ(ErrorMessageMode::ERROR_MESSAGE_WITH_PAYLOAD,
            analyzer_options.error_message_mode());

  EXPECT_FALSE(analyzer_options.language().LanguageFeatureEnabled(
      FEATURE_V_1_1_ORDER_BY_COLLATE));
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_V_1_1_ORDER_BY_COLLATE);
  EXPECT_TRUE(analyzer_options.language().LanguageFeatureEnabled(
      FEATURE_V_1_1_ORDER_BY_COLLATE));

  const AnalyzerOptions analyzer_options2(language_options);
  EXPECT_TRUE(
      analyzer_options2.language().SupportsStatementKind(RESOLVED_DELETE_STMT));
  EXPECT_EQ(ErrorMessageMode::ERROR_MESSAGE_ONE_LINE,
            analyzer_options2.error_message_mode());
}

TEST(SQLBuilderTest, Int32ParameterForLimit) {
  auto cast_limit = MakeResolvedCast(types::Int64Type(),
                                     MakeResolvedLiteral(values::Int32(2)),
                                     /*return_null_on_error=*/false);
  auto cast_offset = MakeResolvedCast(types::Int64Type(),
                                      MakeResolvedLiteral(values::Int32(1)),
                                      /*return_null_on_error=*/false);

  auto limit_offset_scan = MakeResolvedLimitOffsetScan(
      /*column_list=*/{}, MakeResolvedSingleRowScan(), std::move(cast_limit),
      std::move(cast_offset));
  SQLBuilder sql_builder;
  ZETASQL_ASSERT_OK(sql_builder.Process(*limit_offset_scan));
  std::string formatted_sql;
  ZETASQL_ASSERT_OK(FormatSql(sql_builder.sql(), &formatted_sql));
  EXPECT_EQ("SELECT\n  1\nLIMIT CAST(2 AS INT32) OFFSET CAST(1 AS INT32);",
            formatted_sql);
}

// Adding specific unit test to input provided by Random Query Generator tree.
// From a SQL String (like in golden file sql_builder.test), we get a different
// tree (FilterScan is under other ResolvedScans and this scenario isn't tested.
TEST(SQLBuilderTest, WithScanWithFilterScan) {
  const std::string with_query_name = "WithTable";
  const std::string table_name = "T1";
  const std::string col_name = "C";

  TypeFactory type_factory;
  auto table = absl::make_unique<SimpleTable>(table_name);

  // With entry list.
  const ResolvedColumn scan_column(
      10, zetasql::IdString::MakeGlobal(table_name),
      zetasql::IdString::MakeGlobal(col_name), type_factory.get_int32());
  auto table_scan = MakeResolvedTableScan({scan_column}, table.get(),
                                          /*for_system_time_expr=*/nullptr);
  auto filter_scan = MakeResolvedFilterScan(
      {scan_column}, std::move(table_scan),
      MakeResolvedLiteral(type_factory.get_bool(), Value::Bool(true)));
  std::vector<std::unique_ptr<const ResolvedWithEntry>> with_entry_list;
  with_entry_list.emplace_back(
      MakeResolvedWithEntry(with_query_name, std::move(filter_scan)));

  // With scan query.
  const ResolvedColumn query_column(
      20, zetasql::IdString::MakeGlobal(with_query_name),
      zetasql::IdString::MakeGlobal(col_name), type_factory.get_int32());
  auto with_ref_scan = MakeResolvedWithRefScan({query_column}, with_query_name);
  auto query = MakeResolvedProjectScan({query_column}, /*expr_list=*/{},
                                       std::move(with_ref_scan));
  auto with_scan =
      MakeResolvedWithScan({scan_column}, std::move(with_entry_list),
                           std::move(query), /*recursive=*/false);

  // Test that this scan is correctly supported in WITH clause.
  SQLBuilder sql_builder;
  ZETASQL_ASSERT_OK(sql_builder.Process(*with_scan));
  std::string formatted_sql;
  ZETASQL_ASSERT_OK(FormatSql(sql_builder.sql(), &formatted_sql));
  EXPECT_EQ(
      "WITH\n  WithTable AS (\n"
      "    SELECT\n      T1.C AS a_1\n    FROM\n      T1\n"
      "    WHERE\n      true\n  )\n"
      "SELECT\n  withrefscan_2.a_1 AS a_1\nFROM\n  WithTable AS withrefscan_2;",
      formatted_sql);
}

// Test that SqlBuilder prefer ResolvedTableScan.column_index_list over column
// and table names in ResolvedTableScan, which should have no semantic meaning.
// See the class comment on `ResolvedTableScan`.
TEST(SQLBuilderTest, TableScanPrefersColumnIndexList) {
  const std::string table_name = "T1";
  const std::string col_name = "C";
  const std::string unused_name = "UNUSED_NAME";
  const int column_id = 9;

  TypeFactory type_factory;
  auto table = absl::make_unique<SimpleTable>(table_name);
  ZETASQL_ASSERT_OK(table->AddColumn(
      new SimpleColumn(table_name, col_name, type_factory.get_int32()),
      /*is_owned=*/true));
  const ResolvedColumn scan_column(column_id, IdString::MakeGlobal(unused_name),
                                   IdString::MakeGlobal(unused_name),
                                   type_factory.get_int32());
  auto table_scan = MakeResolvedTableScan({scan_column}, table.get(),
                                          /*for_system_time_expr=*/nullptr);
  table_scan->set_column_index_list({0});
  const ResolvedColumn query_column(
      column_id, IdString::MakeGlobal(unused_name),
      IdString::MakeGlobal(unused_name), type_factory.get_int32());
  auto query = MakeResolvedProjectScan({query_column}, /*expr_list=*/{},
                                       std::move(table_scan));

  SQLBuilder sql_builder;
  ZETASQL_ASSERT_OK(sql_builder.Process(*query));
  std::string formatted_sql;
  ZETASQL_ASSERT_OK(FormatSql(sql_builder.sql(), &formatted_sql));
  EXPECT_EQ(
      "SELECT\n  t1_2.a_1 AS a_1\nFROM\n  (\n"
      "    SELECT\n      T1.C AS a_1\n    FROM\n      T1\n  ) AS t1_2;",
      formatted_sql);
}

// Adding specific unit test to input provided by Random Query Generator tree.
// From a SQL String (like in golden file sql_builder.test), we get a different
// tree (JoinScan is under other ResolvedScans and this scenario isn't tested.
TEST(SQLBuilderTest, WithScanWithJoinScan) {
  const std::string with_query_name = "WithTable";
  const std::string table_name1 = "T1";
  const std::string table_name2 = "T2";
  const std::string col_name = "C";

  TypeFactory type_factory;
  auto table1 = absl::make_unique<SimpleTable>(table_name1);
  auto table2 = absl::make_unique<SimpleTable>(table_name2);

  // With entry list.
  const ResolvedColumn scan_column(
      10, zetasql::IdString::MakeGlobal(table_name1),
      zetasql::IdString::MakeGlobal(col_name), type_factory.get_int32());
  auto table_scan1 = MakeResolvedTableScan({scan_column}, table1.get(),
                                           /*for_system_time_expr=*/nullptr);
  auto table_scan2 = MakeResolvedTableScan(/*column_list=*/{}, table2.get(),
                                           /*for_system_time_expr=*/nullptr);
  auto join_scan =
      MakeResolvedJoinScan({scan_column}, ResolvedJoinScan::INNER,
                           std::move(table_scan1), std::move(table_scan2),
                           /*join_expr=*/nullptr);
  std::vector<std::unique_ptr<const ResolvedWithEntry>> with_entry_list;
  with_entry_list.emplace_back(
      MakeResolvedWithEntry(with_query_name, std::move(join_scan)));

  // With scan query.
  const ResolvedColumn query_column(
      20, zetasql::IdString::MakeGlobal(with_query_name),
      zetasql::IdString::MakeGlobal(col_name), type_factory.get_int32());
  std::unique_ptr<ResolvedWithRefScan> with_ref_scan =
      MakeResolvedWithRefScan({query_column}, with_query_name);

  auto query = MakeResolvedProjectScan({query_column}, /*expr_list=*/{},
                                       std::move(with_ref_scan));
  auto with_scan =
      MakeResolvedWithScan({scan_column}, std::move(with_entry_list),
                           std::move(query), /*recursive=*/false);

  // Test that this scan is correctly supported in WITH clause.
  SQLBuilder sql_builder;
  ZETASQL_ASSERT_OK(sql_builder.Process(*with_scan));
  std::string formatted_sql;
  ZETASQL_ASSERT_OK(FormatSql(sql_builder.sql(), &formatted_sql));
  EXPECT_EQ(
      "WITH\n  WithTable AS (\n"
      "    SELECT\n      t1_2.a_1 AS a_1\n    FROM\n"
      "      (\n        SELECT\n          T1.C AS a_1\n        FROM\n"
      "          T1\n      ) AS t1_2\n"
      "      CROSS JOIN\n"
      "      (\n        SELECT\n          NULL\n        FROM\n"
      "          T2\n      ) AS t2_3\n"
      "  )\n"
      "SELECT\n  withrefscan_4.a_1 AS a_1\nFROM\n  WithTable AS withrefscan_4;",
      formatted_sql);
}

// Adding specific unit test to input provided by Random Query Generator tree.
// From a SQL String (like in golden file sql_builder.test), we get a different
// tree (ArrayScan is under other ResolvedScans and this scenario isn't tested.
TEST(SQLBuilderTest, WithScanWithArrayScan) {
  const std::string with_query_name = "WithTable";
  const std::string table_name = "T1";
  const std::string col_name = "C";

  TypeFactory type_factory;
  auto table = absl::make_unique<SimpleTable>(table_name);

  // With entry list.
  const ResolvedColumn scan_column(
      10, zetasql::IdString::MakeGlobal(table_name),
      zetasql::IdString::MakeGlobal(col_name), type_factory.get_int32());
  auto table_scan = MakeResolvedTableScan({scan_column}, table.get(),
                                          /*for_system_time_expr=*/nullptr);
  const ResolvedColumn array_column(
      15, zetasql::IdString::MakeGlobal("ArrayName"),
      zetasql::IdString::MakeGlobal("ArrayCol"), type_factory.get_int32());
  auto array_expr = MakeResolvedSubqueryExpr(
      type_factory.get_bool(), ResolvedSubqueryExpr::ARRAY,
      /*parameter_list=*/{}, /*in_expr=*/nullptr,
      /*subquery=*/MakeResolvedSingleRowScan());
  auto array_scan = MakeResolvedArrayScan(
      {scan_column}, std::move(table_scan), std::move(array_expr), array_column,
      /*array_offset_column=*/nullptr,
      /*join_expr=*/nullptr, /*is_outer=*/true);
  std::vector<std::unique_ptr<const ResolvedWithEntry>> with_entry_list;
  with_entry_list.emplace_back(
      MakeResolvedWithEntry(with_query_name, std::move(array_scan)));

  // With scan query.
  const ResolvedColumn query_column(
      20, zetasql::IdString::MakeGlobal(with_query_name),
      zetasql::IdString::MakeGlobal(col_name), type_factory.get_int32());
  std::unique_ptr<ResolvedWithRefScan> with_ref_scan =
      MakeResolvedWithRefScan({query_column}, with_query_name);
  auto query = MakeResolvedProjectScan({query_column}, /*expr_list=*/{},
                                       std::move(with_ref_scan));
  auto with_scan =
      MakeResolvedWithScan({scan_column}, std::move(with_entry_list),
                           std::move(query), /*recursive=*/false);

  // Test that this scan is correctly supported in WITH clause.
  SQLBuilder sql_builder;
  ZETASQL_ASSERT_OK(sql_builder.Process(*with_scan));
  std::string formatted_sql;
  ZETASQL_ASSERT_OK(FormatSql(sql_builder.sql(), &formatted_sql));
  EXPECT_EQ(
      "WITH\n  WithTable AS (\n"
      "    SELECT\n      t1_2.a_1 AS a_1\n    FROM\n"
      "      (\n        SELECT\n          T1.C AS a_1\n        FROM\n"
      "          T1\n      ) AS t1_2\n"
      "      LEFT JOIN\n"
      "      UNNEST(ARRAY(\n"
      "        SELECT\n          1\n      )) AS a_3\n"
      "  )\n"
      "SELECT\n  withrefscan_4.a_1 AS a_1\nFROM\n  WithTable AS withrefscan_4;",
      formatted_sql);
}

TEST(AnalyzerTest, AnalyzeStatementsOfScript) {
  SampleCatalog catalog;
  std::string script = "SELECT * FROM KeyValue;\nSELECT garbage;";
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_ASSERT_OK(ParseScript(script, ParserOptions(), ERROR_MESSAGE_ONE_LINE,
                        &parser_output));
  ASSERT_EQ(parser_output->script()->statement_list().size(), 2);

  AnalyzerOptions analyzer_options;
  std::unique_ptr<const AnalyzerOutput> analyzer_output;

  // Analyze first statement in script - this should succeed.
  ZETASQL_ASSERT_OK(AnalyzeStatementFromParserAST(
      *parser_output->script()->statement_list()[0], analyzer_options, script,
      catalog.catalog(), catalog.type_factory(), &analyzer_output));
  ASSERT_EQ(analyzer_output->resolved_statement()->node_kind(),
            RESOLVED_QUERY_STMT);

  // Analyze second statement in script - this should fail, and the error
  // message should be relative to the entire script, not just the particular
  // statement being analyzed.
  absl::Status status = AnalyzeStatementFromParserAST(
      *parser_output->script()->statement_list()[1], analyzer_options, script,
      catalog.catalog(), catalog.type_factory(), &analyzer_output);
  ASSERT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               "Unrecognized name: garbage [at 2:8]"));
}

}  // namespace zetasql
