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

#include "zetasql/common/lazy_resolution_catalog.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/parsed_templated_sql_function.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/builtin_function.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/cycle_detector.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/module_details.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/sql_constant.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/sql_view.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/testdata/sample_catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

using zetasql::internal::StatusToString;
using testing::ElementsAre;
using testing::HasSubstr;
using testing::IsNull;
using testing::IsTrue;
using testing::NotNull;
using testing::StrEq;
using zetasql_base::testing::IsOk;
using zetasql_base::testing::StatusIs;

static ParseResumeLocation ComputeExpressionResumeLocation(
    absl::string_view module_filename, const absl::string_view sql,
    const ASTNode* sql_body) {
  absl::string_view trimmed_sql = sql;
  if (sql_body != nullptr) {
    const int expression_end_byte_offset =
        sql_body->location().end().GetByteOffset();
    trimmed_sql = trimmed_sql.substr(0, expression_end_byte_offset);
  }
  ParseResumeLocation expression_resume_location =
      ParseResumeLocation::FromStringView(module_filename, trimmed_sql);
  if (sql_body != nullptr) {
    const int expression_start_byte_offset =
        sql_body->location().start().GetByteOffset();
    expression_resume_location.set_byte_position(expression_start_byte_offset);
  }
  return expression_resume_location;
}

class LazyResolutionCatalogTest : public ::testing::Test {
 public:
  LazyResolutionCatalogTest() {
    // We must enable CREATE FUNCTION statements to run these tests.
    analyzer_options_.mutable_language()->SetSupportsAllStatementKinds();
    analyzer_options_.mutable_language()->EnableMaximumLanguageFeatures();
    // This makes it easier to verify that the error locations indicated
    // in the message line up with the statements in the module string/file.
    analyzer_options_.set_error_message_mode(
        ERROR_MESSAGE_MULTI_LINE_WITH_CARET);
    analyzer_options_.set_statement_context(CONTEXT_MODULE);

    absl::flat_hash_map<std::string, std::unique_ptr<Function>> functions;
    absl::flat_hash_map<std::string, const Type*> types_ignored;
    absl::Status status = GetBuiltinFunctionsAndTypes(
        BuiltinFunctionOptions(analyzer_options_.language()), type_factory_,
        functions, types_ignored);
    ZETASQL_DCHECK_OK(status);
    builtin_function_catalog_ = std::make_unique<SimpleCatalog>(
        "builtin_function_catalog", &type_factory_);
    for (auto& [name, function] : functions) {
      builtin_function_catalog_->AddOwnedFunction(name, std::move(function));
    }
    ZETASQL_CHECK_OK(LazyResolutionCatalog::Create(
        "source_filename", *ModuleDetails::Create("lazy catalog", {}),
        analyzer_options_, &type_factory_, &lazy_resolution_catalog_));
    ZETASQL_CHECK_OK(lazy_resolution_catalog_->AppendResolutionCatalog(
        builtin_function_catalog_.get()));
    find_options_.set_cycle_detector(&cycle_detector_);
  }

  LazyResolutionCatalogTest(const LazyResolutionCatalogTest&) = delete;
  LazyResolutionCatalogTest& operator=(const LazyResolutionCatalogTest&) =
      delete;

  LazyResolutionCatalog* catalog() { return lazy_resolution_catalog_.get(); }

  const AnalyzerOptions& analyzer_options() const { return analyzer_options_; }

  const ParserOptions& parser_options() const { return parser_options_; }

  const Catalog::FindOptions& find_options() { return find_options_; }

  // Parses <statement> and returns parser status.  If successful, sets
  // <parser_output>.
  // Returns an error if <statement> does not parse successfully.
  absl::Status ParseTestStatement(
      absl::string_view statement,
      std::unique_ptr<ParserOutput>* parser_output) {
    return ParseStatement(statement, parser_options_, parser_output);
  }

 private:
  // Analyzer options to use for the test.
  AnalyzerOptions analyzer_options_;

  // Parser options to use for the test.
  ParserOptions parser_options_;

  // FindOptions to use during FindFunction() calls.  This FindOptions
  // includes a CycleDetector.
  Catalog::FindOptions find_options_;

  std::unique_ptr<LazyResolutionCatalog> lazy_resolution_catalog_;

  // TypeFactory to use for the test.
  TypeFactory type_factory_;

  // CycleDetector to use for the test.
  CycleDetector cycle_detector_;

  // A Catalog for builtin functions that can be referenced by module
  // statements.
  std::unique_ptr<SimpleCatalog> builtin_function_catalog_;
};

TEST_F(LazyResolutionCatalogTest, basic_tests) {
  std::unique_ptr<LazyResolutionFunction> function;
  std::unique_ptr<ParserOutput> parser_output;
  const std::string filename = "test_filename";
  {
    // Basic successful case, for CREATE FUNCTION statement.
    const std::string statement(
        "create public function foo(a INT64) as (a+1);");
    ZETASQL_ASSERT_OK(ParseTestStatement(statement, &parser_output));
    ParseResumeLocation parse_resume_location =
        ParseResumeLocation::FromStringView(filename, statement);
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        function,
        LazyResolutionFunction::Create(
            parse_resume_location, std::move(parser_output),
            /* function_status=*/absl::OkStatus(),
            ErrorMessageOptions{
                .mode = ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET},
            Function::SCALAR, ModuleDetails::CreateEmpty()));
    ZETASQL_EXPECT_OK(catalog()->AddLazyResolutionFunction(std::move(function)));
  }
  {
    // Failure case, for CREATE FUNCTION statement where we have a multi-part
    // function name.
    const std::string statement(
        "create public function foo.bar(a INT64) as (a+1);");
    ZETASQL_ASSERT_OK(ParseTestStatement(statement, &parser_output));
    ParseResumeLocation parse_resume_location =
        ParseResumeLocation::FromStringView(filename, statement);
    EXPECT_THAT(
        LazyResolutionFunction::Create(
            parse_resume_location, std::move(parser_output),
            /* function_status=*/absl::OkStatus(),
            ErrorMessageOptions{
                .mode = ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET},
            Function::SCALAR, ModuleDetails::CreateEmpty()),
        StatusIs(absl::StatusCode::kInternal, HasSubstr("RET_CHECK")));
  }

  {
    // Failure case, where we try to create a LazyResolutionFunction with an
    // invalid AST type.
    const std::string statement("select * from foo");
    ZETASQL_ASSERT_OK(ParseTestStatement(statement, &parser_output));
    ParseResumeLocation parse_resume_location =
        ParseResumeLocation::FromStringView(filename, statement);
    EXPECT_THAT(
        LazyResolutionFunction::Create(
            parse_resume_location, std::move(parser_output),
            /* function_status=*/absl::OkStatus(),
            ErrorMessageOptions{
                .mode = ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET},
            Function::SCALAR, ModuleDetails::CreateEmpty()),
        StatusIs(absl::StatusCode::kInternal, HasSubstr("RET_CHECK")));
  }
}

TEST_F(LazyResolutionCatalogTest, multi_statement_test) {
  const std::string filename = "test_filename";
  const std::string create_function_statements(
      "create public function foo(a INT64) as (a+1);"
      "create public function bar(b INT32) as (b+1);"
      "create public function baz(c UINT32, d UINT32) as (c+d)");

  ParseResumeLocation parse_resume_location =
      ParseResumeLocation::FromStringView(filename, create_function_statements);

  bool is_end_of_input = false;
  while (!is_end_of_input) {
    std::unique_ptr<ParserOutput> parser_output;
    ParseResumeLocation this_parse_resume_location(parse_resume_location);
    ZETASQL_ASSERT_OK(ParseNextStatement(&parse_resume_location, parser_options(),
                                 &parser_output, &is_end_of_input));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<LazyResolutionFunction> function,
        LazyResolutionFunction::Create(
            this_parse_resume_location, std::move(parser_output),
            /* function_status=*/absl::OkStatus(),
            ErrorMessageOptions{
                .mode = ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET},
            Function::SCALAR, ModuleDetails::CreateEmpty()));
    ZETASQL_ASSERT_OK(catalog()->AddLazyResolutionFunction(std::move(function)));
  }

  const Function* function;
  ZETASQL_ASSERT_OK(catalog()->FindFunction({"foo"}, &function, find_options()));
  EXPECT_EQ("Lazy_resolution_function:foo\n  (INT64 a) -> INT64",
            function->DebugString(/*verbose=*/true));
  ASSERT_EQ("Lazy_resolution_function", function->GetGroup());
  ASSERT_TRUE(function->Is<SQLFunction>());
  const SQLFunction* sql_function = function->GetAs<SQLFunction>();
  EXPECT_EQ(R"(Lazy_resolution_function:foo
  (INT64 a) -> INT64
argument names (a)
FunctionCall(ZetaSQL:$add(INT64, INT64) -> INT64)
+-ArgumentRef(type=INT64, name="a")
+-Literal(type=INT64, value=1)
)",
            sql_function->FullDebugString());

  // Look up the same function name, get the same Function.
  const Function* same_function;
  ZETASQL_ASSERT_OK(catalog()->FindFunction({"foo"}, &same_function, find_options()));
  EXPECT_EQ(function, same_function);

  // The caller should never try to look up a function without any name.
  EXPECT_THAT(catalog()->FindFunction({}, &function, find_options()),
              StatusIs(absl::StatusCode::kInternal));

  // Looking up a function with a multipart name is currently unsupported.
  EXPECT_THAT(catalog()->FindFunction({"a", "b"}, &function, find_options()),
              StatusIs(absl::StatusCode::kNotFound));

  ABSL_LOG(INFO) << catalog()->ObjectsDebugString();

  ZETASQL_ASSERT_OK(catalog()->FindFunction({"baz"}, &function, find_options()));
  ASSERT_TRUE(function->Is<SQLFunction>());
  sql_function = function->GetAs<SQLFunction>();
  EXPECT_EQ(R"(FunctionCall(ZetaSQL:$add(UINT64, UINT64) -> UINT64)
+-Cast(UINT32 -> UINT64)
| +-ArgumentRef(type=UINT32, name="c")
+-Cast(UINT32 -> UINT64)
  +-ArgumentRef(type=UINT32, name="d")
)",
            sql_function->FunctionExpression()->DebugString());
  EXPECT_EQ("c,d", absl::StrJoin(sql_function->GetArgumentNames(), ","));

  const SQLFunctionInterface* sql_function_interface = nullptr;
  ASSERT_TRUE(function->Is<SQLFunctionInterface>());
  sql_function_interface = function->GetAs<const SQLFunctionInterface>();
  EXPECT_EQ(R"(FunctionCall(ZetaSQL:$add(UINT64, UINT64) -> UINT64)
+-Cast(UINT32 -> UINT64)
| +-ArgumentRef(type=UINT32, name="c")
+-Cast(UINT32 -> UINT64)
  +-ArgumentRef(type=UINT32, name="d")
)",
            sql_function_interface->FunctionExpression()->DebugString());
  EXPECT_EQ("c,d",
            absl::StrJoin(sql_function_interface->GetArgumentNames(), ","));
}

TEST_F(LazyResolutionCatalogTest, parsed_templated_sql_function_test) {
  const std::string filename = "test_filename";
  const std::string create_function_statements(
      "create public function foo(x any type) as (x);");

  ParseResumeLocation parse_resume_location =
      ParseResumeLocation::FromStringView(filename, create_function_statements);

  bool is_end_of_input = false;
  while (!is_end_of_input) {
    std::unique_ptr<ParserOutput> parser_output;
    ParseResumeLocation this_parse_resume_location(parse_resume_location);
    ZETASQL_ASSERT_OK(ParseNextStatement(&parse_resume_location, parser_options(),
                                 &parser_output, &is_end_of_input));
    const ASTCreateFunctionStatement* create_function_ast =
        parser_output->statement()->GetAs<const ASTCreateFunctionStatement>();
    ParseResumeLocation templated_expression_resume_location =
        ComputeExpressionResumeLocation(
            this_parse_resume_location.filename(),
            this_parse_resume_location.input(),
            create_function_ast->sql_function_body());
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<LazyResolutionFunction> function,
        LazyResolutionFunction::CreateTemplatedFunction(
            this_parse_resume_location, templated_expression_resume_location,
            std::move(parser_output),
            /* function_status=*/absl::OkStatus(),
            ErrorMessageOptions{
                .mode = ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET},
            Function::SCALAR));
    ZETASQL_ASSERT_OK(catalog()->AddLazyResolutionFunction(std::move(function)));
  }

  const Function* function;
  ZETASQL_ASSERT_OK(catalog()->FindFunction({"foo"}, &function, find_options()));
  ASSERT_THAT(function->Is<ParsedTemplatedSQLFunction>(), IsTrue());
  const auto* parsed_function = function->GetAs<ParsedTemplatedSQLFunction>();
  const ParserOutput* parser_output = parsed_function->parser_output();
  ASSERT_THAT(parser_output, NotNull());
  ASSERT_THAT(
      parser_output->statement()->GetAs<const ASTCreateFunctionStatement>(),
      NotNull());
}

TEST_F(LazyResolutionCatalogTest, duplicate_object_name_test) {
  const std::string filename = "test_filename";
  const std::string create_function_statements(
      "create public function foo(a INT64) as (a);"
      "create public function foo(a INT64) as (a+1);"
      "create private table function bar(a INT64) as (select a);"
      "create public table function bar(a INT64) as (select a+1);"
      "create public table function bar(a INT64) as (select a+1);"
      "create private constant baz = 1;"
      "create public constant baz = 2;"
      "create private constant baz = 3;"
      "create public view qux as (select 1);"
      "create private view qux as (select 2);");

  ParseResumeLocation parse_resume_location =
      ParseResumeLocation::FromStringView(filename, create_function_statements);

  bool is_end_of_input = false;
  while (!is_end_of_input) {
    std::unique_ptr<ParserOutput> parser_output;
    ParseResumeLocation this_parse_resume_location(parse_resume_location);
    ZETASQL_ASSERT_OK(ParseNextStatement(&parse_resume_location, parser_options(),
                                 &parser_output, &is_end_of_input));
    switch (parser_output->statement()->node_kind()) {
      case (AST_CREATE_FUNCTION_STATEMENT): {
        ZETASQL_ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<LazyResolutionFunction> lazy_resolution_function,
            LazyResolutionFunction::Create(
                this_parse_resume_location, std::move(parser_output),
                /*function_status=*/absl::OkStatus(),
                ErrorMessageOptions{
                    .mode =
                        ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET},
                Function::SCALAR, ModuleDetails::CreateEmpty()));
        if (catalog()->ContainsFunction(lazy_resolution_function->Name())) {
          // The Function already exists, so re-inserting it fails.
          ASSERT_FALSE(catalog()
                           ->AddLazyResolutionFunction(
                               std::move(lazy_resolution_function))
                           .ok());
        } else {
          ZETASQL_ASSERT_OK(catalog()->AddLazyResolutionFunction(
              std::move(lazy_resolution_function)));
        }
        break;
      }
      case (AST_CREATE_TABLE_FUNCTION_STATEMENT): {
        ZETASQL_ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<LazyResolutionTableFunction>
                lazy_resolution_table_function,
            LazyResolutionTableFunction::Create(
                this_parse_resume_location, std::move(parser_output),
                /*function_status=*/absl::OkStatus(),
                ErrorMessageOptions{
                    .mode =
                        ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET},
                /*remote_tvf_factory=*/nullptr, ModuleDetails::CreateEmpty()));
        if (catalog()->ContainsTableFunction(
                lazy_resolution_table_function->Name())) {
          // The TableValuedFunction already exists, so re-inserting it fails.
          ASSERT_FALSE(catalog()
                           ->AddLazyResolutionTableFunction(
                               std::move(lazy_resolution_table_function))
                           .ok());
        } else {
          ZETASQL_ASSERT_OK(catalog()->AddLazyResolutionTableFunction(
              std::move(lazy_resolution_table_function)));
        }
        break;
      }
      case (AST_CREATE_CONSTANT_STATEMENT): {
        ZETASQL_ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<LazyResolutionConstant> lazy_resolution_constant,
            LazyResolutionConstant::Create(
                this_parse_resume_location, std::move(parser_output),
                /*constant_status=*/absl::OkStatus(),
                ErrorMessageOptions{.mode = ErrorMessageMode::
                                        ERROR_MESSAGE_MULTI_LINE_WITH_CARET}));
        if (catalog()->ContainsConstant(lazy_resolution_constant->Name())) {
          // The Constant already exists, so re-inserting it fails.
          ASSERT_FALSE(catalog()
                           ->AddLazyResolutionConstant(
                               std::move(lazy_resolution_constant))
                           .ok());
        } else {
          ZETASQL_ASSERT_OK(catalog()->AddLazyResolutionConstant(
              std::move(lazy_resolution_constant)));
        }
        break;
      }
      case (AST_CREATE_VIEW_STATEMENT): {
        ZETASQL_ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<LazyResolutionView> lazy_resolution_view,
            LazyResolutionView::Create(
                this_parse_resume_location, std::move(parser_output),
                /*view_status=*/absl::OkStatus(),
                ErrorMessageOptions{.mode = ErrorMessageMode::
                                        ERROR_MESSAGE_MULTI_LINE_WITH_CARET}));
        if (catalog()->ContainsView(lazy_resolution_view->Name())) {
          // The view already exists, so re-inserting it fails.
          ASSERT_FALSE(
              catalog()
                  ->AddLazyResolutionView(std::move(lazy_resolution_view))
                  .ok());
        } else {
          ZETASQL_ASSERT_OK(catalog()->AddLazyResolutionView(
              std::move(lazy_resolution_view)));
        }
        break;
      }
      default:
        FAIL() << "Unsupported node kind";
    }
  }
}

class LazyResolutionTableFunctionTest : public ::testing::Test {
 public:
  LazyResolutionTableFunctionTest() {
    // We must enable CREATE TABLE FUNCTION statements to run these tests.
    analyzer_options_.mutable_language()->SetSupportsAllStatementKinds();
    analyzer_options_.mutable_language()->EnableMaximumLanguageFeatures();
    LanguageOptions::LanguageFeatureSet features =
        analyzer_options_.language().GetEnabledLanguageFeatures();
    analyzer_options_.mutable_language()->SetEnabledLanguageFeatures(features);
    // This makes it easier to verify that the error locations indicated
    // in the message line up with the statements in the module string/file.
    analyzer_options_.set_error_message_mode(
        ERROR_MESSAGE_MULTI_LINE_WITH_CARET);
    analyzer_options_.set_statement_context(CONTEXT_MODULE);

    sample_catalog_ =
        std::make_unique<SampleCatalog>(analyzer_options_.language());
  }

  absl::StatusOr<std::unique_ptr<LazyResolutionTableFunction>>
  CreateAndInitializeTableFunction(absl::string_view sql) {
    const std::string filename = "test_filename";
    std::unique_ptr<ParserOutput> parser_output;
    const absl::Status parse_status =
        ParseStatement(sql, parser_options_, &parser_output);
    return LazyResolutionTableFunction::Create(
        ParseResumeLocation::FromStringView(filename, sql),
        std::move(parser_output), parse_status,
        ErrorMessageOptions{
            .mode = ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET},
        /*remote_tvf_factory=*/nullptr, ModuleDetails::CreateEmpty());
  }

 protected:
  // Analyzer options to use for the test.
  AnalyzerOptions analyzer_options_;

  // Parser options to use for the test.
  ParserOptions parser_options_;

  // TypeFactory to use for the test.
  TypeFactory type_factory_;

  // We use a SampleCatalog to have access to non-trivial types.
  std::unique_ptr<SampleCatalog> sample_catalog_;
};

// TODO: Consider adding more tests here, but note that we
// already have some coverage from the tests for modules.

// Tests that any deprecation warnings from the body of a TVF are propagated to
// the TVFSignature. (This is trivial for UDFs because the FunctionSignature is
// used everywhere, whereas for TVFs there is a translation from
// FunctionSignature to TVFSignature.)
TEST_F(LazyResolutionTableFunctionTest, PropagateDeprecationWarnings) {
  const std::string sql =
      "CREATE PUBLIC TABLE FUNCTION foo()"
      "  RETURNS TABLE<x INT64> AS SELECT deprecation_warning() x ;";

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LazyResolutionTableFunction> function,
                       CreateAndInitializeTableFunction(sql));
  ASSERT_TRUE(function->NeedsResolution());
  ZETASQL_ASSERT_OK(function->ResolveAndUpdateIfNeeded(
      analyzer_options_, sample_catalog_->catalog(), &type_factory_));
  ASSERT_FALSE(function->NeedsResolution());

  const TableValuedFunction* tvf = function->ResolvedObject();
  ASSERT_EQ(tvf->NumSignatures(), 1);

  const FunctionSignature* concrete_signature = tvf->GetSignature(0);
  EXPECT_EQ("foo() -> TABLE<x INT64> (1 deprecation warning)",
            concrete_signature->DebugString(/*function_name=*/"foo",
                                            /*verbose=*/true));

  std::vector<TVFInputArgumentType> input_arguments;
  for (int i = 0; i < concrete_signature->NumConcreteArguments(); ++i) {
    const Type* type = concrete_signature->ConcreteArgumentType(i);
    input_arguments.emplace_back(InputArgumentType(type));
  }

  std::shared_ptr<TVFSignature> tvf_signature;
  ZETASQL_ASSERT_OK(tvf->Resolve(&analyzer_options_, input_arguments,
                         *concrete_signature, sample_catalog_->catalog(),
                         &type_factory_, &tvf_signature));

  EXPECT_EQ("() -> TABLE<x INT64> (1 deprecation warning)",
            tvf_signature->DebugString(/*verbose=*/true));
}

// Tests for LazyResolutionConstant.

class LazyResolutionConstantTest : public ::testing::Test {
 protected:
  LazyResolutionConstantTest() {
    // We must enable CREATE CONSTANT statements to run these tests.
    analyzer_options_.mutable_language()->SetSupportsAllStatementKinds();
    analyzer_options_.mutable_language()->EnableMaximumLanguageFeatures();
    // This makes it easier to verify that the error locations indicated
    // in the message line up with the statements in the module string/file.
    analyzer_options_.set_error_message_mode(
        ERROR_MESSAGE_MULTI_LINE_WITH_CARET);
    analyzer_options_.set_statement_context(CONTEXT_MODULE);
    analyzer_options_.mutable_find_options()->set_cycle_detector(
        &cycle_detector_);

    builtin_function_catalog_ = std::make_unique<SimpleCatalog>(
        "builtin_function_catalog", &type_factory_);

    ZETASQL_CHECK_OK(LazyResolutionCatalog::Create(
        "source_file", *ModuleDetails::Create("test_catalog", {}),
        analyzer_options_, &type_factory_, &lazy_resolution_catalog_));
    ZETASQL_CHECK_OK(lazy_resolution_catalog_->AppendResolutionCatalog(
        builtin_function_catalog_.get()));
  }

  absl::StatusOr<std::unique_ptr<LazyResolutionConstant>>
  CreateAndInitializeConstant(absl::string_view sql) {
    std::unique_ptr<ParserOutput> parser_output;
    const absl::Status parse_status =
        ParseStatement(sql, parser_options_, &parser_output);
    ZETASQL_RET_CHECK_OK(parse_status);
    ZETASQL_RET_CHECK_NE(parser_output.get(), nullptr);
    return LazyResolutionConstant::Create(
        ParseResumeLocation::FromStringView(sql), std::move(parser_output),
        parse_status,
        ErrorMessageOptions{
            .mode = ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET});
  }

  absl::Status ResolveAndUpdateConstant(LazyResolutionConstant* constant) {
    return constant->ResolveAndUpdateIfNeeded(
        analyzer_options_, builtin_function_catalog_.get(), &type_factory_);
  }

  // Cycle detector to use for the test.
  CycleDetector cycle_detector_;

  // Analyzer options to use for the test.
  AnalyzerOptions analyzer_options_;

  // Parser options to use for the test.
  ParserOptions parser_options_;

  // TypeFactory to use for the test.
  TypeFactory type_factory_;

  // A Catalog for builtin functions that can be referenced by
  // LazyResolutionConstants.
  std::unique_ptr<SimpleCatalog> builtin_function_catalog_;

  // LazyResolutionCatalog to use for the test.
  std::unique_ptr<LazyResolutionCatalog> lazy_resolution_catalog_;
};

TEST_F(LazyResolutionConstantTest, CreateRejectsNonConstant) {
  EXPECT_THAT(CreateAndInitializeConstant(
                  "CREATE PUBLIC FUNCTION foo(a INT64) AS (a+1);"),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("LazyResolutionConstants only support "
                                 "ASTCreateConstantStatements")));
}

TEST_F(LazyResolutionConstantTest, CreateRejectsMultiPartName) {
  EXPECT_THAT(
      CreateAndInitializeConstant("CREATE PUBLIC CONSTANT foo.bar = 2;"),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("LazyResolutionConstants only support named "
                         "constants with single-part names")));
}

TEST_F(LazyResolutionConstantTest, DebugStringContainsName) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LazyResolutionConstant> constant,
      CreateAndInitializeConstant("CREATE PUBLIC CONSTANT foo = 2;"));
  ASSERT_THAT(constant, NotNull());
  EXPECT_EQ(constant->DebugString(), "CONSTANT foo (unknown type)");
}

TEST_F(LazyResolutionConstantTest, DebugStringContainsPrivateScope) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LazyResolutionConstant> constant,
      CreateAndInitializeConstant("CREATE PRIVATE CONSTANT foo = 'bar';"));
  ASSERT_THAT(constant, NotNull());
  EXPECT_EQ(constant->DebugString(), "PRIVATE CONSTANT foo (unknown type)");
}

TEST_F(LazyResolutionConstantTest,
       VerboseDebugStringContainsPrivateScopeAndValue) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LazyResolutionConstant> constant,
      CreateAndInitializeConstant("CREATE PRIVATE CONSTANT foo = 2;"));
  ASSERT_THAT(constant, NotNull());
  // Debug string after creation, before resolution.
  EXPECT_EQ(constant->DebugString(/*verbose=*/true),
            "PRIVATE CONSTANT foo=Uninitialized value (unknown type)");

  // The constant value cannot be set if the LazyResolutionConstant does not
  // have a ResolvedExpr.
  EXPECT_FALSE(constant->SetValue(Value::Int64(2)).ok());
  // Resolve the LazyResolutionConstant.
  ZETASQL_ASSERT_OK(ResolveAndUpdateConstant(constant.get()));
  ASSERT_THAT(constant->ResolvedObject(), NotNull());
  EXPECT_TRUE(constant->ResolvedObject()->type()->IsInt64());

  // Debug string after resolution.
  EXPECT_EQ(constant->DebugString(/*verbose=*/true),
            "PRIVATE CONSTANT foo=Uninitialized value (INT64)");

  ASSERT_TRUE(constant->ResolvedObject()->Is<SQLConstant>());
  const SQLConstant* sql_constant =
      constant->ResolvedObject()->GetAs<const SQLConstant>();
  EXPECT_FALSE(sql_constant->evaluation_result().ok());

  // Setting the constant value to a different Type fails.
  EXPECT_FALSE(constant->SetValue(Value::Uint64(uint64_t{2})).ok());
  EXPECT_FALSE(sql_constant->evaluation_result().ok());

  // Setting the constant value with the same type succeeds.
  ZETASQL_EXPECT_OK(constant->SetValue(Value::Int64(5)));
  ZETASQL_EXPECT_OK(sql_constant->evaluation_result());

  // Setting the constant value after it is already set fails.
  EXPECT_FALSE(constant->SetValue(Value::Int64(5)).ok());
  ZETASQL_EXPECT_OK(sql_constant->evaluation_result());

  // Debug string after setting the constant value.
  EXPECT_EQ(constant->DebugString(/*verbose=*/true),
            "PRIVATE CONSTANT foo=Int64(5) (INT64)");
}

TEST_F(LazyResolutionConstantTest,
       FullDebugStringContainsPrivateScopeAndValue) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LazyResolutionConstant> constant,
      CreateAndInitializeConstant("CREATE PRIVATE CONSTANT foo = 'bar';"));
  ASSERT_THAT(constant, NotNull());
  ZETASQL_ASSERT_OK(ResolveAndUpdateConstant(constant.get()));
  ASSERT_THAT(constant->ResolvedObject(), NotNull());
  ZETASQL_EXPECT_OK(constant->SetValue(Value::String("bar")));
  EXPECT_EQ(constant->DebugString(/*verbose=*/true),
            "PRIVATE CONSTANT foo=String(\"bar\") (STRING)");
  EXPECT_EQ(constant->FullDebugString(),
            R"(PRIVATE CONSTANT foo=String("bar") (STRING)
CreateConstantStmt
+-name_path=foo
+-create_scope=CREATE_PRIVATE
+-expr=
  +-Literal(type=STRING, value="bar")
)");
}

TEST_F(LazyResolutionConstantTest, DebugStringContainsResolutionError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LazyResolutionConstant> constant,
      CreateAndInitializeConstant("CREATE PUBLIC CONSTANT foo = bar;"));
  ASSERT_THAT(constant, NotNull());
  ASSERT_TRUE(constant->NeedsResolution());
  const absl::Status resolution_status = constant->ResolveAndUpdateIfNeeded(
      analyzer_options_, lazy_resolution_catalog_.get(), &type_factory_);
  EXPECT_THAT(resolution_status, StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(StatusToString(resolution_status),
              HasSubstr("Constant foo is invalid"));
  EXPECT_THAT(StatusToString(resolution_status),
              HasSubstr("Unrecognized name: bar"));
  EXPECT_FALSE(constant->NeedsResolution());
  EXPECT_EQ(R"(CONSTANT foo (unknown type)
ERROR during resolution: Constant foo is invalid [at 1:24]
CREATE PUBLIC CONSTANT foo = bar;
                       ^
Unrecognized name: bar [at 1:30]
CREATE PUBLIC CONSTANT foo = bar;
                             ^)",
            constant->DebugString());
}

TEST_F(LazyResolutionConstantTest, DebugStringContainsEvaluationError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LazyResolutionConstant> constant,
      CreateAndInitializeConstant("CREATE PUBLIC CONSTANT foo = 0;"));
  ASSERT_THAT(constant, NotNull());
  ASSERT_TRUE(constant->NeedsResolution());
  ZETASQL_ASSERT_OK(constant->ResolveAndUpdateIfNeeded(
      analyzer_options_, lazy_resolution_catalog_.get(), &type_factory_));
  ZETASQL_EXPECT_OK(constant->set_evaluation_status(
      absl::Status(absl::StatusCode::kInternal, "division by zero")));
  EXPECT_EQ(constant->DebugString(), R"(CONSTANT foo (INT64)
ERROR during evaluation: generic::internal: division by zero)");
}

TEST_F(LazyResolutionConstantTest, ResolvedDebugStringContainsType) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LazyResolutionConstant> constant,
      CreateAndInitializeConstant("CREATE PRIVATE CONSTANT foo = 'bar';"));
  ASSERT_THAT(constant, NotNull());
  EXPECT_TRUE(constant->NeedsResolution());
  ZETASQL_ASSERT_OK(constant->ResolveAndUpdateIfNeeded(
      analyzer_options_, lazy_resolution_catalog_.get(), &type_factory_));
  ZETASQL_EXPECT_OK(constant->SetValue(Value::String("bar")));
  EXPECT_EQ(constant->DebugString(), R"(PRIVATE CONSTANT foo (STRING))");
}

TEST_F(LazyResolutionConstantTest,
       ResolvedFullDebugStringContainsTypeAndResolvedAST) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LazyResolutionConstant> constant,
      CreateAndInitializeConstant("CREATE PRIVATE CONSTANT foo = 'bar';"));
  ASSERT_THAT(constant, NotNull());
  EXPECT_TRUE(constant->NeedsResolution());
  ZETASQL_ASSERT_OK(constant->ResolveAndUpdateIfNeeded(
      analyzer_options_, lazy_resolution_catalog_.get(), &type_factory_));
  ZETASQL_EXPECT_OK(constant->SetValue(Value::String("bar")));
  EXPECT_EQ(constant->FullDebugString(),
            R"(PRIVATE CONSTANT foo=String("bar") (STRING)
CreateConstantStmt
+-name_path=foo
+-create_scope=CREATE_PRIVATE
+-expr=
  +-Literal(type=STRING, value="bar")
)");
}

TEST_F(LazyResolutionConstantTest,
       ResolutionSetsResolvedASTAndTypeButNotValue) {
  // Create a new LazyResolutionConstant.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LazyResolutionConstant> constant,
      CreateAndInitializeConstant("CREATE PRIVATE CONSTANT foo = 'bar';"));
  ASSERT_THAT(constant, NotNull());

  // The resolved AST, type and value are not initialized.
  EXPECT_THAT(constant->ResolvedStatement(), IsNull());
  EXPECT_THAT(constant->constant_expression(), IsNull());
  EXPECT_THAT(constant->ResolvedObject(), IsNull());

  // Resolve the constant definition.
  EXPECT_TRUE(constant->NeedsResolution()) << constant->FullDebugString();
  EXPECT_TRUE(constant->NeedsEvaluation()) << constant->FullDebugString();
  ZETASQL_ASSERT_OK(constant->ResolveAndUpdateIfNeeded(
      analyzer_options_, lazy_resolution_catalog_.get(), &type_factory_));

  // The resolved AST and the type are initialized, but not the value.
  EXPECT_FALSE(constant->NeedsResolution()) << constant->FullDebugString();
  EXPECT_TRUE(constant->NeedsEvaluation());
  EXPECT_THAT(constant->ResolvedStatement(), NotNull());
  EXPECT_THAT(constant->constant_expression(), NotNull());
  EXPECT_THAT(constant->ResolvedObject(), NotNull());
  EXPECT_TRUE(constant->ResolvedObject()->type()->IsString());

  ASSERT_TRUE(constant->ResolvedObject()->Is<SQLConstant>());
  const SQLConstant* sql_constant =
      constant->ResolvedObject()->GetAs<const SQLConstant>();
  EXPECT_THAT(sql_constant->type(), NotNull());
  EXPECT_TRUE(sql_constant->type()->IsString());
  EXPECT_FALSE(sql_constant->evaluation_result().ok());
}

// Tests for LazyResolutionConstant::NeedsEvaluation().
class LazyResolutionConstantNeedsEvaluationTest
    : public LazyResolutionConstantTest {
 protected:
  LazyResolutionConstantNeedsEvaluationTest() {
    absl::StatusOr<std::unique_ptr<LazyResolutionConstant>> constant =
        CreateAndInitializeConstant("CREATE PRIVATE CONSTANT foo = 'bar';");
    ZETASQL_CHECK_OK(constant.status());
    constant_ = *std::move(constant);
    ABSL_CHECK_NE(constant_.get(), nullptr);
  }

  ~LazyResolutionConstantNeedsEvaluationTest() override = default;

  std::unique_ptr<LazyResolutionConstant> constant_;
};

TEST_F(LazyResolutionConstantNeedsEvaluationTest,
       EvaluationIsNeededWithInvalidValueAndNoEvaluationError) {
  // The value is invalid and the evaluation status is OK.
  // This is the state before evaluation.
  EXPECT_THAT(constant_->ResolvedObject(), IsNull());
  ZETASQL_EXPECT_OK(constant_->resolution_or_evaluation_status());
  EXPECT_TRUE(constant_->NeedsEvaluation());
}

TEST_F(LazyResolutionConstantNeedsEvaluationTest,
       EvaluationIsNotNeededWithInvalidValueAndEvaluationError) {
  // The value is invalid and the evaluation status is an error.
  // This simulates a failed evaluation.
  EXPECT_THAT(constant_->ResolvedObject(), IsNull());
  const absl::Status evaluation_error =
      absl::Status(absl::StatusCode::kInternal, "divide by zero");
  // Setting the evaluation status fails if there is no resolved object.
  EXPECT_FALSE(constant_->set_evaluation_status(evaluation_error).ok());
  EXPECT_TRUE(constant_->NeedsResolution());
  EXPECT_TRUE(constant_->NeedsEvaluation());

  ZETASQL_ASSERT_OK(constant_->ResolveAndUpdateIfNeeded(
      analyzer_options_, builtin_function_catalog_.get(), &type_factory_));
  EXPECT_FALSE(constant_->NeedsResolution());
  EXPECT_TRUE(constant_->NeedsEvaluation());

  // Setting the evaluation status succeeds after constant resolution.
  ZETASQL_EXPECT_OK(constant_->set_evaluation_status(evaluation_error));
  EXPECT_FALSE(constant_->NeedsResolution());
  EXPECT_FALSE(constant_->NeedsEvaluation());
}

TEST_F(LazyResolutionConstantNeedsEvaluationTest,
       EvaluationIsNotNeededWithValidValueAndEvaluationError) {
  // The value is valid and the evaluation status is an error.
  // This case shouldn't occur in practice as a failed evaluation should return
  // an invalid value.
  ZETASQL_ASSERT_OK(constant_->ResolveAndUpdateIfNeeded(
      analyzer_options_, builtin_function_catalog_.get(), &type_factory_));
  ZETASQL_EXPECT_OK(constant_->SetValue(Value::String("bar")));
  EXPECT_FALSE(constant_->NeedsResolution());
  EXPECT_FALSE(constant_->NeedsEvaluation());

  ASSERT_THAT(constant_->ResolvedObject(), NotNull());
  ASSERT_TRUE(constant_->ResolvedObject()->Is<SQLConstant>());
  const SQLConstant* sql_constant =
      constant_->ResolvedObject()->GetAs<const SQLConstant>();
  EXPECT_THAT(sql_constant->type(), NotNull());
  EXPECT_TRUE(sql_constant->type()->IsString());
  ZETASQL_EXPECT_OK(sql_constant->evaluation_result());

  // Trying to set an invalid status on a Constant with a valid Value fails.
  const absl::Status evaluation_error =
      absl::Status(absl::StatusCode::kInternal, "divide by zero");
  EXPECT_FALSE(constant_->set_evaluation_status(evaluation_error).ok());
  EXPECT_FALSE(constant_->NeedsEvaluation());
}

TEST_F(LazyResolutionConstantNeedsEvaluationTest,
       EvaluationIsNotNeededWithValidValueAndNoEvaluationError) {
  // Setting evaluation status to OK is not allowed, at any stage.
  //
  // Invalid to set OK status before resolution.
  EXPECT_FALSE(constant_->set_evaluation_status(absl::OkStatus()).ok());
  ZETASQL_ASSERT_OK(constant_->ResolveAndUpdateIfNeeded(
      analyzer_options_, builtin_function_catalog_.get(), &type_factory_));
  // Invalid to set OK status after resolution and before evaluation.
  EXPECT_FALSE(constant_->set_evaluation_status(absl::OkStatus()).ok());
  ZETASQL_EXPECT_OK(constant_->SetValue(Value::String("bar")));
  // Invalid to set OK status after evaluation.
  EXPECT_FALSE(constant_->set_evaluation_status(absl::OkStatus()).ok());
}

// Tests for LazyResolutionConstant::status().
class LazyResolutionConstantStatusTest : public LazyResolutionConstantTest {
 protected:
  LazyResolutionConstantStatusTest() = default;

  ~LazyResolutionConstantStatusTest() override = default;

  std::unique_ptr<LazyResolutionConstant> constant_;
};

TEST_F(LazyResolutionConstantStatusTest,
       StatusIsOKWithNoResolutionOrEvaluationError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(constant_, CreateAndInitializeConstant(
                                      "CREATE PRIVATE CONSTANT foo = 'bar';"));
  ASSERT_THAT(constant_, NotNull());

  ZETASQL_EXPECT_OK(constant_->resolution_status());
  ZETASQL_EXPECT_OK(constant_->resolution_or_evaluation_status());
}

TEST_F(LazyResolutionConstantStatusTest, StatusIsNotOKWithResolutionError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(constant_, CreateAndInitializeConstant(
                                      "CREATE PUBLIC CONSTANT foo = bar;"));
  ASSERT_THAT(constant_, NotNull());

  const absl::Status resolution_status = constant_->ResolveAndUpdateIfNeeded(
      analyzer_options_, lazy_resolution_catalog_.get(), &type_factory_);
  EXPECT_THAT(resolution_status, StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(StatusToString(resolution_status),
              HasSubstr("Constant foo is invalid"));
  EXPECT_EQ(resolution_status, constant_->resolution_status());
  EXPECT_EQ(resolution_status, constant_->resolution_or_evaluation_status());

  EXPECT_THAT(constant_->resolution_or_evaluation_status(),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(StatusToString(constant_->resolution_or_evaluation_status()),
              HasSubstr("Constant foo is invalid"));
}

TEST_F(LazyResolutionConstantStatusTest, StatusIsNotOKWithNoEvaluationError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(constant_, CreateAndInitializeConstant(
                                      "CREATE PRIVATE CONSTANT foo = 'bar';"));
  ASSERT_THAT(constant_, NotNull());
  ZETASQL_EXPECT_OK(constant_->ResolveAndUpdateIfNeeded(
      analyzer_options_, lazy_resolution_catalog_.get(), &type_factory_));
  ZETASQL_EXPECT_OK(constant_->resolution_status());

  const absl::Status evaluation_error =
      absl::Status(absl::StatusCode::kInternal, "divide by zero");
  ZETASQL_EXPECT_OK(constant_->set_evaluation_status(evaluation_error));

  EXPECT_THAT(constant_->resolution_or_evaluation_status(),
              StatusIs(absl::StatusCode::kInternal, "divide by zero"));
}

TEST_F(LazyResolutionConstantStatusTest,
       StatusIsNotOKWithResolutionAndEvaluationError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(constant_, CreateAndInitializeConstant(
                                      "CREATE PUBLIC CONSTANT foo = bar;"));
  ASSERT_THAT(constant_, NotNull());

  // Resolution error test.
  absl::Status resolution_status = constant_->ResolveAndUpdateIfNeeded(
      analyzer_options_, lazy_resolution_catalog_.get(), &type_factory_);
  EXPECT_THAT(resolution_status, StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(StatusToString(resolution_status),
              HasSubstr("Constant foo is invalid"));
  EXPECT_EQ(resolution_status, constant_->resolution_status());
  EXPECT_EQ(resolution_status, constant_->resolution_or_evaluation_status());

  // Evaluation error test.
  ZETASQL_ASSERT_OK_AND_ASSIGN(constant_, CreateAndInitializeConstant(
                                      "CREATE PUBLIC CONSTANT foo = 1;"));
  ASSERT_THAT(constant_, NotNull());
  resolution_status = constant_->ResolveAndUpdateIfNeeded(
      analyzer_options_, lazy_resolution_catalog_.get(), &type_factory_);
  ZETASQL_ASSERT_OK(resolution_status);

  const absl::Status evaluation_error =
      absl::Status(absl::StatusCode::kInternal, "divide by zero");
  ZETASQL_EXPECT_OK(constant_->set_evaluation_status(evaluation_error));

  ZETASQL_EXPECT_OK(constant_->resolution_status());
  EXPECT_THAT(constant_->resolution_or_evaluation_status(),
              StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(StatusToString(constant_->resolution_or_evaluation_status()),
              HasSubstr("divide by zero"));
}

// Tests for LazyResolutionFunction.

class LazyResolutionFunctionTest : public ::testing::Test {
 protected:
  LazyResolutionFunctionTest() {
    // We must enable CREATE CONSTANT statements to run these tests.
    analyzer_options_.mutable_language()->SetSupportsAllStatementKinds();
    analyzer_options_.mutable_language()->EnableMaximumLanguageFeatures();
    // This makes it easier to verify that the error locations indicated
    // in the message line up with the statements in the module string/file.
    analyzer_options_.set_error_message_mode(
        ERROR_MESSAGE_MULTI_LINE_WITH_CARET);
    analyzer_options_.set_statement_context(CONTEXT_MODULE);
    analyzer_options_.mutable_find_options()->set_cycle_detector(
        &cycle_detector_);

    builtin_function_catalog_ = std::make_unique<SimpleCatalog>(
        "builtin_function_catalog", &type_factory_);
  }

  absl::StatusOr<std::unique_ptr<LazyResolutionFunction>>
  CreateAndInitializeFunction(absl::string_view sql,
                              FunctionEnums::Mode function_mode) {
    std::unique_ptr<ParserOutput> parser_output;
    const absl::Status parse_status =
        ParseStatement(sql, parser_options_, &parser_output);
    ZETASQL_RETURN_IF_ERROR(parse_status);
    ZETASQL_RET_CHECK_NE(parser_output.get(), nullptr);
    ZETASQL_ASSIGN_OR_RETURN(
        module_details_,
        ModuleDetails::Create(
            /*module_fullname=*/"x.y.z", /*resolved_options=*/{},
            /*constant_evaluator=*/nullptr, /*module_options=*/ModuleOptions(),
            /*module_name_from_import=*/{"x", "y", "z"}));
    return LazyResolutionFunction::Create(
        ParseResumeLocation::FromStringView(sql), std::move(parser_output),
        parse_status,
        ErrorMessageOptions{
            .mode = ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET},
        function_mode, module_details_);
  }

  absl::Status ResolveAndUpdateFunction(LazyResolutionFunction* function) {
    return function->ResolveAndUpdateIfNeeded(
        analyzer_options_, builtin_function_catalog_.get(), &type_factory_);
  }

  // Cycle detector to use for the test.
  CycleDetector cycle_detector_;

  // Analyzer options to use for the test.
  AnalyzerOptions analyzer_options_;

  // Parser options to use for the test.
  ParserOptions parser_options_;

  // TypeFactory to use for the test.
  TypeFactory type_factory_;

  // A Catalog for builtin functions that can be referenced by
  // LazyResolutionFunctions.
  std::unique_ptr<SimpleCatalog> builtin_function_catalog_;

  // Details about the test module.
  ModuleDetails module_details_ = ModuleDetails::CreateEmpty();
};

TEST_F(LazyResolutionFunctionTest, ShouldNotUppercaseName) {
  absl::StatusOr<std::unique_ptr<LazyResolutionFunction>> function =
      CreateAndInitializeFunction(
          "CREATE PUBLIC FUNCTION Foo(x STRING) RETURNS STRING AS (x);",
          FunctionEnums::SCALAR);
  ASSERT_THAT(function.status(), IsOk());
  ASSERT_THAT(*function, NotNull());

  ASSERT_TRUE((*function)->NeedsResolution());
  ZETASQL_ASSERT_OK(ResolveAndUpdateFunction((*function).get()));

  ASSERT_THAT((*function)->ResolvedObject()->SQLName(), StrEq("Foo"));
}

TEST_F(LazyResolutionFunctionTest, ModuleNameFromImport) {
  absl::StatusOr<std::unique_ptr<LazyResolutionFunction>> function =
      CreateAndInitializeFunction(
          "CREATE PUBLIC FUNCTION Foo(x STRING) RETURNS STRING AS (x);",
          FunctionEnums::SCALAR);
  ASSERT_THAT(function.status(), IsOk());
  ASSERT_THAT(*function, NotNull());

  ASSERT_TRUE((*function)->NeedsResolution());
  ZETASQL_ASSERT_OK(ResolveAndUpdateFunction((*function).get()));
  ASSERT_THAT(
      (*function)->ResolvedObject()->function_options().module_name_from_import,
      ElementsAre("x", "y", "z"));
}

class LazyResolutionViewTest : public ::testing::Test {
 protected:
  LazyResolutionViewTest() {
    // We must enable CREATE VIEW statements to run these tests.
    analyzer_options_.mutable_language()->SetSupportsAllStatementKinds();
    analyzer_options_.mutable_language()->EnableMaximumLanguageFeatures();
    // TODO: Remove explicitly enabling this feature after removing
    // in-development option from the views-in-modules language feature.
    analyzer_options_.mutable_language()->EnableLanguageFeature(
        FEATURE_VIEWS_IN_MODULES);
    // This makes it easier to verify that the error locations indicated
    // in the message line up with the statements in the module string/file.
    analyzer_options_.set_error_message_mode(
        ERROR_MESSAGE_MULTI_LINE_WITH_CARET);
    analyzer_options_.set_statement_context(CONTEXT_MODULE);
  }

  absl::Status CreateAndInitializeCatalog() {
    return LazyResolutionCatalog::Create(
        "source_file", *ModuleDetails::Create("test_catalog", {}),
        analyzer_options_, &type_factory_, &lazy_resolution_catalog_);
  }

  absl::StatusOr<std::unique_ptr<LazyResolutionView>> CreateAndInitializeView(
      absl::string_view sql) {
    std::unique_ptr<ParserOutput> parser_output;
    const absl::Status parse_status =
        ParseStatement(sql, parser_options_, &parser_output);
    ZETASQL_RET_CHECK_OK(parse_status);
    ZETASQL_RET_CHECK_NE(parser_output.get(), nullptr);
    return LazyResolutionView::Create(
        ParseResumeLocation::FromStringView(sql), std::move(parser_output),
        parse_status, analyzer_options_.error_message_options());
  }

  absl::Status ResolveAndUpdateView(LazyResolutionView* view) {
    return view->ResolveAndUpdateIfNeeded(
        analyzer_options_, lazy_resolution_catalog_.get(), &type_factory_);
  }

  AnalyzerOptions analyzer_options_;

  ParserOptions parser_options_;

  TypeFactory type_factory_;

  // LazyResolutionCatalog to use for the test.
  std::unique_ptr<LazyResolutionCatalog> lazy_resolution_catalog_;
};

TEST_F(LazyResolutionViewTest, SimpleView) {
  ZETASQL_ASSERT_OK(CreateAndInitializeCatalog());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LazyResolutionView> view,
      CreateAndInitializeView(
          "CREATE PUBLIC View Foo AS (select 1 as a, 2 as b);"));

  ASSERT_THAT(view, NotNull());
  ZETASQL_EXPECT_OK(view->resolution_status());
  EXPECT_EQ(view->Name(), "Foo");
  EXPECT_TRUE(view->NeedsResolution());

  ZETASQL_ASSERT_OK(ResolveAndUpdateView(view.get()));
  ZETASQL_EXPECT_OK(view->resolution_status());
  EXPECT_FALSE(view->NeedsResolution());
  const SQLView* sql_view = view->ResolvedObject();
  EXPECT_EQ(sql_view->Name(), "Foo");
  EXPECT_EQ(sql_view->NumColumns(), 2);
  EXPECT_EQ(sql_view->sql_security(), SQLView::kSecurityInvoker);
}

TEST_F(LazyResolutionViewTest, ViewWithInvalidQuery) {
  ZETASQL_ASSERT_OK(CreateAndInitializeCatalog());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LazyResolutionView> view,
      CreateAndInitializeView("CREATE PUBLIC View Foo AS (select a);"));
  EXPECT_THAT(view, NotNull());
  ZETASQL_EXPECT_OK(view->resolution_status());
  EXPECT_TRUE(view->NeedsResolution());

  absl::Status status = ResolveAndUpdateView(view.get());
  ASSERT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("View Foo is invalid")));
  EXPECT_THAT(StatusToString(status), HasSubstr("Unrecognized name: a"));
  EXPECT_FALSE(view->NeedsResolution());
}

TEST_F(LazyResolutionViewTest, NotAnASTCreateViewStatement) {
  ZETASQL_ASSERT_OK(CreateAndInitializeCatalog());

  ASSERT_THAT(
      CreateAndInitializeView(
          "CREATE PUBLIC FUNCTION Foo(x STRING) RETURNS STRING AS (x);"),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr(
              "LazyResolutionViews only support ASTCreateViewStatements")));
}

TEST_F(LazyResolutionViewTest, ViewWithMultiPartName) {
  ZETASQL_ASSERT_OK(CreateAndInitializeCatalog());

  ASSERT_THAT(
      CreateAndInitializeView(
          "CREATE PUBLIC View foo.bar AS (select 1 as a, 2 as b);"),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("LazyResolutionViews only support views with a "
                         "single-part view name, but found: foo.bar")));
}

}  // namespace zetasql
