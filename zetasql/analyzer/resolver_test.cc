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

#include "zetasql/analyzer/resolver.h"

#include <memory>

#include "zetasql/base/logging.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/testdata/error_catalog.h"
#include "zetasql/testdata/sample_catalog.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"

namespace zetasql {

using testing::HasSubstr;
using testing::IsNull;
using testing::NotNull;
using testing::_;
using zetasql_base::testing::StatusIs;

class ResolverTest : public ::testing::Test {
 public:
  // Used by benchmarks, this wrapper is to access a private member in Resolver.
  static absl::Status ResolveScalarExpr(
      Resolver* resolver,
      const ASTExpression* ast_expr,
      const NameScope* name_scope,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
    return resolver->ResolveScalarExpr(ast_expr, name_scope,
                                       "ResolveScalarExpr", resolved_expr_out);
  }

 protected:
  IdStringPool id_string_pool_;

  ResolverTest() {}
  ResolverTest(const ResolverTest&) = delete;
  ResolverTest& operator=(const ResolverTest&) = delete;
  ~ResolverTest() override {}

  void SetUp() override {
    InitializeQueryParameters();
    analyzer_options_.mutable_language()->SetSupportsAllStatementKinds();
    analyzer_options_.mutable_language()->EnableMaximumLanguageFeatures();
    analyzer_options_.CreateDefaultArenasIfNotSet();
    sample_catalog_ = absl::make_unique<SampleCatalog>(
        analyzer_options_.language(), &type_factory_);
    resolver_ = absl::make_unique<Resolver>(sample_catalog_->catalog(),
                                            &type_factory_, &analyzer_options_);
    // Initialize the resolver state, which is necessary because the tests do
    // not necessarily call the public methods that call Reset().
    resolver_->Reset("" /* sql */);
  }

  void TearDown() override {}

  // Resets 'resolver_' with a new Catalog.  Does not take ownership of
  // 'catalog'.
  void ResetResolver(Catalog* catalog) {
    resolver_ = absl::make_unique<Resolver>(catalog, &type_factory_,
                                            &analyzer_options_);
    resolver_->Reset("" /* sql */);
  }

  void ResolveSimpleTypeName(const std::string& name,
                             const Type* expected_type) {
    const Type* type;
    ZETASQL_EXPECT_OK(resolver_->ResolveTypeName(name, &type)) << name;
    EXPECT_THAT(type, NotNull()) << name;
    EXPECT_TRUE(type->Equals(expected_type))
        << "type: " << type->DebugString()
        << "expected type: " << expected_type->DebugString();

    // Round trip it and re-resolve.
    const Type* round_trip_type;
    std::string round_trip_name = type->DebugString();
    ZETASQL_EXPECT_OK(resolver_->ResolveTypeName(round_trip_name, &round_trip_type))
        << round_trip_name;
    EXPECT_THAT(round_trip_type, NotNull()) << round_trip_name;
    EXPECT_TRUE(round_trip_type->Equals(type))
        << "type: " << type->DebugString()
        << "round trip type: " << round_trip_type->DebugString();
  }

  absl::Status ResolveExpr(
      const ASTExpression* expression,
      std::unique_ptr<const ResolvedExpr>* resolved_expression,
      bool aggregation_allowed = false) {
    if (aggregation_allowed) {
      QueryResolutionInfo query_resolution_info(resolver_.get());
      ExprResolutionInfo with_aggregation(name_scope_.get(),
                                          &query_resolution_info);
      return resolver_->ResolveExpr(expression, &with_aggregation,
                                    resolved_expression);
    }
    return resolver_->ResolveScalarExpr(expression, name_scope_.get(),
                                        "ResolveScalarExpr",
                                        resolved_expression);
  }

  absl::Status FindFieldDescriptors(
      absl::Span<const ASTIdentifier* const> path_vector,
      const google::protobuf::Descriptor* root_descriptor,
      std::vector<const google::protobuf::FieldDescriptor*>* field_descriptors) {
    return resolver_->FindFieldDescriptors(path_vector, root_descriptor,
                                           field_descriptors);
  }

  // Test that <cast_expression> successfully resolves to an expression
  // of the expected type.
  void TestCastExpression(const std::string& cast_expression,
                          const Type* expected_cast_type) {
    std::unique_ptr<ParserOutput> parser_output;
    std::unique_ptr<const ResolvedExpr> resolved_expression;
    ZETASQL_ASSERT_OK(ParseExpression(cast_expression, ParserOptions(), &parser_output))
        << cast_expression;
    const ASTExpression* parsed_expression = parser_output->expression();
    ASSERT_THAT(parsed_expression, NotNull()) << cast_expression;
    ZETASQL_EXPECT_OK(ResolveExpr(parsed_expression, &resolved_expression))
        << cast_expression;
    ASSERT_THAT(resolved_expression.get(), NotNull()) << cast_expression;
    EXPECT_TRUE(expected_cast_type->Equals(resolved_expression->type()))
        << cast_expression;
  }

  zetasql_base::StatusOr<std::unique_ptr<const ResolvedExpr>> ResolveAndCoerce(
      const std::string& query, const Type* target_type,
      bool assignment_semantics, const char* clause_name) {
    std::unique_ptr<ParserOutput> parser_output;
    std::unique_ptr<const ResolvedExpr> resolved_expression;
    ZETASQL_CHECK_OK(ParseExpression(query, ParserOptions(), &parser_output)) << query;
    const ASTExpression* parsed_expression = parser_output->expression();
    ZETASQL_CHECK_OK(ResolveExpr(parsed_expression, &resolved_expression))
        << "Query: " << query
        << "\nParsed/Unparsed expression: " << Unparse(parsed_expression);
    ZETASQL_RETURN_IF_ERROR(resolver_->CoerceExprToType(
        parsed_expression, target_type, assignment_semantics, clause_name,
        &resolved_expression));
    return resolved_expression;
  }

  void TestCaseExpression(const std::string& query,
                          const std::string& expected_case_function_name) {
    std::unique_ptr<ParserOutput> parser_output;
    std::unique_ptr<const ResolvedExpr> resolved_expression;
    ZETASQL_ASSERT_OK(ParseExpression(query, ParserOptions(), &parser_output)) << query;
    const ASTExpression* parsed_expression = parser_output->expression();
    ASSERT_THAT(parsed_expression, NotNull());
    if (parsed_expression->node_kind() == AST_CASE_VALUE_EXPRESSION) {
      const auto& arguments =
          parsed_expression->GetAsOrDie<ASTCaseValueExpression>()->arguments();
      EXPECT_GE(arguments.size(), 3) << query;
    }
    if (parsed_expression->node_kind() == AST_CASE_NO_VALUE_EXPRESSION) {
      const auto& arguments =
          parsed_expression->GetAsOrDie<ASTCaseNoValueExpression>()
              ->arguments();
      EXPECT_GE(arguments.size(), 2) << query;
    }
    ZETASQL_EXPECT_OK(ResolveExpr(parsed_expression, &resolved_expression))
        << "Query: " << query
        << "\nParsed/Unparsed expression: " << Unparse(parsed_expression);
    EXPECT_THAT(resolved_expression.get(), NotNull());
    const ResolvedFunctionCall* resolved_function_call =
        resolved_expression->GetAs<ResolvedFunctionCall>();
    ASSERT_THAT(resolved_function_call, NotNull());
    EXPECT_EQ(expected_case_function_name,
              resolved_function_call->function()->FullName());
  }

  void ParseAndResolveFunction(const std::string& query,
                               const std::string& expected_function_name,
                               bool is_aggregate_function = false,
                               bool aggregation_allowed = false) {
    std::unique_ptr<ParserOutput> parser_output;
    std::unique_ptr<const ResolvedExpr> resolved_expression;
    ZETASQL_ASSERT_OK(ParseExpression(query, ParserOptions(), &parser_output)) << query;
    const ASTExpression* parsed_expression = parser_output->expression();
    ASSERT_THAT(parsed_expression, NotNull());
    ZETASQL_EXPECT_OK(ResolveExpr(parsed_expression, &resolved_expression,
                          aggregation_allowed))
        << "Query: " << query
        << "\nParsed expression: " << Unparse(parsed_expression);
    ASSERT_THAT(resolved_expression.get(), NotNull())
        << "Query: " << query
        << "\nParsed expression: " << Unparse(parsed_expression);
    if (is_aggregate_function) {
      // Aggregate functions resolve to a column reference.
      ASSERT_TRUE(resolved_expression->node_kind() == RESOLVED_COLUMN_REF)
          << resolved_expression->DebugString();
      const ResolvedColumnRef* resolved_column_ref =
          resolved_expression->GetAs<ResolvedColumnRef>();
      ASSERT_THAT(resolved_column_ref, NotNull());
      // TODO: Need to have a better check that we got what we
      // expected, in particular we should somewhere find the
      // <expected_function_name>.
      EXPECT_EQ("$agg1", resolved_column_ref->column().name());
    } else {
      ASSERT_TRUE(resolved_expression->node_kind() == RESOLVED_FUNCTION_CALL)
          << resolved_expression->DebugString();
      const ResolvedFunctionCall* resolved_function_call =
          resolved_expression->GetAs<ResolvedFunctionCall>();
      ASSERT_THAT(resolved_function_call, NotNull());
      EXPECT_EQ(expected_function_name,
              resolved_function_call->function()->FullName());
    }
  }

  void ResolveFunctionFails(const std::string& query,
                            const std::string& expected_error_substr,
                            bool aggregation_allowed = false) {
    std::unique_ptr<ParserOutput> parser_output;
    std::unique_ptr<const ResolvedExpr> resolved_expression;
    // Parsing should succeed.
    ZETASQL_ASSERT_OK(ParseExpression(query, ParserOptions(), &parser_output)) << query;
    const ASTExpression* parsed_expression = parser_output->expression();
    ASSERT_THAT(parsed_expression, NotNull());
    EXPECT_THAT(ResolveExpr(parsed_expression, &resolved_expression,
                            aggregation_allowed),
                StatusIs(_, HasSubstr(expected_error_substr)))
        << "Query: " << query
        << "\nParsed expression: " << Unparse(parsed_expression);
    EXPECT_THAT(resolved_expression.get(), IsNull())
        << resolved_expression->DebugString();
  }

  void ParseFunctionFails(const std::string& query,
                          const std::string& expected_error_substr) {
    std::unique_ptr<ParserOutput> parser_output;

    // Parsing should fail.
    EXPECT_THAT(ParseExpression(query, ParserOptions(), &parser_output),
                StatusIs(_, HasSubstr(expected_error_substr)))
        << "Query: " << query;
  }

  void TestResolveParameterExprSuccess(const std::string& query,
                                       const Type* expected_type) {
    std::unique_ptr<ParserOutput> parser_output;
    std::unique_ptr<const ResolvedExpr> resolved_expr;
    // Parsing should succeed.
    ZETASQL_ASSERT_OK(ParseExpression(query, ParserOptions(), &parser_output)) << query;
    const ASTExpression* parsed_expression = parser_output->expression();
    ASSERT_THAT(parsed_expression, NotNull());
    ZETASQL_EXPECT_OK(ResolveExpr(parsed_expression, &resolved_expr))
        << "Query: " << query
        << "\nParsed expression: " << Unparse(parsed_expression);
    EXPECT_THAT(resolved_expr.get(), NotNull());
    EXPECT_TRUE(resolved_expr->type()->Equals(expected_type));
  }

  void TestResolveParameterExprFails(const std::string& query,
                                     const std::string& expected_error_substr) {
    std::unique_ptr<ParserOutput> parser_output;
    std::unique_ptr<const ResolvedExpr> resolved_expr;
    // Parsing should succeed.
    ZETASQL_ASSERT_OK(ParseExpression(query, ParserOptions(), &parser_output)) << query;
    const ASTExpression* parsed_expression = parser_output->expression();
    ASSERT_THAT(parsed_expression, NotNull());
    EXPECT_THAT(ResolveExpr(parsed_expression, &resolved_expr),
                StatusIs(_, HasSubstr(expected_error_substr)))
        << "Query: " << query
        << "\nParsed expression: " << Unparse(parsed_expression);
    EXPECT_THAT(resolved_expr.get(), IsNull());
  }

  // 'path_expression' must parse to a ASTPathExpression or ASTIdentifier.
  void TestFindFieldDescriptorsSuccess(
      const std::string& path_expression,
      const google::protobuf::Descriptor* root_descriptor) {
    std::unique_ptr<ParserOutput> parser_output;
    ZETASQL_ASSERT_OK(ParseExpression(path_expression, ParserOptions(), &parser_output))
        << path_expression;
    const ASTExpression* parsed_expression = parser_output->expression();
    EXPECT_TRUE(parsed_expression->node_kind() == AST_IDENTIFIER ||
                parsed_expression->node_kind() == AST_PATH_EXPRESSION);
    absl::Span<const ASTIdentifier* const> path_vector;
    if (parsed_expression->node_kind() == AST_IDENTIFIER) {
      path_vector = {parsed_expression->GetAsOrDie<ASTIdentifier>()};
    } else {
      path_vector = parsed_expression->GetAsOrDie<ASTPathExpression>()->names();
    }
    std::vector<const google::protobuf::FieldDescriptor*> field_descriptors;
    ZETASQL_ASSERT_OK(
        FindFieldDescriptors(path_vector, root_descriptor, &field_descriptors));

    // Ensure that the field path is valid by checking field containment.
    std::string containing_proto_name = root_descriptor->full_name();
    EXPECT_EQ(path_vector.size(), field_descriptors.size());
    for (int i = 0; i < field_descriptors.size(); ++i) {
      if (!field_descriptors[i]->is_extension()) {
        EXPECT_EQ(path_vector[i]->GetAsString(), field_descriptors[i]->name());
      }
      EXPECT_EQ(containing_proto_name,
                field_descriptors[i]->containing_type()->full_name())
          << "Mismatched proto message " << containing_proto_name
          << " and field " << field_descriptors[i]->full_name();
      if (field_descriptors[i]->message_type() != nullptr) {
        containing_proto_name =
            field_descriptors[i]->message_type()->full_name();
      }
    }
  }

  // 'path_expression' must parse to a ASTPathExpression or ASTIdentifier.
  void TestFindFieldDescriptorsFail(const std::string& path_expression,
                                    const google::protobuf::Descriptor* root_descriptor,
                                    const std::string& expected_error_substr) {
    std::unique_ptr<ParserOutput> parser_output;
    ZETASQL_ASSERT_OK(ParseExpression(path_expression, ParserOptions(), &parser_output))
        << path_expression;
    const ASTExpression* parsed_expression = parser_output->expression();
    EXPECT_TRUE(parsed_expression->node_kind() == AST_IDENTIFIER ||
                parsed_expression->node_kind() == AST_PATH_EXPRESSION);
    absl::Span<const ASTIdentifier* const> path_vector;
    if (parsed_expression->node_kind() == AST_IDENTIFIER) {
      path_vector = {parsed_expression->GetAsOrDie<ASTIdentifier>()};
    } else {
      path_vector = parsed_expression->GetAsOrDie<ASTPathExpression>()->names();
    }
    std::vector<const google::protobuf::FieldDescriptor*> field_descriptors;
    EXPECT_THAT(
        FindFieldDescriptors(path_vector, root_descriptor, &field_descriptors),
        StatusIs(_, HasSubstr(expected_error_substr)));
  }

  void TestResolverErrorMessage(const std::string& query,
                                const std::string& expected_error_substr) {
    std::unique_ptr<ParserOutput> parser_output;
    std::unique_ptr<const ResolvedExpr> resolved_expression;
    // Parsing should succeed.
    ZETASQL_ASSERT_OK(ParseExpression(query, ParserOptions(), &parser_output)) << query;
    const ASTExpression* parsed_expression = parser_output->expression();
    ASSERT_THAT(parsed_expression, NotNull());
    EXPECT_THAT(ResolveExpr(parsed_expression, &resolved_expression),
                StatusIs(_, HasSubstr(expected_error_substr)))
        << "Query: " << query
        << "\nParsed expression: " << Unparse(parsed_expression);
    EXPECT_THAT(resolved_expression.get(), IsNull())
        << resolved_expression->DebugString();
  }

  void InitializeQueryParameters() {
    ZETASQL_ASSERT_OK(analyzer_options_.AddQueryParameter(
        "param_BOOL", type_factory_.get_bool()));
    ZETASQL_ASSERT_OK(analyzer_options_.AddQueryParameter(
        "param_BYTES", type_factory_.get_bytes()));
    ZETASQL_ASSERT_OK(analyzer_options_.AddQueryParameter(
        "param_INT32", type_factory_.get_int32()));
    ZETASQL_ASSERT_OK(analyzer_options_.AddQueryParameter(
        "PARAM_string", type_factory_.get_string()));
    ZETASQL_ASSERT_OK(analyzer_options_.AddQueryParameter(
        "pArAm_mIxEdcaSe", type_factory_.get_string()));
  }

  TypeFactory type_factory_;
  std::unique_ptr<SampleCatalog> sample_catalog_;
  AnalyzerOptions analyzer_options_;
  std::unique_ptr<Resolver> resolver_;
  std::unique_ptr<const NameScope> name_scope_;
};

TEST_F(ResolverTest, TestResolveTypeName) {
  ResolveSimpleTypeName("BOOL", type_factory_.get_bool());
  ResolveSimpleTypeName("int32", type_factory_.get_int32());
  ResolveSimpleTypeName("int64", type_factory_.get_int64());
  ResolveSimpleTypeName("uint32", type_factory_.get_uint32());
  ResolveSimpleTypeName("uint64", type_factory_.get_uint64());
  ResolveSimpleTypeName("float", type_factory_.get_float());
  ResolveSimpleTypeName("double", type_factory_.get_double());
  ResolveSimpleTypeName("string", type_factory_.get_string());
  ResolveSimpleTypeName("bytes", type_factory_.get_bytes());
  ResolveSimpleTypeName("date", type_factory_.get_date());
  ResolveSimpleTypeName("timestamp", type_factory_.get_timestamp());

  const Type* type;
  ZETASQL_EXPECT_OK(resolver_->ResolveTypeName("ARRAY<INT32>", &type));
  EXPECT_THAT(type, NotNull());
  EXPECT_TRUE(type->IsArray()) << type->DebugString();

  ZETASQL_EXPECT_OK(resolver_->ResolveTypeName("STRUCT<>", &type));
  EXPECT_THAT(type, NotNull());
  EXPECT_TRUE(type->IsStruct()) << type->DebugString();

  ZETASQL_EXPECT_OK(resolver_->ResolveTypeName("STRUCT<A INT32, B ARRAY<STRING>>",
                                       &type));
  EXPECT_THAT(type, NotNull());
  EXPECT_TRUE(type->IsStruct()) << type->DebugString();

  ZETASQL_EXPECT_OK(resolver_->ResolveTypeName(
      "struct<a INT32, a INT64>",
      &type));
  EXPECT_EQ("STRUCT<a INT32, a INT64>", type->DebugString());

  ZETASQL_EXPECT_OK(resolver_->ResolveTypeName(
      "struct<INT32, x INT64, double>",
       &type));
  EXPECT_EQ("STRUCT<INT32, x INT64, DOUBLE>", type->DebugString());

  ZETASQL_EXPECT_OK(resolver_->ResolveTypeName("`zetasql_test.KitchenSinkPB`",
                                       &type));
  EXPECT_THAT(type, NotNull());
  EXPECT_TRUE(type->IsProto()) << type->DebugString();

  ZETASQL_EXPECT_OK(resolver_->ResolveTypeName("zetasql_test.KitchenSinkPB.Nested",
                                       &type));
  EXPECT_THAT(type, NotNull());
  EXPECT_TRUE(type->IsProto()) << type->DebugString();

  EXPECT_THAT(
      resolver_->ResolveTypeName("zetasql_test.KitchenSinkPBXXX", &type),
      StatusIs(_,
               HasSubstr("Type not found: zetasql_test.KitchenSinkPBXXX")));

  ZETASQL_EXPECT_OK(resolver_->ResolveTypeName("`zetasql_test.TestEnum`",
                                       &type));
  EXPECT_THAT(type, NotNull());
  EXPECT_TRUE(type->IsEnum()) << type->DebugString();

  ZETASQL_EXPECT_OK(resolver_->ResolveTypeName("zetasql_test.TestEnum",
                                       &type));
  EXPECT_THAT(type, NotNull());
  EXPECT_TRUE(type->IsEnum()) << type->DebugString();

  EXPECT_THAT(
      resolver_->ResolveTypeName("`zetasql_TEST.TeSTeNum`", &type),
      StatusIs(_, HasSubstr("Type not found: `zetasql_TEST.TeSTeNum`")));
  EXPECT_EQ(type, nullptr);
}

TEST_F(ResolverTest, ResolveTypeInvalidTypeNameTests) {
  const Type* type;
  EXPECT_THAT(
      resolver_->ResolveTypeName("Array<ARRAY<INT32>>", &type),
      StatusIs(_, HasSubstr("Arrays of arrays are not supported")));

  EXPECT_THAT(resolver_->ResolveTypeName("blahblahblah", &type),
              StatusIs(_, HasSubstr("Type not found: blahblahblah")));

  EXPECT_THAT(resolver_->ResolveTypeName("CONCAT", &type),
              StatusIs(_, HasSubstr("Type not found: CONCAT")));

  EXPECT_THAT(resolver_->ResolveTypeName("timestamp(0)", &type),
              StatusIs(_, HasSubstr("Expected end of input but got \"(\"")));
}

TEST_F(ResolverTest, TestErrorCatalogNameTests) {
  std::unique_ptr<ErrorCatalog> error_catalog;
  ZETASQL_ASSERT_OK(
      ErrorCatalog::Create(absl::StatusCode::kInvalidArgument, &error_catalog));
  ResetResolver(error_catalog.get());

  // Name lookups into the error_catalog return an error (INVALID_ARGUMENT)
  std::unique_ptr<ParserOutput> parser_output;
  std::unique_ptr<const ResolvedStatement> resolved_ast;

  // Table
  const std::string query_with_table = "SELECT * FROM T";
  ZETASQL_ASSERT_OK(ParseStatement(query_with_table, ParserOptions(), &parser_output));
  EXPECT_THAT(resolver_->ResolveStatement(
                  query_with_table, parser_output->statement(), &resolved_ast),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("FindTable error")));

  // Type
  const std::string query_with_type = "SELECT cast(1 as foo_type)";
  ZETASQL_ASSERT_OK(ParseStatement(query_with_type, ParserOptions(), &parser_output));
  EXPECT_THAT(resolver_->ResolveStatement(
                  query_with_type, parser_output->statement(), &resolved_ast),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("FindType error")));

  // Procedure
  const std::string query_with_procedure = "CALL foo()";
  ZETASQL_ASSERT_OK(
      ParseStatement(query_with_procedure, ParserOptions(), &parser_output));
  EXPECT_THAT(
      resolver_->ResolveStatement(query_with_procedure,
                                  parser_output->statement(), &resolved_ast),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("FindProcedure error")));

  // Constant
  const std::string query_with_constant = "SELECT constant";
  ZETASQL_ASSERT_OK(
      ParseStatement(query_with_constant, ParserOptions(), &parser_output));
  EXPECT_THAT(
      resolver_->ResolveStatement(query_with_constant,
                                  parser_output->statement(), &resolved_ast),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("FindConstant error")));

  // Function
  const std::string query_with_function = "SELECT foo()";
  ZETASQL_ASSERT_OK(
      ParseStatement(query_with_function, ParserOptions(), &parser_output));
  EXPECT_THAT(
      resolver_->ResolveStatement(query_with_function,
                                  parser_output->statement(), &resolved_ast),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid function foo")));

  // TableValuedFunction
  const std::string query_with_tvf = "SELECT * from foo()";
  ZETASQL_ASSERT_OK(ParseStatement(query_with_tvf, ParserOptions(), &parser_output));
  EXPECT_THAT(resolver_->ResolveStatement(
                  query_with_tvf, parser_output->statement(), &resolved_ast),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid table-valued function foo")));
}

TEST_F(ResolverTest, TestResolveCastExpression) {
  TestCastExpression("CAST(true as BOoL)", types::BoolType());
  TestCastExpression("CAST(False as Boolean)", types::BoolType());
  TestCastExpression("CAST(1 as INT32)", types::Int32Type());
  TestCastExpression("CAST(1 as INT64)", types::Int64Type());
  TestCastExpression("CAST(1 as UINT32)", types::Uint32Type());
  TestCastExpression("CAST(1 as UINT64)", types::Uint64Type());
  TestCastExpression("CAST(1 as FLOAT)", types::FloatType());
  TestCastExpression("CAST(1 as DOUBLE)", types::DoubleType());
  TestCastExpression("CAST('x' as STRING)", types::StringType());
  TestCastExpression("CAST('x' as bytes)", types::BytesType());

  TestCastExpression("cast('2013-11-26' as date)", types::DateType());

  TestCastExpression("cast('2013-11-26 12:23:34' as timestamp)",
                     types::TimestampType());

  TestCastExpression("cast(timestamp('2013-11-26 12:23:34') as timestamp)",
                     types::TimestampType());

  TestCastExpression("CAST(true as STRING)", types::StringType());
  TestCastExpression("CAST(false as STRING)", types::StringType());

  TestCastExpression("CAST('true' as Bool)", types::BoolType());
  TestCastExpression("CAST('FaLsE' as Boolean)", types::BoolType());
  TestCastExpression("CAST(1 as BOOL)", types::BoolType());

  TestCastExpression("CAST('1' as INT32)", types::Int32Type());

  TestCastExpression("CAST('1' as INT64)", types::Int64Type());

  TestCastExpression("CAST('1' as UINT32)", types::Uint32Type());
  TestCastExpression("CAST(true as UINT32)", types::Uint32Type());

  TestCastExpression("CAST('1' as UINT64)", types::Uint64Type());
  TestCastExpression("CAST(true as UINT64)", types::Uint64Type());

  TestCastExpression("CAST('1' as FLOAT)", types::FloatType());
  TestCastExpression("CAST('1' as DOUBLE)", types::DoubleType());
  TestCastExpression("CAST(1 as STRING)", types::StringType());
  TestCastExpression("CAST(1.0 as STRING)", types::StringType());

  TestCastExpression(
      "CAST(CAST('TESTENUM1' as `zetasql_test.TestEnum`) as STRING)",
      types::StringType());

  TestCastExpression("CAST(CAST(1 as `zetasql_test.TestEnum`) as INT32)",
                     types::Int32Type());
  TestCastExpression("CAST(CAST(1 as `zetasql_test.TestEnum`) as INT64)",
                     types::Int64Type());
  TestCastExpression("CAST(CAST(1 as `zetasql_test.TestEnum`) as UINT32)",
                     types::Uint32Type());
  TestCastExpression("CAST(CAST(1 as `zetasql_test.TestEnum`) as UINT64)",
                     types::Uint64Type());

  // TODO: Add basic CAST resolution tests for ENUM, PROTO, STRUCT,
  // ARRAY (some will be errors - to be added in TestResolverErrors).

  // Cast shorthands do not work.
  ResolveFunctionFails("INT32(1)", "Function not found: INT32");

  // Casts disallowed between date/time and integer.
  ResolveFunctionFails("cast(cast(1 as INT32) as date)",
                       "Invalid cast from INT32 to DATE");
  ResolveFunctionFails("cast(cast(1 as INT64) as date)",
                       "Invalid cast from INT64 to DATE");

  ResolveFunctionFails("cast(cast('2013-11-26' as date) as INT32)",
                       "Invalid cast from DATE to INT32");
  ResolveFunctionFails("cast(cast('2013-11-26' as date) as INT64)",
                       "Invalid cast from DATE to INT64");

  ResolveFunctionFails("CAST(10000000000 as timestamp)",
                       "Invalid cast from INT64 to TIMESTAMP");
  ResolveFunctionFails("CAST(CAST(1 as INT64) as timestamp)",
                       "Invalid cast from INT64 to TIMESTAMP");

  ResolveFunctionFails(
      "cast(cast('2013-11-26 12:23:34' as timestamp) as INT64)",
      "Invalid cast from TIMESTAMP to INT64");

  // Enum value names are case-sensitive.
  // 'TESTENUM1' is good, 'TestEnum1' is not.
  ResolveFunctionFails(
      "CAST(CAST('TestEnum1' as `zetasql_test.TestEnum`) as STRING)",
      "Could not cast literal \"TestEnum1\" to type "
        "ENUM<zetasql_test.TestEnum>");

  ResolveFunctionFails("CAST(1 as blah)", "Type not found: blah");
  ResolveFunctionFails("CAST(1.0 as bool)",
                       "Invalid cast from DOUBLE to BOOL");
  ResolveFunctionFails("CAST(true as float)",
                       "Invalid cast from BOOL to FLOAT");
  ResolveFunctionFails("CAST(true as double)",
                       "Invalid cast from BOOL to DOUBLE");

  // Invalid type names
  ResolveFunctionFails("CAST(1 as blah)", "Type not found: blah");

  // Type names in SQL Standard which are explicitly not supported by ZetaSQL
  ResolveFunctionFails("CAST(1 as INTEGER)", "Type not found: INTEGER");
  ResolveFunctionFails("CAST(1 as BIGINT)", "Type not found: BIGINT");
  ResolveFunctionFails("CAST(1 as SMALLINT)", "Type not found: SMALLINT");
  ResolveFunctionFails("CAST(1 as real)", "Type not found: real");
  ResolveFunctionFails("CAST(1 as NUMBER)", "Type not found: NUMBER");
  ResolveFunctionFails("CAST(b'0' as binary)", "Type not found: binary");
  ResolveFunctionFails("CAST(b'0' as BLOB)", "Type not found: BLOB");
  ResolveFunctionFails("CAST('foo' as CHAR)", "Type not found: CHAR");

  // SQL Standard type names which are not even parsable in ZetaSQL
  ParseFunctionFails("CAST('foo' AS VARCHAR(5))",
                     R"error(Expected ")" but got "(")error");
  ParseFunctionFails(
      "CAST(1 as DOUBLE PRECISION)",
      R"error(Expected ")" but got identifier "PRECISION")error");
  ParseFunctionFails("CAST('foo' as CHAR VARYING(10))",
                     R"error(Expected ")" but got identifier "VARYING")error");
}

TEST_F(ResolverTest, TestCoerceToBoolSuccess) {
  for (bool assignment_semantics : {true, false}) {
    std::unique_ptr<const ResolvedExpr> resolved;
    ZETASQL_ASSERT_OK_AND_ASSIGN(resolved,
                         ResolveAndCoerce("TRUE", type_factory_.get_bool(),
                                          assignment_semantics, "test clause"));
    EXPECT_THAT(resolved->node_kind(), RESOLVED_LITERAL)
        << resolved->DebugString();
  }
}

TEST_F(ResolverTest, TestCoerceToBoolFail) {
  for (bool assignment_semantics : {true, false}) {
    EXPECT_THAT(
        ResolveAndCoerce("5", type_factory_.get_bool(), assignment_semantics,
                         "test clause"),
        StatusIs(_, ::testing::AllOf(HasSubstr("test clause"),
                                     HasSubstr("INT64"), HasSubstr("BOOL"))));
  }
}

TEST_F(ResolverTest, TestCoerceToBoolFailNoClauseName) {
  for (bool assignment_semantics : {true, false}) {
    EXPECT_THAT(
        ResolveAndCoerce("5", type_factory_.get_bool(), assignment_semantics,
                         nullptr),
        StatusIs(_, ::testing::AllOf(HasSubstr("INT64"), HasSubstr("BOOL"))));
  }
}

TEST_F(ResolverTest, TestImplicitCoerceInt64LiteralToInt32Succeed) {
  std::unique_ptr<const ResolvedExpr> resolved;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      resolved,
      ResolveAndCoerce("5", type_factory_.get_int32(),
                       /*assignment_semantics=*/false, "test clause"));
  EXPECT_THAT(resolved->node_kind(), RESOLVED_LITERAL)
      << resolved->DebugString();
}

TEST_F(ResolverTest, TestImplicitCoerceInt64ExprToInt32Fail) {
  EXPECT_THAT(
      ResolveAndCoerce("TestConstantInt64 + 1", type_factory_.get_int32(),
                       /*assignment_semantics=*/false, "test clause"),
      StatusIs(_, ::testing::AllOf(HasSubstr("test clause"), HasSubstr("INT64"),
                                   HasSubstr("INT32"))));
}

TEST_F(ResolverTest, TestAssignmentCoerceInt64ExprToInt32Succeed) {
  std::unique_ptr<const ResolvedExpr> resolved;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      resolved,
      ResolveAndCoerce("TestConstantInt64 + 1", type_factory_.get_int32(),
                       /*assignment_semantics=*/true, "test clause"));
  EXPECT_THAT(resolved->node_kind(), RESOLVED_CAST) << resolved->DebugString();
}

TEST_F(ResolverTest, TestResolveCaseExpressions) {
  TestCaseExpression("CASE WHEN 2=1 THEN 'a' END", "ZetaSQL:$case_no_value");
  TestCaseExpression("CASE WHEN 2=1 THEN 'a' ELSE 'b' END",
                     "ZetaSQL:$case_no_value");
  TestCaseExpression("CASE WHEN 2=1 THEN 'a' WHEN 3 = 4 THEN 'c' END",
                     "ZetaSQL:$case_no_value");
  TestCaseExpression("CASE WHEN 2=1 THEN 'a' WHEN 3 = 4 THEN 'c' ELSE 'b' END",
                     "ZetaSQL:$case_no_value");

  TestCaseExpression("CASE 1 WHEN 1 THEN 'a' END",
                     "ZetaSQL:$case_with_value");
  TestCaseExpression("CASE 1 WHEN 1 THEN 'a' ELSE 'b' END",
                     "ZetaSQL:$case_with_value");
  TestCaseExpression("CASE 1 WHEN 1 THEN 'a' WHEN 3 THEN 'c' END",
                     "ZetaSQL:$case_with_value");
  TestCaseExpression("CASE 1 WHEN 1 THEN 'a' WHEN 3 THEN 'c' ELSE 'b' END",
                     "ZetaSQL:$case_with_value");

  ResolveFunctionFails("CASE 1 WHEN 1 THEN 2 WHEN a THEN b END",
                       "Unrecognized name: a");
  ResolveFunctionFails("CASE 1 WHEN '1' THEN 2 END",
                       "No matching signature for operator CASE");
}

TEST_F(ResolverTest, TestResolveFunctions) {
  ParseAndResolveFunction("CURRENT_TIMESTAMP()",
                          "ZetaSQL:current_timestamp");
  ParseAndResolveFunction("CONCAT('a', 'b', 'c', 'd')", "ZetaSQL:concat");

  ResolveFunctionFails("sqrt(distinct 49)", "cannot be called with DISTINCT");
  ResolveFunctionFails("CONCAT('a',a)", "Unrecognized name: a");
  ResolveFunctionFails("sqrt(49, 81)",
                       "No matching signature for function SQRT");
}

TEST_F(ResolverTest, TestResolveAggregateExpressions) {
  ParseAndResolveFunction("Count(*)", "ZetaSQL:sum",
                          true /* is aggregation function */,
                          true /* aggregation allowed */);
  ParseAndResolveFunction("Sum(8)", "ZetaSQL:sum",
                          true /* is aggregation function */,
                          true /* aggregation allowed */);
  ParseAndResolveFunction("CASE WHEN 5 = 4 THEN Sum(6) END",
                          "ZetaSQL:$case_no_value",
                          false /* is not aggregation function */,
                          true /* aggregation allowed */);

  ResolveFunctionFails("sum(8)", "Aggregate function SUM not allowed in "
                       "ResolveScalarExpr");
  ResolveFunctionFails("sum(sum(8))",
                       "Aggregations of aggregations are not allowed", true);
  ResolveFunctionFails("count(distinct *)",
                       "COUNT(*) cannot be used with DISTINCT", true);
}

TEST_F(ResolverTest, ResolvingSafeScalarFunctionsSucceed) {
  ParseAndResolveFunction("SAFE.CURRENT_TIMESTAMP()",
                          "ZetaSQL:current_timestamp");
  ParseAndResolveFunction("SAFE.CONCAT('a', 'b', 'c', 'd')",
                          "ZetaSQL:concat");
  ParseAndResolveFunction(
      "safe.timestamp_add(TIMESTAMP \"2017-09-15 23:59:59.999999 UTC\", "
      "INTERVAL 10 DAY)",
      "ZetaSQL:timestamp_add");
  ParseAndResolveFunction("SAFE.GENERATE_ARRAY(11, 33, 2)",
                          "ZetaSQL:generate_array");
}

TEST_F(ResolverTest, ResolvingSafeAggregateFucntionsSucceed) {
  // Aggregate functions
  ParseAndResolveFunction("SAFE.Count(*)", "ZetaSQL:sum",
                          true /* is aggregation function */,
                          true /* aggregation allowed */);
  ParseAndResolveFunction("SAFE.Sum(8)", "ZetaSQL:sum",
                          true /* is aggregation function */,
                          true /* aggregation allowed */);
}

TEST_F(ResolverTest, ResolvingBuiltinFucntionsFail) {
  // Builtin functions
  ParseFunctionFails("SAFE.~(b\"\")", "Syntax error: Unexpected \"~\"");

  ParseFunctionFails(
      "SAFE.EXTRACT(HOUR FROM DATETIME(2008, 12, 25, 15, 30, 00))",
      "Syntax error: Expected \")\" but got keyword FROM");

  ParseFunctionFails("SAFE.(CAST(3152862397390174577 AS UINT64))",
                     "Syntax error: Unexpected keyword CAST");

  ParseFunctionFails("SAFE.ARRAY<DOUBLE>[1, 2, 3]",
                     "Syntax error: Unexpected \">\"");

  ParseFunctionFails(
      "SAFE.('a' IN ('a', 'b', 'c'))",
      "Syntax error: Unexpected string "
      "literal 'a'");

  ParseFunctionFails("SAFE.cast(true as bool)",
                     "Syntax error: Expected \")\" but got keyword AS");

  ParseFunctionFails(
      "SAFE.(case when KitchenSink.int64_key_1 = 0 then NULL else "
      "KitchenSink end)",
      "Syntax error: Unexpected keyword CASE");
}

TEST_F(ResolverTest, TestFindFieldDescriptorsSuccess) {
  zetasql_test::KitchenSinkPB kitchen_sink;
  TestFindFieldDescriptorsSuccess("int64_key_1", kitchen_sink.descriptor());
  TestFindFieldDescriptorsSuccess("int32_val", kitchen_sink.descriptor());
  TestFindFieldDescriptorsSuccess("repeated_int32_val",
                                  kitchen_sink.descriptor());
  TestFindFieldDescriptorsSuccess("date64", kitchen_sink.descriptor());
  TestFindFieldDescriptorsSuccess("nested_value.nested_int64",
                                  kitchen_sink.descriptor());
  TestFindFieldDescriptorsSuccess("nested_value.nested_repeated_int64",
                                  kitchen_sink.descriptor());
  zetasql_test::RecursivePB recursive_pb;
  TestFindFieldDescriptorsSuccess("recursive_pb.recursive_pb.int64_val",
                                  recursive_pb.descriptor());
}

TEST_F(ResolverTest, TestFindFieldDescriptorsFail) {
  zetasql_test::KitchenSinkPB kitchen_sink;
  const std::string& does_not_have_field = "does not have a field named ";
  TestFindFieldDescriptorsFail(
      "invalid_field", kitchen_sink.descriptor(),
      absl::StrCat(does_not_have_field, "invalid_field"));
  TestFindFieldDescriptorsFail(
      "nested_value.invalid_field", kitchen_sink.descriptor(),
      absl::StrCat(does_not_have_field, "invalid_field"));
  const std::string& cannot_access_field = "Cannot access field ";
  TestFindFieldDescriptorsFail(
      "int32_val.invalid_field", kitchen_sink.descriptor(),
      absl::StrCat(cannot_access_field, "invalid_field"));
  TestFindFieldDescriptorsFail(
      "nested_value.nested_int64.invalid_field", kitchen_sink.descriptor(),
      absl::StrCat(cannot_access_field, "invalid_field"));
}

TEST_F(ResolverTest, TestResolveParameterExpr) {
  for (const auto& param : analyzer_options_.query_parameters()) {
    TestResolveParameterExprSuccess(absl::StrCat("@", param.first),
                                    param.second);
  }
  TestResolveParameterExprFails("@InvalidParam",
                                "Query parameter 'InvalidParam' not found");
  // Checks parameters case insensitivity.
  TestResolveParameterExprSuccess(
      "@param_MiXeDcAsE", type_factory_.get_string());
  TestResolveParameterExprSuccess(
      "@param_MIXEDCASE", type_factory_.get_string());
  TestResolveParameterExprSuccess(
      "@param_mixedCASE", type_factory_.get_string());
}

TEST_F(ResolverTest, TestResolveLiteralAsNumericTarget) {
  {
    const std::string numeric_string =
        "99999999999999999999999999999.999999999";
    std::unique_ptr<const AnalyzerOutput> analyzer_output;
    ZETASQL_ASSERT_OK(AnalyzeExpressionForAssignmentToType(
        numeric_string, analyzer_options_, sample_catalog_->catalog(),
        &type_factory_, type_factory_.get_numeric(), &analyzer_output));
    ASSERT_EQ(analyzer_output->resolved_expr()->node_kind(), RESOLVED_LITERAL);
    EXPECT_TRUE(
        analyzer_output->resolved_expr()
            ->GetAs<ResolvedLiteral>()
            ->value()
            .Equals(values::Numeric(
                NumericValue::FromStringStrict(numeric_string).value())));
  }

  {
    const std::string numeric_string = "9999999999999999999";
    std::unique_ptr<const AnalyzerOutput> analyzer_output;
    ZETASQL_ASSERT_OK(AnalyzeExpressionForAssignmentToType(
        numeric_string, analyzer_options_, sample_catalog_->catalog(),
        &type_factory_, type_factory_.get_numeric(), &analyzer_output));
    ASSERT_EQ(analyzer_output->resolved_expr()->node_kind(), RESOLVED_LITERAL);
    EXPECT_TRUE(
        analyzer_output->resolved_expr()
            ->GetAs<ResolvedLiteral>()
            ->value()
            .Equals(values::Numeric(
                NumericValue::FromStringStrict(numeric_string).value())));
  }
}

TEST_F(ResolverTest, TestExpectedErrorMessage) {
  // Comparing Bytes to String Literal (or vice versa) should generate a
  // specific error message (b/18798970)
  const std::string expected_error_substr =
      "STRING and BYTES are different types";
  TestResolverErrorMessage("'abc' < b'abd'", expected_error_substr);
  TestResolverErrorMessage("'abc' <= b'abd'", expected_error_substr);
  TestResolverErrorMessage("'abc' > b'abd'", expected_error_substr);
  TestResolverErrorMessage("'abc' >= b'abd'", expected_error_substr);
  TestResolverErrorMessage("'abc' = b'abd'", expected_error_substr);
  TestResolverErrorMessage("'abc' != b'abd'", expected_error_substr);
  TestResolverErrorMessage("b'abc' < 'abd'", expected_error_substr);
  TestResolverErrorMessage("b'abc' <= 'abd'", expected_error_substr);
  TestResolverErrorMessage("b'abc' > 'abd'", expected_error_substr);
  TestResolverErrorMessage("b'abc' >= 'abd'", expected_error_substr);
  TestResolverErrorMessage("b'abc' = 'abd'", expected_error_substr);
  TestResolverErrorMessage("b'abc' != 'abd'", expected_error_substr);
  TestResolverErrorMessage("b'abc' LIKE 'ab%'", expected_error_substr);
  TestResolverErrorMessage("'abc' LIKE b'ab%'", expected_error_substr);
  TestResolverErrorMessage("b'abc' IN ('abc', 'abd')", expected_error_substr);
  TestResolverErrorMessage("'abc' IN (b'abc', b'abd')", expected_error_substr);
  TestResolverErrorMessage("'abc' IN UNNEST([b'abc', b'abd'])",
                           expected_error_substr);
  TestResolverErrorMessage("b'abc' IN UNNEST(['abc', 'abd'])",
                           expected_error_substr);
}

}  // namespace zetasql
