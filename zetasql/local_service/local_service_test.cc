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

#include "zetasql/local_service/local_service.h"

#include <string>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/base/path.h"
#include "google/protobuf/compiler/importer.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/text_format.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/common/testing/proto_matchers.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/proto/function.pb.h"
#include "zetasql/proto/simple_catalog.pb.h"
#include "zetasql/public/parse_resume_location.pb.h"
#include "zetasql/public/simple_table.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/public/value.pb.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/status.h"

namespace zetasql {

using ::zetasql::testing::EqualsProto;

namespace local_service {

class ZetaSqlLocalServiceImplTest : public ::testing::Test {
 protected:
  void SetUp() override {
    source_tree_.MapPath(
        "", zetasql_base::JoinPath(getenv("TEST_SRCDIR"), "com_google_protobuf"));
    source_tree_.MapPath(
        "", zetasql_base::JoinPath(getenv("TEST_SRCDIR"), "com_google_zetasql"));
    proto_importer_ =
        absl::make_unique<google::protobuf::compiler::Importer>(&source_tree_, nullptr);
    ASSERT_NE(nullptr, proto_importer_->Import(
                           "zetasql/testdata/test_schema.proto"));
    pool_ = absl::make_unique<google::protobuf::DescriptorPool>(proto_importer_->pool());
  }

  zetasql_base::Status Analyze(const AnalyzeRequest& request,
                       AnalyzeResponse* response) {
    return service_.Analyze(request, response);
  }

  zetasql_base::Status ExtractTableNamesFromStatement(
      const ExtractTableNamesFromStatementRequest& request,
      ExtractTableNamesFromStatementResponse* response) {
    return service_.ExtractTableNamesFromStatement(request, response);
  }

  zetasql_base::Status ExtractTableNamesFromNextStatement(
      const ExtractTableNamesFromNextStatementRequest& request,
      ExtractTableNamesFromNextStatementResponse* response) {
    return service_.ExtractTableNamesFromNextStatement(request, response);
  }

  zetasql_base::Status UnregisterCatalog(int64_t id) {
    return service_.UnregisterCatalog(id);
  }

  zetasql_base::Status UnregisterParseResumeLocation(int64_t id) {
    return service_.UnregisterParseResumeLocation(id);
  }

  zetasql_base::Status AddSimpleTable(const AddSimpleTableRequest& request) {
    return service_.AddSimpleTable(request);
  }

  zetasql_base::Status GetTableFromProto(const TableFromProtoRequest& request,
                                 SimpleTableProto* response) {
    return service_.GetTableFromProto(request, response);
  }

  zetasql_base::Status GetBuiltinFunctions(
      const ZetaSQLBuiltinFunctionOptionsProto& proto,
      GetBuiltinFunctionsResponse* response) {
    return service_.GetBuiltinFunctions(proto, response);
  }

  ZetaSqlLocalServiceImpl service_;
  google::protobuf::compiler::DiskSourceTree source_tree_;
  std::unique_ptr<google::protobuf::compiler::Importer> proto_importer_;
  std::unique_ptr<google::protobuf::DescriptorPool> pool_;
  TypeFactory factory_;
};

TEST_F(ZetaSqlLocalServiceImplTest, TableFromProto) {
  TableFromProtoRequest request;
  SimpleTableProto response;
  const ProtoType* proto_type;
  TypeFactory factory;
  TypeProto proto;
  google::protobuf::FileDescriptorSet descriptor_set;

  ZETASQL_CHECK_OK(factory.MakeProtoType(zetasql_test::TestSQLTable::descriptor(),
                                 &proto_type));
  ZETASQL_CHECK_OK(
      proto_type->SerializeToProtoAndFileDescriptors(&proto, &descriptor_set));
  *request.mutable_file_descriptor_set() = descriptor_set;
  *request.mutable_proto() = proto.proto_type();
  ASSERT_TRUE(GetTableFromProto(request, &response).ok());
  EXPECT_EQ("TestSQLTable", response.name());
  EXPECT_FALSE(response.is_value_table());
  EXPECT_EQ(2, response.column_size());
  const SimpleColumnProto column1 = response.column(0);
  EXPECT_EQ("f1", column1.name());
  EXPECT_EQ(TYPE_INT32, column1.type().type_kind());
  const SimpleColumnProto column2 = response.column(1);
  EXPECT_EQ("f2", column2.name());
  EXPECT_EQ(TYPE_INT32, column2.type().type_kind());
}

TEST_F(ZetaSqlLocalServiceImplTest, NonZetaSQLTableFromProto) {
  TableFromProtoRequest request;
  SimpleTableProto response;
  const ProtoType* proto_type;
  TypeFactory factory;
  ZETASQL_CHECK_OK(factory.MakeProtoType(zetasql_test::KitchenSinkPB::descriptor(),
                                 &proto_type));
  TypeProto proto;
  google::protobuf::FileDescriptorSet descriptor_set;
  ZETASQL_CHECK_OK(
      proto_type->SerializeToProtoAndFileDescriptors(&proto, &descriptor_set));
  *request.mutable_file_descriptor_set() = descriptor_set;
  *request.mutable_proto() = proto.proto_type();
  ASSERT_TRUE(GetTableFromProto(request, &response).ok());
  SimpleTableProto expected;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(R"pb(
    name: "KitchenSinkPB"
    is_value_table: true
    column {
      name: "value"
      type {
        type_kind: TYPE_PROTO
        proto_type {
          proto_name: "zetasql_test.KitchenSinkPB"
          proto_file_name: "zetasql/testdata/test_schema.proto"
        }
      }
      is_pseudo_column: false
    }
  )pb", &expected));
  EXPECT_EQ(expected.DebugString(), response.DebugString());
}

TEST_F(ZetaSqlLocalServiceImplTest, BadTableFromProto) {
  TableFromProtoRequest request;
  SimpleTableProto response;
  const ProtoType* proto_type;
  TypeFactory factory;
  ZETASQL_CHECK_OK(factory.MakeProtoType(zetasql_test::KitchenSinkPB::descriptor(),
                                 &proto_type));
  TypeProto proto;
  google::protobuf::FileDescriptorSet descriptor_set;
  ZETASQL_CHECK_OK(
      proto_type->SerializeToProtoAndFileDescriptors(&proto, &descriptor_set));
  *request.mutable_proto() = proto.proto_type();
  zetasql_base::Status status = GetTableFromProto(request, &response);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ("Proto type name not found: zetasql_test.KitchenSinkPB",
            status.message());
  EXPECT_TRUE(response.DebugString().empty());

  request.mutable_proto()->set_proto_file_name("unmatched_file_name");
  *request.mutable_file_descriptor_set() = descriptor_set;
  status = GetTableFromProto(request, &response);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(absl::StrCat("Proto zetasql_test.KitchenSinkPB found in ",
                         "zetasql/testdata/test_schema.proto",
                         ", not unmatched_file_name as specified."),
            status.message());
}

TEST_F(ZetaSqlLocalServiceImplTest, AnalyzeWrongCatalogId) {
  AnalyzeRequest request;
  request.set_registered_catalog_id(12345);

  AnalyzeResponse response;
  zetasql_base::Status status = Analyze(request, &response);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ("generic::invalid_argument: Registered catalog 12345 unknown.",
            internal::StatusToString(status));
}

TEST_F(ZetaSqlLocalServiceImplTest, ExtractTableNamesFromStatement) {
  ExtractTableNamesFromStatementRequest request;
  request.set_sql_statement("select count(1) from foo.bar;");

  ExtractTableNamesFromStatementResponse response;
  ZETASQL_ASSERT_OK(ExtractTableNamesFromStatement(request, &response));
  ExtractTableNamesFromStatementResponse expectedResponse;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(R"pb(table_name {
                                                    table_name_segment: "foo"
                                                    table_name_segment: "bar"
                                                  })pb", &expectedResponse));
  EXPECT_THAT(response, EqualsProto(expectedResponse));
}

TEST_F(ZetaSqlLocalServiceImplTest, ExtractTableNamesFromFirstStatement) {
  ExtractTableNamesFromNextStatementRequest request;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(parse_resume_location {
             input: "select count(1) from foo.bar; select id from baz;"
           })pb",
      &request));

  ExtractTableNamesFromNextStatementResponse response;
  ZETASQL_ASSERT_OK(ExtractTableNamesFromNextStatement(request, &response));
  ExtractTableNamesFromNextStatementResponse expectedResponse;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(table_name {
             table_name_segment: "foo"
             table_name_segment: "bar"
           }
           resume_byte_position: 29)pb",
      &expectedResponse));
  EXPECT_THAT(response, EqualsProto(expectedResponse));
}

TEST_F(ZetaSqlLocalServiceImplTest, ExtractWithUnsupportedStatement) {
  ExtractTableNamesFromNextStatementRequest request;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(parse_resume_location {
             input: "CREATE TABLE test AS SELECT COUNT(1) FROM foo.bar;"
             byte_position: 0
           })pb",
      &request));

  ExtractTableNamesFromNextStatementResponse response;
  zetasql_base::Status status = ExtractTableNamesFromNextStatement(request, &response);
  EXPECT_THAT(status,
              ::zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST_F(ZetaSqlLocalServiceImplTest, ExtractWithWrongStatementSupported) {
  ExtractTableNamesFromNextStatementRequest request;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(parse_resume_location {
             input: "CREATE TABLE test AS SELECT COUNT(1) FROM foo.bar;"
             byte_position: 0
           }
           options {
             supported_statement_kinds: RESOLVED_CONSTANT
           }
           )pb",
      &request));

  ExtractTableNamesFromNextStatementResponse response;
  zetasql_base::Status status = ExtractTableNamesFromNextStatement(request, &response);
  EXPECT_THAT(status,
              ::zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST_F(ZetaSqlLocalServiceImplTest, ExtractWithAllStatementsSupported) {
  ExtractTableNamesFromNextStatementRequest request;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(parse_resume_location {
             input: "CREATE TABLE test AS SELECT COUNT(1) FROM foo.bar;"
             byte_position: 0
           }
           options {}
           )pb",
      &request));

  ExtractTableNamesFromNextStatementResponse response;
  ZETASQL_ASSERT_OK(ExtractTableNamesFromNextStatement(request, &response));
  ExtractTableNamesFromNextStatementResponse expectedResponse;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(table_name {
             table_name_segment: "foo"
             table_name_segment: "bar"
           }
           resume_byte_position: 50)pb",
      &expectedResponse));
  EXPECT_THAT(response, EqualsProto(expectedResponse));
}

TEST_F(ZetaSqlLocalServiceImplTest, ExtractTableNamesFromEmptyStatement) {
  ExtractTableNamesFromNextStatementRequest request;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(parse_resume_location {
             input: "select count(1) from foo.bar;   "
             byte_position: 29
           })pb",
      &request));

  ExtractTableNamesFromNextStatementResponse response;
  zetasql_base::Status status = ExtractTableNamesFromNextStatement(request, &response);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ("Syntax error: Unexpected end of statement [at 1:30]",
            status.message());
}

TEST_F(ZetaSqlLocalServiceImplTest, ExtractWithBigResumePosition) {
  ExtractTableNamesFromNextStatementRequest request;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(parse_resume_location {
             input: "select count(1) from foo.bar;"
             byte_position: 9000
           })pb",
      &request));

  ExtractTableNamesFromNextStatementResponse response;
  zetasql_base::Status status = ExtractTableNamesFromNextStatement(request, &response);
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status, ::zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInternal));
}

TEST_F(ZetaSqlLocalServiceImplTest, ExtractWithNegativeResumePosition) {
  ExtractTableNamesFromNextStatementRequest request;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(parse_resume_location {
             input: "select count(1) from foo.bar;"
             byte_position: -1
           })pb",
      &request));

  ExtractTableNamesFromNextStatementResponse response;
  zetasql_base::Status status = ExtractTableNamesFromNextStatement(request, &response);
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status, ::zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInternal));
}

TEST_F(ZetaSqlLocalServiceImplTest, ExtractTableNamesFromNextStatement) {
  ExtractTableNamesFromNextStatementRequest request;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(parse_resume_location {
             input: "select count(1) from foo.bar; select id from baz;"
             byte_position: 29
           })pb",
      &request));

  ExtractTableNamesFromNextStatementResponse response;
  ZETASQL_ASSERT_OK(ExtractTableNamesFromNextStatement(request, &response));
  ExtractTableNamesFromNextStatementResponse expectedResponse;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(table_name {
             table_name_segment: "baz"
           }
           resume_byte_position: 49)pb",
      &expectedResponse));
  EXPECT_THAT(response, EqualsProto(expectedResponse));
}

TEST_F(ZetaSqlLocalServiceImplTest, ExtractTableNamesWithNoSemicolon) {
  ExtractTableNamesFromNextStatementRequest request;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(parse_resume_location {
             input: "select count(1) from foo.bar; select id from baz"
             byte_position: 29
           })pb",
      &request));

  ExtractTableNamesFromNextStatementResponse response;
  ZETASQL_ASSERT_OK(ExtractTableNamesFromNextStatement(request, &response));
  ExtractTableNamesFromNextStatementResponse expectedResponse;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(table_name {
             table_name_segment: "baz"
           }
           resume_byte_position: 48)pb",
      &expectedResponse));
  EXPECT_THAT(response, EqualsProto(expectedResponse));
}

TEST_F(ZetaSqlLocalServiceImplTest, UnregisterWrongCatalogId) {
  zetasql_base::Status status = UnregisterCatalog(12345);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ("generic::invalid_argument: Unknown catalog ID: 12345",
            internal::StatusToString(status));
}

TEST_F(ZetaSqlLocalServiceImplTest, AnalyzeWrongParseResumeLocationId) {
  AnalyzeRequest request;
  RegisteredParseResumeLocationProto location;
  location.set_registered_id(12345);
  location.set_byte_position(0);
  *request.mutable_registered_parse_resume_location() = location;

  AnalyzeResponse response;
  zetasql_base::Status status = Analyze(request, &response);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(
      "generic::invalid_argument: "
      "Registered parse resume location 12345 unknown.",
      internal::StatusToString(status));
}

TEST_F(ZetaSqlLocalServiceImplTest, UnregisterWrongParseResumeLocationId) {
  zetasql_base::Status status = UnregisterParseResumeLocation(12345);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ("generic::invalid_argument: Unknown ParseResumeLocation ID: 12345",
            internal::StatusToString(status));
}

TEST_F(ZetaSqlLocalServiceImplTest, AddSimpleTableWithWrongCatalogId) {
  AddSimpleTableRequest request;
  request.set_registered_catalog_id(12345);
  zetasql_base::Status status = AddSimpleTable(request);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ("generic::invalid_argument: Unknown catalog ID: 12345",
            internal::StatusToString(status));
}

TEST_F(ZetaSqlLocalServiceImplTest, Analyze) {
  const std::string catalog_proto_text = R"pb(
    name: "foo"
    table {
      name: "bar"
      serialization_id: 1
      column {
        name: "baz"
        type { type_kind: TYPE_INT32 }
        is_pseudo_column: false
      }
    })pb";

  SimpleCatalogProto catalog;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(catalog_proto_text, &catalog));

  AnalyzeRequest request;
  *request.mutable_simple_catalog() = catalog;
  request.set_sql_statement("select baz from bar;");

  AnalyzeResponse response;
  ZETASQL_EXPECT_OK(Analyze(request, &response));

  AnalyzeRequest request2;
  *request2.mutable_simple_catalog() = catalog;
  ParseResumeLocationProto* parse_resume_location2 =
      request2.mutable_parse_resume_location();
  parse_resume_location2->set_input("select baz from bar;select baz from bar;");
  parse_resume_location2->set_byte_position(0);
  AnalyzeResponse response2;
  ZETASQL_EXPECT_OK(Analyze(request2, &response2));
  EXPECT_EQ(20, response2.resume_byte_position());

  AnalyzeRequest request3;
  *request3.mutable_simple_catalog() = catalog;
  ParseResumeLocationProto* parse_resume_location3 =
      request3.mutable_parse_resume_location();
  parse_resume_location3->set_input("select baz from bar;select baz from bar;");
  parse_resume_location3->set_byte_position(response2.resume_byte_position());
  AnalyzeResponse response3;
  ZETASQL_EXPECT_OK(Analyze(request3, &response3));
  EXPECT_EQ(40, response3.resume_byte_position());
}

TEST_F(ZetaSqlLocalServiceImplTest, GetBuiltinFunctions) {
  ZetaSQLBuiltinFunctionOptionsProto proto;
  GetBuiltinFunctionsResponse response;
  FunctionProto function1;
  FunctionProto function2;
  google::protobuf::TextFormat::ParseFromString(R"(
      language_options {
        name_resolution_mode: NAME_RESOLUTION_DEFAULT
        product_mode: PRODUCT_INTERNAL
        error_on_deprecated_syntax: false
        supported_statement_kinds: RESOLVED_QUERY_STMT
      }
      include_function_ids: FN_CEIL_DOUBLE
      include_function_ids: FN_EQUAL
      include_function_ids: FN_ANY_VALUE
      exclude_function_ids: FN_ABS_DOUBLE
      exclude_function_ids: FN_ANY_VALUE)",
                                      &proto);
  google::protobuf::TextFormat::ParseFromString(R"(
      name_path: "$equal"
      group: "ZetaSQL"
      mode: SCALAR
      signature {
        argument {
          kind: ARG_TYPE_ANY_1
          options {
            cardinality: REQUIRED
            extra_relation_input_columns_allowed: true
          }
          num_occurrences: -1
        }
        argument {
          kind: ARG_TYPE_ANY_1
          options {
            cardinality: REQUIRED
            extra_relation_input_columns_allowed: true
          }
          num_occurrences: -1
        }
        return_type {
          kind: ARG_TYPE_FIXED
          type {
            type_kind: TYPE_BOOL
          }
          options {
            cardinality: REQUIRED
            extra_relation_input_columns_allowed: true
          }
          num_occurrences: -1
        }
        context_id: 42
        options {
          is_deprecated: false
        }
      }
      options {
        supports_over_clause: false
        window_ordering_support: ORDER_UNSUPPORTED
        supports_window_framing: false
        arguments_are_coercible: true
        is_deprecated: false
        alias_name: ""
        sql_name: "="
        allow_external_usage: true
        volatility: IMMUTABLE
        supports_order_by: false
        supports_limit: false
        supports_null_handling_modifier: false
        supports_safe_error_mode: false
        supports_having_modifier: true
        uses_upper_case_sql_name: true
      })",
                                      &function1);
  google::protobuf::TextFormat::ParseFromString(R"(
      name_path: "ceil"
      group: "ZetaSQL"
      mode: SCALAR
      signature {
        argument {
          kind: ARG_TYPE_FIXED
          type {
            type_kind: TYPE_DOUBLE
          }
          options {
            cardinality: REQUIRED
            extra_relation_input_columns_allowed: true
          }
          num_occurrences: -1
        }
        return_type {
          kind: ARG_TYPE_FIXED
          type {
            type_kind: TYPE_DOUBLE
          }
          options {
            cardinality: REQUIRED
            extra_relation_input_columns_allowed: true
          }
          num_occurrences: -1
        }
        context_id: 1313
        options {
          is_deprecated: false
        }
      }
      options {
        supports_over_clause: false
        window_ordering_support: ORDER_UNSUPPORTED
        supports_window_framing: false
        arguments_are_coercible: true
        is_deprecated: false
        alias_name: "ceiling"
        sql_name: ""
        allow_external_usage: true
        volatility: IMMUTABLE
        supports_order_by: false
        supports_limit: false
        supports_null_handling_modifier: false
        supports_safe_error_mode: true
        supports_having_modifier: true
        uses_upper_case_sql_name: true
      })",
                                      &function2);

  ASSERT_TRUE(GetBuiltinFunctions(proto, &response).ok());
  EXPECT_EQ(2, response.function_size());
  EXPECT_EQ(function1.DebugString(), response.function(0).DebugString());
  EXPECT_EQ(function2.DebugString(), response.function(1).DebugString());
}

}  // namespace local_service
}  // namespace zetasql
