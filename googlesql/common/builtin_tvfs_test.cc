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

#include <memory>

#include "googlesql/common/builtin_function_internal.h"
#include "googlesql/common/builtins_output_properties.h"
#include "googlesql/base/testing/status_matchers.h"
#include "googlesql/public/builtin_function_options.h"
#include "googlesql/public/language_options.h"
#include "googlesql/public/options.pb.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"

namespace googlesql {
namespace {

class GetVectorSearchTableValuedFunctionsTest : public ::testing::Test {
 protected:
  GetVectorSearchTableValuedFunctionsTest() {
    language_options_.EnableLanguageFeature(FEATURE_VECTOR_SEARCH_TVF);
    language_options_.EnableLanguageFeature(
        FEATURE_SINGLE_HYBRID_VECTOR_SEARCH_TVF);
    language_options_.EnableLanguageFeature(FEATURE_JSON_TYPE);
    options_ =
        std::make_unique<GoogleSQLBuiltinFunctionOptions>(language_options_);
  }

  TypeFactory type_factory_;
  BuiltinsOutputProperties output_properties_;
  LanguageOptions language_options_;
  std::unique_ptr<GoogleSQLBuiltinFunctionOptions> options_;
};

class GetVectorSearchTVFsWithoutHybridVectorSearchTest
    : public ::testing::Test {
 protected:
  GetVectorSearchTVFsWithoutHybridVectorSearchTest() {
    language_options_.EnableLanguageFeature(FEATURE_VECTOR_SEARCH_TVF);
    language_options_.EnableLanguageFeature(FEATURE_JSON_TYPE);
    options_ =
        std::make_unique<GoogleSQLBuiltinFunctionOptions>(language_options_);
  }

  TypeFactory type_factory_;
  BuiltinsOutputProperties output_properties_;
  LanguageOptions language_options_;
  std::unique_ptr<GoogleSQLBuiltinFunctionOptions> options_;
};

class GetKMeansTableValuedFunctionsTest : public ::testing::Test {
 protected:
  GetKMeansTableValuedFunctionsTest() {
    options_ =
        std::make_unique<GoogleSQLBuiltinFunctionOptions>(language_options_);
  }

  TypeFactory type_factory_;
  BuiltinsOutputProperties output_properties_;
  LanguageOptions language_options_;
  std::unique_ptr<GoogleSQLBuiltinFunctionOptions> options_;
};

TEST_F(GetVectorSearchTableValuedFunctionsTest, VectorSearchFunction) {
  NameToTableValuedFunctionMap functions;
  GOOGLESQL_ASSERT_OK(GetVectorSearchTableValuedFunctions(
      &type_factory_, *options_, &functions, output_properties_));
  constexpr absl::string_view kVectorSearch = "vector_search";
  ASSERT_TRUE(functions.contains(kVectorSearch));
  EXPECT_EQ(
      functions[kVectorSearch]->DebugString(),
      "GoogleSQL:vector_search\n  (ANY TABLE, STRING column_to_search, ANY "
      "TABLE, optional "
      "STRING query_column_to_search, optional INT64 top_k, optional STRING "
      "distance_type, optional DOUBLE max_distance) "
      "-> "
      "ANY TABLE\n  (ANY TABLE, STRING column_to_search, ANY TABLE, optional "
      "STRING query_column_to_search, optional JSON options, optional INT64 "
      "top_k, optional STRING distance_type, optional DOUBLE max_distance) -> "
      "ANY TABLE\n  (ANY TABLE, STRING column_to_search, ARRAY<FLOAT> "
      "query_value, optional INT64 top_k, optional STRING distance_type, "
      "optional DOUBLE max_distance) -> ANY TABLE\n  (ANY TABLE, STRING "
      "column_to_search, ARRAY<FLOAT> query_value, optional JSON options, "
      "optional INT64 top_k, optional STRING distance_type, optional DOUBLE "
      "max_distance) -> ANY TABLE\n  (ANY TABLE, STRING "
      "column_to_search, ARRAY<DOUBLE> query_value, optional INT64 top_k, "
      "optional STRING distance_type, optional DOUBLE max_distance) -> ANY "
      "TABLE\n  (ANY TABLE, STRING column_to_search, ARRAY<DOUBLE> "
      "query_value, optional JSON options, optional INT64 top_k, optional "
      "STRING distance_type, optional DOUBLE max_distance) -> ANY TABLE\n  "
      "(ANY TABLE, STRING column_to_search, STRING query_value, "
      "optional INT64 top_k, optional STRING distance_type, optional DOUBLE "
      "max_distance) -> ANY TABLE\n  (ANY TABLE, STRING column_to_search, "
      "STRING query_value, optional JSON options, optional INT64 top_k, "
      "optional STRING distance_type, optional DOUBLE max_distance) -> ANY "
      "TABLE");
  EXPECT_EQ(output_properties_.SupportsSuppliedArgumentType(
                FN_BATCH_VECTOR_SEARCH_TVF_WITH_PROTO_OPTIONS, 4),
            true);
}

TEST_F(GetVectorSearchTVFsWithoutHybridVectorSearchTest, VectorSearchFunction) {
  NameToTableValuedFunctionMap functions;
  GOOGLESQL_ASSERT_OK(GetVectorSearchTableValuedFunctions(
      &type_factory_, *options_, &functions, output_properties_));
  constexpr absl::string_view kVectorSearch = "vector_search";
  ASSERT_TRUE(functions.contains(kVectorSearch));
  EXPECT_EQ(
      functions[kVectorSearch]->DebugString(),
      "GoogleSQL:vector_search\n  (ANY TABLE, STRING column_to_search, ANY "
      "TABLE, optional "
      "STRING query_column_to_search, optional INT64 top_k, optional STRING "
      "distance_type, optional DOUBLE max_distance) "
      "-> "
      "ANY TABLE");
  EXPECT_EQ(output_properties_.SupportsSuppliedArgumentType(
                FN_BATCH_VECTOR_SEARCH_TVF_WITH_PROTO_OPTIONS, 4),
            true);
}

TEST_F(GetKMeansTableValuedFunctionsTest, KMeansFunction) {
  NameToTableValuedFunctionMap functions;
  GOOGLESQL_ASSERT_OK(
      GetKMeansTableValuedFunction(&type_factory_, *options_, &functions));
  constexpr absl::string_view kKMeans = "kmeans";
  ASSERT_TRUE(functions.contains(kKMeans));
  EXPECT_EQ(
      functions[kKMeans]->DebugString(),
      "GoogleSQL:kmeans\n  (ANY TABLE, STRING vectors_column, optional INT64 "
      "k, "
      "optional PROTO<googlesql.KMeansOptions> options) -> ANY TABLE");
}

TEST_F(GetVectorSearchTableValuedFunctionsTest,
       BatchHybridVectorSearchFunction) {
  NameToTableValuedFunctionMap functions;
  GOOGLESQL_ASSERT_OK(GetVectorSearchTableValuedFunctions(
      &type_factory_, *options_, &functions, output_properties_));
  constexpr absl::string_view kHybridBatchVectorSearch = "hybrid_vector_search";
  ASSERT_TRUE(functions.contains(kHybridBatchVectorSearch));
  EXPECT_EQ(
      functions[kHybridBatchVectorSearch]->DebugString(),
      "GoogleSQL:hybrid_vector_search\n  (ANY TABLE, STRING column_to_search, "
      "ANY "
      "TABLE, ARRAY<STRING> lexical_search_columns, optional STRING "
      "query_column_to_search, optional STRING lexical_search_query_column, "
      "optional INT64 top_k, optional STRING distance_type, optional DOUBLE "
      "max_distance) -> ANY TABLE\n  (ANY TABLE, STRING column_to_search, "
      "ANY "
      "TABLE, ARRAY<STRING> lexical_search_columns, optional STRING "
      "query_column_to_search, optional STRING lexical_search_query_column, "
      "optional JSON options, optional INT64 top_k, optional STRING "
      "distance_type, optional DOUBLE max_distance) -> ANY TABLE\n  (ANY "
      "TABLE, STRING column_to_search, ARRAY<FLOAT> query_value, ARRAY<STRING> "
      "lexical_search_columns, STRING lexical_search_query_value, optional "
      "INT64 top_k, optional STRING distance_type, optional DOUBLE "
      "max_distance) -> ANY TABLE\n  (ANY TABLE, STRING column_to_search, "
      "ARRAY<FLOAT> query_value, ARRAY<STRING> lexical_search_columns, STRING "
      "lexical_search_query_value, optional JSON options, optional INT64 "
      "top_k, optional STRING distance_type, optional DOUBLE max_distance) -> "
      "ANY TABLE\n  (ANY TABLE, STRING column_to_search, ARRAY<DOUBLE> "
      "query_value, ARRAY<STRING> lexical_search_columns, STRING "
      "lexical_search_query_value, optional INT64 top_k, optional STRING "
      "distance_type, optional DOUBLE max_distance) -> ANY TABLE\n  (ANY "
      "TABLE, STRING column_to_search, ARRAY<DOUBLE> query_value, "
      "ARRAY<STRING> "
      "lexical_search_columns, STRING lexical_search_query_value, optional "
      "JSON options, optional INT64 top_k, optional STRING distance_type, "
      "optional DOUBLE max_distance) -> ANY TABLE\n  (ANY TABLE, STRING "
      "column_to_search, STRING query_value, ARRAY<STRING> "
      "lexical_search_columns, STRING lexical_search_query_value, optional "
      "INT64 top_k, optional STRING distance_type, optional DOUBLE "
      "max_distance) -> ANY TABLE\n  (ANY TABLE, STRING column_to_search, "
      "STRING query_value, ARRAY<STRING> lexical_search_columns, STRING "
      "lexical_search_query_value, optional JSON options, optional INT64 "
      "top_k, optional STRING distance_type, optional DOUBLE max_distance) -> "
      "ANY TABLE");
  EXPECT_EQ(output_properties_.SupportsSuppliedArgumentType(
                FN_BATCH_HYBRID_VECTOR_SEARCH_TVF_WITH_PROTO_OPTIONS, 6),
            true);
  EXPECT_EQ(
      output_properties_.SupportsSuppliedArgumentType(
          FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_FLOAT_ARRAY_WITH_PROTO_OPTIONS, 5),
      true);
  EXPECT_EQ(
      output_properties_.SupportsSuppliedArgumentType(
          FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_DOUBLE_ARRAY_WITH_PROTO_OPTIONS,
          5),
      true);
  EXPECT_EQ(
      output_properties_.SupportsSuppliedArgumentType(
          FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_STRING_WITH_PROTO_OPTIONS, 5),
      true);
}

TEST(GetLookupTableTableValuedFunctionTest, LookupTableFunction) {
  TypeFactory type_factory;
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_LOOKUP_TABLE_TVF);
  GoogleSQLBuiltinFunctionOptions options(language_options);
  NameToTableValuedFunctionMap functions;
  GOOGLESQL_ASSERT_OK(
      GetLookupTableTableValuedFunction(&type_factory, options, &functions));
  constexpr absl::string_view kLookupTable = "LookupTable";
  ASSERT_TRUE(functions.contains(kLookupTable));
  EXPECT_EQ(functions[kLookupTable]->DebugString(),
            "GoogleSQL:LookupTable\n  (STRING) -> ANY TABLE");
}

TEST(GetLookupTableTableValuedFunctionTest,
     LookupTableFunctionFeatureDisabled) {
  TypeFactory type_factory;
  LanguageOptions language_options;
  GoogleSQLBuiltinFunctionOptions options(language_options);
  NameToTableValuedFunctionMap functions;
  GOOGLESQL_ASSERT_OK(
      GetLookupTableTableValuedFunction(&type_factory, options, &functions));
  EXPECT_FALSE(functions.contains("LookupTable"));
}

TEST(GetLookupTableTableValuedFunctionTest, LookupTableFunctionExcluded) {
  TypeFactory type_factory;
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_LOOKUP_TABLE_TVF);
  GoogleSQLBuiltinFunctionOptions options(language_options);
  options.exclude_function_ids.insert(FN_TVF_LOOKUP_TABLE);
  NameToTableValuedFunctionMap functions;
  GOOGLESQL_ASSERT_OK(
      GetLookupTableTableValuedFunction(&type_factory, options, &functions));
  EXPECT_FALSE(functions.contains("LookupTable"));
}

}  // namespace
}  // namespace googlesql
