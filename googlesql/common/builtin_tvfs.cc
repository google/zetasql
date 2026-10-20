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
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "googlesql/common/builtin_function_internal.h"
#include "googlesql/common/builtins_output_properties.h"
#include "googlesql/common/errors.h"
#include "googlesql/proto/kmeans_options.pb.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/builtin_function.pb.h"
#include "googlesql/public/builtin_function_options.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/constant_evaluator.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/input_argument_type.h"
#include "googlesql/public/kmeans_options.h"
#include "googlesql/public/language_options.h"
#include "googlesql/public/options.pb.h"
#include "googlesql/public/strings.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/types/struct_type.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"

namespace googlesql {
namespace {

// This constant represents the index of the `options` argument of
// batch VECTOR_SEARCH TVF. Only the `options` argument allows a
// Type to be defined through BuiltinFunctionOptions.
static constexpr int kBatchVectorSearchTVFOptionsArgIdx = 4;

// This constant represents the index of the `options` argument of
// single VECTOR_SEARCH TVF. Only the `options` argument allows a
// Type to be defined through BuiltinFunctionOptions.
static constexpr int kSingleVectorSearchTVFOptionsArgIdx = 3;

// This constant represents the index of the `options` argument of
// HYBRID_VECTOR_SEARCH. Only the `options` argument allows a
// Type to be defined through BuiltinFunctionOptions.
static constexpr int kHybridBatchVectorSearchTVFOptionsArgIdx = 6;

// This constant represents the index of the `options` argument of
// hybrid single VECTOR_SEARCH TVF. Only the `options` argument allows a
// Type to be defined through BuiltinFunctionOptions.
static constexpr int kHybridSingleVectorSearchTVFOptionsArgIdx = 5;

// Reused for hybrid vector search since there is commonality.
absl::Status CheckVectorSearchPostResolutionArguments(
    const FunctionSignature& signature,
    absl::Span<const TVFInputArgumentType> arguments,
    const LanguageOptions& language_options) {
  if (signature.context_id() == FN_BATCH_VECTOR_SEARCH_TVF_WITH_PROTO_OPTIONS ||
      signature.context_id() == FN_BATCH_VECTOR_SEARCH_TVF_WITH_JSON_OPTIONS) {
    GOOGLESQL_RET_CHECK_EQ(arguments.size(), 8);
  } else if (signature.context_id() ==
                 FN_BATCH_HYBRID_VECTOR_SEARCH_TVF_WITH_PROTO_OPTIONS ||
             signature.context_id() ==
                 FN_BATCH_HYBRID_VECTOR_SEARCH_TVF_WITH_JSON_OPTIONS) {
    GOOGLESQL_RET_CHECK_EQ(arguments.size(), 10);
  } else if (
      signature.context_id() ==
          FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_FLOAT_ARRAY_WITH_PROTO_OPTIONS ||
      signature.context_id() ==
          FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_FLOAT_ARRAY_WITH_JSON_OPTIONS ||
      signature.context_id() ==
          FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_DOUBLE_ARRAY_WITH_PROTO_OPTIONS ||
      signature.context_id() ==
          FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_DOUBLE_ARRAY_WITH_JSON_OPTIONS ||
      signature.context_id() ==
          FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_STRING_WITH_PROTO_OPTIONS ||
      signature.context_id() ==
          FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_STRING_WITH_JSON_OPTIONS) {
    GOOGLESQL_RET_CHECK_EQ(arguments.size(), 9);
  } else {
    // Single VECTOR_SEARCH TVF.
    GOOGLESQL_RET_CHECK_EQ(arguments.size(), 7);
  }
  return absl::OkStatus();
}

absl::StatusOr<TVFSchemaColumn> GetVectorColumnTypeKMeans(
    const TVFInputArgumentType& input_table_arg,
    const TVFInputArgumentType& vectors_column_name_arg,
    absl::string_view argument_name, const AnalyzerOptions& analyzer_options) {
  const ResolvedExpr* scalar_expr = vectors_column_name_arg.scalar_expr();
  GOOGLESQL_RET_CHECK_NE(scalar_expr, nullptr);
  GOOGLESQL_RET_CHECK(analyzer_options.constant_evaluator() != nullptr)
      << "KMeans TVF requires a constant evaluator, but none was configured in "
         "AnalyzerOptions.";
  GOOGLESQL_ASSIGN_OR_RETURN(
      const Value vectors_column_name_value,
      analyzer_options.constant_evaluator()->Evaluate(*scalar_expr));
  if (vectors_column_name_value.is_null()) {
    return absl::InvalidArgumentError(absl::Substitute(
        "The $0 argument of KMeans TVF must be a non-null string",
        ToAlwaysQuotedIdentifierLiteral(argument_name)));
  }
  GOOGLESQL_RET_CHECK(vectors_column_name_value.type()->IsString());
  int vector_column_index = -1;
  int found_count = 0;
  for (int i = 0; i < input_table_arg.relation().num_columns(); ++i) {
    const TVFRelation::Column& column = input_table_arg.relation().columns()[i];
    if (googlesql_base::CaseEqual(column.name,
                               vectors_column_name_value.string_value())) {
      if ((column.type->IsArray() &&
           (column.type->AsArray()->element_type()->IsFloat() ||
            column.type->AsArray()->element_type()->IsDouble()))) {
        vector_column_index = i;
      }
      found_count++;
    }
  }

  if (vector_column_index == -1) {
    // If no column was found, return an error.
    if (found_count == 0) {
      return absl::InvalidArgumentError(
          absl::Substitute("Unrecognized name: $0 in input table arg",
                           ToAlwaysQuotedIdentifierLiteral(
                               vectors_column_name_value.string_value())));
    } else {
      // If one or more columns were found, but none of them were ARRAY<DOUBLE>
      // or ARRAY<FLOAT>, return an error.
      return absl::InvalidArgumentError(absl::Substitute(
          "The column specified by the $0 argument of KMeans TVF must be "
          " of type ARRAY<DOUBLE> or ARRAY<FLOAT>",
          ToAlwaysQuotedIdentifierLiteral(argument_name)));
    }
  }
  if (found_count > 1) {
    return absl::InvalidArgumentError(
        absl::Substitute("Column $0 is ambiguous in the base table",
                         vectors_column_name_value.string_value()));
  }

  return input_table_arg.relation().columns()[vector_column_index];
}

absl::StatusOr<googlesql::TVFRelation::Column> BuildStructColumn(
    googlesql::TypeFactory* type_factory,
    const googlesql::TVFRelation& relation, std::string_view output_name) {
  std::vector<googlesql::StructField> struct_fields;
  for (const auto& column : relation.columns()) {
    struct_fields.push_back({column.name, column.type});
  }
  const googlesql::Type* struct_type;
  GOOGLESQL_RETURN_IF_ERROR(type_factory->MakeStructType(struct_fields, &struct_type));
  return googlesql::TVFRelation::Column(output_name, struct_type);
}

// Reused for hybrid vector search since there is commonality.
absl::StatusOr<std::shared_ptr<TVFSignature>>
ComputeResultTypeForBatchVectorSearchTVF(
    Catalog* catalog, TypeFactory* type_factory,
    const FunctionSignature& signature,
    const std::vector<TVFInputArgumentType>& input_arguments,
    const AnalyzerOptions& analyzer_options) {
  GOOGLESQL_ASSIGN_OR_RETURN(
      auto base_struct_column,
      BuildStructColumn(type_factory, input_arguments[0].relation(), "base"));

  GOOGLESQL_ASSIGN_OR_RETURN(
      auto query_struct_column,
      BuildStructColumn(type_factory, input_arguments[2].relation(), "query"));

  TVFRelation::ColumnList output_columns = {
      query_struct_column, base_struct_column,
      googlesql::TVFRelation::Column("distance", type_factory->get_double())};

  googlesql::TVFRelation output_schema(output_columns);
  return TVFSignature::Create(input_arguments, output_schema);
}

absl::StatusOr<std::shared_ptr<TVFSignature>>
ComputeResultTypeForSingleVectorSearchTVF(
    Catalog* catalog, TypeFactory* type_factory,
    const FunctionSignature& signature,
    const std::vector<TVFInputArgumentType>& input_arguments,
    const AnalyzerOptions& analyzer_options) {
  GOOGLESQL_ASSIGN_OR_RETURN(
      auto base_struct_column,
      BuildStructColumn(type_factory, input_arguments[0].relation(), "base"));

  TVFRelation::ColumnList output_columns = {
      base_struct_column,
      googlesql::TVFRelation::Column("distance", type_factory->get_double())};

  googlesql::TVFRelation output_schema(output_columns);
  return TVFSignature::Create(input_arguments, output_schema);
}

// Reused for hybrid vector search since there is commonality.
absl::StatusOr<std::shared_ptr<TVFSignature>>
ComputeResultTypeForVectorSearchTVF(
    Catalog* catalog, TypeFactory* type_factory,
    const FunctionSignature& signature,
    const std::vector<TVFInputArgumentType>& input_arguments,
    const AnalyzerOptions& analyzer_options) {
  if (signature.context_id() == FN_BATCH_VECTOR_SEARCH_TVF_WITH_PROTO_OPTIONS ||
      signature.context_id() == FN_BATCH_VECTOR_SEARCH_TVF_WITH_JSON_OPTIONS ||
      signature.context_id() ==
          FN_BATCH_HYBRID_VECTOR_SEARCH_TVF_WITH_PROTO_OPTIONS ||
      signature.context_id() ==
          FN_BATCH_HYBRID_VECTOR_SEARCH_TVF_WITH_JSON_OPTIONS) {
    return ComputeResultTypeForBatchVectorSearchTVF(
        catalog, type_factory, signature, input_arguments, analyzer_options);
  }
  return ComputeResultTypeForSingleVectorSearchTVF(
      catalog, type_factory, signature, input_arguments, analyzer_options);
}

absl::StatusOr<std::shared_ptr<TVFSignature>> ComputeResultTypeForKmeansTVF(
    Catalog* catalog, TypeFactory* type_factory,
    const FunctionSignature& signature,
    const std::vector<TVFInputArgumentType>& input_arguments,
    const AnalyzerOptions& analyzer_options) {
  const TVFInputArgumentType& input_table_arg = input_arguments[0];
  const TVFInputArgumentType& vectors_column_name_arg = input_arguments[1];

  std::string_view argument_name =
      signature.arguments()[1].options().argument_name();
  GOOGLESQL_ASSIGN_OR_RETURN(
      auto vector_column_type,
      GetVectorColumnTypeKMeans(input_table_arg, vectors_column_name_arg,
                                argument_name, analyzer_options));

  TVFRelation::ColumnList output_columns = {
      googlesql::TVFRelation::Column("cluster_id", type_factory->get_int64()),
      googlesql::TVFRelation::Column("cluster_vector",
                                     vector_column_type.type)};

  googlesql::TVFRelation output_schema(output_columns);
  return TVFSignature::Create(input_arguments, output_schema);
}

// Reused for hybrid vector search since there is commonality.
std::vector<googlesql::FunctionArgumentType> CommonVectorSearchArguments() {
  std::vector<googlesql::FunctionArgumentType> arguments = {
      // Base table.
      {googlesql::FunctionArgumentType::AnyRelation()},
      // Column to search.
      {googlesql::FunctionArgumentType(
          googlesql::types::StringType(),
          googlesql::FunctionArgumentTypeOptions()
              .set_must_be_non_null()
              .set_argument_name("column_to_search", kPositionalOnly))},
      // top_k
      {googlesql::FunctionArgumentType(
          googlesql::types::Int64Type(),
          googlesql::FunctionArgumentTypeOptions()
              .set_cardinality(googlesql::FunctionArgumentType::OPTIONAL)
              .set_argument_name("top_k", googlesql::kNamedOnly)
              .set_min_value(1)
              .set_default(Value::Int64(10)))},
      // distance_type
      {googlesql::FunctionArgumentType(
          googlesql::types::StringType(),
          googlesql::FunctionArgumentTypeOptions()
              .set_argument_name("distance_type", googlesql::kNamedOnly)
              .set_default(Value::String("EUCLIDEAN"))
              .set_cardinality(googlesql::FunctionArgumentType::OPTIONAL))},
      // max_distance
      {googlesql::FunctionArgumentType(
          googlesql::types::DoubleType(),
          googlesql::FunctionArgumentTypeOptions()
              .set_argument_name("max_distance", googlesql::kNamedOnly)
              .set_default(Value::Null(googlesql::types::DoubleType()))
              .set_cardinality(googlesql::FunctionArgumentType::OPTIONAL))}};
  return arguments;
}

// Reused for hybrid vector search since there is commonality.
absl::Status MaybeAddVectorSearchTVFProtoOptionsArgument(
    const BuiltinFunctionOptions& options, absl::string_view fn_name,
    FunctionSignatureId id, BuiltinsOutputProperties& output_properties,
    FunctionArgumentTypeList& arguments, const int arg_idx) {
  // Mark this signature as one that supports a supplied Type.
  output_properties.MarkSupportsSuppliedArgumentType(id, arg_idx);
  // Check if a Type was actually supplied in `options`.
  if (auto it = options.argument_types.find({id, arg_idx});
      it != options.argument_types.end()) {
    const Type* proto_type = it->second;
    if (!proto_type->IsProto()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Supplied argument type for the `options` argument of function ",
          fn_name, " must be a proto"));
    }

    FunctionArgumentType proto_options_arg = FunctionArgumentType(
        proto_type, FunctionArgumentTypeOptions(FunctionArgumentType::OPTIONAL)
                        .set_argument_name("options", kNamedOnly)
                        .set_must_be_immutable_constant());
    arguments.push_back(std::move(proto_options_arg));
  }
  return absl::OkStatus();
}

absl::Status GetBatchVectorSearchTVFSignatures(
    TypeFactory* type_factory, const GoogleSQLBuiltinFunctionOptions& options,
    NameToTableValuedFunctionMap* table_valued_functions,
    BuiltinsOutputProperties& output_properties,
    std::vector<FunctionSignatureOnHeap>& signatures) {
  std::vector<googlesql::FunctionArgumentType> common_vector_search_arguments =
      CommonVectorSearchArguments();
  GOOGLESQL_RET_CHECK_EQ(common_vector_search_arguments.size(), 5);
  for (const auto& [proto_options, signature_id] :
       std::vector<std::pair<bool, FunctionSignatureId>>{
           {true, FN_BATCH_VECTOR_SEARCH_TVF_WITH_PROTO_OPTIONS},
           {false, FN_BATCH_VECTOR_SEARCH_TVF_WITH_JSON_OPTIONS}}) {
    FunctionSignatureOptions function_signature_options;
    FunctionArgumentTypeList batch_vector_search_arguments = {
        // Base table.
        common_vector_search_arguments[0],
        // Column to search.
        common_vector_search_arguments[1],
        // Query data.
        {googlesql::FunctionArgumentType::AnyRelation()},
        // Query column to search.
        {googlesql::FunctionArgumentType(
            googlesql::types::StringType(),
            googlesql::FunctionArgumentTypeOptions()
                .set_cardinality(googlesql::FunctionArgumentType::OPTIONAL)
                .set_argument_name("query_column_to_search",
                                   googlesql::kPositionalOrNamed))}};
    if (proto_options) {
      GOOGLESQL_RETURN_IF_ERROR(MaybeAddVectorSearchTVFProtoOptionsArgument(
          options, "vector_search",
          FN_BATCH_VECTOR_SEARCH_TVF_WITH_PROTO_OPTIONS, output_properties,
          batch_vector_search_arguments, kBatchVectorSearchTVFOptionsArgIdx));
    } else {
      batch_vector_search_arguments.push_back({googlesql::FunctionArgumentType(
          googlesql::types::JsonType(),
          googlesql::FunctionArgumentTypeOptions()
              .set_cardinality(googlesql::FunctionArgumentType::OPTIONAL)
              .set_argument_name("options", googlesql::kNamedOnly)
              .set_must_be_immutable_constant())});
      function_signature_options.AddRequiredLanguageFeature(
          FEATURE_SINGLE_HYBRID_VECTOR_SEARCH_TVF);
    }
    // top_k
    batch_vector_search_arguments.push_back(common_vector_search_arguments[2]);
    // distance_type
    batch_vector_search_arguments.push_back(common_vector_search_arguments[3]);
    // max_distance
    batch_vector_search_arguments.push_back(common_vector_search_arguments[4]);

    signatures.push_back(FunctionSignatureOnHeap(
        /*result_type=*/googlesql::FunctionArgumentType::AnyRelation(),
        /*arguments=*/batch_vector_search_arguments,
        /*context_id=*/
        signature_id, function_signature_options));
  }
  return absl::OkStatus();
}

absl::Status GetSingleVectorSearchTVFSignatures(
    TypeFactory* type_factory, const GoogleSQLBuiltinFunctionOptions& options,
    NameToTableValuedFunctionMap* table_valued_functions,
    BuiltinsOutputProperties& output_properties,
    std::vector<FunctionSignatureOnHeap>& signatures) {
  for (const auto& [query_type, proto_signature_id, json_signature_id] :
       std::vector<std::tuple<const googlesql::Type*, FunctionSignatureId,
                              FunctionSignatureId>>{
           {googlesql::types::FloatArrayType(),
            FN_SINGLE_VECTOR_SEARCH_TVF_FLOAT_ARRAY_WITH_PROTO_OPTIONS,
            FN_SINGLE_VECTOR_SEARCH_TVF_FLOAT_ARRAY_WITH_JSON_OPTIONS},
           {googlesql::types::DoubleArrayType(),
            FN_SINGLE_VECTOR_SEARCH_TVF_DOUBLE_ARRAY_WITH_PROTO_OPTIONS,
            FN_SINGLE_VECTOR_SEARCH_TVF_DOUBLE_ARRAY_WITH_JSON_OPTIONS},
           {googlesql::types::StringType(),
            FN_SINGLE_VECTOR_SEARCH_TVF_STRING_WITH_PROTO_OPTIONS,
            FN_SINGLE_VECTOR_SEARCH_TVF_STRING_WITH_JSON_OPTIONS},
       }) {
    for (const auto& [proto_options, signature_id] :
         std::vector<std::pair<bool, FunctionSignatureId>>{
             {true, proto_signature_id}, {false, json_signature_id}}) {
      std::vector<googlesql::FunctionArgumentType>
          common_vector_search_arguments = CommonVectorSearchArguments();
      GOOGLESQL_RET_CHECK_EQ(common_vector_search_arguments.size(), 5);
      FunctionArgumentTypeList single_vector_search_arguments = {
          // Base table.
          common_vector_search_arguments[0],
          // Column to search.
          common_vector_search_arguments[1],
          // Query value.
          googlesql::FunctionArgumentType(
              query_type,
              googlesql::FunctionArgumentTypeOptions()
                  .set_argument_name("query_value", googlesql::kNamedOnly)
                  .set_must_be_non_null()),
      };

      if (proto_options) {
        GOOGLESQL_RETURN_IF_ERROR(MaybeAddVectorSearchTVFProtoOptionsArgument(
            options, "vector_search", signature_id, output_properties,
            single_vector_search_arguments,
            kSingleVectorSearchTVFOptionsArgIdx));
      } else {
        single_vector_search_arguments.push_back(
            {googlesql::FunctionArgumentType(
                googlesql::types::JsonType(),
                googlesql::FunctionArgumentTypeOptions()
                    .set_cardinality(googlesql::FunctionArgumentType::OPTIONAL)
                    .set_argument_name("options", googlesql::kNamedOnly)
                    .set_must_be_immutable_constant())});
      }

      // top_k
      single_vector_search_arguments.push_back(
          common_vector_search_arguments[2]);
      // distance_type
      single_vector_search_arguments.push_back(
          common_vector_search_arguments[3]);
      // max_distance
      single_vector_search_arguments.push_back(
          common_vector_search_arguments[4]);

      FunctionSignatureOptions function_signature_options;
      function_signature_options.AddRequiredLanguageFeature(
          FEATURE_SINGLE_HYBRID_VECTOR_SEARCH_TVF);
      signatures.push_back(FunctionSignatureOnHeap(
          /*result_type=*/googlesql::FunctionArgumentType::AnyRelation(),
          /*arguments=*/single_vector_search_arguments,
          /*context_id=*/
          signature_id, function_signature_options));
    }
  }
  return absl::OkStatus();
}

absl::Status GetBatchHybridVectorSearchSignatures(
    TypeFactory* type_factory, const GoogleSQLBuiltinFunctionOptions& options,
    NameToTableValuedFunctionMap* table_valued_functions,
    BuiltinsOutputProperties& output_properties,
    std::vector<FunctionSignatureOnHeap>& signatures) {
  for (const auto& [proto_options, signature_id] :
       std::vector<std::pair<bool, FunctionSignatureId>>{
           {true, FN_BATCH_HYBRID_VECTOR_SEARCH_TVF_WITH_PROTO_OPTIONS},
           {false, FN_BATCH_HYBRID_VECTOR_SEARCH_TVF_WITH_JSON_OPTIONS}}) {
    // Reusing vector_search related helper functions even though this is hybrid
    // search as there is commonality.
    std::vector<googlesql::FunctionArgumentType>
        common_vector_search_arguments = CommonVectorSearchArguments();
    GOOGLESQL_RET_CHECK_EQ(common_vector_search_arguments.size(), 5);

    FunctionArgumentTypeList hybrid_batch_vector_search_arguments = {
        // Base table.
        common_vector_search_arguments[0],
        // Column to search.
        common_vector_search_arguments[1],
        // Query table.
        {googlesql::FunctionArgumentType::AnyRelation()},
        // lexical_search_columns.
        {googlesql::FunctionArgumentType(
            googlesql::types::StringArrayType(),
            googlesql::FunctionArgumentTypeOptions()
                .set_argument_name("lexical_search_columns",
                                   googlesql::kNamedOnly)
                .set_must_be_non_null())},
        // Query column to search.
        {googlesql::FunctionArgumentType(
            googlesql::types::StringType(),
            googlesql::FunctionArgumentTypeOptions()
                .set_cardinality(googlesql::FunctionArgumentType::OPTIONAL)
                .set_argument_name("query_column_to_search",
                                   googlesql::kNamedOnly))},
        // lexical_search_query_column.
        {googlesql::FunctionArgumentType(
            googlesql::types::StringType(),
            googlesql::FunctionArgumentTypeOptions()
                .set_cardinality(googlesql::FunctionArgumentType::OPTIONAL)
                .set_argument_name("lexical_search_query_column",
                                   googlesql::kNamedOnly))},
    };

    if (proto_options) {
      // Reusing vector_search related helper functions even though this is
      // hybrid search as there is commonality.
      GOOGLESQL_RETURN_IF_ERROR(MaybeAddVectorSearchTVFProtoOptionsArgument(
          options, "hybrid_vector_search", signature_id, output_properties,
          hybrid_batch_vector_search_arguments,
          kHybridBatchVectorSearchTVFOptionsArgIdx));
    } else {
      hybrid_batch_vector_search_arguments.push_back(
          {googlesql::FunctionArgumentType(
              googlesql::types::JsonType(),
              googlesql::FunctionArgumentTypeOptions()
                  .set_cardinality(googlesql::FunctionArgumentType::OPTIONAL)
                  .set_argument_name("options", googlesql::kNamedOnly)
                  .set_must_be_immutable_constant())});
    }

    // top_k
    hybrid_batch_vector_search_arguments.push_back(
        common_vector_search_arguments[2]);
    // distance_type
    hybrid_batch_vector_search_arguments.push_back(
        common_vector_search_arguments[3]);
    // max_distance
    hybrid_batch_vector_search_arguments.push_back(
        common_vector_search_arguments[4]);

    signatures.push_back(FunctionSignatureOnHeap(
        /*result_type=*/googlesql::FunctionArgumentType::AnyRelation(),
        /*arguments=*/hybrid_batch_vector_search_arguments,
        /*context_id=*/
        signature_id));
  }
  return absl::OkStatus();
}

absl::Status GetHybridSingleVectorSearchSignatures(
    TypeFactory* type_factory, const GoogleSQLBuiltinFunctionOptions& options,
    NameToTableValuedFunctionMap* table_valued_functions,
    BuiltinsOutputProperties& output_properties,
    std::vector<FunctionSignatureOnHeap>& signatures) {
  for (const auto& [query_type, proto_signature_id, json_signature_id] :
       std::vector<std::tuple<const googlesql::Type*, FunctionSignatureId,
                              FunctionSignatureId>>{
           {googlesql::types::FloatArrayType(),
            FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_FLOAT_ARRAY_WITH_PROTO_OPTIONS,
            FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_FLOAT_ARRAY_WITH_JSON_OPTIONS},
           {googlesql::types::DoubleArrayType(),
            FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_DOUBLE_ARRAY_WITH_PROTO_OPTIONS,
            FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_DOUBLE_ARRAY_WITH_JSON_OPTIONS},
           {googlesql::types::StringType(),
            FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_STRING_WITH_PROTO_OPTIONS,
            FN_HYBRID_SINGLE_VECTOR_SEARCH_TVF_STRING_WITH_JSON_OPTIONS},
       }) {
    for (const auto& [proto_options, signature_id] :
         std::vector<std::pair<bool, FunctionSignatureId>>{
             {true, proto_signature_id}, {false, json_signature_id}}) {
      std::vector<googlesql::FunctionArgumentType>
          common_vector_search_arguments = CommonVectorSearchArguments();
      GOOGLESQL_RET_CHECK_EQ(common_vector_search_arguments.size(), 5);
      FunctionArgumentTypeList hybrid_single_vector_search_arguments = {
          // Base table.
          common_vector_search_arguments[0],
          // Column to search.
          common_vector_search_arguments[1],
          // Query value.
          googlesql::FunctionArgumentType(
              query_type,
              googlesql::FunctionArgumentTypeOptions()
                  .set_argument_name("query_value", googlesql::kNamedOnly)
                  .set_must_be_non_null()),
          // lexical_search_columns.
          googlesql::FunctionArgumentType(
              googlesql::types::StringArrayType(),
              googlesql::FunctionArgumentTypeOptions()
                  .set_argument_name("lexical_search_columns",
                                     googlesql::kNamedOnly)
                  .set_must_be_non_null()),
          // lexical_search_query_value.
          googlesql::FunctionArgumentType(
              googlesql::types::StringType(),
              googlesql::FunctionArgumentTypeOptions()
                  .set_argument_name("lexical_search_query_value",
                                     googlesql::kNamedOnly)
                  .set_must_be_non_null()),
      };

      if (proto_options) {
        GOOGLESQL_RETURN_IF_ERROR(MaybeAddVectorSearchTVFProtoOptionsArgument(
            options, "hybrid_vector_search", signature_id, output_properties,
            hybrid_single_vector_search_arguments,
            kHybridSingleVectorSearchTVFOptionsArgIdx));
      } else {
        hybrid_single_vector_search_arguments.push_back(
            {googlesql::FunctionArgumentType(
                googlesql::types::JsonType(),
                googlesql::FunctionArgumentTypeOptions()
                    .set_cardinality(googlesql::FunctionArgumentType::OPTIONAL)
                    .set_argument_name("options", googlesql::kNamedOnly)
                    .set_must_be_immutable_constant())});
      }

      // top_k
      hybrid_single_vector_search_arguments.push_back(
          common_vector_search_arguments[2]);
      // distance_type
      hybrid_single_vector_search_arguments.push_back(
          common_vector_search_arguments[3]);
      // max_distance
      hybrid_single_vector_search_arguments.push_back(
          common_vector_search_arguments[4]);

      signatures.push_back(FunctionSignatureOnHeap(
          /*result_type=*/googlesql::FunctionArgumentType::AnyRelation(),
          /*arguments=*/hybrid_single_vector_search_arguments,
          /*context_id=*/
          signature_id));
    }
  }
  return absl::OkStatus();
}

absl::Status InsertVectorSearchTVFSignatures(
    TypeFactory* type_factory, const GoogleSQLBuiltinFunctionOptions& options,
    NameToTableValuedFunctionMap* table_valued_functions,
    BuiltinsOutputProperties& output_properties) {
  std::vector<FunctionSignatureOnHeap> signatures;
  signatures.reserve(8);
  GOOGLESQL_RETURN_IF_ERROR(GetBatchVectorSearchTVFSignatures(
      type_factory, options, table_valued_functions, output_properties,
      signatures));
  GOOGLESQL_RETURN_IF_ERROR(GetSingleVectorSearchTVFSignatures(
      type_factory, options, table_valued_functions, output_properties,
      signatures));

  GOOGLESQL_RETURN_IF_ERROR(InsertSimpleTableValuedFunction(
      table_valued_functions, options, "vector_search", signatures,
      TableValuedFunctionOptions()
          .AddRequiredLanguageFeature(FEATURE_VECTOR_SEARCH_TVF)
          .set_post_resolution_argument_constraint(
              &CheckVectorSearchPostResolutionArguments)
          .set_compute_result_type_callback(
              &ComputeResultTypeForVectorSearchTVF)));
  return absl::OkStatus();
}

absl::Status InsertHybridVectorSearchTVFSignatures(
    TypeFactory* type_factory, const GoogleSQLBuiltinFunctionOptions& options,
    NameToTableValuedFunctionMap* table_valued_functions,
    BuiltinsOutputProperties& output_properties) {
  std::vector<FunctionSignatureOnHeap> signatures;
  signatures.reserve(8);
  GOOGLESQL_RETURN_IF_ERROR(GetBatchHybridVectorSearchSignatures(
      type_factory, options, table_valued_functions, output_properties,
      signatures));
  GOOGLESQL_RETURN_IF_ERROR(GetHybridSingleVectorSearchSignatures(
      type_factory, options, table_valued_functions, output_properties,
      signatures));

  GOOGLESQL_RETURN_IF_ERROR(InsertSimpleTableValuedFunction(
      table_valued_functions, options, "hybrid_vector_search", signatures,
      TableValuedFunctionOptions()
          .AddRequiredLanguageFeature(FEATURE_SINGLE_HYBRID_VECTOR_SEARCH_TVF)
          // Reusing vector_search related helper functions even though this is
          // hybrid search as there is commonality.
          .set_post_resolution_argument_constraint(
              &CheckVectorSearchPostResolutionArguments)
          .set_compute_result_type_callback(
              &ComputeResultTypeForVectorSearchTVF)));
  return absl::OkStatus();
}

absl::Status InsertKMeansTVFSignature(
    TypeFactory* type_factory, const GoogleSQLBuiltinFunctionOptions& options,
    NameToTableValuedFunctionMap* table_valued_functions) {
  const Type* kmeans_options_type;
  // Default number of clusters is 10.
  const Value kDefaultK = Value::Int64(10);
  GOOGLESQL_RETURN_IF_ERROR(type_factory->MakeProtoType(KMeansOptions::descriptor(),
                                              &kmeans_options_type));
  FunctionArgumentType kmeans_options_arg = FunctionArgumentType(
      kmeans_options_type,
      FunctionArgumentTypeOptions(FunctionArgumentType::OPTIONAL)
          .set_argument_name("options", kNamedOnly)
          .set_default(Value::Proto(kmeans_options_type->AsProto(),
                                    DefaultKMeansOptions().SerializeAsCord())));
  GOOGLESQL_RETURN_IF_ERROR(InsertSimpleTableValuedFunction(
      table_valued_functions, options, "kmeans",
      {{FunctionSignatureOnHeap(
          /*result_type=*/googlesql::FunctionArgumentType::AnyRelation(),
          /*arguments=*/
          FunctionArgumentTypeList{
              // Input table.
              {googlesql::FunctionArgumentType::AnyRelation()},
              // Column containing the vectors to cluster.
              {googlesql::FunctionArgumentType(
                  googlesql::types::StringType(),
                  googlesql::FunctionArgumentTypeOptions()
                      .set_argument_name("vectors_column",
                                         googlesql::kPositionalOnly)
                      .set_must_be_non_null()
                      .set_must_be_analysis_constant())},
              // Number of clusters (k).
              {googlesql::FunctionArgumentType(
                  googlesql::types::Int64Type(),
                  googlesql::FunctionArgumentTypeOptions()
                      .set_cardinality(
                          googlesql::FunctionArgumentType::OPTIONAL)
                      .set_argument_name("k", googlesql::kPositionalOrNamed)
                      .set_must_be_non_null()
                      .set_min_value(1)
                      .set_default(kDefaultK))},
              // KMeans options
              {kmeans_options_arg}},
          /*context_id=*/FN_KMEANS_TVF)}},
      TableValuedFunctionOptions().set_compute_result_type_callback(
          &ComputeResultTypeForKmeansTVF)));
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<TVFSignature>> ComputeLookupTableResultType(
    Catalog* catalog, TypeFactory* type_factory,
    const FunctionSignature& signature,
    const std::vector<TVFInputArgumentType>& actual_arguments,
    const AnalyzerOptions& analyzer_options) {
  GOOGLESQL_RET_CHECK_EQ(actual_arguments.size(), 1);
  const ResolvedExpr* scalar_expr = actual_arguments[0].scalar_expr();
  GOOGLESQL_RET_CHECK_NE(scalar_expr, nullptr);

  Value table_name_value;
  if (scalar_expr->Is<ResolvedLiteral>()) {
    table_name_value = scalar_expr->GetAs<ResolvedLiteral>()->value();
  } else {
    GOOGLESQL_RET_CHECK(analyzer_options.constant_evaluator() != nullptr)
        << "LookupTable TVF requires a constant evaluator, but none was "
           "configured in AnalyzerOptions.";
    GOOGLESQL_ASSIGN_OR_RETURN(
        table_name_value,
        analyzer_options.constant_evaluator()->Evaluate(*scalar_expr));
  }
  if (table_name_value.is_null()) {
    return MakeSqlError() << "The argument must be non-NULL";
  }
  GOOGLESQL_RET_CHECK(table_name_value.type()->IsString());
  absl::string_view tbl_name = table_name_value.string_value();

  std::vector<std::string> path;
  GOOGLESQL_RETURN_IF_ERROR(
      ParseIdentifierPath(tbl_name, analyzer_options.language(), &path));

  const Table* table = nullptr;
  GOOGLESQL_RETURN_IF_ERROR(
      catalog->FindTable(path, &table, analyzer_options.find_options()));
  GOOGLESQL_RET_CHECK_NE(table, nullptr);

  TVFSignatureOptions tvf_signature_options;
  tvf_signature_options.additional_deprecation_warnings =
      signature.AdditionalDeprecationWarnings();
  tvf_signature_options.row_type_rewrite_callback =
      TVFSignature::BaseRowTypeRewriteCallback;
  GOOGLESQL_ASSIGN_OR_RETURN(
      std::shared_ptr<TVFSignature> tvf_signature,
      TVFSignature::Create(actual_arguments, table, tvf_signature_options));
  // Propagate AnonymizationInfo from the resolved catalog Table to the
  // TVFSignature so that anonymization / differential privacy queries and
  // rewrites can identify that the TVF scan reads private user data and access
  // its userid column metadata.
  if (table->GetAnonymizationInfo().has_value()) {
    tvf_signature->SetAnonymizationInfo(
        std::make_unique<AnonymizationInfo>(*table->GetAnonymizationInfo()));
  }
  return tvf_signature;
}

}  // namespace

absl::Status GetVectorSearchTableValuedFunctions(
    TypeFactory* type_factory, const GoogleSQLBuiltinFunctionOptions& options,
    NameToTableValuedFunctionMap* table_valued_functions,
    BuiltinsOutputProperties& output_properties) {
  GOOGLESQL_RETURN_IF_ERROR(InsertVectorSearchTVFSignatures(
      type_factory, options, table_valued_functions, output_properties));
  GOOGLESQL_RETURN_IF_ERROR(InsertHybridVectorSearchTVFSignatures(
      type_factory, options, table_valued_functions, output_properties));
  return absl::OkStatus();
}

absl::Status GetKMeansTableValuedFunction(
    TypeFactory* type_factory, const GoogleSQLBuiltinFunctionOptions& options,
    NameToTableValuedFunctionMap* table_valued_functions) {
  GOOGLESQL_RETURN_IF_ERROR(
      InsertKMeansTVFSignature(type_factory, options, table_valued_functions));
  return absl::OkStatus();
}

absl::Status GetLookupTableTableValuedFunction(
    TypeFactory* type_factory, const GoogleSQLBuiltinFunctionOptions& options,
    NameToTableValuedFunctionMap* table_valued_functions) {
  GOOGLESQL_RETURN_IF_ERROR(InsertSimpleTableValuedFunction(
      table_valued_functions, options, "LookupTable",
      {{FunctionSignatureOnHeap(
          /*result_type=*/FunctionArgumentType::AnyRelation(),
          /*arguments=*/
          FunctionArgumentTypeList{
              FunctionArgumentType(type_factory->get_string(),
                                   FunctionArgumentTypeOptions()
                                       .set_must_be_non_null()
                                       .set_must_be_immutable_constant())},
          /*context_id=*/FN_TVF_LOOKUP_TABLE)}},
      TableValuedFunctionOptions()
          .AddRequiredLanguageFeature(FEATURE_LOOKUP_TABLE_TVF)
          .set_compute_result_type_callback(&ComputeLookupTableResultType)));
  return absl::OkStatus();
}

}  // namespace googlesql
