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

#include <array>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "zetasql/common/builtin_function_internal.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/algorithm/container.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

static constexpr std::array<absl::string_view, 3> kAllowedDistanceTypes = {
    "COSINE", "DOT_PRODUCT", "EUCLIDEAN"};

// Checks if a column with the given `column_name` exists in the `relation_arg`
// and returns an error if it does not.
absl::StatusOr<TVFSchemaColumn> FindColumnInRelation(
    const TVFInputArgumentType& relation_arg, absl::string_view column_name) {
  for (const auto& column : relation_arg.relation().columns()) {
    if (zetasql_base::CaseEqual(column.name, column_name)) {
      return column;
    }
  }
  return absl::InvalidArgumentError(
      absl::Substitute("Unrecognized name: $0 in table $1", column_name,
                       relation_arg.DebugString()));
}

// Validates that the given `column_name_arg` is a constant string that refers
// to a column in `relation_arg` of type ARRAY<DOUBLE>. Returns the appropriate
// error if not. This is used for both the `column_to_search` and
// `query_column_to_search` arguments of the `vector_search` TVF.
absl::Status ValidateColumnToSearchArgument(
    const TVFInputArgumentType& column_name_arg,
    const TVFInputArgumentType& relation_arg, std::string_view argument_name) {
  if (!column_name_arg.is_scalar() ||
      !column_name_arg.GetScalarArgType()->type()->IsString()) {
    return absl::InvalidArgumentError(absl::Substitute(
        "$0 argument of vector_search TVF must be a scalar of type STRING",
        ToAlwaysQuotedIdentifierLiteral(argument_name)));
  }
  const ResolvedExpr* scalar_expr = column_name_arg.scalar_expr();
  if (scalar_expr == nullptr || !scalar_expr->Is<ResolvedLiteral>()) {
    return absl::InvalidArgumentError(absl::Substitute(
        "$0 argument of vector_search TVF must be a constant STRING literal",
        ToAlwaysQuotedIdentifierLiteral(argument_name)));
  }

  const ResolvedLiteral* literal = scalar_expr->GetAs<ResolvedLiteral>();
  // The default value of query_column_to_search is NULL, which is a valid value
  if (literal->value().is_null()) {
    return absl::OkStatus();
  }
  std::string column_to_search_name = literal->value().string_value();

  ZETASQL_ASSIGN_OR_RETURN(TVFSchemaColumn column_to_search,
                   FindColumnInRelation(relation_arg, column_to_search_name));
  if (!column_to_search.type->IsArray() ||
      !column_to_search.type->AsArray()->element_type()->IsDouble()) {
    return absl::InvalidArgumentError(absl::Substitute(
        "The column specified by the $0 "
        "argument of vector_search TVF must be an array of doubles",
        ToAlwaysQuotedIdentifierLiteral(argument_name)));
  }
  return absl::OkStatus();
}

bool IsValidDistanceType(absl::string_view distance_type) {
  return absl::c_linear_search(kAllowedDistanceTypes, distance_type);
}

absl::Status CheckVectorSearchPostResolutionArguments(
    const FunctionSignature& signature,
    const std::vector<TVFInputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  // The first argument is the base table, which must be a relation.
  const TVFInputArgumentType& base_table_arg = arguments[0];

  // The second argument is the column to search, which must be a constant
  // string literal that refers to a column in the base table of type
  // ARRAY<DOUBLE>.
  ZETASQL_RETURN_IF_ERROR(ValidateColumnToSearchArgument(
      arguments[1], base_table_arg,
      signature.arguments()[1].options().argument_name()));

  // The third argument is the query_table, which must be a relation.
  const TVFInputArgumentType& query_table_arg = arguments[2];

  // The fourth argument is the query column to search, which must be a constant
  // string literal that refers to a column in the query table of type
  // ARRAY<DOUBLE>. If not provided, it defaults to the column specified by the
  // second argument (column_to_search) although that's not enforced here.
  ZETASQL_RETURN_IF_ERROR(ValidateColumnToSearchArgument(
      arguments[3], query_table_arg,
      signature.arguments()[3].options().argument_name()));

  // The fifth argument is distance type of type STRING.
  const zetasql::TVFInputArgumentType& distance_type_argument = arguments[5];
  if (!distance_type_argument.is_scalar() ||
      !distance_type_argument.GetScalarArgType()->type()->IsString()) {
    return absl::InvalidArgumentError(absl::Substitute(
        "$0 argument of vector_search TVF must be a scalar of type STRING",
        ToAlwaysQuotedIdentifierLiteral(
            signature.arguments()[5].options().argument_name())));
  }

  std::string distance_type =
      absl::AsciiStrToUpper(distance_type_argument.scalar_expr()
                                ->GetAs<ResolvedLiteral>()
                                ->value()
                                .string_value());

  // Check if the distance type is one of the allowed distance types.
  if (!IsValidDistanceType(distance_type)) {
    return absl::InvalidArgumentError(absl::Substitute(
        "`$0` argument of vector_search TVF must be set to one of $1",
        signature.arguments()[5].options().argument_name(),
        absl::StrJoin(kAllowedDistanceTypes, " or ")));
  }

  if (!arguments[7].GetScalarArgType()->is_literal_for_constness()) {
    return absl::InvalidArgumentError(absl::Substitute(
        "`$0` Argument of vector_search TVF must be a STRING literal",
        signature.arguments()[7].options().argument_name()));
  }
  std::string options = arguments[7]
                            .scalar_expr()
                            ->GetAs<ResolvedLiteral>()
                            ->value()
                            .string_value();
  absl::StatusOr<JSONValue> json_value = JSONValue::ParseJSONString(options);

  if (!json_value.ok() || !json_value.value().GetConstRef().IsObject()) {
    return absl::InvalidArgumentError(
        absl::Substitute("`$0` Argument of vector_search TVF must be a valid "
                         "JSON-formatted string literal",
                         signature.arguments()[7].options().argument_name()));
  }

  return absl::OkStatus();
}

absl::StatusOr<zetasql::TVFRelation::Column> BuildStructColumn(
    zetasql::TypeFactory* type_factory,
    const zetasql::TVFRelation& relation, std::string_view output_name) {
  std::vector<zetasql::StructField> struct_fields;
  for (const auto& column : relation.columns()) {
    struct_fields.push_back({column.name, column.type});
  }
  const zetasql::Type* struct_type;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeStructType(struct_fields, &struct_type));
  return zetasql::TVFRelation::Column(output_name, struct_type);
}

absl::StatusOr<std::unique_ptr<TVFSignature>>
ComputeResultTypeForVectorSearchTVF(
    Catalog* catalog, TypeFactory* type_factory,
    const FunctionSignature& signature,
    const std::vector<TVFInputArgumentType>& input_arguments,
    const AnalyzerOptions& analyzer_options) {
  ZETASQL_ASSIGN_OR_RETURN(
      auto base_struct_column,
      BuildStructColumn(type_factory, input_arguments[0].relation(), "base"));

  ZETASQL_ASSIGN_OR_RETURN(
      auto query_struct_column,
      BuildStructColumn(type_factory, input_arguments[2].relation(), "query"));

  std::vector<TVFRelation::Column> output_columns = {
      query_struct_column, base_struct_column,
      zetasql::TVFRelation::Column("distance", type_factory->get_double())};

  zetasql::TVFRelation output_schema(output_columns);
  return std::make_unique<TVFSignature>(input_arguments, output_schema);
}

}  // namespace

absl::Status GetVectorSearchTableValuedFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToTableValuedFunctionMap* table_valued_functions) {
  FunctionArgumentType json_options_arg = FunctionArgumentType(
      types::StringType(),
      FunctionArgumentTypeOptions(FunctionArgumentType::OPTIONAL)
          .set_argument_name("options", kNamedOnly)
          .set_must_be_constant()
          .set_default(Value::String(
              "{}")));  // Defaults to "{}" to denote that all underlying
                        // options use their corresponding default values
  ZETASQL_RETURN_IF_ERROR(InsertSimpleTableValuedFunction(
      table_valued_functions, options, "vector_search",
      {{FunctionSignatureOnHeap(
          /*result_type=*/zetasql::FunctionArgumentType::AnyRelation(),
          /*arguments=*/
          FunctionArgumentTypeList{
              // Base table.
              {zetasql::FunctionArgumentType::AnyRelation()},
              // Column to search.
              {zetasql::FunctionArgumentType(
                  zetasql::types::StringType(),
                  zetasql::FunctionArgumentTypeOptions()
                      .set_must_be_constant()
                      .set_argument_name("column_to_search", kPositionalOnly))},
              // Query data.
              {zetasql::FunctionArgumentType::AnyRelation()},
              // Query column to search.
              {zetasql::FunctionArgumentType(
                  zetasql::types::StringType(),
                  zetasql::FunctionArgumentTypeOptions()
                      .set_must_be_constant()
                      .set_cardinality(
                          zetasql::FunctionArgumentType::OPTIONAL)
                      .set_argument_name("query_column_to_search",
                                         zetasql::kPositionalOrNamed))},
              // top_k
              {zetasql::FunctionArgumentType(
                  zetasql::types::Int64Type(),
                  zetasql::FunctionArgumentTypeOptions()
                      .set_cardinality(
                          zetasql::FunctionArgumentType::OPTIONAL)
                      .set_argument_name("top_k", zetasql::kNamedOnly)
                      .set_default(Value::Int64(10)))},
              // distance_type
              {zetasql::FunctionArgumentType(
                  zetasql::types::StringType(),
                  zetasql::FunctionArgumentTypeOptions()
                      .set_argument_name("distance_type", zetasql::kNamedOnly)
                      .set_default(Value::String("EUCLIDEAN"))
                      .set_must_be_constant()
                      .set_cardinality(
                          zetasql::FunctionArgumentType::OPTIONAL))},
              // max_distance
              {zetasql::FunctionArgumentType(
                  zetasql::types::DoubleType(),
                  zetasql::FunctionArgumentTypeOptions()
                      .set_argument_name("max_distance", zetasql::kNamedOnly)
                      .set_default(Value::Null(zetasql::types::DoubleType()))
                      .set_cardinality(
                          zetasql::FunctionArgumentType::OPTIONAL))},
              // options
              {json_options_arg}},
          /*context_id=*/FN_VECTOR_SEARCH_TVF)}},
      TableValuedFunctionOptions()
          .AddRequiredLanguageFeature(FEATURE_VECTOR_SEARCH_TVF)
          .set_post_resolution_argument_constraint(
              &CheckVectorSearchPostResolutionArguments)
          .set_compute_result_type_callback(
              &ComputeResultTypeForVectorSearchTVF)));
  return absl::OkStatus();
}

}  // namespace zetasql
