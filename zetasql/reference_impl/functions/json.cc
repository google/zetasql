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

#include "zetasql/reference_impl/functions/json.h"

#include "zetasql/public/functions/json.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/reference_impl/function.h"
#include "zetasql/base/statusor.h"

namespace zetasql {
namespace {

// Implementation of:
// JSON_EXTRACT/JSON_QUERY(string, string) -> string
// JSON_EXTRACT/JSON_QUERY(json, string) -> json
// JSON_EXTRACT_SCALAR/JSON_VALUE(string, string) -> string
// JSON_EXTRACT_SCALAR/JSON_VALUE(json, string) -> string
class JsonFunction : public SimpleBuiltinScalarFunction {
 public:
  JsonFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {
    DCHECK(output_type == types::JsonType() ||
           output_type == types::StringType());
  }
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class JsonExtractArrayFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit JsonExtractArrayFunction()
      : SimpleBuiltinScalarFunction(FunctionKind::kJsonExtractArray,
                                    types::StringArrayType()) {}
  zetasql_base::StatusOr<Value> Eval(absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

// Helper function for the string version of JSON_QUERY, JSON_VALUE,
// JSON_EXTRACT and JSON_EXTRACT_SCALAR.
zetasql_base::StatusOr<Value> JsonExtractString(
    const functions::JsonPathEvaluator& evaluator, absl::string_view json,
    bool scalar) {
  std::string output;
  bool is_null;

  if (scalar) {
    ZETASQL_RETURN_IF_ERROR(evaluator.ExtractScalar(json, &output, &is_null));
  } else {
    ZETASQL_RETURN_IF_ERROR(evaluator.Extract(json, &output, &is_null));
  }
  if (!is_null) {
    return Value::String(std::move(output));
  }
  return Value::NullString();
}

// Helper function for the JSON version of JSON_QUERY, JSON_VALUE,
// JSON_EXTRACT and JSON_EXTRACT_SCALAR.
zetasql_base::StatusOr<Value> JsonExtractJson(
    const functions::JsonPathEvaluator& evaluator, const Value& json,
    const Type* output_type, bool scalar, bool json_legacy_parsing_mode) {
  if (scalar) {
    absl::optional<std::string> output_string_or;
    if (json.is_validated_json()) {
      output_string_or = evaluator.ExtractScalar(json.json_value());
    } else {
      ZETASQL_ASSIGN_OR_RETURN(JSONValue input_json,
                       JSONValue::ParseJSONString(json.json_value_unparsed(),
                                                  json_legacy_parsing_mode));
      output_string_or = evaluator.ExtractScalar(input_json.GetConstRef());
    }
    if (output_string_or.has_value()) {
      return Value::String(std::move(output_string_or).value());
    }
  } else {
    absl::optional<JSONValueConstRef> output_json_or;
    if (json.is_validated_json()) {
      output_json_or = evaluator.Extract(json.json_value());
    } else {
      ZETASQL_ASSIGN_OR_RETURN(JSONValue input_json,
                       JSONValue::ParseJSONString(json.json_value_unparsed(),
                                                  json_legacy_parsing_mode));
      output_json_or = evaluator.Extract(input_json.GetConstRef());
    }
    if (output_json_or.has_value()) {
      return Value::Json(JSONValue::CopyFrom(output_json_or.value()));
    }
  }
  return Value::Null(output_type);
}

zetasql_base::StatusOr<Value> JsonFunction::Eval(absl::Span<const Value> args,
                                         EvaluationContext* context) const {
  DCHECK_EQ(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  // Note that since the second argument, json_path, is always a constant, it
  // would be better performance-wise just to create the JsonPathEvaluator once.
  bool sql_standard_mode = (kind() == FunctionKind::kJsonQuery ||
                            kind() == FunctionKind::kJsonValue);

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<functions::JsonPathEvaluator> evaluator,
      functions::JsonPathEvaluator::Create(
          /*json_path=*/args[1].string_value(), sql_standard_mode));
  evaluator->enable_special_character_escaping();
  bool scalar = kind() == FunctionKind::kJsonValue ||
                kind() == FunctionKind::kJsonExtractScalar;
  if (args[0].type_kind() == TYPE_STRING) {
    return JsonExtractString(*evaluator, args[0].string_value(),
                             scalar);
  } else {
    return JsonExtractJson(*evaluator, args[0], output_type(), scalar,
                           context->GetLanguageOptions().LanguageFeatureEnabled(
                               FEATURE_JSON_LEGACY_PARSE));
  }
}

zetasql_base::StatusOr<Value> JsonExtractArrayFunction::Eval(
    absl::Span<const Value> args, EvaluationContext* context) const {
  DCHECK_GE(args.size(), 1);
  DCHECK_LE(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(types::StringArrayType());
  }
  std::string json_path = args.size() == 2 ? args[1].string_value() : "$";

  // sql_standard_mode is set to false for all JSON_EXTRACT functions to keep
  // the JSONPath syntax the same.
  ZETASQL_ASSIGN_OR_RETURN(
      const std::unique_ptr<functions::JsonPathEvaluator> evaluator,
      functions::JsonPathEvaluator::Create(json_path,
                                           /*sql_standard_mode=*/false));
  evaluator->enable_special_character_escaping();
  std::vector<std::string> output;
  bool is_null = false;
  ZETASQL_RETURN_IF_ERROR(
      evaluator->ExtractArray(args[0].string_value(), &output, &is_null));
  if (is_null) {
    return Value::Null(types::StringArrayType());
  }
  return values::StringArray(output);
}

}  // namespace

void RegisterBuiltinJsonFunctions() {
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kJsonExtract, FunctionKind::kJsonExtractScalar,
       FunctionKind::kJsonQuery, FunctionKind::kJsonValue},
      [](FunctionKind kind, const Type* output_type) {
        return new JsonFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kJsonExtractArray},
      [](FunctionKind kind, const Type* output_type) {
        return new JsonExtractArrayFunction();
      });
}

}  // namespace zetasql
