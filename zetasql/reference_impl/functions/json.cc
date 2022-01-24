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

#include <cstdint>

#include "zetasql/common/errors.h"
#include "zetasql/public/functions/json.h"
#include "zetasql/public/functions/json_format.h"
#include "zetasql/public/functions/to_json.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/reference_impl/function.h"
#include "absl/status/statusor.h"
#include "zetasql/base/ret_check.h"

namespace zetasql {
namespace {

// Sets the provided EvaluationContext to have non-deterministic output based on
// <arg> type.
void MaybeSetNonDeterministicContext(const Value& arg,
                                     EvaluationContext* context) {
  if (!context->IsDeterministicOutput()) {
    return;
  }
  const Type* arg_type = arg.type();
  if (HasFloatingPoint(arg_type)) {
    context->SetNonDeterministicOutput();
  } else if (arg_type->IsArray()) {
    // For non-order-preserving arrays, mark the current evaluation context
    // as non-deterministic. (See e.g. b/38248983.)
    MaybeSetNonDeterministicArrayOutput(arg, context);
  }
}

// Implementation of:
// JSON_EXTRACT/JSON_QUERY(string, string) -> string
// JSON_EXTRACT/JSON_QUERY(json, string) -> json
// JSON_EXTRACT_SCALAR/JSON_VALUE(string[, string]) -> string
// JSON_EXTRACT_SCALAR/JSON_VALUE(json[, string]) -> string
class JsonFunction : public SimpleBuiltinScalarFunction {
 public:
  JsonFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {
    ZETASQL_DCHECK(output_type->Equals(types::JsonType()) ||
           output_type->Equals(types::StringType()));
  }
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class JsonArrayFunction : public SimpleBuiltinScalarFunction {
 public:
  JsonArrayFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {
    ZETASQL_DCHECK(output_type->Equals(types::JsonArrayType()) ||
           output_type->Equals(types::StringArrayType()));
  }
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class JsonSubscriptFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit JsonSubscriptFunction()
      : SimpleBuiltinScalarFunction(FunctionKind::kSubscript,
                                    types::JsonType()) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class ToJsonFunction : public SimpleBuiltinScalarFunction {
 public:
  ToJsonFunction()
      : SimpleBuiltinScalarFunction(FunctionKind::kToJson, types::JsonType()) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class ToJsonStringFunction : public SimpleBuiltinScalarFunction {
 public:
  ToJsonStringFunction()
      : SimpleBuiltinScalarFunction(FunctionKind::kToJsonString,
                                    types::StringType()) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class ParseJsonFunction : public SimpleBuiltinScalarFunction {
 public:
  ParseJsonFunction()
      : SimpleBuiltinScalarFunction(FunctionKind::kParseJson,
                                    types::JsonType()) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

// Helper function for the string version of JSON_QUERY, JSON_VALUE,
// JSON_EXTRACT and JSON_EXTRACT_SCALAR.
absl::StatusOr<Value> JsonExtractString(
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
absl::StatusOr<Value> JsonExtractJson(
    const functions::JsonPathEvaluator& evaluator, const Value& json,
    const Type* output_type, bool scalar, JSONParsingOptions parsing_options) {
  if (scalar) {
    absl::optional<std::string> output_string_or;
    if (json.is_validated_json()) {
      output_string_or = evaluator.ExtractScalar(json.json_value());
    } else {
      ZETASQL_ASSIGN_OR_RETURN(JSONValue input_json,
                       JSONValue::ParseJSONString(json.json_value_unparsed(),
                                                  parsing_options));
      output_string_or = evaluator.ExtractScalar(input_json.GetConstRef());
    }
    if (output_string_or.has_value()) {
      return Value::String(std::move(output_string_or).value());
    }
  } else {
    absl::optional<JSONValueConstRef> output_json_or;
    JSONValue input_json;
    if (json.is_validated_json()) {
      output_json_or = evaluator.Extract(json.json_value());
    } else {
      ZETASQL_ASSIGN_OR_RETURN(input_json,
                       JSONValue::ParseJSONString(json.json_value_unparsed(),
                                                  parsing_options));
      output_json_or = evaluator.Extract(input_json.GetConstRef());
    }
    if (output_json_or.has_value()) {
      return Value::Json(JSONValue::CopyFrom(output_json_or.value()));
    }
  }
  return Value::Null(output_type);
}

absl::StatusOr<Value> JsonSubscriptFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  absl::optional<JSONValueConstRef> json_value_const_ref;
  JSONValue input_json;
  if (args[0].is_validated_json()) {
    json_value_const_ref = args[0].json_value();
  } else {
    const auto& language_options = context->GetLanguageOptions();
    ZETASQL_ASSIGN_OR_RETURN(
        input_json,
        JSONValue::ParseJSONString(
            args[0].json_value_unparsed(),
            JSONParsingOptions{
                .legacy_mode = language_options.LanguageFeatureEnabled(
                    FEATURE_JSON_LEGACY_PARSE),
                .strict_number_parsing =
                    language_options.LanguageFeatureEnabled(
                        FEATURE_JSON_STRICT_NUMBER_PARSING)}));
    json_value_const_ref = input_json.GetConstRef();
  }
  ZETASQL_RET_CHECK(json_value_const_ref.has_value());
  absl::optional<JSONValueConstRef> member_const_ref;
  if (args[1].type_kind() == TYPE_STRING) {
    member_const_ref =
        json_value_const_ref->GetMemberIfExists(args[1].string_value());
  } else {
    int64_t index = args[1].int64_value();
    if (json_value_const_ref->IsArray() && index >= 0 &&
        index < json_value_const_ref->GetArraySize()) {
      member_const_ref = json_value_const_ref->GetArrayElement(index);
    }
  }
  return member_const_ref.has_value()
             ? Value::Json(JSONValue::CopyFrom(*member_const_ref))
             : Value::Null(output_type());
}

absl::StatusOr<Value> JsonFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (kind() == FunctionKind::kJsonValue ||
      kind() == FunctionKind::kJsonExtractScalar) {
    ZETASQL_RET_CHECK_LE(args.size(), 2);
    ZETASQL_RET_CHECK_GE(args.size(), 1);
  } else {
    ZETASQL_RET_CHECK_EQ(args.size(), 2);
  }
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
          /*json_path=*/((args.size() > 1) ? args[1].string_value() : "$"),
          sql_standard_mode));
  evaluator->enable_special_character_escaping();
  bool scalar = kind() == FunctionKind::kJsonValue ||
                kind() == FunctionKind::kJsonExtractScalar;
  if (args[0].type_kind() == TYPE_STRING) {
    return JsonExtractString(*evaluator, args[0].string_value(),
                             scalar);
  } else {
    const auto& language_options = context->GetLanguageOptions();
    return JsonExtractJson(
        *evaluator, args[0], output_type(), scalar,
        JSONParsingOptions{
            .legacy_mode = language_options.LanguageFeatureEnabled(
                FEATURE_JSON_LEGACY_PARSE),
            .strict_number_parsing = language_options.LanguageFeatureEnabled(
                FEATURE_JSON_STRICT_NUMBER_PARSING)});
  }
}

// Helper function for the string version of JSON_VALUE_ARRAY and
// JSON_EXTRACT_STRING_ARRAY.
absl::StatusOr<Value> JsonExtractStringArrayString(
    const functions::JsonPathEvaluator& evaluator, absl::string_view json) {
  std::vector<absl::optional<std::string>> output;
  bool is_null;

  ZETASQL_RETURN_IF_ERROR(evaluator.ExtractStringArray(json, &output, &is_null));
  if (!is_null) {
    std::vector<Value> result_array;
    result_array.reserve(output.size());
    for (auto& element : output) {
      result_array.push_back(element.has_value()
                                 ? values::String(std::move(*element))
                                 : values::NullString());
    }
    // To avoid a copy, we use the unsafe version. However this is actually
    // safe, because each element of the array is the same type (String).
    return values::UnsafeArray(types::StringArrayType(),
                               std::move(result_array));
  }
  return Value::Null(types::StringArrayType());
}

// Helper function for the string version of JSON_QUERY_ARRAY and
// JSON_EXTRACT_ARRAY.
absl::StatusOr<Value> JsonExtractArrayString(
    const functions::JsonPathEvaluator& evaluator, absl::string_view json) {
  std::vector<std::string> output;
  bool is_null;

  ZETASQL_RETURN_IF_ERROR(evaluator.ExtractArray(json, &output, &is_null));
  if (!is_null) {
    return values::StringArray(output);
  }
  return Value::Null(types::StringArrayType());
}

// Helper function for the JSON version of JSON_VALUE_ARRAY and
// JSON_EXTRACT_STRING_ARRAY.
absl::StatusOr<Value> JsonExtractStringArrayJson(
    const functions::JsonPathEvaluator& evaluator, const Value& json,
    JSONParsingOptions parsing_options) {
  absl::optional<std::vector<absl::optional<std::string>>> output;
  if (json.is_validated_json()) {
    output = evaluator.ExtractStringArray(json.json_value());
  } else {
    ZETASQL_ASSIGN_OR_RETURN(JSONValue input_json,
                     JSONValue::ParseJSONString(json.json_value_unparsed(),
                                                parsing_options));
    output = evaluator.ExtractStringArray(input_json.GetConstRef());
  }
  if (output.has_value()) {
    std::vector<Value> result_array;
    result_array.reserve(output->size());
    for (auto& element : *output) {
      result_array.push_back(element.has_value()
                                 ? values::String(std::move(*element))
                                 : values::NullString());
    }
    // To avoid a copy, we use the unsafe version. However this is actually
    // safe, because each element of the array is the same type (String).
    return values::UnsafeArray(types::StringArrayType(),
                               std::move(result_array));
  }
  return Value::Null(types::StringArrayType());
}

// Helper function for the JSON version of JSON_QUERY_ARRAY and
// JSON_EXTRACT_ARRAY.
absl::StatusOr<Value> JsonExtractArrayJson(
    const functions::JsonPathEvaluator& evaluator, const Value& json,
    JSONParsingOptions parsing_options) {
  absl::optional<std::vector<JSONValueConstRef>> output;
  JSONValue input_json;
  if (json.is_validated_json()) {
    output = evaluator.ExtractArray(json.json_value());
  } else {
    ZETASQL_ASSIGN_OR_RETURN(input_json,
                     JSONValue::ParseJSONString(json.json_value_unparsed(),
                                                parsing_options));
    output = evaluator.ExtractArray(input_json.GetConstRef());
  }
  if (output.has_value()) {
    std::vector<Value> json_value_array;
    json_value_array.reserve(output->size());
    for (const auto& json_element : *output) {
      json_value_array.push_back(
          Value::Json(JSONValue::CopyFrom(json_element)));
    }
    // To avoid a copy, we use the unsafe version. However this is actually
    // safe, because each element of the array is the same type (Json).
    return Value::UnsafeArray(types::JsonArrayType(),
                              std::move(json_value_array));
  }
  return Value::Null(types::JsonArrayType());
}

absl::StatusOr<Value> JsonArrayFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_GE(args.size(), 1);
  ZETASQL_RET_CHECK_LE(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  // Note that since the second argument, json_path, is always a constant, it
  // would be better performance-wise just to create the JsonPathEvaluator once.
  bool sql_standard_mode = (kind() == FunctionKind::kJsonQueryArray ||
                            kind() == FunctionKind::kJsonValueArray);

  std::string json_path = args.size() == 2 ? args[1].string_value() : "$";

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<functions::JsonPathEvaluator> evaluator,
      functions::JsonPathEvaluator::Create(json_path, sql_standard_mode));
  evaluator->enable_special_character_escaping();
  bool scalar = kind() == FunctionKind::kJsonValueArray ||
                kind() == FunctionKind::kJsonExtractStringArray;
  if (args[0].type_kind() == TYPE_STRING) {
    return scalar ? JsonExtractStringArrayString(*evaluator,
                                                 args[0].string_value())
                  : JsonExtractArrayString(*evaluator, args[0].string_value());
  } else {
    const auto& language_options = context->GetLanguageOptions();
    JSONParsingOptions parsing_options{
        .legacy_mode =
            language_options.LanguageFeatureEnabled(FEATURE_JSON_LEGACY_PARSE),
        .strict_number_parsing = language_options.LanguageFeatureEnabled(
            FEATURE_JSON_STRICT_NUMBER_PARSING)};

    return scalar ? JsonExtractStringArrayJson(*evaluator, args[0],
                                               parsing_options)
                  : JsonExtractArrayJson(*evaluator, args[0], parsing_options);
  }
}

absl::StatusOr<Value> ToJsonFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);
  if (args[1].is_null()) {
    return Value::Null(output_type());
  }
  const bool stringify_wide_numbers = args[1].bool_value();
  ZETASQL_ASSIGN_OR_RETURN(JSONValue outputJson,
                   functions::ToJson(args[0], stringify_wide_numbers,
                                     context->GetLanguageOptions()));
  MaybeSetNonDeterministicContext(args[0], context);
  return Value::Json(std::move(outputJson));
}

absl::StatusOr<Value> ToJsonStringFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (args.size() == 2 && args[1].is_null()) {
    return Value::Null(output_type());
  }

  const bool pretty_print = args.size() == 2 ? args[1].bool_value() : false;
  functions::JsonPrettyPrinter pretty_printer(
      pretty_print, context->GetLanguageOptions().product_mode());
  std::string output;
  ZETASQL_RETURN_IF_ERROR(functions::JsonFromValue(
      args[0], &pretty_printer, &output,
      JSONParsingOptions{
          .legacy_mode = context->GetLanguageOptions().LanguageFeatureEnabled(
              FEATURE_JSON_LEGACY_PARSE),
          .strict_number_parsing =
              context->GetLanguageOptions().LanguageFeatureEnabled(
                  FEATURE_JSON_STRICT_NUMBER_PARSING)}));
  MaybeSetNonDeterministicContext(args[0], context);
  return Value::String(output);
}

absl::StatusOr<Value> ParseJsonFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);
  if (args[0].is_null() || args[1].is_null()) {
    return Value::NullJson();
  }
  // TODO : Add support for ‘stringify’ parsing mode
  if (args[1].string_value() != "exact" && args[1].string_value() != "round") {
    return MakeEvalError()
           << "Invalid `wide_number_mode` specified for PARSE_JSON: "
           << args[1].string_value();
  }

  JSONParsingOptions options{
      .legacy_mode = context->GetLanguageOptions().LanguageFeatureEnabled(
          FEATURE_JSON_LEGACY_PARSE),
      .strict_number_parsing = (args[1].string_value() == "exact")};
  auto result = JSONValue::ParseJSONString(args[0].string_value(), options);
  if (!result.ok()) {
    return MakeEvalError() << "Invalid input to PARSE_JSON: "
                           << result.status().message();
  }
  return Value::Json(std::move(*result));
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
      {FunctionKind::kJsonExtractArray, FunctionKind::kJsonExtractStringArray,
       FunctionKind::kJsonQueryArray, FunctionKind::kJsonValueArray},
      [](FunctionKind kind, const Type* output_type) {
        return new JsonArrayFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kSubscript},
      [](FunctionKind kind, const Type* output_type) {
        return new JsonSubscriptFunction();
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kToJson}, [](FunctionKind kind, const Type* output_type) {
        return new ToJsonFunction();
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kToJsonString},
      [](FunctionKind kind, const Type* output_type) {
        return new ToJsonStringFunction();
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kParseJson},
      [](FunctionKind kind, const zetasql::Type* output_type) {
        return new ParseJsonFunction();
      });
}

}  // namespace zetasql
