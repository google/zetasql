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
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/common/errors.h"
#include "zetasql/public/functions/json.h"
#include "zetasql/public/functions/json_format.h"
#include "zetasql/public/functions/json_internal.h"
#include "zetasql/public/functions/to_json.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/reference_impl/function.h"
#include "absl/status/statusor.h"
#include "zetasql/base/ret_check.h"

namespace zetasql {
namespace {

using functions::json_internal::StrictJSONPathIterator;

absl::StatusOr<JSONValueConstRef> GetJSONValueConstRef(
    const Value& json, const JSONParsingOptions& json_parsing_options,
    JSONValue& json_storage) {
  if (json.is_validated_json()) {
    return json.json_value();
  }
  ZETASQL_ASSIGN_OR_RETURN(json_storage,
                   JSONValue::ParseJSONString(json.json_value_unparsed(),
                                              json_parsing_options));
  return json_storage.GetConstRef();
}

absl::StatusOr<JSONValue> GetJSONValueCopy(
    const Value& json, const LanguageOptions& language_options) {
  JSONParsingOptions json_parsing_options = JSONParsingOptions{
      .wide_number_mode = (language_options.LanguageFeatureEnabled(
                               FEATURE_JSON_STRICT_NUMBER_PARSING)
                               ? JSONParsingOptions::WideNumberMode::kExact
                               : JSONParsingOptions::WideNumberMode::kRound)};

  if (json.is_validated_json()) {
    return JSONValue::CopyFrom(json.json_value());
  }
  return JSONValue::ParseJSONString(json.json_value_unparsed(),
                                    json_parsing_options);
}

// Create a StrictJSONPathIterator from `path`. Returns an error if
// the path is invalid.
// `function_name` is used in the error message.
absl::StatusOr<std::unique_ptr<StrictJSONPathIterator>>
BuildStrictJSONPathIterator(absl::string_view path,
                            absl::string_view function_name) {
  auto path_iterator = StrictJSONPathIterator::Create(path);
  if (!path_iterator.ok()) {
    return MakeEvalError() << "Invalid input to " << function_name << ": "
                           << path_iterator.status().message();
  }
  return *std::move(path_iterator);
}

template <typename T>
Value CreateValueFromOptional(std::optional<T> opt) {
  if (opt.has_value()) {
    return Value::Make<T>(opt.value());
  }
  return Value::MakeNull<T>();
}

// Signal that statement evaluation encountered non-determinism if a potentially
// imprecise value is converted to JSON or to JSON_STRING.
void MaybeSetNonDeterministicContext(const Value& arg,
                                     EvaluationContext* context) {
  if (!context->IsDeterministicOutput()) {
    return;
  }
  if (HasFloatingPoint(arg.type()) ||
      InternalValue::ContainsArrayWithUncertainOrder(arg)) {
    context->SetNonDeterministicOutput();
  }
}

// Implementation of:
// JSON_EXTRACT/JSON_QUERY(string, string) -> string
// JSON_EXTRACT/JSON_QUERY(json, string) -> json
// JSON_EXTRACT_SCALAR/JSON_VALUE(string[, string]) -> string
// JSON_EXTRACT_SCALAR/JSON_VALUE(json[, string]) -> string
class JsonExtractFunction : public SimpleBuiltinScalarFunction {
 public:
  JsonExtractFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {
    ABSL_DCHECK(output_type->Equals(types::JsonType()) ||
           output_type->Equals(types::StringType()));
  }
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class JsonExtractArrayFunction : public SimpleBuiltinScalarFunction {
 public:
  JsonExtractArrayFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {
    ABSL_DCHECK(output_type->Equals(types::JsonArrayType()) ||
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

class ConvertJsonFunction : public SimpleBuiltinScalarFunction {
 public:
  ConvertJsonFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class ConvertJsonLaxFunction : public SimpleBuiltinScalarFunction {
 public:
  ConvertJsonLaxFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class JsonTypeFunction : public SimpleBuiltinScalarFunction {
 public:
  JsonTypeFunction()
      : SimpleBuiltinScalarFunction(FunctionKind::kJsonType,
                                    types::StringType()) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class JsonArrayFunction : public SimpleBuiltinScalarFunction {
 public:
  JsonArrayFunction()
      : SimpleBuiltinScalarFunction(FunctionKind::kJsonArray,
                                    types::JsonType()) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class JsonObjectFunction : public SimpleBuiltinScalarFunction {
 public:
  JsonObjectFunction()
      : SimpleBuiltinScalarFunction(FunctionKind::kJsonObject,
                                    types::JsonType()) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class JsonRemoveFunction : public SimpleBuiltinScalarFunction {
 public:
  JsonRemoveFunction()
      : SimpleBuiltinScalarFunction(FunctionKind::kJsonRemove,
                                    types::JsonType()) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class JsonSetFunction : public SimpleBuiltinScalarFunction {
 public:
  JsonSetFunction()
      : SimpleBuiltinScalarFunction(FunctionKind::kJsonSet, types::JsonType()) {
  }
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class JsonStripNullsFunction : public SimpleBuiltinScalarFunction {
 public:
  JsonStripNullsFunction()
      : SimpleBuiltinScalarFunction(FunctionKind::kJsonStripNulls,
                                    types::JsonType()) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

class JsonArrayInsertAppendFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit JsonArrayInsertAppendFunction(FunctionKind kind)
      : SimpleBuiltinScalarFunction(kind, types::JsonType()) {}

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
    std::optional<std::string> output_string_or;
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
    std::optional<JSONValueConstRef> output_json_or;
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
  std::optional<JSONValueConstRef> json_value_const_ref;
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
                .wide_number_mode =
                    (language_options.LanguageFeatureEnabled(
                         FEATURE_JSON_STRICT_NUMBER_PARSING)
                         ? JSONParsingOptions::WideNumberMode::kExact
                         : JSONParsingOptions::WideNumberMode::kRound)}));
    json_value_const_ref = input_json.GetConstRef();
  }
  ZETASQL_RET_CHECK(json_value_const_ref.has_value());
  std::optional<JSONValueConstRef> member_const_ref;
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

absl::StatusOr<Value> JsonExtractFunction::Eval(
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
          sql_standard_mode,
          /*enable_special_character_escaping_in_values=*/true,
          /*enable_special_character_escaping_in_keys=*/true));
  bool scalar = kind() == FunctionKind::kJsonValue ||
                kind() == FunctionKind::kJsonExtractScalar;
  if (args[0].type_kind() == TYPE_STRING) {
    return JsonExtractString(*evaluator, args[0].string_value(), scalar);
  } else {
    const auto& language_options = context->GetLanguageOptions();
    return JsonExtractJson(
        *evaluator, args[0], output_type(), scalar,
        JSONParsingOptions{
            .wide_number_mode =
                (language_options.LanguageFeatureEnabled(
                     FEATURE_JSON_STRICT_NUMBER_PARSING)
                     ? JSONParsingOptions::WideNumberMode::kExact
                     : JSONParsingOptions::WideNumberMode::kRound)});
  }
}

// Helper function for the string version of JSON_VALUE_ARRAY and
// JSON_EXTRACT_STRING_ARRAY.
absl::StatusOr<Value> JsonExtractStringArrayString(
    const functions::JsonPathEvaluator& evaluator, absl::string_view json) {
  std::vector<std::optional<std::string>> output;
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
  std::optional<std::vector<std::optional<std::string>>> output;
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
  std::optional<std::vector<JSONValueConstRef>> output;
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

absl::StatusOr<Value> JsonExtractArrayFunction::Eval(
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

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<functions::JsonPathEvaluator> evaluator,
                   functions::JsonPathEvaluator::Create(
                       json_path, sql_standard_mode,
                       /*enable_special_character_escaping_in_values=*/true,
                       /*enable_special_character_escaping_in_keys=*/true));
  bool scalar = kind() == FunctionKind::kJsonValueArray ||
                kind() == FunctionKind::kJsonExtractStringArray;
  if (args[0].type_kind() == TYPE_STRING) {
    return scalar ? JsonExtractStringArrayString(*evaluator,
                                                 args[0].string_value())
                  : JsonExtractArrayString(*evaluator, args[0].string_value());
  } else {
    const auto& language_options = context->GetLanguageOptions();
    JSONParsingOptions parsing_options{
        .wide_number_mode = (language_options.LanguageFeatureEnabled(
                                 FEATURE_JSON_STRICT_NUMBER_PARSING)
                                 ? JSONParsingOptions::WideNumberMode::kExact
                                 : JSONParsingOptions::WideNumberMode::kRound)};

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
  ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
  ZETASQL_ASSIGN_OR_RETURN(JSONValue outputJson,
                   functions::ToJson(args[0], stringify_wide_numbers,
                                     context->GetLanguageOptions(),
                                     /*canonicalize_zero=*/true));
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
  ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[0], context));
  ZETASQL_RETURN_IF_ERROR(functions::JsonFromValue(
      args[0], &pretty_printer, &output,
      JSONParsingOptions{
          .wide_number_mode =
              (context->GetLanguageOptions().LanguageFeatureEnabled(
                   FEATURE_JSON_STRICT_NUMBER_PARSING)
                   ? JSONParsingOptions::WideNumberMode::kExact
                   : JSONParsingOptions::WideNumberMode::kRound),
          .canonicalize_zero = true}));
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
      .wide_number_mode = (args[1].string_value() == "exact"
                               ? JSONParsingOptions::WideNumberMode::kExact
                               : JSONParsingOptions::WideNumberMode::kRound)};
  auto result = JSONValue::ParseJSONString(args[0].string_value(), options);
  if (!result.ok()) {
    return MakeEvalError() << "Invalid input to PARSE_JSON: "
                           << result.status().message();
  }
  return Value::Json(std::move(*result));
}

absl::StatusOr<Value> JsonTypeFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  JSONValue json_storage;
  LanguageOptions language_options = context->GetLanguageOptions();
  JSONParsingOptions json_parsing_options = JSONParsingOptions{
      .wide_number_mode = (language_options.LanguageFeatureEnabled(
                               FEATURE_JSON_STRICT_NUMBER_PARSING)
                               ? JSONParsingOptions::WideNumberMode::kExact
                               : JSONParsingOptions::WideNumberMode::kRound)};
  ZETASQL_ASSIGN_OR_RETURN(
      JSONValueConstRef json_value_const_ref,
      GetJSONValueConstRef(args[0], json_parsing_options, json_storage));
  ZETASQL_ASSIGN_OR_RETURN(const std::string output,
                   functions::GetJsonType(json_value_const_ref));
  return Value::String(output);
}

absl::StatusOr<Value> ConvertJsonFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_GE(args.size(), 1);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  JSONValue json_storage;
  LanguageOptions language_options = context->GetLanguageOptions();
  JSONParsingOptions json_parsing_options = JSONParsingOptions{
      .wide_number_mode = (language_options.LanguageFeatureEnabled(
                               FEATURE_JSON_STRICT_NUMBER_PARSING)
                               ? JSONParsingOptions::WideNumberMode::kExact
                               : JSONParsingOptions::WideNumberMode::kRound)};
  switch (kind()) {
    case FunctionKind::kInt64: {
      ZETASQL_RET_CHECK_EQ(args.size(), 1);
      ZETASQL_ASSIGN_OR_RETURN(
          JSONValueConstRef json_value_const_ref,
          GetJSONValueConstRef(args[0], json_parsing_options, json_storage));
      ZETASQL_ASSIGN_OR_RETURN(const int64_t output,
                       functions::ConvertJsonToInt64(json_value_const_ref));
      return Value::Int64(output);
    }
    case FunctionKind::kDouble: {
      ZETASQL_RET_CHECK_EQ(args.size(), 2);
      ZETASQL_RET_CHECK(args[1].type()->IsString());
      std::string wide_number_mode_as_string = args[1].string_value();
      functions::WideNumberMode wide_number_mode;
      if (wide_number_mode_as_string == "exact") {
        wide_number_mode = functions::WideNumberMode::kExact;
      } else if (wide_number_mode_as_string == "round") {
        wide_number_mode = functions::WideNumberMode::kRound;
      } else {
        return MakeEvalError() << "Invalid `wide_number_mode` specified: "
                               << wide_number_mode_as_string;
      }
      json_parsing_options.wide_number_mode =
          (wide_number_mode == functions::WideNumberMode::kExact
               ? JSONParsingOptions::WideNumberMode::kExact
               : JSONParsingOptions::WideNumberMode::kRound);
      ZETASQL_ASSIGN_OR_RETURN(
          JSONValueConstRef json_value_const_ref,
          GetJSONValueConstRef(args[0], json_parsing_options, json_storage));
      ZETASQL_ASSIGN_OR_RETURN(
          const double output,
          functions::ConvertJsonToDouble(json_value_const_ref, wide_number_mode,
                                         language_options.product_mode()));
      return Value::Double(output);
    }
    case FunctionKind::kBool: {
      ZETASQL_RET_CHECK_EQ(args.size(), 1);
      ZETASQL_ASSIGN_OR_RETURN(
          JSONValueConstRef json_value_const_ref,
          GetJSONValueConstRef(args[0], json_parsing_options, json_storage));
      ZETASQL_ASSIGN_OR_RETURN(const bool output,
                       functions::ConvertJsonToBool(json_value_const_ref));
      return Value::Bool(output);
    }
    default:
      return ::zetasql_base::InvalidArgumentErrorBuilder() << "Unsupported function";
  }
}

absl::StatusOr<Value> ConvertJsonLaxFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }
  JSONValue json_storage;
  JSONParsingOptions json_parsing_options = JSONParsingOptions{
      .wide_number_mode = (context->GetLanguageOptions().LanguageFeatureEnabled(
                               FEATURE_JSON_STRICT_NUMBER_PARSING)
                               ? JSONParsingOptions::WideNumberMode::kExact
                               : JSONParsingOptions::WideNumberMode::kRound)};
  ZETASQL_ASSIGN_OR_RETURN(
      JSONValueConstRef json_value_const_ref,
      GetJSONValueConstRef(args[0], json_parsing_options, json_storage));
  switch (kind()) {
    case FunctionKind::kLaxBool: {
      auto result = functions::LaxConvertJsonToBool(json_value_const_ref);
      ZETASQL_RET_CHECK(result.ok());
      return CreateValueFromOptional(*result);
    }
    case FunctionKind::kLaxInt64: {
      auto result = functions::LaxConvertJsonToInt64(json_value_const_ref);
      ZETASQL_RET_CHECK(result.ok());
      return CreateValueFromOptional(*result);
    }
    case FunctionKind::kLaxDouble: {
      auto result = functions::LaxConvertJsonToFloat64(json_value_const_ref);
      ZETASQL_RET_CHECK(result.ok());
      return CreateValueFromOptional(*result);
    }
    case FunctionKind::kLaxString: {
      auto result = functions::LaxConvertJsonToString(json_value_const_ref);
      ZETASQL_RET_CHECK(result.ok());
      return CreateValueFromOptional(*result);
    }
    default:
      return ::zetasql_base::InvalidArgumentErrorBuilder() << "Unsupported function";
  }
}

absl::StatusOr<Value> JsonArrayFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  for (const Value& arg : args) {
    ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(arg, context));
  }
  auto result = functions::JsonArray(args, context->GetLanguageOptions(),
                                     /*canonicalize_zero=*/true);

  if (!result.ok()) {
    return MakeEvalError() << "Invalid input to JSON_ARRAY: "
                           << result.status().message();
  }
  for (const Value& arg : args) {
    MaybeSetNonDeterministicContext(arg, context);
  }
  return Value::Json(*std::move(result));
}

absl::StatusOr<Value> JsonObjectFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  // JSON_OBJECT has 2 signatures:
  // 1. JSON_OBJECT(STRING, ANY, ...)
  // 2. JSON_OBJECT(ARRAY<STRING>, ARRAY<ANY>)
  // 'is_array_args' indicates it is the second one.
  bool is_array_args = false;
  if (args.size() == 2 && args[0].type()->IsArray()) {
    is_array_args = true;
    ZETASQL_RET_CHECK(args[0].type()->AsArray()->element_type()->IsString());
    ZETASQL_RET_CHECK(args[1].type()->IsArray());
    if (args[0].is_null()) {
      return MakeEvalError()
             << "Invalid input to JSON_OBJECT: The keys array cannot be NULL";
    }
    if (args[1].is_null()) {
      return MakeEvalError()
             << "Invalid input to JSON_OBJECT: The values array cannot be NULL";
    }
    if (args[0].num_elements() != args[1].num_elements()) {
      return MakeEvalError() << "Invalid input to JSON_OBJECT: The number of "
                                "keys and values must match";
    }
  }

  for (const Value& arg : args) {
    ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(arg, context));
  }

  const size_t num_keys =
      is_array_args ? args[0].num_elements() : args.size() / 2;

  std::vector<absl::string_view> keys;
  std::vector<const Value*> values;
  keys.reserve(num_keys);
  values.reserve(num_keys);

  if (is_array_args) {
    for (const Value& key : args[0].elements()) {
      if (key.is_null()) {
        return MakeEvalError()
               << "Invalid input to JSON_OBJECT: A key cannot be NULL";
      }
      keys.push_back(key.string_value());
    }
    for (const Value& value : args[1].elements()) {
      values.push_back(&value);
    }
  } else {
    for (int i = 0; i < args.size(); ++i) {
      if (i % 2 == 0) {
        if (args[i].is_null()) {
          return MakeEvalError()
                 << "Invalid input to JSON_OBJECT: A key cannot be NULL";
        }
        ZETASQL_RET_CHECK(args[i].type()->IsString());
        keys.push_back(args[i].string_value());
      } else {
        values.push_back(&args[i]);
      }
    }
  }

  functions::JsonObjectBuilder builder(context->GetLanguageOptions(),
                                       /*canonicalize_zero=*/true);
  auto result = functions::JsonObject(keys, absl::MakeSpan(values), builder);

  if (!result.ok()) {
    return MakeEvalError() << "Invalid input to JSON_OBJECT: "
                           << result.status().message();
  }
  for (const Value& arg : args) {
    MaybeSetNonDeterministicContext(arg, context);
  }
  return Value::Json(*std::move(result));
}

absl::StatusOr<Value> JsonRemoveFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_GE(args.size(), 2);
  if (HasNulls(args)) {
    return Value::Null(output_type());
  }

  ZETASQL_ASSIGN_OR_RETURN(JSONValue result,
                   GetJSONValueCopy(args[0], context->GetLanguageOptions()));
  JSONValueRef ref = result.GetRef();

  for (int i = 1; i < args.size(); ++i) {
    auto path_iterator = StrictJSONPathIterator::Create(args[i].string_value());
    if (!path_iterator.ok()) {
      return MakeEvalError() << "Invalid input to JSON_REMOVE: "
                             << path_iterator.status().message();
    }
    auto status = functions::JsonRemove(ref, **path_iterator).status();
    if (!status.ok()) {
      return MakeEvalError()
             << "Invalid input to JSON_REMOVE: " << status.message();
    }
  }

  return Value::Json(std::move(result));
}

// JSON_ARRAY_{INSERT,APPEND}(JSON input, STRING path, ANY value
//                            [, STRING path, ANY value...]
//                            , BOOL {insert,append}_each_element=>true)
absl::StatusOr<Value> JsonArrayInsertAppendFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  if (kind() != FunctionKind::kJsonArrayInsert &&
      kind() != FunctionKind::kJsonArrayAppend) {
    return zetasql_base::InvalidArgumentErrorBuilder() << "Unsupported function";
  }

  ZETASQL_RET_CHECK_GE(args.size(), 4);
  // The number of arguments is 4 + 2*n
  // TODO: ZETASQL_RET_CHECK for incorrect args size instead of error.
  // ZETASQL_RET_CHECK((args.size() - 4) % 2 == 0);
  if ((args.size() - 4) % 2 == 1) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Incorrect number of args for "
           << (kind() == FunctionKind::kJsonArrayInsert ? "JSON_ARRAY_INSERT"
                                                        : "JSON_ARRAY_APPEND")
           << ": paths and values must come in pairs";
  }
  if (args[0].is_null() || args.back().is_null()) {
    return Value::NullJson();
  }

  int64_t num_value_pairs = (args.size() - 2) / 2;

  for (int i = 0; i < num_value_pairs; ++i) {
    // If any JSONPath is NULL, return NULL.
    if (args[2 * i + 1].is_null()) {
      return Value::NullJson();
    }
  }

  for (int i = 0; i < num_value_pairs; ++i) {
    ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[2 * i + 2], context));
  }
  const LanguageOptions& language_options = context->GetLanguageOptions();

  ZETASQL_ASSIGN_OR_RETURN(JSONValue result,
                   GetJSONValueCopy(args[0], language_options));
  JSONValueRef ref = result.GetRef();

  ZETASQL_RET_CHECK(args.back().type()->IsBool());
  const bool insert_each_element = args.back().bool_value();

  // Function name in error message.
  absl::string_view function_name;
  if (kind() == FunctionKind::kJsonArrayInsert) {
    function_name = "JSON_ARRAY_INSERT";
  } else {
    function_name = "JSON_ARRAY_APPEND";
  }

  for (int i = 0; i < num_value_pairs; ++i) {
    ZETASQL_RET_CHECK(args[2 * i + 1].type()->IsString());
    ZETASQL_ASSIGN_OR_RETURN(auto path_iterator,
                     BuildStrictJSONPathIterator(args[2 * i + 1].string_value(),
                                                 function_name));
    absl::Status status;
    if (kind() == FunctionKind::kJsonArrayInsert) {
      status = functions::JsonInsertArrayElement(
          ref, *path_iterator, args[2 * i + 2], language_options,
          /*canonicalize_zero=*/true, insert_each_element);
    } else {
      status = functions::JsonAppendArrayElement(
          ref, *path_iterator, args[2 * i + 2], language_options,
          /*canonicalize_zero=*/true, insert_each_element);
    }
    if (!status.ok()) {
      return MakeEvalError() << "Invalid input to " << function_name << ": "
                             << status.message();
    }
  }

  for (const Value& arg : args) {
    MaybeSetNonDeterministicContext(arg, context);
  }
  return Value::Json(std::move(result));
}

absl::StatusOr<Value> JsonSetFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  // Num of args: 2n + 3, n >= 0
  ZETASQL_RET_CHECK_GE(args.size(), 3);
  ZETASQL_RET_CHECK_EQ(args.size() % 2, 1);
  ZETASQL_RET_CHECK(args[0].type()->IsJson());

  const size_t num_value_pairs = (args.size() - 1) / 2;

  if (args[0].is_null()) {
    return Value::NullJson();
  }

  // Validity verification steps:
  // 1) Check validity of all non-null JSON paths. Return error if invalid.
  // 2) If any JSON path is NULL return NULL.
  // Parsing errors take precedence over NULL inputs.
  bool paths_contain_nulls = false;
  std::vector<std::unique_ptr<StrictJSONPathIterator>> path_iterators;
  path_iterators.reserve(num_value_pairs);
  for (int i = 0; i < num_value_pairs; ++i) {
    int index = 2 * i + 1;
    ZETASQL_RET_CHECK(args[index].type()->IsString());
    if (args[index].is_null()) {
      paths_contain_nulls = true;
      continue;
    }
    ZETASQL_ASSIGN_OR_RETURN(
        path_iterators.emplace_back(),
        BuildStrictJSONPathIterator(args[index].string_value(), "JSON_SET"));
  }
  if (paths_contain_nulls) {
    return Value::NullJson();
  }

  for (int i = 0; i < num_value_pairs; ++i) {
    ZETASQL_RETURN_IF_ERROR(ValidateMicrosPrecision(args[2 * i + 2], context));
  }

  const LanguageOptions& language_options = context->GetLanguageOptions();
  ZETASQL_ASSIGN_OR_RETURN(JSONValue result,
                   GetJSONValueCopy(args[0], language_options));
  JSONValueRef ref = result.GetRef();

  for (int i = 0; i < num_value_pairs; ++i) {
    if (absl::Status status = functions::JsonSet(
            ref, *path_iterators[i], args[2 * i + 2], language_options,
            /*canonicalize_zero=*/true);
        !status.ok()) {
      return MakeEvalError()
             << "Invalid input to JSON_SET: " << status.message();
    }
  }

  for (const Value& arg : args) {
    MaybeSetNonDeterministicContext(arg, context);
  }
  return Value::Json(std::move(result));
}

absl::StatusOr<Value> JsonStripNullsFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(args.size(), 4);
  if (HasNulls(args)) {
    return Value::NullJson();
  }
  ZETASQL_RET_CHECK(args[0].type()->IsJson());
  ZETASQL_RET_CHECK(args[1].type()->IsString());
  ZETASQL_RET_CHECK(args[2].type()->IsBool());
  ZETASQL_RET_CHECK(args[3].type()->IsBool());

  const LanguageOptions& language_options = context->GetLanguageOptions();

  ZETASQL_ASSIGN_OR_RETURN(JSONValue result,
                   GetJSONValueCopy(args[0], language_options));
  ZETASQL_ASSIGN_OR_RETURN(
      auto path_iterator,
      BuildStrictJSONPathIterator(args[1].string_value(), "JSON_STRIP_NULLS"));
  if (absl::Status status =
          functions::JsonStripNulls(result.GetRef(), *path_iterator,
                                    args[2].bool_value(), args[3].bool_value());
      !status.ok()) {
    return MakeEvalError() << "Invalid input to JSON_STRIP_NULLS: "
                           << status.message();
  }

  return Value::Json(std::move(result));
}

}  // namespace

void RegisterBuiltinJsonFunctions() {
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kJsonExtract, FunctionKind::kJsonExtractScalar,
       FunctionKind::kJsonQuery, FunctionKind::kJsonValue},
      [](FunctionKind kind, const Type* output_type) {
        return new JsonExtractFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kJsonExtractArray, FunctionKind::kJsonExtractStringArray,
       FunctionKind::kJsonQueryArray, FunctionKind::kJsonValueArray},
      [](FunctionKind kind, const Type* output_type) {
        return new JsonExtractArrayFunction(kind, output_type);
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
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kInt64, FunctionKind::kDouble, FunctionKind::kBool},
      [](FunctionKind kind, const zetasql::Type* output_type) {
        return new ConvertJsonFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kLaxInt64, FunctionKind::kLaxDouble,
       FunctionKind::kLaxBool, FunctionKind::kLaxString},
      [](FunctionKind kind, const zetasql::Type* output_type) {
        return new ConvertJsonLaxFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kJsonType},
      [](FunctionKind kind, const zetasql::Type* output_type) {
        return new JsonTypeFunction();
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kJsonArray},
      [](FunctionKind kind, const zetasql::Type* output_type) {
        return new JsonArrayFunction();
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kJsonObject},
      [](FunctionKind kind, const zetasql::Type* output_type) {
        return new JsonObjectFunction();
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kJsonRemove},
      [](FunctionKind kind, const zetasql::Type* output_type) {
        return new JsonRemoveFunction();
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kJsonSet},
      [](FunctionKind kind, const zetasql::Type* output_type) {
        return new JsonSetFunction();
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kJsonStripNulls},
      [](FunctionKind kind, const zetasql::Type* output_type) {
        return new JsonStripNullsFunction();
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kJsonArrayInsert},
      [](FunctionKind kind, const zetasql::Type* output_type) {
        return new JsonArrayInsertAppendFunction(kind);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kJsonArrayAppend},
      [](FunctionKind kind, const zetasql::Type* output_type) {
        return new JsonArrayInsertAppendFunction(kind);
      });
}

}  // namespace zetasql
