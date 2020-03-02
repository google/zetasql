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

#include "zetasql/reference_impl/functions/json.h"

#include "zetasql/public/functions/json.h"
#include "zetasql/reference_impl/function.h"

namespace zetasql {
namespace {

class JsonFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit JsonFunction(FunctionKind kind)
      : SimpleBuiltinScalarFunction(kind, types::StringType()) {}
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
      const std::unique_ptr<functions::JsonPathEvaluator> evaluator,
      functions::JsonPathEvaluator::Create(
          /*json_path=*/args[1].string_value(), sql_standard_mode));
  evaluator->enable_special_character_escaping();
  std::string output;
  bool is_null = true;
  if (kind() == FunctionKind::kJsonQuery ||
      kind() == FunctionKind::kJsonExtract) {
    ZETASQL_RETURN_IF_ERROR(
        evaluator->Extract(args[0].string_value(), &output, &is_null));
  } else {  // kJsonValue || kJsonExtractScalar
    ZETASQL_RETURN_IF_ERROR(
        evaluator->ExtractScalar(args[0].string_value(), &output, &is_null));
  }
  if (is_null) {
    return Value::Null(output_type());
  } else {
    return Value::String(output);
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
        return new JsonFunction(kind);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kJsonExtractArray},
      [](FunctionKind kind, const Type* output_type) {
        return new JsonExtractArrayFunction();
      });
}

}  // namespace zetasql
