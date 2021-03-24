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

#include "zetasql/testing/test_function.h"

#include <iosfwd>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_value.h"
#include "absl/status/status.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/status.h"

namespace zetasql {

const QueryParamsWithResult::FeatureSet
    QueryParamsWithResult::kEmptyFeatureSet = {};

QueryParamsWithResult::QueryParamsWithResult(
    const std::vector<ValueConstructor>& arguments,
    const ValueConstructor& result, absl::Status status)
    : params_(ValueConstructor::ToValues(arguments)),
      results_({{kEmptyFeatureSet, {result, status}}}) {}

QueryParamsWithResult::QueryParamsWithResult(
    const std::vector<ValueConstructor>& arguments,
    const ValueConstructor& result, FloatMargin float_margin_arg)
    : params_(ValueConstructor::ToValues(arguments)),
      results_({{kEmptyFeatureSet, {result, float_margin_arg}}}) {}

QueryParamsWithResult::QueryParamsWithResult(
    const std::vector<ValueConstructor>& arguments,
    const ValueConstructor& result, const std::string& error_substring)
    : params_(ValueConstructor::ToValues(arguments)),
      results_({{kEmptyFeatureSet,
                 {result, error_substring.empty()
                              ? absl::OkStatus()
                              : absl::Status(absl::StatusCode::kUnknown,
                                             error_substring)}}}) {}

QueryParamsWithResult::QueryParamsWithResult(
    const std::vector<ValueConstructor>& arguments,
    const ValueConstructor& result, absl::StatusCode code)
    : params_(ValueConstructor::ToValues(arguments)),
      results_({{kEmptyFeatureSet, {result, code}}}) {}

const Value& QueryParamsWithResult::result() const {
  ZETASQL_CHECK(HasEmptyFeatureSetAndNothingElse()) << *this;
  return result(kEmptyFeatureSet);
}
const absl::Status& QueryParamsWithResult::status() const {
  ZETASQL_CHECK(HasEmptyFeatureSetAndNothingElse()) << *this;
  return status(kEmptyFeatureSet);
}
const FloatMargin& QueryParamsWithResult::float_margin() const {
  ZETASQL_CHECK(HasEmptyFeatureSetAndNothingElse()) << *this;
  return float_margin(kEmptyFeatureSet);
}

QueryParamsWithResult::Result::Result(const ValueConstructor& result_in)
    : result(result_in.get()), status() {}

QueryParamsWithResult::Result::Result(const ValueConstructor& result_in,
                                      FloatMargin float_margin_in)
    : result(result_in.get()), status(), float_margin(float_margin_in) {}

QueryParamsWithResult::Result::Result(const ValueConstructor& result_in,
                                      absl::StatusCode code)
    : result(result_in.get()), status(code, "") {}

QueryParamsWithResult::Result::Result(const ValueConstructor& result_in,
                                      const absl::Status& status_in)
    : result(result_in.get()), status(status_in) {}

QueryParamsWithResult::QueryParamsWithResult(
    const std::vector<ValueConstructor>& params, const ResultMap& results)
    : params_(ValueConstructor::ToValues(params)), results_(results) {
  ZETASQL_CHECK(!results_.empty()) << *this;
}

QueryParamsWithResult QueryParamsWithResult::CopyWithInvertedResult() const {
  ZETASQL_CHECK(HasEmptyFeatureSetAndNothingElse()) << *this;
  const Result& result = zetasql_base::FindOrDie(results_, kEmptyFeatureSet);
  const Value& value = result.result;
  ZETASQL_CHECK_EQ(value.type_kind(), TYPE_BOOL);
  return QueryParamsWithResult(
      std::vector<ValueConstructor>(params_.begin(), params_.end()),
      {{kEmptyFeatureSet,
        {value.is_null() ? Value::NullBool() : Value::Bool(!value.bool_value()),
         result.status}}});
}

QueryParamsWithResult QueryParamsWithResult::WrapWithFeature(
    LanguageFeature feature) const {
  FeatureSet feature_set;
  feature_set.insert(feature);
  return WrapWithFeatureSet(feature_set);
}

QueryParamsWithResult QueryParamsWithResult::WrapWithFeatureSet(
    FeatureSet feature_set) const {
  ZETASQL_CHECK(HasEmptyFeatureSetAndNothingElse()) << *this;
  const Result& result = zetasql_base::FindOrDie(results_, kEmptyFeatureSet);
  ResultMap result_map;
  result_map.emplace(feature_set, result);
  return QueryParamsWithResult(ValueConstructor::FromValues(params_),
                               result_map);
}

const Value& QueryParamsWithResult::result(
    const FeatureSet& feature_set) const {
  return zetasql_base::FindOrDie(results_, feature_set).result;
}

const absl::Status& QueryParamsWithResult::status(
    const FeatureSet& feature_set) const {
  return zetasql_base::FindOrDie(results_, feature_set).status;
}

const FloatMargin& QueryParamsWithResult::float_margin(
    const FeatureSet& feature_set) const {
  return zetasql_base::FindOrDie(results_, feature_set).float_margin;
}

std::vector<QueryParamsWithResult> InvertResults(
    const std::vector<QueryParamsWithResult>& tests) {
  std::vector<QueryParamsWithResult> new_tests;
  new_tests.reserve(tests.size());
  for (const QueryParamsWithResult& test : tests) {
    new_tests.push_back(test.CopyWithInvertedResult());
  }
  return new_tests;
}

FunctionTestCall::FunctionTestCall(
    absl::string_view function_name,
    const std::vector<ValueConstructor>& arguments,
    const ValueConstructor& result, FloatMargin float_margin)
    : function_name(function_name), params(arguments, result, float_margin) {}

FunctionTestCall::FunctionTestCall(
    absl::string_view function_name,
    const std::vector<ValueConstructor>& arguments,
    const ValueConstructor& result, absl::StatusCode code)
    : function_name(function_name), params(arguments, result, code) {}

FunctionTestCall::FunctionTestCall(
    absl::string_view function_name,
    const std::vector<ValueConstructor>& arguments,
    const ValueConstructor& result, absl::Status status)
    : function_name(function_name), params(arguments, result, status) {}

FunctionTestCall::FunctionTestCall(
    absl::string_view function_name_in,
    const std::vector<ValueConstructor>& arguments_in,
    const QueryParamsWithResult::ResultMap& results_in)
    : function_name(function_name_in), params(arguments_in, results_in) {}

FunctionTestCall::FunctionTestCall(absl::string_view function_name_in,
                                   const QueryParamsWithResult& params_in)
    : function_name(function_name_in), params(params_in) {}

std::ostream& operator<<(std::ostream& out,
                         const QueryParamsWithResult::FeatureSet& f) {
  std::vector<std::string> features;
  for (const LanguageFeature feature : f) {
    features.push_back(LanguageFeature_Name(feature));
  }
  return out << "{" << absl::StrJoin(features, ", ") << "}";
}

std::ostream& operator<<(std::ostream& out, const FunctionTestCall& f) {
  return out
      << "FunctionTestCall[function_name: " << f.function_name
      << ", params: " << f.params << "]";
}

std::ostream& operator<<(std::ostream& out, const QueryParamsWithResult& p) {
  std::vector<std::string> arguments;
  arguments.reserve(p.params().size());
  for (int i = 0; i < p.params().size(); ++i) {
    std::string param_string = p.param(i).FullDebugString();
    arguments.push_back(param_string);
  }

  out << "QueryParamsWithResult[params: {" << absl::StrJoin(arguments, ", ")
      << "}, ";
  if (p.HasEmptyFeatureSetAndNothingElse()) {
    return out << "result: " << p.result().FullDebugString()
               << ", float_margin: " << p.float_margin()
               << ", status: " << p.status() << "]";
  } else {
    out << "results: {";
    for (const auto& pair : p.results()) {
      const QueryParamsWithResult::FeatureSet& feature_set = pair.first;
      const QueryParamsWithResult::Result& result = pair.second;
      out << " with features: " << feature_set
          << ", { result: " << result.result.FullDebugString()
          << ", float_margin: " << result.float_margin
          << "  status: " << result.status << "} ";
    }
    return out << "}]";
  }
}


}  // namespace zetasql
