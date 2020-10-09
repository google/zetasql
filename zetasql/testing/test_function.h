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

#ifndef ZETASQL_TESTING_TEST_FUNCTION_H_
#define ZETASQL_TESTING_TEST_FUNCTION_H_

#include <stddef.h>

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/float_margin.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {

class ValueConstructor;

// Represents the inputs and expected output of a query. If the expected
// 'result' is a value, 'status' must be set to absl::Status::OK. Otherwise,
// 'result' must be set to the NULL value of the proper result type.
// 'status' should be set to OUT_OF_RANGE to indicate execution errors, or
// INVALID_ARGUMENT to indicate analysis errors.
//
// Note: The error status and NULL result type are used in AddSafeFunctionCalls
// in compliance_test_cases.cc to automatically add SAFE-mode versions of all
// cases that return errors.  In SAFE mode, functions return NULL instead
// of errors.
//
// Optionally a query may have multiple results, with each mapping to a
// different feature set.
class QueryParamsWithResult {
 public:
  struct Result {
    Value result;
    absl::Status status;
    FloatMargin float_margin = kExactFloatMargin;

    explicit Result(const ValueConstructor& result_in);

    Result(const ValueConstructor& result_in, FloatMargin float_margin_in);

    Result(const ValueConstructor& result_in, absl::StatusCode code);

    Result(const ValueConstructor& result_in, const absl::Status& status_in);
  };

  // Constructs an instance that contains a single result.
  QueryParamsWithResult(const std::vector<ValueConstructor>& arguments,
                        const ValueConstructor& result,
                        absl::Status status = absl::OkStatus());

  QueryParamsWithResult(const std::vector<ValueConstructor>& arguments,
                        const ValueConstructor& result,
                        FloatMargin float_margin_arg);

  QueryParamsWithResult(const std::vector<ValueConstructor>& arguments,
                        const ValueConstructor& result, absl::StatusCode code);

  QueryParamsWithResult(const std::vector<ValueConstructor>& arguments,
                        const ValueConstructor& result,
                        const std::string& error_substring);

  // If the instance contains multiple results, results are keyed on
  // FeatureSets.
  typedef std::set<LanguageFeature> FeatureSet;
  static const FeatureSet kEmptyFeatureSet;  // To avoid ugly use of {}.
  typedef std::map<FeatureSet, Result> ResultMap;

  // Constructs an instance that contains multiple results.
  QueryParamsWithResult(const std::vector<ValueConstructor>& params,
                        const ResultMap& results);

  // Returns a copy of this test case with the result value inverted.  The
  // result type must be bool and there must be exactly one feature set:
  // kEmptyFeatureSet.  Nulls stays as nulls.
  QueryParamsWithResult CopyWithInvertedResult() const;

  // Returns a copy of this test case with the result associated with the
  // specified language feature. The original result must have exactly one
  // feature set: kEmptyFeatureSet.
  QueryParamsWithResult WrapWithFeature(LanguageFeature feature) const;

  // Returns a copy of this test case with the result associated with the
  // specified feature set. The original result must have exactly one feature
  // set: kEmptyFeatureSet.
  QueryParamsWithResult WrapWithFeatureSet(FeatureSet feature_set) const;

  // Returns the list of parameters.
  const std::vector<Value>& params() const { return params_; }

  // Returns the i-th parameter.
  const Value& param(size_t i) const { return params_[i]; }
  Value* mutable_param(size_t i) { return &params_[i]; }

  // Returns the number of parameters.
  size_t num_params() const { return params_.size(); }

  // Accessors for the common case where there is only one feature set and it is
  // empty. Otherwise, it is a fatal error to call these accessors.
  const Value& result() const;
  const absl::Status& status() const;
  const FloatMargin& float_margin() const;

  // Accessor/setter for the ResultMap. We do not allow mutating the ResultMap
  // directly because we need to ensure that it can never be empty.
  const ResultMap& results() const { return results_; }
  void set_results(const ResultMap& results) {
    ZETASQL_CHECK(!results.empty());
    results_ = results;
  }

  // Returns true if there is only one feature set and it is empty.
  bool HasEmptyFeatureSetAndNothingElse() const {
    return results_.size() == 1 && results_.count(kEmptyFeatureSet) == 1;
  }

  // Returns a guess of the result type corresponding to this object.
  //
  // Note that it isn't possible to define a result type for a
  // QueryParamsWithResult in the general case, because it's possible that it
  // can be different (or not even defined) depending on the set of features
  // enabled. This method just picks one.
  //
  // TODO: Find a way to rewrite the code-based compliance tests so
  // that they do not need a concept of result type for a
  // QueryParamsWithResult. It should be sufficient to iterate over the
  // individual entries in the ResultMap and use a potentially different result
  // type for each entry.
  const Type* GetResultType() const {
    return results_.begin()->second.result.type();
  }

 private:
  // Accessors to the fields mapped to a 'feature_set'.
  const Value& result(const FeatureSet& feature_set) const;
  const absl::Status& status(const FeatureSet& feature_set) const;
  const FloatMargin& float_margin(const FeatureSet& feature_set) const;

  std::vector<Value> params_;
  ResultMap results_;

  // Copyable.
};

// Return a vector of test cases with boolean results inverted, as in
// CopyWithInvertedResult above.
std::vector<QueryParamsWithResult> InvertResults(
    const std::vector<QueryParamsWithResult>& tests);

struct FunctionTestCall {
  std::string function_name;
  QueryParamsWithResult params;

  FunctionTestCall(absl::string_view function_name,
                   const std::vector<ValueConstructor>& arguments,
                   const ValueConstructor& result,
                   FloatMargin float_margin = kExactFloatMargin);

  FunctionTestCall(absl::string_view function_name,
                   const std::vector<ValueConstructor>& arguments,
                   const ValueConstructor& result, absl::StatusCode code);

  FunctionTestCall(absl::string_view function_name,
                   const std::vector<ValueConstructor>& arguments,
                   const ValueConstructor& result, absl::Status status);

  FunctionTestCall(absl::string_view function_name_in,
                   const std::vector<ValueConstructor>& arguments_in,
                   const QueryParamsWithResult::ResultMap& results_in);
  FunctionTestCall(absl::string_view function_name_in,
                   const QueryParamsWithResult& params_in);
};

std::ostream& operator<<(std::ostream& out,
                         const QueryParamsWithResult::FeatureSet& f);
std::ostream& operator<<(std::ostream& out, const FunctionTestCall& f);
std::ostream& operator<<(std::ostream& out, const QueryParamsWithResult& p);

}  // namespace zetasql

#endif  // ZETASQL_TESTING_TEST_FUNCTION_H_
