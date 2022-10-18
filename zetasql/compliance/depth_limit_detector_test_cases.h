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

#ifndef ZETASQL_COMPLIANCE_DEPTH_LIMIT_DETECTOR_TEST_CASES_H_
#define ZETASQL_COMPLIANCE_DEPTH_LIMIT_DETECTOR_TEST_CASES_H_

#include <functional>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "zetasql/public/language_options.h"
#include "absl/status/status.h"

namespace zetasql {
class DepthLimitDetectorTemplate;
class DepthLimitDetectorTestResult;
class DepthLimitDetectorTestCase;

LanguageOptions DepthLimitDetectorTestCaseLanguageOptions(
    const DepthLimitDetectorTestCase& depth_case);

// Find all return statuses of the test_driver_function for all
// instantiations of a query template and return them as ordered ranges.
// While the query may get very complex, the test_driver_function should never
// crash.
DepthLimitDetectorTestResult RunDepthLimitDetectorTestCase(
    DepthLimitDetectorTestCase const& depth_limit_case,
    std::function<absl::Status(std::string_view)> test_driver_function);

absl::Span<const std::reference_wrapper<const DepthLimitDetectorTestCase>>
AllDepthLimitDetectorTestCases();

std::string DepthLimitDetectorTemplateToString(
    const DepthLimitDetectorTemplate& depth_limit_template, int depth);
std::string DepthLimitDetectorTemplateToString(
    const DepthLimitDetectorTestCase& depth_case, int depth);

struct DepthLimitDetectorReturnCondition {
  int starting_depth;
  int ending_depth;
  absl::Status return_status;
};
std::ostream& operator<<(std::ostream& os,
                         const DepthLimitDetectorReturnCondition& condition);

struct DepthLimitDetectorTestResult {
  std::string_view depth_limit_test_case_name;
  std::vector<DepthLimitDetectorReturnCondition>
      depth_limit_detector_return_conditions;
};
std::ostream& operator<<(std::ostream& os,
                         const DepthLimitDetectorTestResult& test_result);

std::ostream& operator<<(std::ostream& os,
                         const DepthLimitDetectorTestCase& test_case);

}  // namespace zetasql

#endif  // ZETASQL_COMPLIANCE_DEPTH_LIMIT_DETECTOR_TEST_CASES_H_
