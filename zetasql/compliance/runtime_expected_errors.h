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

#ifndef ZETASQL_COMPLIANCE_RUNTIME_EXPECTED_ERRORS_H_
#define ZETASQL_COMPLIANCE_RUNTIME_EXPECTED_ERRORS_H_

#include <memory>
#include <string>

#include "zetasql/compliance/matchers.h"
#include "zetasql/base/status.h"

namespace zetasql {

// RuntimeExpectedErrorMatcher() returns a  matcher that matches all
// legitimate runtime errors for queries.
// E.g., int64_t overflow (multiplying or adding two int64_t could overflow). It's
// difficult to prevent this type of error while generating random queries.
std::unique_ptr<MatcherCollection<absl::Status>> RuntimeExpectedErrorMatcher(
    std::string matcher_name);

// RuntimeExpectedErrorMatcher() returns a  matcher that matches all
// legitimate runtime errors for dml statements.
// E.g., int64_t overflow (multiplying or adding two int64_t could overflow). It's
// difficult to prevent this type of error while generating random queries.
std::unique_ptr<MatcherCollection<absl::Status>>
RuntimeDMLExpectedErrorMatcher(std::string matcher_name);

}  // namespace zetasql
#endif  // ZETASQL_COMPLIANCE_RUNTIME_EXPECTED_ERRORS_H_
