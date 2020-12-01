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

#ifndef ZETASQL_REFERENCE_IMPL_EXPECTED_ERRORS_H_
#define ZETASQL_REFERENCE_IMPL_EXPECTED_ERRORS_H_

#include <memory>
#include <string>

#include "zetasql/compliance/matchers.h"
#include "zetasql/base/status.h"

namespace zetasql {

// Produces matcher for expected errors that should be ignored from the
// reference implementation. All expected errors should have an associated bug.
std::unique_ptr<MatcherCollection<absl::Status>>
ReferenceExpectedErrorMatcher(std::string matcher_name);

// Produces a matcher for errors returned by the reference implementation
// that indicate an invalid input condition encountered during the evaluation
// of an expression and that can be ignored in scenarios such as random query
// testing. The reference implementation returns all such errors with
// with the OUT_OF_RANGE error code. The matcher returned by this function
// is helpful in matching the errors based on their text.
std::unique_ptr<MatcherCollection<absl::Status>>
ReferenceInvalidInputErrorMatcher(std::string matcher_name);

}  // namespace zetasql

#endif  // ZETASQL_REFERENCE_IMPL_EXPECTED_ERRORS_H_
