//
// Copyright 2018 Google LLC
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

#include "zetasql/base/testing/status_matchers.h"

#include "absl/strings/str_cat.h"

namespace zetasql_base {
namespace testing {
namespace internal_status {

void StatusIsMatcherCommonImpl::DescribeTo(std::ostream* os) const {
  *os << ", has a status code that ";
  code_matcher_.DescribeTo(os);
  *os << ", and has an error message that ";
  message_matcher_.DescribeTo(os);
}

void StatusIsMatcherCommonImpl::DescribeNegationTo(std::ostream* os) const {
  *os << ", or has a status code that ";
  code_matcher_.DescribeNegationTo(os);
  *os << ", or has an error message that ";
  message_matcher_.DescribeNegationTo(os);
}

bool StatusIsMatcherCommonImpl::MatchAndExplain(
    const absl::Status& status,
    ::testing::MatchResultListener* result_listener) const {
  ::testing::StringMatchResultListener inner_listener;

  inner_listener.Clear();
  if (!code_matcher_.MatchAndExplain(status.code(), &inner_listener)) {
    *result_listener << (inner_listener.str().empty()
                             ? "whose status code is wrong"
                             : "which has a status code " +
                                   inner_listener.str());
    return false;
  }

  if (!message_matcher_.Matches(std::string(status.message()))) {
    *result_listener << "whose error message is wrong";
    return false;
  }

  return true;
}

void AddFatalFailure(absl::string_view expression,
                     const ::zetasql_base::StatusBuilder& builder) {
  GTEST_MESSAGE_AT_(
      builder.source_location().file_name(), builder.source_location().line(),
      ::absl::StrCat(expression,
                     " returned error: ", absl::Status(builder).ToString())
          .c_str(),
      ::testing::TestPartResult::kFatalFailure);
}

}  // namespace internal_status
}  // namespace testing
}  // namespace zetasql_base
