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

#include "zetasql/analyzer/expr_resolver_helper.h"

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/templated_sql_tvf.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"

namespace zetasql {

using ::zetasql_base::testing::StatusIs;

TEST(ResolvedTVFArgTest, GetScan) {
  ResolvedTVFArg arg;
  EXPECT_FALSE(arg.IsScan());
  EXPECT_THAT(arg.GetScan(), StatusIs(absl::StatusCode::kInternal));
}

}  // namespace zetasql
