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

#include "zetasql/public/functions/util.h"

#include "zetasql/base/testing/status_matchers.h"
#include "gtest/gtest.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {
namespace internal {

namespace {

using zetasql_base::testing::StatusIs;

TEST(UpdateError, UpdateError) {
  // Regular std::string.
  ::zetasql_base::Status status;
  UpdateError(&status, "xyz");
  EXPECT_THAT(status, StatusIs(zetasql_base::OUT_OF_RANGE, "xyz"));

  // Valid UTF-8.
  status = zetasql_base::OkStatus();
  UpdateError(&status, "xyzñ");
  EXPECT_THAT(status, StatusIs(zetasql_base::OUT_OF_RANGE, "xyzñ"));

  // Does nothing if status is already set.
  status = zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument, "msg");
  UpdateError(&status, "xyzñ");
  EXPECT_THAT(status, StatusIs(zetasql_base::INVALID_ARGUMENT, "msg"));

  // Invalid UTF-8 gets converted to valid UTF-8 with REPLACEMENT CHARACTER
  // used for invalid characters.
  status = zetasql_base::OkStatus();
  UpdateError(&status, "xyz\xc3(");
  EXPECT_THAT(status, StatusIs(zetasql_base::OUT_OF_RANGE, "xyz\uFFFD("));
}

}  // anonymous namespace
}  // namespace internal
}  // namespace functions
}  // namespace zetasql
