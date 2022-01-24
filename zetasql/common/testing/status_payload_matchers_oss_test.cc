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

#include "zetasql/common/testing/status_payload_matchers_oss.h"

#include <string>

#include "zetasql/common/status_payload_utils.h"
#include "zetasql/common/testing/status_payload_matchers_test.pb.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "zetasql/base/testing/proto_matchers_oss.h"

using ::testing::AllOf;
using ::testing::Not;

namespace zetasql {
namespace testing {
namespace {

using zetasql::CustomPayload;
using zetasql::OtherPayload;
using zetasql::internal::AttachPayload;
using zetasql::testing::EqualsProto;
using zetasql::testing::StatusHasPayload;
using zetasql_base::testing::StatusIs;

absl::Status StatusWithPayload(const std::string &text) {
  absl::Status status{absl::StatusCode::kUnknown, "message"};

  CustomPayload payload;
  payload.set_text(text);
  AttachPayload(&status, payload);

  return status;
}

absl::StatusOr<int> StatusOrWithPayload(const std::string &text) {
  return StatusWithPayload(text);
}

TEST(StatusHasPayload, Status) {
  EXPECT_THAT(absl::OkStatus(), Not(StatusHasPayload<CustomPayload>()));
  EXPECT_THAT(absl::OkStatus(), Not(StatusHasPayload<OtherPayload>()));

  EXPECT_THAT(StatusWithPayload("foo"), StatusHasPayload<CustomPayload>());
  EXPECT_THAT(StatusWithPayload("foo"),
              StatusHasPayload<CustomPayload>(EqualsProto(R"pb(
                text: "foo"
              )pb")));

  EXPECT_THAT(StatusWithPayload("hello"),
              AllOf(StatusIs(absl::StatusCode::kUnknown, "message"),
                    Not(StatusHasPayload<OtherPayload>())));
}

TEST(StatusHasPayload, StatusOr) {
  EXPECT_THAT(StatusOrWithPayload("aaa"), StatusHasPayload<CustomPayload>());
  EXPECT_THAT(StatusOrWithPayload("bbb"),
              Not(StatusHasPayload<OtherPayload>()));
  EXPECT_THAT(StatusOrWithPayload("ccc"),
              StatusHasPayload<CustomPayload>(EqualsProto(R"pb(
                text: "ccc"
              )pb")));
  EXPECT_THAT(StatusOrWithPayload("ccc"),
              Not(StatusHasPayload<OtherPayload>(EqualsProto(R"pb(
                text: "ccc"
              )pb"))));
}

}  // namespace
}  // namespace testing
}  // namespace zetasql
