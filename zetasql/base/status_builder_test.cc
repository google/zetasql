//
// Copyright 2018 ZetaSQL Authors
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

#include "zetasql/base/status_builder.h"

#include <string>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/canonical_errors.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"
#include "zetasql/base/test_payload.pb.h"
#include "zetasql/base/testing/status_matchers.h"

namespace zetasql_base {
namespace {

using ::testing::Eq;
using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

// Converts a StatusBuilder to a Status.
Status ToStatus(const StatusBuilder& s) { return s; }

Status Cancelled() {
  return Status(StatusCode::kCancelled, "");
}

const StatusCode kZomg = StatusCode::kUnimplemented;
const SourceLocation kLoc = ZETASQL_LOC;

// Converts a StatusBuilder to a StatusOr<T>.
template <typename T>
StatusOr<T> ToStatusOr(const StatusBuilder& s) {
  return s;
}

// Makes a payload proto suitable for attaching to a status.
TestPayload MakePayloadProto(const std::string& str) {
  TestPayload proto;
  proto.set_message(str);
  return proto;
}

// Makes a message set contains a payload proto, suitable for passing to the
// constructor of `Status`.
void SetTestPayload(const std::string& str, Status* s) {
  TestPayload payload = MakePayloadProto(str);
  AttachPayload(s, payload);
}

Status StatusWithPayload(StatusCode code, const std::string& msg,
                         const std::string& str) {
  Status s(code, msg);
  SetTestPayload(str, &s);
  return s;
}

Status StatusWithPayload(StatusCode code, const std::string& msg, const std::string& str1,
                         const std::string& str2) {
  Status s(code, msg);
  SetTestPayload(str1, &s);
  SetTestPayload(str2, &s);
  return s;
}

TEST(StatusBuilderTest, Ctors) {
  EXPECT_EQ(ToStatus(StatusBuilder(kZomg, ZETASQL_LOC) << "zomg"),
            Status(kZomg, "zomg"));
}

TEST(StatusBuilderTest, Identity) {
  SourceLocation loc(ZETASQL_LOC);

  const std::vector<Status> statuses = {
      OkStatus(),
      Cancelled(),
      InvalidArgumentError("yup"),
  };

  for (const Status& base : statuses) {
    EXPECT_THAT(ToStatus(StatusBuilder(base, loc)), Eq(base));
    EXPECT_EQ(StatusBuilder(base, loc).ok(), base.ok());
    if (!base.ok()) {
      EXPECT_THAT(ToStatusOr<int>(StatusBuilder(base, loc)).status(), Eq(base));
    }
  }
}

TEST(StatusBuilderTest, SourceLocation) {
  const SourceLocation kLocation =
      SourceLocation::DoNotInvokeDirectly(0x42, "my_file");

  {
    const StatusBuilder builder(OkStatus(), kLocation);
    EXPECT_THAT(builder.source_location().file_name(),
                Eq(kLocation.file_name()));
    EXPECT_THAT(builder.source_location().line(), Eq(kLocation.line()));
  }
}

TEST(StatusBuilderTest, ErrorCode) {
  // OK
  {
    const StatusBuilder builder(OkStatus(), kLoc);
    EXPECT_TRUE(builder.ok());
    EXPECT_THAT(builder.code(), Eq(OK));
    EXPECT_FALSE(builder.Is(kZomg));
  }

  // Non-OK canonical code
  {
    const StatusBuilder builder(INVALID_ARGUMENT, kLoc);
    EXPECT_FALSE(builder.ok());
    EXPECT_THAT(builder.code(), Eq(INVALID_ARGUMENT));
    EXPECT_FALSE(builder.Is(kZomg));
  }
}

TEST(StatusBuilderTest, OkIgnoresStuff) {
  EXPECT_THAT(ToStatus(StatusBuilder(OkStatus(), kLoc) << "booyah"),
              Eq(OkStatus()));
  EXPECT_THAT(
      ToStatus(StatusBuilder(OkStatus(), kLoc)
          .Attach(MakePayloadProto("omg"))
               << "zombies"),
      Eq(OkStatus()));
}

TEST(StatusBuilderTest, Streaming) {
  EXPECT_THAT(ToStatus(StatusBuilder(Cancelled(), kLoc) << "booyah"),
              Eq(CancelledError("booyah")));
  EXPECT_THAT(ToStatus(StatusBuilder(AbortedError("hello"), kLoc) << "world"),
              Eq(AbortedError("hello; world")));
}

TEST(StatusBuilderTest, Prepend) {
  EXPECT_THAT(ToStatus(StatusBuilder(Cancelled(), kLoc).SetPrepend()
                       << "booyah"),
              Eq(CancelledError("booyah")));
  EXPECT_THAT(
      ToStatus(StatusBuilder(AbortedError(" hello"), kLoc).SetPrepend()
                       << "world"),
              Eq(AbortedError("world hello")));
}

TEST(StatusBuilderTest, Append) {
  EXPECT_THAT(
      ToStatus(StatusBuilder(Cancelled(), kLoc).SetAppend() << "booyah"),
      Eq(CancelledError("booyah")));
  EXPECT_THAT(ToStatus(StatusBuilder(AbortedError("hello"), kLoc).SetAppend()
                       << " world"),
              Eq(AbortedError("hello world")));
}

TEST(StatusBuilderTest, SetErrorCode) {
  EXPECT_THAT(ToStatus(StatusBuilder(Cancelled(), kLoc)
                           .Attach(MakePayloadProto("oops"))
                           .SetErrorCode(FAILED_PRECONDITION)),
              Eq(StatusWithPayload(FAILED_PRECONDITION, "", "oops")));
  EXPECT_THAT(ToStatus(StatusBuilder(CancelledError("monkey"), kLoc)
                           .SetErrorCode(FAILED_PRECONDITION)
                       << "taco"),
              Eq(Status(FAILED_PRECONDITION, "monkey; taco")));
}

TEST(StatusBuilderTest, Attach) {
  EXPECT_THAT(ToStatus(StatusBuilder(Cancelled(), kLoc)
                           .Attach(MakePayloadProto("oops"))),
      Eq(StatusWithPayload(CANCELLED, "", "oops")));
  EXPECT_THAT(ToStatus(StatusBuilder(Cancelled(), kLoc)
                           .Attach(MakePayloadProto("boom"))
               << "stick"),
      Eq(StatusWithPayload(CANCELLED, "stick", "boom")));
}

TEST(StatusBuilderTest, MultiAttach) {
  EXPECT_THAT(ToStatus(StatusBuilder(Cancelled(), kLoc)
                           .Attach(MakePayloadProto("msg1"))
                           .Attach(MakePayloadProto("msg2"))),
              Eq(StatusWithPayload(CANCELLED, "", "msg1", "msg2")));
  EXPECT_THAT(ToStatus(StatusBuilder(Cancelled(), kLoc)
                           .Attach(MakePayloadProto("boom"))
               << "stick"),
      Eq(StatusWithPayload(CANCELLED, "stick", "boom")));
}

TEST(StatusBuilderTest, BuilderOnNestedType) {
  static const char* const kError = "My custom error.";
  auto return_builder = []() -> StatusOr<StatusOr<int>> {
    return NotFoundErrorBuilder(ZETASQL_LOC) << kError;
  };
  EXPECT_THAT(return_builder(), StatusIs(NOT_FOUND, HasSubstr(kError)));
}

// This structure holds the details for testing a single canonical error code,
// its creator, and its classifier.
struct CanonicalErrorTest {
  StatusCode code;
  StatusBuilder (*creator)(SourceLocation);
};

constexpr CanonicalErrorTest kCanonicalErrorTests[]{
    {ABORTED, AbortedErrorBuilder},
    {ALREADY_EXISTS, AlreadyExistsErrorBuilder},
    {CANCELLED, CancelledErrorBuilder},
    {DATA_LOSS, DataLossErrorBuilder},
    {DEADLINE_EXCEEDED, DeadlineExceededErrorBuilder},
    {FAILED_PRECONDITION, FailedPreconditionErrorBuilder},
    {INTERNAL, InternalErrorBuilder},
    {INVALID_ARGUMENT, InvalidArgumentErrorBuilder},
    {NOT_FOUND, NotFoundErrorBuilder},
    {OUT_OF_RANGE, OutOfRangeErrorBuilder},
    {PERMISSION_DENIED, PermissionDeniedErrorBuilder},
    {UNAUTHENTICATED, UnauthenticatedErrorBuilder},
    {RESOURCE_EXHAUSTED, ResourceExhaustedErrorBuilder},
    {UNAVAILABLE, UnavailableErrorBuilder},
    {UNIMPLEMENTED, UnimplementedErrorBuilder},
    {UNKNOWN, UnknownErrorBuilder},
};

TEST(CanonicalErrorsTest, CreateAndClassify) {
  for (const auto& test : kCanonicalErrorTests) {
    SCOPED_TRACE(absl::StrCat("", StatusCodeToString(test.code)));

    // Ensure that the creator does, in fact, create status objects in the
    // canonical space, with the expected error code and message.
    std::string message = absl::StrCat("error code ", test.code, " test message");
    Status status = test.creator(ZETASQL_LOC) << message;
    EXPECT_EQ(test.code, status.code());
    EXPECT_EQ(message, status.message());
  }
}


}  // namespace
}  // namespace zetasql_base
