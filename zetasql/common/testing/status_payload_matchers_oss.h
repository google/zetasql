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

#ifndef ZETASQL_COMMON_TESTING_STATUS_PAYLOAD_MATCHERS_OSS_H_
#define ZETASQL_COMMON_TESTING_STATUS_PAYLOAD_MATCHERS_OSS_H_

// Testing utilities for working with absl::Status and absl::StatusOr.
//
// Defines the following utilities:
//
//   =================================
//   StatusHasPayload(payload_matcher)
//   =================================
//
//   Matches an absl::Status or absl::StatusOr<T> value whose status value is
//   not absl::Status::Ok and whose payload has a matching message. Note
//   that it is sufficient for the payload_matcher to match a single payload
//   message which is selected based on the requested type.
//
//   Example:
//     using ::zetasql_base::testing::StatusHasPayload;
//
//     StatusOr<string> error = absl::InternalError("error");
//     MyProto my_proto;
//     my_proto....
//     zetasql::internal::AttachPayload(&rror, my_proto);
//
//     // Test for an actual matching message.
//     EXPECT_THAT(error, StatusHasPayload<MyProto>(EqualsProto(my_proto)));
//
//     // Test for the presence of an attachment of the specified type.
//     EXPECT_THAT(error, StatusHasPayload<MyProto>());
//
//     // Test for the presence of any attachment of any type.
//     EXPECT_THAT(error, StatusHasPayload());
//

#include <ostream>
#include <string>
#include <type_traits>

#include "google/protobuf/descriptor.h"
#include "zetasql/common/status_payload_utils.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/testing/status_matchers.h"

namespace zetasql {
namespace testing {
namespace internal_status {

using ::zetasql_base::testing::internal_status::GetStatus;

template <typename T, typename PayloadProtoType>
class PayloadMatcherImpl : public ::testing::MatcherInterface<T> {
 public:
  using PayloadProtoMatcher = ::testing::Matcher<PayloadProtoType>;

  explicit PayloadMatcherImpl(const PayloadProtoMatcher& payload_proto_matcher)
      : payload_proto_matcher_(payload_proto_matcher) {}

  void DescribeTo(std::ostream* os) const override {
    *os << "has a payload that ";
    payload_proto_matcher_.DescribeTo(os);
  }

  void DescribeNegationTo(std::ostream* os) const override {
    *os << "has no payload that ";
    payload_proto_matcher_.DescribeTo(os);
  }

  bool MatchAndExplain(T actual,
                       ::testing::MatchResultListener* o) const override {
    const auto& actual_status = GetStatus(actual);
    if (actual_status.ok()) {
      *o << "which is OK and has no payload";
      return false;
    }

    if (!zetasql::internal::HasPayloadWithType<PayloadProtoType>(
            actual_status)) {
      *o << "which has no payload of type "
         << PayloadProtoType::descriptor()->full_name();
      return false;
    }

    const PayloadProtoType message =
        zetasql::internal::GetPayload<PayloadProtoType>(actual_status);

    return payload_proto_matcher_.MatchAndExplain(message, o);
  }

 private:
  const PayloadProtoMatcher payload_proto_matcher_;
};

template <typename PayloadProtoType, typename Enable = void>
class PayloadMatcher {};

template <typename T>
using IsMessageType = std::is_base_of<::google::protobuf::Message, T>;

template <typename PayloadProtoType>
class PayloadMatcher<
    PayloadProtoType,
    absl::enable_if_t<IsMessageType<PayloadProtoType>::value>> {
 public:
  using PayloadProtoMatcher = ::testing::Matcher<PayloadProtoType>;

  explicit PayloadMatcher(PayloadProtoMatcher&& payload_proto_matcher)
      : payload_proto_matcher_(std::move(payload_proto_matcher)) {}

  // Converts this polymorphic matcher to a monomorphic matcher of the
  // given type. StatusOrType can be either StatusOr<T> or a
  // reference to StatusOr<T>.
  template <typename StatusOrType>
  operator ::testing::Matcher<StatusOrType>() const {  // NOLINT
    return ::testing::Matcher<StatusOrType>(
        new PayloadMatcherImpl<const StatusOrType&, PayloadProtoType>(
            payload_proto_matcher_));
  }

 private:
  const PayloadProtoMatcher payload_proto_matcher_;
};

}  // namespace internal_status

// Returns a gMock matcher that matches the payload of a Status or StatusOr<>
// against payload_matcher.
// We do not default to google::protobuf::Message since we want the exact type to be
// specified.
template <typename PayloadProtoType>
inline internal_status::PayloadMatcher<PayloadProtoType> StatusHasPayload(
    ::testing::Matcher<PayloadProtoType> payload_proto_matcher) {
  return internal_status::PayloadMatcher<PayloadProtoType>(
      std::move(::testing::Matcher<PayloadProtoType>(payload_proto_matcher)));
}

// Returns a gMock matcher that matches if a Status or StatusOr<> has a payload
// of the requested type.
// The requested type defaults to google::protobuf::Message, so that we can test for the
// presence of any type and thus any payload of any type.
template <typename PayloadProtoType = google::protobuf::Message>
inline internal_status::PayloadMatcher<PayloadProtoType> StatusHasPayload() {
  return internal_status::PayloadMatcher<PayloadProtoType>(
      ::testing::A<PayloadProtoType>());
}

}  // namespace testing
}  // namespace zetasql

#endif  // ZETASQL_COMMON_TESTING_STATUS_PAYLOAD_MATCHERS_OSS_H_
