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

#ifndef ZETASQL_COMMON_TESTING_PROTO_MATCHERS_H_
#define ZETASQL_COMMON_TESTING_PROTO_MATCHERS_H_

#include "gmock/gmock.h"  

#include "zetasql/base/logging.h"
#include "google/protobuf/text_format.h"
#include "google/protobuf/util/message_differencer.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace proto_matchers_internal {

bool InternalProtoEqual(const google::protobuf::Message& msg1,
                        const google::protobuf::Message& msg2) {
  return google::protobuf::util::MessageDifferencer::Equals(msg1, msg2);
}

bool InternalProtoEqual(const google::protobuf::Message& msg1,
                        absl::string_view msg2_text) {
  google::protobuf::Message* msg2 = msg1.New();
  CHECK(google::protobuf::TextFormat::ParseFromString(std::string(msg2_text), msg2));
  return InternalProtoEqual(msg1, *msg2);
}
}  // namespace proto_matchers_internal

namespace testing {

MATCHER_P(EqualsProto, expected, "") {
  return ::zetasql::proto_matchers_internal::InternalProtoEqual(arg, expected);
}

}  // namespace testing
}  // namespace zetasql

#endif  // ZETASQL_COMMON_TESTING_PROTO_MATCHERS_H_
