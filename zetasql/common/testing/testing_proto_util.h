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

#ifndef ZETASQL_COMMON_TESTING_TESTING_PROTO_UTIL_H_
#define ZETASQL_COMMON_TESTING_TESTING_PROTO_UTIL_H_

#include "google/protobuf/message.h"
#include "absl/strings/cord.h"

namespace zetasql {

inline absl::Cord SerializeToCord(const google::protobuf::Message& pb) {
  absl::Cord bytes;
  std::string bytes_str;
  CHECK(pb.SerializeToString(&bytes_str));
  bytes = absl::Cord(bytes_str);
  return bytes;
}

inline absl::Cord SerializePartialToCord(const google::protobuf::Message& pb) {
  absl::Cord bytes;
  std::string bytes_str;
  CHECK(pb.SerializePartialToString(&bytes_str));
  bytes = absl::Cord(bytes_str);
  return bytes;
}

inline bool ParseFromCord(absl::Cord bytes, google::protobuf::Message* pb) {
  std::string bytes_str(bytes);
  return pb->ParseFromString(std::string(bytes));
}

inline bool ParsePartialFromCord(absl::Cord bytes, google::protobuf::Message* pb) {
  std::string bytes_str(bytes);
  return pb->ParsePartialFromString(std::string(bytes_str));
}

}  // namespace zetasql

#endif  // ZETASQL_COMMON_TESTING_TESTING_PROTO_UTIL_H_
