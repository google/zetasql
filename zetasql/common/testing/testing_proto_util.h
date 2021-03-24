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

#ifndef ZETASQL_COMMON_TESTING_TESTING_PROTO_UTIL_H_
#define ZETASQL_COMMON_TESTING_TESTING_PROTO_UTIL_H_

#include "zetasql/base/path.h"
#include "google/protobuf/compiler/importer.h"
#include "google/protobuf/message.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/strings/cord.h"

namespace zetasql {

inline absl::Cord SerializeToCord(const google::protobuf::Message& pb) {
  absl::Cord bytes;
  std::string bytes_str;
  ZETASQL_CHECK(pb.SerializeToString(&bytes_str));
  bytes = absl::Cord(bytes_str);
  return bytes;
}

inline absl::Cord SerializePartialToCord(const google::protobuf::Message& pb) {
  absl::Cord bytes;
  std::string bytes_str;
  ZETASQL_CHECK(pb.SerializePartialToString(&bytes_str));
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

inline std::unique_ptr<google::protobuf::compiler::DiskSourceTree>
CreateProtoSourceTree() {
  auto source_tree = absl::make_unique<google::protobuf::compiler::DiskSourceTree>();
  // Support both sides of --noincompatible_generated_protos_in_virtual_imports.
  for (std::string vproto :
       {"any_proto",
        "api_proto",
        "descriptor_proto",
        "duration_proto",
        "empty_proto",
        "field_mask_proto",
        "source_context_proto",
        "struct_proto",
        "timestamp_proto",
        "type_proto",
        "wrappers_proto"}) {
    source_tree->MapPath("",
      zetasql_base::JoinPath(getenv("TEST_SRCDIR"), "com_google_protobuf",
                             "_virtual_imports", vproto));
  }
  source_tree->MapPath(
      "", zetasql_base::JoinPath(getenv("TEST_SRCDIR"), "com_google_protobuf"));
  source_tree->MapPath(
      "", zetasql_base::JoinPath(getenv("TEST_SRCDIR"), "com_googleapis_googleapis"));
  source_tree->MapPath(
      "", zetasql_base::JoinPath(getenv("TEST_SRCDIR"), "com_google_zetasql"));
  return source_tree;
}

}  // namespace zetasql

#endif  // ZETASQL_COMMON_TESTING_TESTING_PROTO_UTIL_H_
