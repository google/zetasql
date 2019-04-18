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

#include "zetasql/common/status_payload_utils.h"
#include "google/protobuf/descriptor_database.h"
#include "google/protobuf/message.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/strip.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_payload.h"

namespace zetasql {
namespace internal {

// Whether the given status has any payload at all. The payloads themselves
// may be empty.
bool HasPayload(const zetasql_base::Status& status) {
  return GetPayloadCount(status) > 0;
}

// Get the number of items within the payload.
int GetPayloadCount(const zetasql_base::Status& status) {
  // Status interface forces scan for counting.
  int count = 0;
  status.ForEachPayload([&count](absl::string_view,
                                 const zetasql_base::StatusCord&) { ++count; });
  return count;
}

std::string PayloadToString(absl::string_view type_url,
                       const zetasql_base::StatusCord& payload) {
  absl::string_view descriptor_full_name = type_url;
  if (absl::ConsumePrefix(&descriptor_full_name,
                          zetasql_base::kZetaSqlTypeUrlPrefix)) {
    const google::protobuf::DescriptorPool* pool =
        google::protobuf::DescriptorPool::generated_pool();
    const google::protobuf::Descriptor* desc =
        pool->FindMessageTypeByName(std::string(descriptor_full_name));
    if (desc != nullptr) {
      google::protobuf::MessageFactory* factory =
          google::protobuf::MessageFactory::generated_factory();
      auto msg = absl::WrapUnique(factory->GetPrototype(desc)->New());
      if (msg->ParseFromString(payload)) {
        return absl::StrCat("[", descriptor_full_name, "] { ",
                            msg->ShortDebugString(), " }");
      }
    }
  }
  return absl::StrCat("[", type_url, "] <unknown type>");
}

// StatusCodeToString does not guarantee any format, this guarantees
// lower-case camel case (except OK, which is just "OK"). Invalid codes
// will be printed as an integer.
std::string LegacyStatusCodeToString(zetasql_base::StatusCode code) {
  // TODO: Fix std::string rendering once there is a
  //                              free function we can clone.
  switch (code) {
    case zetasql_base::StatusCode::kOk:
      return "OK";
    case zetasql_base::StatusCode::kCancelled:
      return "generic::cancelled";
    case zetasql_base::StatusCode::kUnknown:
      return "generic::unknown";
    case zetasql_base::StatusCode::kInvalidArgument:
      return "generic::invalid_argument";
    case zetasql_base::StatusCode::kDeadlineExceeded:
      return "generic::deadline_exceeded";
    case zetasql_base::StatusCode::kNotFound:
      return "generic::not_found";
    case zetasql_base::StatusCode::kAlreadyExists:
      return "generic::already_exists";
    case zetasql_base::StatusCode::kPermissionDenied:
      return "generic::permission_denied";
    case zetasql_base::StatusCode::kUnauthenticated:
      return "generic::unauthenticated";
    case zetasql_base::StatusCode::kResourceExhausted:
      return "generic::resource_exhausted";
    case zetasql_base::StatusCode::kFailedPrecondition:
      return "generic::failed_precondition";
    case zetasql_base::StatusCode::kAborted:
      return "generic::aborted";
    case zetasql_base::StatusCode::kOutOfRange:
      return "generic::out_of_range";
    case zetasql_base::StatusCode::kUnimplemented:
      return "generic::unimplemented";
    case zetasql_base::StatusCode::kInternal:
      return "generic::internal";
    case zetasql_base::StatusCode::kUnavailable:
      return "generic::unavailable";
    case zetasql_base::StatusCode::kDataLoss:
      return "generic::data_loss";
    default:
      return absl::StrCat(code);
  }
}

std::string PayloadToString(const zetasql_base::Status& status) {
  std::string ret;
  // Make our own version of absl::Join
  bool prepend_space = false;
  status.ForEachPayload(
      [&ret, &prepend_space](absl::string_view type_url,
                             const zetasql_base::StatusCord& payload) {
        absl::StrAppend(&ret, prepend_space ? " " : "",
                        PayloadToString(type_url, payload));
        prepend_space = true;
      });

  return ret;
}

// Creates a human readable std::string from the status, including its payload.
// Exact form is not defined.
std::string StatusToString(const zetasql_base::Status& status) {
  if (status.ok()) {
    return "OK";
  }
  std::string ret = absl::StrCat(LegacyStatusCodeToString(status.code()), ": ",
                            status.message());
  if (HasPayload(status)) {
    absl::StrAppend(&ret, " ", PayloadToString(status));
  }
  return ret;
}

}  // namespace internal
}  // namespace zetasql
