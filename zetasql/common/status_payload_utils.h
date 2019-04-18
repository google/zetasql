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

#ifndef ZETASQL_COMMON_STATUS_PAYLOAD_UTILS_OSS_H_
#define ZETASQL_COMMON_STATUS_PAYLOAD_UTILS_OSS_H_

// Utilities to abstract zetasql_base::Status payload interactions.

#include "google/protobuf/message.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_payload.h"

namespace zetasql {
namespace internal {

// Whether the given status has any payload at all.
bool HasPayload(const zetasql_base::Status& status);

// Gets the number payloads.
int GetPayloadCount(const zetasql_base::Status& status);

template <class T>
std::string GetTypeUrl() {
  return zetasql_base::GetTypeUrl<T>();
}

// Whether the given status has exactly one payload of type T.
template <class T>
bool HasPayloadTyped(const zetasql_base::Status& status) {
  return status.GetPayload(zetasql_base::GetTypeUrl<T>()).has_value();
}

// Gets the payload of type T from the status proto. Results undefined if
// the status does not contain a payload of the given type, but will not crash.
template <class T>
T GetPayload(const zetasql_base::Status& status) {
  absl::optional<zetasql_base::StatusCord> payload =
      status.GetPayload(GetTypeUrl<T>());
  if (!payload.has_value()) {
    return T();
  }
  T proto;
  // TODO: switch to cord once available.
  if (!proto.ParseFromString(*payload)) {
    proto.Clear();  // Prefer empty over partial??
  }
  return proto;
}

// Mutates `status` by removing any attached payload of type T.
template <class T>
void ErasePayloadTyped(zetasql_base::Status* status) {
  status->ErasePayload(GetTypeUrl<T>());
}

// Attaches the given payload. This will overwrite any previous payload with
// the same type.
template <class T>
void AttachPayload(zetasql_base::Status* status, const T& payload) {
  zetasql_base::AttachPayload<T>(status, payload);
}

// Creates a human readable std::string from the status payload (or empty if there
// is no payload). Exact form is not defined.
std::string PayloadToString(const zetasql_base::Status& status);

// Creates a human readable std::string from the status, including its payload.
// Exact form is not defined.
std::string StatusToString(const zetasql_base::Status& status);

}  // namespace internal
}  // namespace zetasql

#endif  // ZETASQL_COMMON_STATUS_PAYLOAD_UTILS_OSS_H_
