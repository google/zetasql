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

#include "zetasql/common/warning_sink.h"

#include "zetasql/common/status_payload_utils.h"
#include "zetasql/public/deprecation_warning.pb.h"
#include "absl/status/status.h"
#include "absl/types/span.h"

namespace zetasql {

WarningSink::WarningSink() = default;

absl::Status WarningSink::AddWarning(DeprecationWarning::Kind kind,
                                     absl::Status warning) {
  // If the warning already exists, return early.
  if (!unique_warnings_.emplace(kind, warning.message()).second) {
    return absl::OkStatus();
  }
  DeprecationWarning warning_proto;
  warning_proto.set_kind(kind);
  internal::AttachPayload(&warning, warning_proto);
  warnings_.push_back(warning);
  return absl::OkStatus();
}

absl::Span<const absl::Status> WarningSink::warnings() const {
  return absl::MakeSpan(warnings_);
}

void WarningSink::Reset() {
  warnings_.clear();
  unique_warnings_.clear();
}

}  // namespace zetasql
