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

#include <iostream>
#include <unordered_map>

#include "absl/strings/str_cat.h"
#include "zetasql/base/canonical_errors.h"
#include "zetasql/base/logging.h"
#include "zetasql/base/status.h"

namespace zetasql_base {

static void CopyStatusPayloads(const Status& from, Status* to) {
  from.ForEachPayload([to](absl::string_view type_url, StatusCord payload) {
      to->SetPayload(type_url, payload);});
}

StatusBuilder& StatusBuilder::SetErrorCode(StatusCode code) {
  Status tmp = Status(code, status_.message());
  CopyStatusPayloads(status_, &tmp);
  status_ = std::move(tmp);
  return *this;
}

StatusBuilder::Rep::Rep(const Rep& r)
    : logging_mode(r.logging_mode),
      log_severity(r.log_severity),
      verbose_level(r.verbose_level),
      stream(),
      should_log_stack_trace(r.should_log_stack_trace),
      message_join_style(r.message_join_style) {
  stream << r.stream.str();
}

Status StatusBuilder::JoinMessageToStatus(Status s, absl::string_view msg,
                                          MessageJoinStyle style) {
  if (msg.empty()) return s;

  std::string new_msg;
  if (s.message().empty()) {
    new_msg = std::string(msg);
  } else if (style == MessageJoinStyle::kAnnotate) {
    new_msg = absl::StrCat(s.message(), "; ", msg);
  } else if (style == MessageJoinStyle::kPrepend) {
    new_msg = absl::StrCat(msg, s.message());
  } else {  // kAppend
    new_msg = absl::StrCat(s.message(), msg);
  }
  Status tmp(s.code(), new_msg);
  CopyStatusPayloads(s, &tmp);
  return tmp;
}

void StatusBuilder::ConditionallyLog(const Status& result) const {
  if (rep_->logging_mode == Rep::LoggingMode::kDisabled) return;

  absl::LogSeverity severity = rep_->log_severity;

  zetasql_base::logging_internal::LogMessage log_message(
      location_.file_name(), location_.line(), severity);
  log_message.stream() << result;
  if (rep_->should_log_stack_trace) {
    log_message.stream() << "\n";
    // TODO:   << CurrentStackTrace();
  }
}

Status StatusBuilder::CreateStatusAndConditionallyLog() && {
  Status result = JoinMessageToStatus(std::move(status_), rep_->stream.str(),
                                      rep_->message_join_style);
  ConditionallyLog(result);

  // We consumed the status above, we set it to some error just to prevent
  // people relying on it become OK or something.
  status_ = UnknownError("");
  rep_ = nullptr;
  return result;
}

StatusBuilder AbortedErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(ABORTED, location);
}

StatusBuilder AlreadyExistsErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(ALREADY_EXISTS, location);
}

StatusBuilder CancelledErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(CANCELLED, location);
}

StatusBuilder DataLossErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(DATA_LOSS, location);
}

StatusBuilder DeadlineExceededErrorBuilder(
    zetasql_base::SourceLocation location) {
  return StatusBuilder(DEADLINE_EXCEEDED, location);
}

StatusBuilder FailedPreconditionErrorBuilder(
    zetasql_base::SourceLocation location) {
  return StatusBuilder(FAILED_PRECONDITION, location);
}

StatusBuilder InternalErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(INTERNAL, location);
}

StatusBuilder InvalidArgumentErrorBuilder(
    zetasql_base::SourceLocation location) {
  return StatusBuilder(INVALID_ARGUMENT, location);
}

StatusBuilder NotFoundErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(NOT_FOUND, location);
}

StatusBuilder OutOfRangeErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(OUT_OF_RANGE, location);
}

StatusBuilder PermissionDeniedErrorBuilder(
    zetasql_base::SourceLocation location) {
  return StatusBuilder(PERMISSION_DENIED, location);
}

StatusBuilder UnauthenticatedErrorBuilder(
    zetasql_base::SourceLocation location) {
  return StatusBuilder(UNAUTHENTICATED, location);
}

StatusBuilder ResourceExhaustedErrorBuilder(
    zetasql_base::SourceLocation location) {
  return StatusBuilder(RESOURCE_EXHAUSTED, location);
}

StatusBuilder UnavailableErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(UNAVAILABLE, location);
}

StatusBuilder UnimplementedErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(UNIMPLEMENTED, location);
}

StatusBuilder UnknownErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(UNKNOWN, location);
}

}  // namespace zetasql_base
