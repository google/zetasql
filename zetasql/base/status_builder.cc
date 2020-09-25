//
// Copyright 2018 Google LLC
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

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/logging.h"

namespace zetasql_base {

static void CopyStatusPayloads(const absl::Status& from, absl::Status* to) {
  from.ForEachPayload([to](absl::string_view type_url, absl::Cord payload) {
    to->SetPayload(type_url, payload);
  });
}

StatusBuilder& StatusBuilder::SetErrorCode(absl::StatusCode code) {
  absl::Status tmp = absl::Status(code, status_.message());
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

absl::Status StatusBuilder::JoinMessageToStatus(absl::Status s,
                                                absl::string_view msg,
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
  absl::Status tmp(s.code(), new_msg);
  CopyStatusPayloads(s, &tmp);
  return tmp;
}

void StatusBuilder::ConditionallyLog(const absl::Status& result) const {
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

absl::Status StatusBuilder::CreateStatusAndConditionallyLog() && {
  absl::Status result = JoinMessageToStatus(
      std::move(status_), rep_->stream.str(), rep_->message_join_style);
  ConditionallyLog(result);

  // We consumed the status above, we set it to some error just to prevent
  // people relying on it become OK or something.
  status_ = absl::UnknownError("");
  rep_ = nullptr;
  return result;
}

StatusBuilder AbortedErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kAborted, location);
}

StatusBuilder AlreadyExistsErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kAlreadyExists, location);
}

StatusBuilder CancelledErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kCancelled, location);
}

StatusBuilder DataLossErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kDataLoss, location);
}

StatusBuilder DeadlineExceededErrorBuilder(
    zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kDeadlineExceeded, location);
}

StatusBuilder FailedPreconditionErrorBuilder(
    zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kFailedPrecondition, location);
}

StatusBuilder InternalErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kInternal, location);
}

StatusBuilder InvalidArgumentErrorBuilder(
    zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kInvalidArgument, location);
}

StatusBuilder NotFoundErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kNotFound, location);
}

StatusBuilder OutOfRangeErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kOutOfRange, location);
}

StatusBuilder PermissionDeniedErrorBuilder(
    zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kPermissionDenied, location);
}

StatusBuilder UnauthenticatedErrorBuilder(
    zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kUnauthenticated, location);
}

StatusBuilder ResourceExhaustedErrorBuilder(
    zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kResourceExhausted, location);
}

StatusBuilder UnavailableErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kUnavailable, location);
}

StatusBuilder UnimplementedErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kUnimplemented, location);
}

StatusBuilder UnknownErrorBuilder(zetasql_base::SourceLocation location) {
  return StatusBuilder(absl::StatusCode::kUnknown, location);
}

}  // namespace zetasql_base
