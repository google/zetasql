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

#ifndef THIRD_PARTY_ZETASQL_ZETASQL_BASE_STATUS_H_
#define THIRD_PARTY_ZETASQL_ZETASQL_BASE_STATUS_H_

#include "absl/status/status.h"  

namespace zetasql_base {
using absl::OkStatus;
using absl::Status;
using absl::StatusCode;
using absl::StatusCodeToString;

// Handle both for now. This is meant to be _very short lived. Once internal
// code can safely use the new naming, we will switch that that and drop this.
constexpr StatusCode OK = StatusCode::kOk;
constexpr StatusCode CANCELLED = StatusCode::kCancelled;
constexpr StatusCode UNKNOWN = StatusCode::kUnknown;
constexpr StatusCode INVALID_ARGUMENT = StatusCode::kInvalidArgument;
constexpr StatusCode DEADLINE_EXCEEDED = StatusCode::kDeadlineExceeded;
constexpr StatusCode NOT_FOUND = StatusCode::kNotFound;
constexpr StatusCode ALREADY_EXISTS = StatusCode::kAlreadyExists;
constexpr StatusCode PERMISSION_DENIED = StatusCode::kPermissionDenied;
constexpr StatusCode UNAUTHENTICATED = StatusCode::kUnauthenticated;
constexpr StatusCode RESOURCE_EXHAUSTED = StatusCode::kResourceExhausted;
constexpr StatusCode FAILED_PRECONDITION = StatusCode::kFailedPrecondition;
constexpr StatusCode ABORTED = StatusCode::kAborted;
constexpr StatusCode OUT_OF_RANGE = StatusCode::kOutOfRange;
constexpr StatusCode UNIMPLEMENTED = StatusCode::kUnimplemented;
constexpr StatusCode INTERNAL = StatusCode::kInternal;
constexpr StatusCode UNAVAILABLE = StatusCode::kUnavailable;
constexpr StatusCode DATA_LOSS = StatusCode::kDataLoss;

using absl::AbortedError;
using absl::AlreadyExistsError;
using absl::CancelledError;
using absl::DataLossError;
using absl::DeadlineExceededError;
using absl::FailedPreconditionError;
using absl::InternalError;
using absl::InvalidArgumentError;
using absl::NotFoundError;
using absl::OutOfRangeError;
using absl::PermissionDeniedError;
using absl::ResourceExhaustedError;
using absl::UnauthenticatedError;
using absl::UnavailableError;
using absl::UnimplementedError;
using absl::UnknownError;

using absl::IsAborted;
using absl::IsAlreadyExists;
using absl::IsCancelled;
using absl::IsDataLoss;
using absl::IsDeadlineExceeded;
using absl::IsFailedPrecondition;
using absl::IsInternal;
using absl::IsInvalidArgument;
using absl::IsNotFound;
using absl::IsOutOfRange;
using absl::IsPermissionDenied;
using absl::IsResourceExhausted;
using absl::IsUnauthenticated;
using absl::IsUnavailable;
using absl::IsUnimplemented;
using absl::IsUnknown;

}  // namespace zetasql_base

// This is better than CHECK((val).ok()) because the embedded
// error string gets printed by the CHECK_EQ.
#define ZETASQL_CHECK_OK(val) CHECK_EQ(::absl::OkStatus(), (val))
#define ZETASQL_DCHECK_OK(val) DCHECK_EQ(::absl::OkStatus(), (val))
#define ZETASQL_ZETASQL_CHECK_OK(val) DCHECK_EQ(::absl::OkStatus(), (val))

#endif  // THIRD_PARTY_ZETASQL_ZETASQL_BASE_STATUS_H_
