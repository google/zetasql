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

#ifndef THIRD_PARTY_ZETASQL_ZETASQL_BASE_RET_CHECK_H_
#define THIRD_PARTY_ZETASQL_ZETASQL_BASE_RET_CHECK_H_

// Macros for non-fatal assertions.  The `ZETASQL_RET_CHECK` family of macros
// mirrors the `ZETASQL_CHECK` family from "base/logging.h", but instead of aborting the
// process on failure, these return a absl::Status with code
// `zetasql::StatusCode::kInternal` from the current method.
//
//   ZETASQL_RET_CHECK(ptr != nullptr);
//   ZETASQL_RET_CHECK_GT(value, 0) << "Optional additional message";
//   ZETASQL_RET_CHECK_FAIL() << "Always fails";
//   ZETASQL_RET_CHECK_OK(status) << "If status is not OK, return an internal
//   error";
//
// The ZETASQL_RET_CHECK* macros can only be used in functions that return
// absl::Status or absl::StatusOr.  The generated
// `absl::Status` will contain the string "ZETASQL_RET_CHECK failure".
//
// On failure these routines will log a stack trace to `ERROR`.  The
// `ZETASQL_RET_CHECK` macros end with a `zetasql_base::StatusBuilder` in their
// tail position and can be customized like calls to `ZETASQL_RETURN_IF_ERROR`
// from `status_macros.h`.
//

#include <string>

#include "absl/status/status.h"
#include "zetasql/base/logging.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql_base {
namespace internal_ret_check {

// Returns a StatusBuilder that corresponds to a `ZETASQL_RET_CHECK` failure.
StatusBuilder RetCheckFailSlowPath(SourceLocation location);
StatusBuilder RetCheckFailSlowPath(SourceLocation location,
                                   const char* condition);
StatusBuilder RetCheckFailSlowPath(SourceLocation location,
                                   const char* condition,
                                   const absl::Status& s);

// Takes ownership of `condition`.  This API is a little quirky because it is
// designed to make use of the `::Check_*Impl` methods that implement `CHECK_*`
// and `DCHECK_*`.
StatusBuilder RetCheckFailSlowPath(SourceLocation location,
                                   std::string* condition);

inline StatusBuilder RetCheckImpl(const absl::Status& status,
                                  const char* condition,
                                  SourceLocation location) {
  if (ABSL_PREDICT_TRUE(status.ok()))
    return StatusBuilder(absl::OkStatus(), location);
  return RetCheckFailSlowPath(location, condition, status);
}

}  // namespace internal_ret_check
}  // namespace zetasql_base

#define ZETASQL_RET_CHECK(cond)                                                \
  while (ABSL_PREDICT_FALSE(!(cond)))                                          \
  return ::zetasql_base::internal_ret_check::RetCheckFailSlowPath(ZETASQL_LOC, \
                                                                  #cond)

#define ZETASQL_RET_CHECK_FAIL() \
  return ::zetasql_base::internal_ret_check::RetCheckFailSlowPath(ZETASQL_LOC)

// Takes an expression returning absl::Status and asserts that the
// status is `ok()`.  If not, it returns an internal error.
//
// This is similar to `ZETASQL_RETURN_IF_ERROR` in that it propagates errors.
// The difference is that it follows the behavior of `ZETASQL_RET_CHECK`,
// returning an internal error (wrapping the original error text), including the
// filename and line number, and logging a stack trace.
//
// This is appropriate to use to write an assertion that a function that returns
// `absl::Status` cannot fail, particularly when the error code itself
// should not be surfaced.
#define ZETASQL_RET_CHECK_OK(status)                                        \
  ZETASQL_RETURN_IF_ERROR(::zetasql_base::internal_ret_check::RetCheckImpl( \
      (status), #status, ZETASQL_LOC))

#if defined(STATIC_ANALYSIS)
#define ZETASQL_STATUS_MACROS_INTERNAL_RET_CHECK_OP(name, op, lhs, rhs) \
  ZETASQL_RET_CHECK((lhs)op(rhs))
#else
#define ZETASQL_STATUS_MACROS_INTERNAL_RET_CHECK_OP(name, op, lhs, rhs)        \
  while (std::string* _result = zetasql_base::Check_##name##Impl(              \
             ::zetasql_base::GetReferenceableValue(lhs),                       \
             ::zetasql_base::GetReferenceableValue(rhs),                       \
             #lhs " " #op " " #rhs))                                           \
  return ::zetasql_base::internal_ret_check::RetCheckFailSlowPath(ZETASQL_LOC, \
                                                                  _result)
#endif

#define ZETASQL_RET_CHECK_EQ(lhs, rhs) \
  ZETASQL_STATUS_MACROS_INTERNAL_RET_CHECK_OP(EQ, ==, lhs, rhs)
#define ZETASQL_RET_CHECK_NE(lhs, rhs) \
  ZETASQL_STATUS_MACROS_INTERNAL_RET_CHECK_OP(NE, !=, lhs, rhs)
#define ZETASQL_RET_CHECK_LE(lhs, rhs) \
  ZETASQL_STATUS_MACROS_INTERNAL_RET_CHECK_OP(LE, <=, lhs, rhs)
#define ZETASQL_RET_CHECK_LT(lhs, rhs) \
  ZETASQL_STATUS_MACROS_INTERNAL_RET_CHECK_OP(LT, <, lhs, rhs)
#define ZETASQL_RET_CHECK_GE(lhs, rhs) \
  ZETASQL_STATUS_MACROS_INTERNAL_RET_CHECK_OP(GE, >=, lhs, rhs)
#define ZETASQL_RET_CHECK_GT(lhs, rhs) \
  ZETASQL_STATUS_MACROS_INTERNAL_RET_CHECK_OP(GT, >, lhs, rhs)

#endif  // THIRD_PARTY_ZETASQL_ZETASQL_BASE_RET_CHECK_H_
