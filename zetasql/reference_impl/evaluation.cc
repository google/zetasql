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

#include "zetasql/reference_impl/evaluation.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/internal_value.h"
#include "zetasql/common/thread_stack.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/tuple.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/clock.h"

ABSL_FLAG(
    int64_t, zetasql_call_verify_not_aborted_rows_period, 1000,
    "Only call EvaluationContext::VerifyNotAborted() every this many rows");

namespace zetasql {

absl::Status ValidateFirstColumnPrimaryKey(
    absl::string_view table_name, const Value& array,
    const LanguageOptions& language_options) {
  ZETASQL_RET_CHECK(array.type()->IsArray());

  const Type* row_type = array.type()->AsArray()->element_type();
  ZETASQL_RET_CHECK(row_type->IsStruct()) << table_name;
  ZETASQL_RET_CHECK_GT(row_type->AsStruct()->num_fields(), 0);
  const Type* first_column_type = row_type->AsStruct()->field(0).type;

  if (!first_column_type->SupportsGrouping(language_options,
                                           nullptr /* no_grouping_type */)) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "The first column of table " << table_name
           << " does not support grouping";
  }

  absl::flat_hash_set<Value> values_in_first_column;
  for (int i = 0; i < array.num_elements(); i++) {
    const Value& first_column = array.element(i).field(0);
    if (first_column.is_null()) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "The first column of table " << table_name
             << " has a NULL Value";
    }
    if (!zetasql_base::InsertIfNotPresent(&values_in_first_column, first_column)) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "The first column of table " << table_name
             << " has duplicate Value "
             << first_column.DebugString(true /* verbose */);
    }
  }

  return absl::OkStatus();
}

EvaluationContext::EvaluationContext(const EvaluationOptions& options)
    : EvaluationContext(
          options,
          std::make_shared<MemoryAccountant>(options.max_intermediate_byte_size,
                                             "max_intermediate_byte_size"),
          /*parent_context=*/nullptr) {}
EvaluationContext::EvaluationContext(
    const EvaluationOptions& options,
    std::shared_ptr<MemoryAccountant> memory_accountant,
    EvaluationContext* parent_context)
    : options_(options),
      memory_accountant_(memory_accountant),
      deterministic_output_(true),
      parent_context_(parent_context) {}

std::unique_ptr<EvaluationContext> EvaluationContext::MakeChildContext() const {
  EvaluationContext* mutable_parent_ref = const_cast<EvaluationContext*>(this);
  std::unique_ptr<EvaluationContext> child_context = absl::WrapUnique(
      new EvaluationContext(options_, memory_accountant_, mutable_parent_ref));
  child_context->tables_ = tables_;
  child_context->SetLanguageOptions(language_options_);
  child_context->SetSessionUser(session_user_);
  child_context->SetStatementEvaluationDeadline(statement_eval_deadline_);
  if (!IsDeterministicOutput()) {
    child_context->SetNonDeterministicOutput();
  }
  return child_context;
}

absl::Status EvaluationContext::AddTableAsArray(
    absl::string_view table_name, bool is_value_table, Value array,
    const LanguageOptions& language_options) {
  ZETASQL_RET_CHECK(array.type()->IsArray());
  if (!is_value_table && options_.emulate_primary_keys) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateFirstColumnPrimaryKey(table_name, array, language_options));
  }
  if (InternalValue::GetOrderKind(array) == InternalValue::kPreservesOrder) {
    // Make array unordered, since tables are unordered.
    std::vector<Value> elements = array.elements();
    array = InternalValue::ArrayNotChecked(array.type()->AsArray(),
                                           InternalValue::kIgnoresOrder,
                                           std::move(elements));
  }
  ZETASQL_RET_CHECK(tables_.emplace(table_name, array).second) << table_name;
  return absl::OkStatus();
}

Value EvaluationContext::GetFunctionArgumentRef(std::string arg_name) {
  const auto it = udf_argument_references_.find(arg_name);
  if (it != udf_argument_references_.end()) {
    return it->second;
  }
  return Value();
}

absl::Status EvaluationContext::AddFunctionArgumentRef(std::string arg_name,
                                                       Value value) {
  ZETASQL_RET_CHECK(value.is_valid());
  ZETASQL_RET_CHECK(udf_argument_references_.emplace(arg_name, value).second)
      << "AddFunctionArgumentRef: Unable to insert key " << arg_name;
  return absl::OkStatus();
}

absl::Status EvaluationContext::VerifyNotAborted() const {
  ZETASQL_RETURN_IF_NOT_ENOUGH_STACK(
      "Out of stack space due to deeply nested evaluation");
  if (cancelled_) {
    return zetasql_base::CancelledErrorBuilder() << "The statement has been cancelled";
  }
  if (clock_->TimeNow() > statement_eval_deadline_) {
    return zetasql_base::ResourceExhaustedErrorBuilder()
           << "The statement has been aborted because the statement deadline ("
           << absl::FormatTime(statement_eval_deadline_, absl::UTCTimeZone())
           << ") was exceeded.";
  }
  return absl::OkStatus();
}

void EvaluationContext::InitializeDefaultTimeZone() {
  absl::TimeZone timezone;
  ABSL_CHECK(absl::LoadTimeZone("America/Los_Angeles", &timezone));
  default_timezone_ = timezone;
}

void EvaluationContext::InitializeCurrentTimestamp() {
  if (parent_context_ != nullptr) {
    current_timestamp_ = parent_context_->GetCurrentTimestamp();
    current_date_in_default_timezone_ =
        parent_context_->GetCurrentDateInDefaultTimezone();
    current_datetime_in_default_timezone_ =
        parent_context_->GetCurrentDatetimeInDefaultTimezone();
    current_time_in_default_timezone_ =
        parent_context_->GetCurrentTimeInDefaultTimezone();
    return;
  }
  current_timestamp_ = absl::ToUnixMicros(clock_->TimeNow());

  LazilyInitializeDefaultTimeZone();

  // Extracting the DATE from the current timestamp should never fail since
  // it will be in the supported range 0001-01-01 to 9999-12-31 (at least
  // before year 10000 - which is a bit after I retire).
  ZETASQL_CHECK_OK(functions::ExtractFromTimestamp(
      functions::DATE, current_timestamp_.value(), functions::kMicroseconds,
      default_timezone_.value(), &current_date_in_default_timezone_));
  // The checks for current datetime and current time should not fail in near
  // feature due to the same reason as above.
  ZETASQL_CHECK_OK(functions::ConvertTimestampToDatetime(
      functions::MakeTime(current_timestamp_.value(), functions::kMicroseconds),
      default_timezone_.value(), &current_datetime_in_default_timezone_));
  ZETASQL_CHECK_OK(functions::ConvertTimestampToTime(
      functions::MakeTime(current_timestamp_.value(), functions::kMicroseconds),
      default_timezone_.value(), &current_time_in_default_timezone_));
}

void EvaluationContext::SetNonDeterministicOutput() {
  deterministic_output_ = false;
  if (parent_context_ != nullptr) {
    parent_context_->SetNonDeterministicOutput();
  }
}

absl::TimeZone EvaluationContext::GetDefaultTimeZone() {
  if (parent_context_ != nullptr) {
    return parent_context_->GetDefaultTimeZone();
  }
  LazilyInitializeDefaultTimeZone();
  return default_timezone_.value();
}

// Indicate which errors should be converted to NULL in SAFE mode.
// For built-in functions, we expect to see only OUT_OF_RANGE.
// We try to handle others here in a reasonable way in case users are
// adding UDFs.
static bool IsSafeModeConvertibleError(const absl::Status& status) {
  switch (status.code()) {
    // These are probably not input-based semantic errors.
    case absl::StatusCode::kOk:
    case absl::StatusCode::kCancelled:
    case absl::StatusCode::kUnknown:
    case absl::StatusCode::kDeadlineExceeded:
    case absl::StatusCode::kPermissionDenied:
    case absl::StatusCode::kUnauthenticated:
    case absl::StatusCode::kResourceExhausted:
    case absl::StatusCode::kAborted:
    case absl::StatusCode::kUnimplemented:
    case absl::StatusCode::kInternal:
    case absl::StatusCode::kUnavailable:
    case absl::StatusCode::kDataLoss:
    case absl::StatusCode::kFailedPrecondition:
    default:
      return false;

    // These are probably errors caused by bad input values, and errors
    // should be replaced with NULL in SAFE mode.
    case absl::StatusCode::kInvalidArgument:
    case absl::StatusCode::kNotFound:
    case absl::StatusCode::kAlreadyExists:
    case absl::StatusCode::kOutOfRange:
      return true;
  }
}

bool ShouldSuppressError(const absl::Status& error,
                         ResolvedFunctionCallBase::ErrorMode error_mode) {
  ABSL_DCHECK(!error.ok());
  return error_mode == ResolvedFunctionCallBase::SAFE_ERROR_MODE &&
         IsSafeModeConvertibleError(error);
}

// Returns ResourceExhausted error when the statement should be aborted.
absl::Status PeriodicallyVerifyNotAborted(EvaluationContext* context,
                                          uint64_t num_steps) {
  if (num_steps %
          absl::GetFlag(FLAGS_zetasql_call_verify_not_aborted_rows_period) ==
      0) {
    return context->VerifyNotAborted();
  }
  return absl::OkStatus();
}

}  // namespace zetasql
