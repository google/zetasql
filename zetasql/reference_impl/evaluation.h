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

#ifndef ZETASQL_REFERENCE_IMPL_EVALUATION_H_
#define ZETASQL_REFERENCE_IMPL_EVALUATION_H_

#include <functional>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/civil_time.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include <cstdint>
#include "absl/container/flat_hash_map.h"
#include "absl/flags/declare.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/status.h"
#include "zetasql/base/clock.h"

// See description in the cc file.
ABSL_DECLARE_FLAG(int64_t, zetasql_call_verify_not_aborted_rows_period);

namespace zetasql {

// Returns OK if the first column is the primary key column for a table named
// 'table_name' represented by 'array'. Specifically, the first column must meet
// the following criteria:
//   1. SupportsGrouping() is true for the type of the column
//   2. Does not have NULL Values
//   3. Does not have duplicate Values
zetasql_base::Status ValidateFirstColumnPrimaryKey(
    const std::string& table_name, const Value& array,
    const LanguageOptions& language_options);

struct EvaluationOptions {
  // If true, operations will act as if the first column of a non-value table is
  // a primary key that may not be NULL. EvaluationContext::AddTable() will also
  // verify tables accordingly.
  bool emulate_primary_keys = false;

  // If true, the reference implementation will deterministically scramble the
  // output of relations whose order is not defined by ZetaSQL. This requires
  // some extra processing and so is only appropriate for tests. See
  // ReorderingTupleIterator in tuple.h for details.
  bool scramble_undefined_orderings = false;

  // If true, the reference implementation will always perform stable sorting
  // where sorting is required. This is useful for obtaining deterministic
  // results in the (text-based) reference implementation compliance tests,
  // especially when running with different implementations of the C++ standard
  // library.
  bool always_use_stable_sort = false;

  // If true, the reference implementation will store proto field values in
  // TupleSlots (to avoid extra deserialization).
  bool store_proto_field_value_maps = false;

  // If true, the reference implementation will use the TopNAccumulator instead
  // of LimitAccumulator(OrderByAccumulator) when possible in order to save
  // memory. Not safe to use in compliance or random query tests because it
  // always says its results are ordered according to ZetaSQL semantics.
  bool use_top_n_accumulator_when_possible = false;

  // Limit on the maximum number of in-memory bytes used by values. Exceeding
  // this limit results in an error. See the implementation of
  // Value::physical_byte_size for more details.
  int64_t max_value_byte_size = 1024 * 1024;

  // The limit on the maximum number of in-memory bytes that can be used for
  // storing accumulated Tuples (e.g., during an ORDER BY query). Exceeding this
  // limit results in an error.
  int64_t max_intermediate_byte_size = 128 * 1024 * 1024;
};

class ProtoFieldReader;

// Contains state about the evaluation in progress.
class EvaluationContext {
 public:
  // Objects can register CancelCallbacks with RegisterCancelCallback() to be
  // notified when CancelStatement() is called. A CancelCallback must not block
  // for a long time.
  using CancelCallback = std::function<zetasql_base::Status()>;

  explicit EvaluationContext(const EvaluationOptions& options);
  EvaluationContext(const EvaluationContext&) = delete;
  EvaluationContext& operator=(const EvaluationContext&) = delete;

  const EvaluationOptions& options() const { return options_; }

  MemoryAccountant* memory_accountant() { return &memory_accountant_; }

  // Returns the contents of table 'table_name' or Value::Invalid().
  Value GetTableAsArray(const std::string& table_name) {
    const auto it = tables_.find(table_name);
    if (it != tables_.end()) {
      return it->second;
    }
    return Value();
  }

  // Makes the given 'array' accessible under 'table_name'.
  ::zetasql_base::Status AddTableAsArray(const std::string& table_name,
                                 bool is_value_table, Value array,
                                 const LanguageOptions& language_options);

  // Indicates that the result of evaluation is non-deterministic.
  void SetNonDeterministicOutput() { deterministic_output_ = false; }

  bool IsDeterministicOutput() const { return deterministic_output_; }

  void SetLanguageOptions(const LanguageOptions& options) {
    language_options_ = options;
  }
  const LanguageOptions& GetLanguageOptions() const {
    return language_options_;
  }

  // Also clears the current timestamp (to force us to lazily regenerate the
  // current timestamp in the default timezone). This should not be called after
  // getting the current timestamp unless the user is using a frozen Clock.
  void SetDefaultTimeZone(absl::TimeZone timezone) {
    default_timezone_ = timezone;
    // Clear current_timestamp_ to force us to regenerate
    // *_in_default_timezone_.
    current_timestamp_.reset();
  }

  // If necessary, (lazily) initializes the default timezone. Lazy
  // initialization saves time for most evaluations, which don't require time
  // zone information.
  absl::TimeZone GetDefaultTimeZone() {
    LazilyInitializeDefaultTimeZone();
    return default_timezone_.value();
  }

  // Sets the clock to use when evaluating CURRENT_TIMESTAMP(),
  // CURRENT_DATE(), CURRENT_DATETIME(), etc functions.
  // Units are microseconds since the unix epoch UTC.
  // The ZetaSQL spec requires that all timestamps are within the range of
  // years [1, 9999].
  void SetClockAndClearCurrentTimestamp(zetasql_base::Clock* clock) {
    clock_ = clock;
    current_timestamp_.reset();
  }

  // If necessary, (lazily) initializes the default timezone and the current
  // timestamp. Lazy initialization saves time for most evaluations, which don't
  // require the current timestamp.
  int64_t GetCurrentTimestamp() {
    LazilyInitializeCurrentTimestamp();
    return current_timestamp_.value();
  }

  // If necessary, (lazily) initializes the default timezone and the current
  // timestamp. Lazy initialization saves time for most evaluations, which don't
  // require the current timestamp.
  int64_t GetCurrentDateInDefaultTimezone() {
    LazilyInitializeCurrentTimestamp();
    return current_date_in_default_timezone_;
  }

  // If necessary, (lazily) initializes the default timezone and the current
  // timestamp. Lazy initialization saves time for most evaluations, which don't
  // require the current timestamp.
  DatetimeValue GetCurrentDatetimeInDefaultTimezone() {
    LazilyInitializeCurrentTimestamp();
    return current_datetime_in_default_timezone_;
  }

  // If necessary, (lazily) initializes the default timezone and the current
  // timestamp. Lazy initialization saves time for most evaluations, which don't
  // require the current timestamp.
  TimeValue GetCurrentTimeInDefaultTimezone() {
    LazilyInitializeCurrentTimestamp();
    return current_time_in_default_timezone_;
  }

  // Sets the statement evaluation deadline of some time duration from now.  If
  // the statement is still being evaluated after that time it will be aborted
  // and an error will be returned.
  void SetStatementEvaluationDeadlineFromNow(absl::Duration time_limit) {
    statement_eval_deadline_ =
        ::zetasql_base::Clock::RealClock()->TimeNow() + time_limit;
  }

  // Sets the statement evaluation deadline.
  // If the statement is still being evaluated after that time it will be
  // aborted and an error will be returned.
  void SetStatementEvaluationDeadline(absl::Time statement_deadline) {
    statement_eval_deadline_ = statement_deadline;
  }

  absl::Time GetStatementEvaluationDeadline() const {
    return statement_eval_deadline_;
  }

  // Register a callback to be notified when CancelStatement() is called. As
  // with all non-const methods in this thread compatible class, using this
  // method in a multithreaded setting requires external synchronization of
  // this object. In practice, this is called by iterators that need to
  // propagate the cancellation request to user code.
  void RegisterCancelCallback(const CancelCallback& cb) {
    cancel_cbs_.push_back(cb);
  }

  // Cancels the current statement and invokes all of the cancellation
  // callbacks. Cancellation support is best-effort, in that iterators should be
  // periodicially polling the cancellation state (by calling VerifyNotAborted)
  // and cancelling if they discover the statement has been cancelled. The
  // callbacks are just a way of notifying user code that the statement has been
  // cancelled if we are stuck in a user's EvaluatorTableIterator.
  ::zetasql_base::Status CancelStatement() {
    cancelled_ = true;
    // Call all the callbacks, returning the first non-OK error code.
    zetasql_base::Status ret = zetasql_base::OkStatus();
    for (const CancelCallback& cb : cancel_cbs_) {
      zetasql_base::Status status = cb();
      if (ret.ok() && !status.ok()) {
        ret = status;
      }
    }
    return ret;
  }

  // Reset the deadline to infinity, uncancel the statement, and clear the
  // cancellation callbacks.
  void ClearDeadlineAndCancellationState() {
    SetStatementEvaluationDeadline(absl::InfiniteFuture());
    cancelled_ = false;
    cancel_cbs_.clear();
  }

  // Returns an error if the statement has been aborted. This function is
  // expensive (it gets the current time).
  ::zetasql_base::Status VerifyNotAborted() const;

  int num_proto_deserializations() const { return num_proto_deserializations_; }

  void set_num_proto_deserializations(int n) {
    num_proto_deserializations_ = n;
  }

  bool used_top_n_accumulator() const { return used_top_n_accumulator_; }

  void set_used_top_n_accumulator(bool value) {
    used_top_n_accumulator_ = value;
  }

  bool populate_last_get_field_value_call_read_fields_from_proto_map() const {
    return populate_last_get_field_value_call_read_fields_from_proto_map_;
  }

  void set_populate_last_get_field_value_call_read_fields_from_proto_map(
      bool value) {
    populate_last_get_field_value_call_read_fields_from_proto_map_ = value;
  }

  bool last_get_field_value_call_read_fields_from_proto(
      const ProtoFieldReader* reader) const {
    return zetasql_base::FindWithDefault(
        last_get_field_value_call_read_fields_from_proto_map_, reader, false);
  }

  void set_last_get_field_value_call_read_fields_from_proto(
      const ProtoFieldReader* reader, bool value) {
    // Only populate in unit tests for performance reasons.
    if (populate_last_get_field_value_call_read_fields_from_proto_map_) {
      last_get_field_value_call_read_fields_from_proto_map_[reader] = value;
    }
  }

 private:
  void LazilyInitializeDefaultTimeZone() {
    if (!default_timezone_.has_value()) {
      InitializeDefaultTimeZone();
    }
  }

  void InitializeDefaultTimeZone();

  void LazilyInitializeCurrentTimestamp() {
    if (!current_timestamp_.has_value()) {
      InitializeCurrentTimestamp();
    }
  }

  void InitializeCurrentTimestamp();

  const EvaluationOptions options_;
  MemoryAccountant memory_accountant_;
  // Tables added by AddTableAsArray().
  std::map<std::string, Value> tables_;
  // Indicates that the result of evaluation is non-deterministic.
  bool deterministic_output_;
  LanguageOptions language_options_;
  // Default is no deadline.
  absl::Time statement_eval_deadline_ = absl::InfiniteFuture();
  bool cancelled_ = false;
  std::vector<CancelCallback> cancel_cbs_;

  // Used to obtain the current timestamp.
  zetasql_base::Clock* clock_ = zetasql_base::Clock::RealClock();

  // Lazily initialized because not all queries need them, and initializing them
  // is expensive.
  absl::optional<absl::TimeZone> default_timezone_;
  absl::optional<int64_t> current_timestamp_;
  // Only valid if 'current_timestamp_' has a value.
  int32_t current_date_in_default_timezone_;
  DatetimeValue current_datetime_in_default_timezone_;
  TimeValue current_time_in_default_timezone_;

  // Records the number of times a proto was deserialized. Only for unit tests.
  int num_proto_deserializations_ = 0;

  // Whether to populate
  // 'last_get_field_value_call_read_fields_from_proto_map_'. For performance
  // reasons, this is only set to true in unit tests.
  bool populate_last_get_field_value_call_read_fields_from_proto_map_ = false;

  // Maps a ProtoFieldReader to whether the last call to its GetFieldValue()
  // method read fields from the proto. Only for unit tests.
  absl::flat_hash_map<const ProtoFieldReader*, bool>
      last_get_field_value_call_read_fields_from_proto_map_;

  // Records whether a TopNAccumulator was used. Only for unit tests.
  bool used_top_n_accumulator_ = false;
};

// Returns true if we should suppress 'error' (which must not be OK) in
// 'error_mode'.
bool ShouldSuppressError(const zetasql_base::Status& error,
                         ResolvedFunctionCallBase::ErrorMode error_mode);

}  // namespace zetasql

#endif  // ZETASQL_REFERENCE_IMPL_EVALUATION_H_
