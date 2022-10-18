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

#include "zetasql/compliance/sql_test_base.h"

#include <string.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <iosfwd>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/base/path.h"
#include "google/protobuf/text_format.h"
#include "zetasql/common/internal_value.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/compliance_label.pb.h"
#include "zetasql/compliance/compliance_label_extractor.h"
#include "zetasql/compliance/known_error.pb.h"
#include "zetasql/compliance/legal_runtime_errors.h"
#include "zetasql/compliance/sql_test_filebased_options.h"
#include "zetasql/compliance/test_driver.h"
#include "zetasql/compliance/test_util.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/functions/string.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_helpers.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/reference_driver.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/testing/type_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/btree_map.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/functional/bind_front.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "file_based_test_driver/file_based_test_driver.h"
#include "zetasql/base/file_util.h"  
#include "zetasql/base/map_util.h"
#include "farmhash.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

ABSL_FLAG(std::vector<std::string>, known_error_files, {},
          "Comma-separated list of known error filenames");
ABSL_FLAG(bool, report_all_errors, false,
          "(mostly) Ignore the known_error_files; run all statements except "
          "CRASHES_DO_NOT_RUN");

ABSL_FLAG(bool, ignore_wrong_results, false,
          "The test will accept any OK status regardless of the produced "
          "value. This is useful when testing compliance of a component but "
          "not a full evaluation engine. For instance, it can be used to write "
          "a test to determine which compliance queries successfully generate "
          "a logical query plan.");
ABSL_FLAG(bool, ignore_wrong_error_codes, false,
          "The test will accept any status regardless of the error code. This "
          "is useful for engines that cannot plumb through the original error "
          "during failure tests.");
ABSL_FLAG(bool, zetasql_compliance_accept_all_test_output, false,
          "Pretend no tests failed by ignoring their output.");
// Ideally we would rename this to --statement_name_pattern, but that might
// break some command line somewhere.
ABSL_FLAG(std::string, query_name_pattern, "",
          "Only test statements with names matching this pattern. This filter "
          "works only for statements in .test files. No effect if left empty.");

// These flag may be enabled to enforce stricter treatment of known_error files.
// It should not be used in presubmits for tests outside the zetasql
// directory, but it can be useful in team triaged continuous builds.
ABSL_FLAG(bool, zetasql_compliance_allow_removable_known_errors, true,
          "When true, labels in known_error file are allowed to apply to "
          "queries which pass.");
ABSL_FLAG(bool, zetasql_compliance_allow_upgradable_known_errors, true,
          "When true, known error listings that need to be upgraded are "
          "allowed in the known errors file.");
ABSL_FLAG(
    bool, zetasql_detect_falsly_required_features, false,
    "When true, the reference implementation test will fail test cases that "
    "declare a required feature but do not actually require that feature.  "
    "This flag has no effect when run with other engines.");
ABSL_FLAG(bool, zetasql_compliance_write_labels_to_file, false,
          "When true, cache compliance labels across different tests and write "
          "the labels to undeclared output in a textproto file format.");

namespace zetasql {

namespace {

// This RTTI helper class allows temporarily modifying language settings on the
// reference driver, and it will automatically restore original settings upon
// destruction.
class AutoLanguageOptions {
 public:
  explicit AutoLanguageOptions(ReferenceDriver* reference_driver)
      : reference_driver_(reference_driver) {
    original_options_ = reference_driver_->GetSupportedLanguageOptions();
  }
  ~AutoLanguageOptions() {
    reference_driver_->SetLanguageOptions(original_options_);
  }

 protected:
  ReferenceDriver* reference_driver_;
  LanguageOptions original_options_;
};

}  // anonymous namespace

// Constants for code-based statement names.
// A name must be RE2 safe. All unsafe characters will be replaced by '@'.
constexpr char kSafeChar = '@';
constexpr absl::string_view kSafeString(&kSafeChar, 1);

// For a long string, a signature is generated, which is the left 8
// characters of the string, followed by the fingerprint of the string,
// followed by the right 8 characters of the string.
constexpr int kLengthOfLeftSlice = 8;
constexpr int kLengthOfRightSlice = 8;

constexpr size_t kLogBufferSize =
    15000;

// Stats over all tests in a test case.
class Stats {
 public:
  Stats();
  Stats(const Stats&) = delete;
  Stats& operator=(const Stats&) = delete;

  static void RecordProperty(const std::string& key, const std::string& value) {
    SQLTestBase::RecordProperty(key, value);
  }
  static void RecordProperty(const std::string& key, int value) {
    SQLTestBase::RecordProperty(key, value);
  }

  // Record executed statements.
  void RecordExecutedStatement();

  // As above, but no-op if `check_only` is true.
  void RecordExecutedStatement(bool check_only) {
    if (!check_only) {
      return RecordExecutedStatement();
    }
  }

  // A to-be-removed-from-known-errors statement was in `was_mode`.
  void RecordToBeRemovedFromKnownErrorsStatement(const std::string& full_name,
                                                 const KnownErrorMode was_mode);

  // As above, but a no-op if `check_only` is true.
  void RecordToBeRemovedFromKnownErrorsStatement(const std::string& full_name,
                                                 const KnownErrorMode was_mode,
                                                 bool check_only) {
    if (!check_only) {
      return RecordToBeRemovedFromKnownErrorsStatement(full_name, was_mode);
    }
  }

  // A to-be-upgraded statement was in `was_mode`, and will
  // upgrade to `new_mode`.
  void RecordToBeUpgradedStatement(const std::string& full_name,
                                   const KnownErrorMode was_mode,
                                   const KnownErrorMode new_mode);

  // As above, but a no-op if `check_only` is true.
  void RecordToBeUpgradedStatement(const std::string& full_name,
                                   const KnownErrorMode was_mode,
                                   const KnownErrorMode new_mode,
                                   bool check_only) {
    if (!check_only) {
      return RecordToBeUpgradedStatement(full_name, was_mode, new_mode);
    }
  }

  // The failed statement will be put into to-be-added-to-known-errors list
  // with `new_mode`. The statement was in `was_mode`.
  void RecordFailedStatement(const std::string& msg,
                             const std::string& location,
                             const std::string& full_name,
                             const KnownErrorMode was_mode,
                             const KnownErrorMode new_mode);

  // As above, but a no-op if `check_only` is true.
  void RecordFailedStatement(const std::string& msg,
                             const std::string& location,
                             const std::string& full_name,
                             const KnownErrorMode was_mode,
                             const KnownErrorMode new_mode, bool check_only) {
    if (!check_only) {
      RecordFailedStatement(msg, location, full_name, was_mode, new_mode);
    }
  }

  // Composes a cancellation report, records it as a failure.
  void RecordCancelledStatement(const std::string& location,
                                const std::string& full_name,
                                const KnownErrorMode was_mode,
                                const KnownErrorMode new_mode,
                                const std::string& reason,
                                const std::string& detail);

  // Records a known error statement with its mode and the set of labels that
  // caused it to fail.
  void RecordKnownErrorStatement(const std::string& location,
                                 const KnownErrorMode mode,
                                 const absl::btree_set<std::string>& by_set);

  // Record the runtime duration of a executed statement.
  void RecordStatementExecutionTime(absl::Duration elapsed);

  // Record a failure that is not resolved by adding a known error entry.
  void RecordFailure(absl::string_view error_string) {
    failures_.emplace_back(error_string);
  }

  // Record compliance label info for one test query.
  // The actual error mode, i.e. test error mode (KnownErrorMode) is generated
  // by KnownErrorFilter::MatchAndExplain.
  // In both reference implementation test and engine test, compliance labels
  // will be extracted and written to undeclared output file.
  // This function can only be called after matchers are invoked by EXPECT_THAT.
  // It only takes effect when the global flag is on, in order to prevent
  // long-running RQG from consuming too much memory. See b/238890147.
  void RecordComplianceTestsLabelsProto(
      absl::string_view test_name, absl::string_view sql,
      KnownErrorMode actual_error_mode,
      const absl::btree_set<std::string>& label_set,
      absl::StatusCode expected_error_code,
      absl::StatusCode actual_error_code) {
    if (absl::GetFlag(FLAGS_zetasql_compliance_write_labels_to_file)) {
      ComplianceTestCaseLabels* test_case = labels_proto_.add_test_cases();
      test_case->set_test_name(std::string(test_name));
      test_case->set_test_query(std::string(sql));
      test_case->set_test_error_mode(actual_error_mode);
      for (const std::string& label : label_set) {
        test_case->add_compliance_labels(label);
      }
    }
  }

  void LogGoogletestProperties() const;

  // Print report to log file.
  void LogReport() const;

  // Write compliance labels proto to undeclared outputs file in textproto file
  // format.
  // This outputs all labels from all test cases stored in labels_proto_.
  // It only takes effect when the global flag is on, in order to prevent
  // long-running RQG from consuming too much memory. See b/238890147.
  void WriteComplianceLabels() {
  }

  // Used to tell RecordFailedCode/FileBasedStatement(...) whether we are
  // running a file-based test.
  void StartFileBasedStatements() { file_based_statements_ = true; }
  void EndFileBasedStatements() { file_based_statements_ = false; }
  // Used by ::testing::Matcher<const absl::StatusOr<Value>&> to call
  // the right RecordFailed*Statement().
  bool IsFileBasedStatement() { return file_based_statements_; }

 private:
  // Accepts an iterable (e.g., set or vector) of strings, and log it in
  // batches.
  template <class Iterable>
  void LogBatches(const Iterable& iterable, const std::string& title,
                  const std::string& delimiter) const;

  // Composes the following string.
  //   "label: <label>    # was: <was_mode> - new mode: <new_mode>"
  std::string LabelString(const std::string& label,
                          const KnownErrorMode was_mode,
                          const KnownErrorMode new_mode) const;

  int num_executed_ = 0;
  std::vector<std::string> failures_;

  std::set<std::string> to_be_added_to_known_errors_;
  std::set<std::string> to_be_removed_from_known_errors_;
  std::set<std::string> to_be_upgraded_;
  std::unordered_map<KnownErrorMode, std::set<std::string>>
      error_mode_to_new_failures_;
  std::unordered_map<KnownErrorMode, std::string>
      error_mode_to_example_message_;

  // Compliance labels represented by ComplianceTestsLabels Proto message.
  // This accumulates labels across different test cases and will be written to
  // file after all tests are processed.
  ComplianceTestsLabels labels_proto_;

  int num_known_errors_ = 0;

  bool file_based_statements_ = false;
};

Stats::Stats()
{}

void Stats::RecordExecutedStatement() { num_executed_++; }

std::string Stats::LabelString(const std::string& label,
                               const KnownErrorMode was_mode,
                               const KnownErrorMode new_mode) const {
  std::string label_string = absl::StrCat("  label: \"", label, "\"");
  if (new_mode) {
    absl::StrAppend(&label_string,
                    "    # mode: ", KnownErrorMode_Name(new_mode));
  }
  if (was_mode) {
    absl::StrAppend(&label_string, new_mode ? "," : "    #",
                    " was: ", KnownErrorMode_Name(was_mode));
  }
  return label_string;
}

void Stats::RecordToBeRemovedFromKnownErrorsStatement(
    const std::string& full_name, const KnownErrorMode was_mode) {
  to_be_removed_from_known_errors_.insert(
      LabelString(full_name, was_mode, KnownErrorMode::NONE));
}

void Stats::RecordToBeUpgradedStatement(const std::string& full_name,
                                        const KnownErrorMode was_mode,
                                        const KnownErrorMode new_mode) {
  to_be_upgraded_.insert(LabelString(full_name, was_mode, new_mode));
}

void Stats::RecordFailedStatement(const std::string& msg,
                                  const std::string& location,
                                  const std::string& full_name,
                                  const KnownErrorMode was_mode,
                                  const KnownErrorMode new_mode) {
  if (file_based_statements_) RecordProperty(location, "Failed");
  to_be_added_to_known_errors_.insert(
      LabelString(full_name, was_mode, new_mode));
  error_mode_to_new_failures_[new_mode].insert(full_name);
  error_mode_to_example_message_[new_mode] = msg;
  failures_.emplace_back(msg);
}

void Stats::RecordCancelledStatement(const std::string& location,
                                     const std::string& full_name,
                                     const KnownErrorMode was_mode,
                                     const KnownErrorMode new_mode,
                                     const std::string& reason,
                                     const std::string& detail) {
  ++num_executed_;
  std::string report =
      absl::StrCat("Location: ", location, "\n", "    Name: ", full_name, "\n",
                   "  Reason: ", reason, "\n", "  Detail: ", detail);
  RecordFailedStatement(report, location, full_name, was_mode, new_mode);
  RecordProperty(location, absl::StrCat("Cancelled: ", reason, ": ", detail));
}

void Stats::RecordKnownErrorStatement(
    const std::string& location, const KnownErrorMode mode,
    const absl::btree_set<std::string>& by_set) {
  num_known_errors_++;

  const std::string by =
      absl::StrCat("Mode: ", KnownErrorMode_Name(mode),
                   ", Due to: ", absl::StrJoin(by_set, ", "));

  RecordProperty(location, by);
}

void Stats::RecordStatementExecutionTime(absl::Duration elapsed) {
}

void Stats::LogGoogletestProperties() const {
  RecordProperty("Passed", num_executed_ - failures_.size());
  RecordProperty("Failed", failures_.size());
  RecordProperty("KnownErrors", num_known_errors_);
  RecordProperty(
      "Compliance",
      absl::StrCat((((num_executed_ - failures_.size()) * 1000 /
                     std::max(1, num_executed_ + num_known_errors_)) /
                    10.0),
                   "%"));
}

template <class Iterable>
void Stats::LogBatches(const Iterable& iterable, const std::string& title,
                       const std::string& delimiter) const {
  std::vector<std::string> batch;
  int batch_string_size = 0;
  int batch_count = 0;

  auto LogOneBatch = [&title, &delimiter, &batch_count, &batch,
                      &batch_string_size] {
    ++batch_count;
    // Always "====" to the beginning and "==== End " to the end so
    // extract_compliance_results.py can recognize it.
    ZETASQL_LOG(INFO) << "\n==== " << title << " #" << batch_count
              << "\n"
              // Leave enough space for "End".
              << absl::StrJoin(batch, delimiter).substr(0, kLogBufferSize - 160)
              << (batch.empty() ? std::string() : "\n") << "==== End " << title
              << " #" << batch_count;
    batch.clear();
    batch_string_size = 0;
  };

  for (const std::string& item : iterable) {
    if (batch_string_size >= kLogBufferSize / 2) {
      LogOneBatch();
    }
    batch.emplace_back(item);
    batch_string_size += item.size();
  }
  LogOneBatch();
}

void Stats::LogReport() const {
  std::vector<std::string> known_error_files =
      absl::GetFlag(FLAGS_known_error_files);
  const std::string compliance_report_title = "ZETASQL COMPLIANCE REPORT";
  // Always "====" to the beginning and "==== End " to the end so
  // extract_compliance_results.py can recognize it.
  ZETASQL_LOG(INFO) << "\n"
            << "==== " << compliance_report_title << "\n"
            << "[  PASSED  ] " << num_executed_ - failures_.size()
            << " statements.\n"
            << "[  FAILED  ] " << failures_.size() << " statements.\n"
            << "[KNOWN_ERRS] " << num_known_errors_ << " statements.\n"
            << "[COMPLIANCE] "
            << (((num_executed_ - failures_.size()) * 1000 /
                 std::max(1, num_executed_ + num_known_errors_)) /
                10.0)
            << "%.\n"
            << "==== End " << compliance_report_title;

  LogBatches(failures_, "Failures Summary", "\n==\n");
  LogBatches(to_be_added_to_known_errors_,
             "To Be Added To Known Errors Statements", "\n");
  LogBatches(to_be_removed_from_known_errors_,
             "To Be Removed From Known Errors Statements", "\n");
  LogBatches(to_be_upgraded_, "To Be Upgraded Statements", "\n");

  ZETASQL_LOG(INFO) << "\n==== RELATED KNOWN ERROR FILES ====\n"
            << absl::StrJoin(known_error_files, "\n")
            << "\n==== END RELATED KNOWN ERROR FILES ====\n";
  if (!error_mode_to_new_failures_.empty()) {
    std::vector<std::string> suggestions;
    for (const auto& [error_mode, cases] : error_mode_to_new_failures_) {
      KnownErrorFile enclosing_proto;
      KnownErrorEntry* labels_proto = enclosing_proto.add_known_errors();
      std::string example_msg =
          error_mode_to_example_message_.find(error_mode)->second;
      labels_proto->set_mode(error_mode);
      // Give an example reason, the last part of the message, which is often
      // more informative. This likely needs to be edited manually.
      labels_proto->set_reason("Example: " +
                               example_msg.substr(example_msg.size() > 100
                                                      ? example_msg.size() - 80
                                                      : 0));
      for (const auto& label : cases) {
        labels_proto->add_label(label);
      }
      google::protobuf::TextFormat::Printer printer;
      printer.SetPrintMessageFieldsInIndexOrder(true);  // So mode comes first

      printer.PrintToString(enclosing_proto, &suggestions.emplace_back());
    }
    LogBatches(suggestions, "Suggestions for adding known errors:", "\n");
  }
}

using ComplianceTestCaseResult = SQLTestBase::ComplianceTestCaseResult;

std::unique_ptr<Stats> SQLTestBase::stats_;
std::unique_ptr<MatcherCollection<absl::Status>>
    SQLTestBase::legal_runtime_errors_;

SQLTestBase::TestCaseInspectorFn* SQLTestBase::test_case_inspector_ = nullptr;

// gMock matcher that checkes if a statement result (StatusOr<Value>) has OK
// status.
MATCHER(ReturnsOk, "OK") { return arg.ok(); }

// gMock matcher that matches a statement result (StatusOr<Value>) with a
// string. Not supposed to be called directly. Use KnownErrorFilter.
MATCHER_P(ReturnsString, expected, expected) {
  return SQLTestBase::ToString(arg) == expected;
}

namespace {

std::string GetLocationString(const StatementResult& result) {
  return absl::StrCat(result.procedure_name,
                      result.procedure_name.empty() ? "" : "() ", "[",
                      result.line, ":", result.column, "]");
}

bool CompareStatementResult(const StatementResult& expected,
                            const StatementResult& actual,
                            FloatMargin float_margin, std::string* reason) {
  if (expected.procedure_name != actual.procedure_name ||
      expected.line != actual.line || expected.column != actual.column) {
    *reason = absl::StrCat("Location mismatch; expected ",
                           GetLocationString(expected), "; got ",
                           GetLocationString(actual));
    return false;
  }
  if (expected.result.ok()) {
    if (actual.result.ok()) {
      if (InternalValue::Equals(
              expected.result.value(), actual.result.value(),
              {.interval_compare_mode = IntervalCompareMode::kAllPartsEqual,
               .float_margin = float_margin,
               .reason = reason})) {
        return true;
      }
    } else {
      *reason = absl::StrCat("Expected statement to succeed, but it failed: ",
                             actual.result.status().ToString());
    }
  } else {
    // Ignore differences in error code or error message, since this is expected
    // across different engines. However, internal errors happen only in case of
    // a bug, so they will always fail.
    if (absl::IsInternal(actual.result.status())) {
      *reason = absl::StrCat("Expected statement to fail with ",
                             expected.result.status().ToString(),
                             ", but got an internal error instead: ",
                             actual.result.status().ToString());
    } else if (actual.result.status().ok()) {
      *reason =
          absl::StrCat("Expected statement to fail with ",
                       expected.result.status().ToString(),
                       ", but the statement instead succeeded ", "with result ",
                       SQLTestBase::ToString(actual.result));
    } else {
      return true;
    }
  }
  absl::StrAppend(reason, " at ", GetLocationString(expected));
  return false;
}
std::string StatementResultToString(const StatementResult& stmt_result) {
  absl::StatusOr<ComplianceTestCaseResult> status_or;
  if (stmt_result.result.ok()) {
    status_or = stmt_result.result.value();
  } else {
    status_or = stmt_result.result.status();
  }

  return absl::StrCat(GetLocationString(stmt_result), " ",
                      SQLTestBase::ToString(status_or));
}

bool CompareScriptResults(const ScriptResult& expected,
                          const ScriptResult& actual, FloatMargin float_margin,
                          std::string* reason) {
  reason->clear();
  // Compare common statement results
  for (int i = 0; i < std::min(expected.statement_results.size(),
                               actual.statement_results.size());
       ++i) {
    if (!CompareStatementResult(expected.statement_results.at(i),
                                actual.statement_results.at(i), float_margin,
                                reason)) {
      return false;
    }
  }

  // Check for missing/extra statement results.
  if (actual.statement_results.size() < expected.statement_results.size()) {
    for (size_t i = actual.statement_results.size();
         i < expected.statement_results.size(); ++i) {
      absl::StrAppend(reason, "Missing statement result ",
                      StatementResultToString(expected.statement_results.at(i)),
                      "\n");
    }
    return false;
  }
  if (actual.statement_results.size() > expected.statement_results.size()) {
    for (size_t i = expected.statement_results.size();
         i < actual.statement_results.size(); ++i) {
      absl::StrAppend(reason, "Extra statement result ",
                      StatementResultToString(actual.statement_results.at(i)));
    }
    return false;
  }
  return true;
}
}  // namespace

std::string ScriptResultToString(const ScriptResult& result) {
  return absl::StrCat(
      "ScriptResult\n",
      absl::StrJoin(result.statement_results, "\n",
                    [](std::string* out, const StatementResult& stmt_result) {
                      absl::StrAppend(out,
                                      StatementResultToString(stmt_result));
                    }));
}

// gMock matcher that matches a statement result (StatusOr<Value>) with a
// StatusOr<value>. Not supposed to be called directly. Use KnownErrorFilter.
MATCHER_P2(ReturnsStatusOrValue, expected, float_margin,
           SQLTestBase::ToString(expected)) {
  bool passed = true;
  std::string reason;
  if (expected.ok() != arg.ok()) {
    passed = false;
  } else if (expected.ok()) {
    if (std::holds_alternative<Value>(expected.value()) &&
        std::holds_alternative<Value>(arg.value())) {
      passed = InternalValue::Equals(
          std::get<Value>(expected.value()), std::get<Value>(arg.value()),
          {.interval_compare_mode = IntervalCompareMode::kAllPartsEqual,
           .float_margin = float_margin,
           .reason = &reason});
    } else if (std::holds_alternative<ScriptResult>(expected.value()) &&
               std::holds_alternative<ScriptResult>(arg.value())) {
      passed = CompareScriptResults(std::get<ScriptResult>(expected.value()),
                                    std::get<ScriptResult>(arg.value()),
                                    float_margin, &reason);
    }
  } else if (!absl::GetFlag(FLAGS_ignore_wrong_error_codes)) {
    // Any known error code would be OK.
    passed = SQLTestBase::legal_runtime_errors()->Matches(arg.status());
  }

  if (!passed) {
    absl::string_view trimmed_reason;
    absl::Status status;
    ZETASQL_CHECK(zetasql::functions::RightTrimBytes(reason, "\n", &trimmed_reason,
                                               &status));

    std::string error;
    if (expected.ok() && std::holds_alternative<Value>(expected.value()) &&
        zetasql::testing::HasFloatingPointNumber(
            std::get<Value>(expected.value()).type())) {
      error = absl::StrCat("\nFloat comparison: ", float_margin.DebugString());
    }
    *result_listener << absl::StrCat(
        error, trimmed_reason.empty() ? "" : "\n Details: ", trimmed_reason);
    return false;
  }
  return true;
}

namespace {

std::string ReplaceChar(absl::string_view str, absl::string_view unsafe_chars,
                        char safe_char) {
  std::string safe = std::string(str);
  for (char& ch : safe) {
    if (absl::StrContains(unsafe_chars, ch)) {
      ch = safe_char;
    }
  }

  return safe;
}

// Returns name of file-based tests.
// Replaces dots in file name with underscores, e.g. 'foo.test' => 'foo_test'.
// Motivation: when a label does not contain regex characters like '.',
// we bypass RE2 construction, see AddKnownErrorEntry, which is much faster.
// Note that label 'foo_test' is still matched by known error 'foo.test', so
// this replacement is safe with regards to legacy known error .
std::string FileBasedTestName(absl::string_view filename,
                              absl::string_view name) {
  std::string cleaned_file_name =
      absl::StrReplaceAll(zetasql_base::Basename(filename), {{".", "_"}});
  return absl::StrCat(cleaned_file_name, ":", name);
}

}  // namespace

// gMock matcher that uses another matcher to match a statement result
// (StatusOr<Value>). Bypasses a known error statement and returns true.
// In '--report_all_errors mode', matches all but CRASHES_DO_NOT_RUN statements.
// If the statement passed, records it in the to-be-removed-from-known-error
// list. Bypasses recording in check-only mode.
class KnownErrorFilter : public ::testing::MatcherInterface<
                             const absl::StatusOr<ComplianceTestCaseResult>&> {
 public:
  // 'expected_status' is the same status expected by 'matcher' or OK if
  // 'matcher' is expecting something other than an error status.
  KnownErrorFilter(
      SQLTestBase* sql_test, absl::Status expected_status,
      const ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>&
          matcher,
      bool check_only = false)
      : sql_test_(sql_test),
        matcher_(matcher),
        expected_status_(expected_status),
        check_only_(check_only) {}

  // Write out a CSV format line with detailed results of each test case.
  // These can be extracted into a file using extract_compliance_results.sh.
  void LogToCSV(bool passed, KnownErrorMode known_error_mode) const {
    // CSV columns:
    // TargetName, TestPrefix, TestName, Passed, KnownError, KnownErrorMode
    // NOTE: Column names are duplicated in extract_compliance_results.py.
    ZETASQL_LOG(INFO) << "CSV: \""
              << "\",\""
              << (sql_test_->stats_->IsFileBasedStatement()
                      ? sql_test_->full_name_
                      : sql_test_->GetNamePrefix())
              << "\",\"" << sql_test_->full_name_ << "\","
              << (passed ? "true" : "false") << ","
              << (known_error_mode != KnownErrorMode::NONE ? "true" : "false")
              << "," << KnownErrorMode_Name(known_error_mode) << std::endl;
  }

  bool MatchAndExplain(
      const absl::StatusOr<ComplianceTestCaseResult>& result,
      ::testing::MatchResultListener* listener) const override {
    const KnownErrorMode from_mode = sql_test_->known_error_mode();
    // CRASHES_DO_NOT_RUN is always skipped, return here.
    if (KnownErrorMode::CRASHES_DO_NOT_RUN == from_mode ||
        absl::GetFlag(FLAGS_zetasql_compliance_accept_all_test_output)) {
      LogToCSV(/*passed=*/false, from_mode);
      return true;
    }

    const bool passed = matcher_.MatchAndExplain(result, listener);
    LogToCSV(passed, from_mode);

    // Gets the right known_error mode for the statement.
    KnownErrorMode to_mode = from_mode;
    if (passed) {
      to_mode = KnownErrorMode::NONE;
    } else if (result.ok()) {
      if (absl::GetFlag(FLAGS_ignore_wrong_results)) {
        return true;  // With this flag, we consider this success.
      }
      to_mode = KnownErrorMode::ALLOW_ERROR_OR_WRONG_ANSWER;
    } else if (result.status().code() != absl::StatusCode::kUnimplemented) {
      to_mode = KnownErrorMode::ALLOW_ERROR;
    } else {
      to_mode = KnownErrorMode::ALLOW_UNIMPLEMENTED;
    }
    // Record actual error mode of current test case to global stats.
    sql_test_->stats_->RecordComplianceTestsLabelsProto(
        sql_test_->full_name_, sql_test_->sql_, to_mode,
        sql_test_->compliance_labels_, expected_status_.code(),
        result.status().code());
    if (to_mode > from_mode) {
      // 1. to_mode > 0 = from_mode: A failed non-known-error statement.
      // 2. to_mode > from_mode > 0: A known-error statement failed in a more
      //    severe mode.
      // In either case, log it as a failed statement.
      std::stringstream describe;
      matcher_.DescribeTo(&describe);
      std::stringstream extra;
      if (listener->stream() != nullptr) {
        // When a listener is not-interested, then its stream pointer may be
        // nullptr.
        // TODO: the correct way to guard against this is to add a
        // check for listener->IsInterested() that gates all generation of debug
        // output.
        extra << listener->stream()->rdbuf();
      }
      std::string report = sql_test_->GenerateFailureReport(
          describe.str(), SQLTestBase::ToString(result), extra.str());
      sql_test_->stats_->RecordFailedStatement(report, sql_test_->location_,
                                               sql_test_->full_name_, from_mode,
                                               to_mode, check_only_);
      // The framework didn't record execution of a known-error statement in
      // ExecuteTestCase(...). Do it now.
      if (from_mode) {
        sql_test_->stats_->RecordExecutedStatement(check_only_);
      }
      // Fail the test.
      return false;
    } else if (from_mode) {
      // A known error statement. Record it.
      sql_test_->RecordKnownErrorStatement(to_mode, check_only_);
      if (!to_mode) {
        // A to-be-removed-from-known_errors statement.
        sql_test_->stats_->RecordToBeRemovedFromKnownErrorsStatement(
            sql_test_->full_name_, from_mode, check_only_);
        if (!absl::GetFlag(
                FLAGS_zetasql_compliance_allow_removable_known_errors)) {
          sql_test_->stats_->RecordFailure(
              absl::StrCat("A known error rule applies to a passing test '",
                           sql_test_->full_name_,
                           "'. Remove or refine the known error entry."));
          return false;
        }
      } else if (to_mode < from_mode) {
        // A to-be-upgraded statement.
        sql_test_->stats_->RecordToBeUpgradedStatement(
            sql_test_->full_name_, from_mode, to_mode, check_only_);
        if (!absl::GetFlag(
                FLAGS_zetasql_compliance_allow_upgradable_known_errors)) {
          sql_test_->stats_->RecordFailure(
              absl::StrCat("A known error rule should be upgraded for '",
                           sql_test_->full_name_, "'."));
          return false;
        }
      }
      // Don't fail the test.
      return true;
    }
    // This implies to_mode <= from_mode && from_mode == 0. Thus to_mode == 0.
    ZETASQL_DCHECK_EQ(to_mode, 0);
    // Not a known error, and the test passed.
    return true;
  }

  void DescribeTo(::std::ostream* os) const override {
    matcher_.DescribeTo(os);
  }

  void DescribeNegationTo(::std::ostream* os) const override {
    matcher_.DescribeNegationTo(os);
  }

 private:
  SQLTestBase* sql_test_;
  ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&> matcher_;
  absl::Status expected_status_;  // used for logging compliance labels
  bool check_only_;
};

std::string SQLTestBase::GenerateFailureReport(const std::string& expected,
                                               const std::string& actual,
                                               const std::string& extra) const {
  return absl::StrCat(
      "   Statement: ", sql_, "\n", "  Params: ", ToString(parameters_), "\n",
      "Location: ", location_, "\n", "    Name: ", full_name_, "\n",
      "  Labels: ", absl::StrJoin(effective_labels_, ", "), "\n",
      "Expected: ", expected, "\n", "  Actual: ", actual, extra,
      extra.empty() ? "" : "\n");
}

// static
absl::Status SQLTestBase::ValidateFirstColumnPrimaryKey(
    const TestDatabase& test_db, const LanguageOptions& language_options) {
  for (const auto& [table_name, test_table] : test_db.tables) {
    // Do not check primary key column if it is a value table.
    if (test_table.options.is_value_table()) continue;

    ZETASQL_RETURN_IF_ERROR(zetasql::ValidateFirstColumnPrimaryKey(
        table_name, test_table.table_as_value, language_options));
  }
  return absl::OkStatus();
}

::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
SQLTestBase::ReturnsSuccess() {
  return ::testing::MakeMatcher(
      new KnownErrorFilter(this, absl::OkStatus(), ReturnsOk()));
}

::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
SQLTestBase::Returns(const ComplianceTestCaseResult& result,
                     const absl::Status& status, FloatMargin float_margin) {
  if (status.ok()) {
    return ::testing::MakeMatcher(new KnownErrorFilter(
        this, status,
        ReturnsStatusOrValue(absl::StatusOr<ComplianceTestCaseResult>(result),
                             float_margin)));
  } else {
    return ::testing::MakeMatcher(new KnownErrorFilter(
        this, status,
        ReturnsStatusOrValue(absl::StatusOr<ComplianceTestCaseResult>(status),
                             float_margin)));
  }
}

::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
SQLTestBase::Returns(const absl::StatusOr<ComplianceTestCaseResult>& result,
                     FloatMargin float_margin) {
  return ::testing::MakeMatcher(new KnownErrorFilter(
      this, result.status(), ReturnsStatusOrValue(result, float_margin)));
}

::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
SQLTestBase::ReturnsCheckOnly(
    const absl::StatusOr<ComplianceTestCaseResult>& result,
    FloatMargin float_margin) {
  return ::testing::MakeMatcher(new KnownErrorFilter(
      this, result.status(), ReturnsStatusOrValue(result, float_margin),
      /*check_only=*/true));
}

::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
SQLTestBase::Returns(const std::string& result) {
  return ::testing::MakeMatcher(
      new KnownErrorFilter(this, absl::OkStatus(), ReturnsString(result)));
}

::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
SQLTestBase::Returns(
    const ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
        matcher) {
  return ::testing::MakeMatcher(
      new KnownErrorFilter(this, absl::OkStatus(), matcher));
}

static bool IsOnResolverErrorFilebasedAllowList(absl::string_view full_name) {
  // These prefixes are for filebased compliance tests that have analyzer error
  // queries in them. Such tests should be analyzer tests. As compliance tests,
  // they don't test any useful engine code. They also cause some noise in
  // compliance scores. By matching prefixes, we aren't fully preventing new
  // such cases from being added to these existing files, but at least we are
  // preventing new files from including such tests.
  // TODO: Clean up the ~900 tests, burn down this list.
  for (absl::string_view prefix :
       {"aggregation_order_by_queries_test:",
        "aggregation_queries_test:aggregation_string_agg_error_",
        "analytic_cume_dist_test:cume_dist_",
        "analytic_dense_rank_test:dense_rank_",
        "analytic_hll_count_test:analytic_hll_count_merge_partial_over_unbound",
        "analytic_kll_quantiles_test:analytic_kll_quantiles_merge_point_",
        "analytic_lag_test:lag_window_frame",
        "analytic_lead_test:lead_window_frame",
        "analytic_ntile_test:ntile_",
        "analytic_partitionby_orderby_test:",
        "analytic_percent_rank_test:percent_rank_",
        "analytic_rank_test:rank_",
        "analytic_row_number_test:row_number_3",
        "analytic_sum_test:analytic_sum_range_orderby_bool_",
        "anonymization_test:",
        "arithmetic_functions_test:arithmetic_functions_3",
        "array_aggregation_test:array_concat_agg_array",
        "array_functions_test:",
        "array_joins_test:array_",
        "array_queries_test:select_array_equality_",
        "bytes_test:",
        "cast_function_test:",
        "civil_time_test:",
        "collation_test:",
        "concat_function_test:",
        "constant_queries_test:",
        "d3a_count_test:",
        "dml_delete_test:",
        "dml_insert_test:",
        "dml_nested_test:",
        "dml_returning_test:",
        "dml_update_proto_test:",
        "dml_update_struct_test:",
        "dml_update_test",
        "dml_value_table_test:",
        "except_intersect_queries_test:",
        "float_and_double_queries_test:",
        "geography_analytic_functions_test:st_clusterdbscan_",
        "geography_functions_test:st_geogfrom",
        "geography_queries_2_test:",
        "groupby_queries_test:",
        "groupby_queries_2_test:",
        "hll_count_test:",
        "in_queries_test:in_",
        "join_queries_test:join_9",
        "json_queries_test:json_",
        "keys_test:keys_2",
        "limit_queries_test:",
        "logical_functions_test:logical_not_",
        "nano_timestamp_test:timestamp_to_string_",
        "orderby_collate_queries_test:",
        "orderby_queries_test:order_by_array_",
        "proto2_unknown_enums_test:",
        "proto_fields_test:has_repeated_scalar_fields",
        "proto_constructor_test:",
        "proto3_fields_test:has_repeated_scalar_fields",
        "replace_fields_test:replace_fields_proto_named_extension",
        "select_distinct_test:",
        "safe_function_test:",
        "strings_test:",
        "timestamp_test:",
        "union_distinct_queries_test:",
        "unionall_queries_test:unionall_",
        "unnest_queries_test:",
        "with_queries_test:",
        "with_recursive_test:"}) {
    if (absl::StartsWith(full_name, prefix)) {
      return true;
    }
  }
  return false;
}

static bool IsOnResolverErrorCodebasedAllowList(absl::string_view full_name) {
  // These are codebased compliance tests that fail in the Resolver during
  // reference implementation tests. Tests that fail in the resolver should be
  // analyzer tests, not compliance tests. These stragglers are allow listed
  // because they were introduced before hygiene enforcement was added, and
  // require a more subtle fix.
  // Don't add to this list.
  // TODO: Fix these and enable enforcement of resolver success.
  return absl::StartsWith(full_name, "code:TablesampleRepeatable") ||
         absl::StartsWith(full_name, "code:Like_with_constant_pattern_");
}

static void ExtractComplianceLabelsFromResolvedAST(
    absl::string_view sql, const std::map<std::string, Value>& parameters,
    bool require_resolver_success, absl::string_view test_name,
    TypeFactory* type_factory, ReferenceDriver* reference_driver,
    const std::set<LanguageFeature>& required_features,
    const std::set<LanguageFeature>& forbidden_features,
    const absl::btree_set<std::string>& explicit_labels,
    absl::btree_set<std::string>& compliance_labels) {
  compliance_labels.clear();
  for (const LanguageFeature feature : required_features) {
    compliance_labels.insert(
        absl::StrCat("LanguageFeature:", LanguageFeature_Name(feature)));
  }
  for (const std::string& label : explicit_labels) {
    if (label != test_name) {
      compliance_labels.insert(label);
    }
  }
  // What are the right LanguageOptions to use for extracting compliance labels?
  // They aren't what the engine supports because we need to see the
  // ResolvedAST nodes that appear in queries that aren't supported by the
  // engine. They must include "required_features" and must not include
  // "forbidden_features". Otherwise, the maximum LanguageFeatures should be
  // set. What about ProductMode?  Actually, we would like to include a label
  // for only supported by PRODUCT_INTERNAL, only supported by PRODUCT_EXTERNAL,
  // or supported in all product modes.
  AutoLanguageOptions options_cleanup(reference_driver);
  LanguageOptions language_options;
  language_options.EnableMaximumLanguageFeaturesForDevelopment();
  for (LanguageFeature feature : required_features) {
    language_options.EnableLanguageFeature(feature);
  }
  for (LanguageFeature feature : forbidden_features) {
    language_options.DisableLanguageFeature(feature);
  }

  // Get a ResolvedAST for PRODUCT_INTERNAL.
  language_options.set_product_mode(PRODUCT_INTERNAL);
  reference_driver->SetLanguageOptions(language_options);
  bool uses_unsupported_type = false;
  // TODO: Refactor ReferenceDriver::GetAnalyzerOptions to take
  //     LanguageOptions as an argument so we don't have to set the state and
  //     then re-set it using AutoLanguageOptions
  absl::StatusOr<AnalyzerOptions> product_internal_analyzer_options_or_err =
      reference_driver->GetAnalyzerOptions(parameters, &uses_unsupported_type);
  EXPECT_FALSE(uses_unsupported_type) << test_name;
  absl::Status product_internal_analyze_status;
  std::unique_ptr<const AnalyzerOutput> product_internal_analyzer_out;
  if (product_internal_analyzer_options_or_err.ok()) {
    AnalyzerOptions analyzer_options =
        *product_internal_analyzer_options_or_err;
    analyzer_options.set_enabled_rewrites({});  // Disable the rewriter.
    product_internal_analyze_status =
        AnalyzeStatement(sql, analyzer_options, reference_driver->catalog(),
                         type_factory, &product_internal_analyzer_out);
  }

  // Repeat with mode PRODUCT_EXTERNAL
  language_options.set_product_mode(PRODUCT_EXTERNAL);
  reference_driver->SetLanguageOptions(language_options);
  absl::StatusOr<AnalyzerOptions> product_external_analyzer_options_or_err =
      reference_driver->GetAnalyzerOptions(parameters, &uses_unsupported_type);
  absl::Status product_external_analyze_status;
  std::unique_ptr<const AnalyzerOutput> product_external_analyzer_out;
  if (product_external_analyzer_options_or_err.ok()) {
    AnalyzerOptions analyzer_options =
        *product_external_analyzer_options_or_err;
    analyzer_options.set_enabled_rewrites({});  // Disable the rewriter.
    product_external_analyze_status =
        AnalyzeStatement(sql, analyzer_options, reference_driver->catalog(),
                         type_factory, &product_external_analyzer_out);
  }

  bool internal_compiles = product_internal_analyzer_options_or_err.ok() &&
                           product_internal_analyze_status.ok();
  bool external_compiles = product_external_analyzer_options_or_err.ok() &&
                           product_external_analyze_status.ok();
  const ResolvedStatement* statement = nullptr;
  if (!internal_compiles && !external_compiles) {
    compliance_labels.insert("Special:NoCompileTest");
    if (require_resolver_success) {
      ADD_FAILURE()
          << "Test '" << test_name << "' does not successfully compile for "
          << "with maximum features plus required less forbidden. This test is "
          << "unhealthy. Probably it represents and analyzer failure test. "
          << "Analyzer failures should be tested in the analyzer tests, not "
          << "compliance tests, please move the test there. Another less "
          << "common possibility, is that the test case should have forbidden "
          << "features that aren't currently explicit. In that case, "
          << "annotate the test with the apporpriate forbidden feature.";
    }
  } else if (internal_compiles && external_compiles) {
    compliance_labels.insert("ProductMode:InternalAndExternal");
    // This is an arbitrary choice.
    statement = product_internal_analyzer_out->resolved_statement();
  } else if (internal_compiles && !external_compiles) {
    compliance_labels.insert("ProductMode:InternalOnly");
    statement = product_internal_analyzer_out->resolved_statement();
  } else if (!internal_compiles && external_compiles) {
    compliance_labels.insert("ProductMode:ExternalOnly");
    statement = product_external_analyzer_out->resolved_statement();
  } else {
    ZETASQL_LOG(FATAL) << "Unreachable";
  }
  if (statement != nullptr) {
    ZETASQL_EXPECT_OK(ExtractComplianceLabels(statement, compliance_labels));
  }
}

absl::StatusOr<ComplianceTestCaseResult> SQLTestBase::RunSQL(
    absl::string_view sql, const SQLTestCase::ParamsMap& params,
    bool permit_compile_failure) {
  ZETASQL_RET_CHECK(!IsFileBasedStatement()) << "This function is for codebased tests.";
  sql_ = sql;
  parameters_ = params;
  full_name_ = GenerateCodeBasedStatementName(sql_, parameters_);

  absl::btree_set<std::string> labels = GetCodeBasedLabels();
  labels.insert(full_name_);
  effective_labels_ = labels;

  ZETASQL_LOG(INFO) << "Starting code-based test: " << full_name_;
  LogStrings(labels, "Effective labels: ");

  ZETASQL_RETURN_IF_ERROR(InspectTestCase());

  known_error_mode_ = IsKnownError(labels, &by_set_);
  if (KnownErrorMode::CRASHES_DO_NOT_RUN == known_error_mode_) {
    // Do not run CRASHES_DO_NOT_RUN.
    RecordKnownErrorStatement();
    return absl::CancelledError("Known Error");
  }
  return ExecuteTestCase();
}

void SQLTestBase::RunSQLOnFeaturesAndValidateResult(
    absl::string_view sql, const std::map<std::string, Value>& params,
    const std::set<LanguageFeature>& required_features,
    const std::set<LanguageFeature>& forbidden_features,
    const Value& expected_value, const absl::Status& expected_status,
    const FloatMargin& float_margin) {
  full_name_ = GenerateCodeBasedStatementName(sql, params);

  ZETASQL_CHECK(!script_mode_) << "Codebased tests don't run in script mode.";
  // TODO: Refactor so that extract labels can be in known_errors.
  bool require_resolver_success =
      !IsOnResolverErrorCodebasedAllowList(full_name_) &&
      IsTestingReferenceImpl();
  ExtractComplianceLabelsFromResolvedAST(
      sql, params, require_resolver_success, full_name_,
      execute_statement_type_factory(), GetReferenceDriver(), required_features,
      forbidden_features, GetCodeBasedLabels(), compliance_labels_);

  if (IsTestingReferenceImpl()) {
    auto* reference_driver = GetReferenceDriver();
    absl::Cleanup reset_language_options =
        [original = driver()->GetSupportedLanguageOptions(),
         reference_driver]() {
          reference_driver->SetLanguageOptions(original);
        };

    LanguageOptions language_options;
    language_options.SetEnabledLanguageFeatures(required_features);
    reference_driver->SetLanguageOptions(language_options);

    auto run_result = RunSQL(sql, params, /*permit_compile_failure=*/false);
    EXPECT_THAT(run_result,
                Returns(expected_value, expected_status, float_margin))
        << "FullName: " << full_name() << "; "
        << "Labels: " << absl::StrJoin(GetCodeBasedLabels(), ", ");

    if (!absl::GetFlag(FLAGS_zetasql_detect_falsly_required_features)) {
      return;
    }

    // For each feature, assert that removing it makes the test fail. This
    // is a "falsely required" feature.
    absl::StatusOr<ComplianceTestCaseResult> result_to_check =
        expected_status.ok()
            ? absl::StatusOr<ComplianceTestCaseResult>(
                  ComplianceTestCaseResult(expected_value))
            : absl::StatusOr<ComplianceTestCaseResult>(expected_status);
    for (const auto& feature : required_features) {
      EXPECT_FALSE(IsFeatureFalselyRequired(
          feature, /*require_inclusive=*/true, sql, params, required_features,
          run_result.status(), result_to_check, float_margin))
          << "Feature is falsely required: " << full_name() << ": " << sql
          << ": " << LanguageFeature_Name(feature);
    }
    for (const auto& feature : forbidden_features) {
      EXPECT_FALSE(IsFeatureFalselyRequired(
          feature, /*require_inclusive=*/false, sql, params, required_features,
          run_result.status(), result_to_check, float_margin))
          << "Feature is falsely prohibited: " << full_name() << ": " << sql
          << ": " << LanguageFeature_Name(feature);
    }
  } else {
    bool driver_enables_right_features = true;
    for (LanguageFeature feature : required_features) {
      driver_enables_right_features &= DriverSupportsFeature(feature);
    }
    for (LanguageFeature feature : forbidden_features) {
      driver_enables_right_features &= !DriverSupportsFeature(feature);
    }

    if (!driver_enables_right_features) {
      stats_->RecordComplianceTestsLabelsProto(
          full_name_, sql_, KnownErrorMode::ALLOW_UNIMPLEMENTED,
          compliance_labels_, expected_status.code(),
          absl::StatusCode::kUnimplemented);
      return;  // Skip this test.
    }
    // Only run once. Use reference engine to check result. We don't compare
    // the reference engine output against QueryParamsWithResult::results()
    // because it isn't clear what feature set to use.
    TypeFactory type_factory;
    bool is_deterministic_output;
    bool uses_unsupported_type = false;
    sql_ = sql;  // To supply a const std::string&
    absl::StatusOr<Value> reference_result =
        reference_driver()->ExecuteStatementForReferenceDriver(
            sql_, params, GetExecuteStatementOptions(), &type_factory,
            &is_deterministic_output, &uses_unsupported_type);
    if (uses_unsupported_type) {
      stats_->RecordComplianceTestsLabelsProto(
          full_name_, sql_, KnownErrorMode::ALLOW_UNIMPLEMENTED,
          compliance_labels_, expected_status.code(),
          absl::StatusCode::kUnimplemented);
      return;  // Skip this test. It uses types not supported by the driver.
    }
    SCOPED_TRACE(sql);
    // TODO: Should we do something with 'is_deterministic_output'?
    EXPECT_THAT(RunSQL(sql, params), Returns(reference_result, float_margin))
        << "FullName: " << full_name() << "; "
        << "Labels: " << absl::StrJoin(GetCodeBasedLabels(), ", ");
  }
}

SimpleCatalog* SQLTestBase::catalog() const {
  return GetReferenceDriver()->catalog();
}

static std::unique_ptr<ReferenceDriver> CreateTestSetupDriver() {
  LanguageOptions options;
  // For test setup (e.g., PrepareTable) always use PRODUCT_INTERNAL mode, since
  // a lot of test tables use internal types, and failure is non recoverable.
  options.set_product_mode(zetasql::ProductMode::PRODUCT_INTERNAL);
  // Enable all possible language features.
  options.EnableMaximumLanguageFeaturesForDevelopment();
  // Allow CREATE TABLE AS SELECT in [prepare_database] statements.
  options.AddSupportedStatementKind(RESOLVED_CREATE_TABLE_AS_SELECT_STMT);
  options.AddSupportedStatementKind(RESOLVED_CREATE_FUNCTION_STMT);

  auto driver = std::make_unique<ReferenceDriver>(options);
  // Create an empty database so that we can later load protos and enums.
  ZETASQL_CHECK_OK(driver->CreateDatabase(TestDatabase{}));

  return driver;
}

SQLTestBase::SQLTestBase()
    : test_setup_driver_(CreateTestSetupDriver()),
      test_driver_owner_(GetComplianceTestDriver()),
      test_driver_(test_driver_owner_.get()),
      reference_driver_owner_(
          test_driver_->IsReferenceImplementation()
              ? nullptr
              : new ReferenceDriver(
                    test_driver_->GetSupportedLanguageOptions())),
      reference_driver_(reference_driver_owner_.get()),
      execute_statement_type_factory_(std::make_unique<TypeFactory>()) {
  std::vector<std::string> known_error_files =
      absl::GetFlag(FLAGS_known_error_files);
  if (!known_error_files.empty()) {
    LoadKnownErrorFiles(known_error_files);
  }
}

SQLTestBase::SQLTestBase(TestDriver* test_driver,
                         ReferenceDriver* reference_driver)
    : test_setup_driver_(CreateTestSetupDriver()),
      test_driver_(test_driver),
      reference_driver_(reference_driver),
      execute_statement_type_factory_(std::make_unique<TypeFactory>()) {
  // Sanity check that the contract is respected.
  ZETASQL_CHECK_EQ(
      reference_driver_ == nullptr,
      test_driver_ == nullptr || test_driver_->IsReferenceImplementation());
  std::vector<std::string> known_error_files =
      absl::GetFlag(FLAGS_known_error_files);
  if (!known_error_files.empty()) {
    LoadKnownErrorFiles(known_error_files);
  }
}

SQLTestBase::~SQLTestBase() {}

const std::unique_ptr<file_based_test_driver::TestCaseOptions>&
SQLTestBase::options() {
  return test_file_options_->options_;
}

absl::Status SQLTestBase::CreateDatabase(const TestDatabase& test_db) {
  ZETASQL_RETURN_IF_ERROR(ValidateFirstColumnPrimaryKey(
      test_db, driver()->GetSupportedLanguageOptions()));
  ZETASQL_RETURN_IF_ERROR(driver()->CreateDatabase(test_db));
  if (!IsTestingReferenceImpl()) {
    ZETASQL_RETURN_IF_ERROR(reference_driver()->CreateDatabase(test_db));
  }
  return absl::OkStatus();
}

absl::StatusOr<ComplianceTestCaseResult> SQLTestBase::ExecuteStatement(
    const std::string& sql, const std::map<std::string, Value>& parameters) {
  return ExecuteTestCase();
}

absl::StatusOr<ComplianceTestCaseResult> SQLTestBase::ExecuteTestCase() {
  sql_ = absl::StripAsciiWhitespace(sql_);

  // Splits this long log statement into smaller pieces that are less than
  // kLogBufferSize and ideally, split on newlines.
  std::string sql_log_string = absl::StrCat(
      "Running statement:\n  ", sql_,
      (parameters_.empty()
           ? "\nNo Parameters\n"
           : absl::StrCat("\nParameters:\n", ToString(parameters_))),
      "\n  Location: ", location_, "\n      Name: ", full_name_);
  if (!driver_language_options().GetEnabledLanguageFeatures().empty()) {
    absl::StrAppend(
        &sql_log_string, "\n  Features: ",
        driver_language_options().GetEnabledLanguageFeaturesAsString());
  }
  for (::absl::string_view chunk :
       ::absl::StrSplit(sql_log_string, ::zetasql::LogChunkDelimiter())) {
    ZETASQL_LOG(INFO) << chunk;
  }

  // A known error can still fail the test if it fails in a more
  // severe mode. Since the framework doesn't know the outcome yet, it defers
  // the recording till a later time, when
  // KnownErrorFilter::MatchAndExplain(...) is executed.
  if (!known_error_mode_) stats_->RecordExecutedStatement();

  // Time the statement execution time to gather some simple performance
  // metrics.
  absl::Time start_time = absl::Now();
  absl::StatusOr<ComplianceTestCaseResult> result;
  if (IsTestingReferenceImpl()) {
    bool is_deterministic_output = true;
    bool uses_unsupported_type = false;  // unused
    if (script_mode_) {
      // Don't support plumbing deterministic output in scripting
      is_deterministic_output = false;
      result = GetReferenceDriver()->ExecuteScriptForReferenceDriver(
          sql_, parameters_, GetExecuteStatementOptions(),
          execute_statement_type_factory(), &uses_unsupported_type);
    } else {
      result = GetReferenceDriver()->ExecuteStatementForReferenceDriver(
          sql_, parameters_, GetExecuteStatementOptions(),
          execute_statement_type_factory(), &is_deterministic_output,
          &uses_unsupported_type);
    }
    if (!is_deterministic_output) {
      // This will log specifically during the reference implementation's
      // compliance test. Its useful in combination with lumbermill to
      // see what compliance test queries the reference implementation considers
      // non-deterministic. There are multiple forms of non-determinism that
      // the reference implementation can detect, but it does not descriminate
      // between them. Some forms are more harmful than others in the context
      // of compliance testing. For instance, a little bit of floating point
      // precision loss is expected and tolarated by result validation. On the
      // other hand, taking the 0th element from an unordered array is probably
      // not something that should happen during compliance tests.
      // TODO: Granularize kinds of non-determinism in the
      //     reference implementations EvaluationContext.
      // TODO: Generate compliance test labels for each kind of
      //     okay non-determinism.
      ZETASQL_LOG(INFO) << "Reference reports non-deterministic: " << full_name_;
    }
  } else {
    if (script_mode_) {
      result = driver()->ExecuteScript(sql_, parameters_,
                                       execute_statement_type_factory());
    } else {
      result = driver()->ExecuteStatement(sql_, parameters_,
                                          execute_statement_type_factory());
    }
  }
  stats_->RecordStatementExecutionTime(absl::Now() - start_time);
  return result;
}

void SQLTestBase::RunSQLTests(absl::string_view filename) {
  // The current test is a file-based test.
  stats_->StartFileBasedStatements();

  InitFileState();

  if (!file_based_test_driver::RunTestCasesFromFiles(
          filename, absl::bind_front(&SQLTestBase::RunTestFromFile, this))) {
    ZETASQL_LOG(ERROR) << "Encountered failures when testing file: " << filename;
  }

  // End of the file-based test.
  stats_->EndFileBasedStatements();
}

// This function is used as a callback from
// file_based_test_driver::RunTestCasesFromFiles
void SQLTestBase::RunTestFromFile(
    absl::string_view sql,
    file_based_test_driver::RunTestCaseResult* test_result) {
  InitStatementState(sql, test_result);
  StepSkipUnsupportedTest();
  StepPrepareTimeZoneProtosEnums();
  StepPrepareDatabase();
  StepCheckKnownErrors();
  StepCreateDatabase();
  SkipEmptyTest();
  StepExecuteStatementCheckResult();
}

absl::btree_set<std::string> SQLTestBase::GetCodeBasedLabels() {
  absl::btree_set<std::string> label_set;
  for (const std::string& label : code_based_labels_) {
    label_set.insert(label);
  }
  return label_set;
}

void SQLTestBase::InitFileState() {
  file_workflow_ = CREATE_DATABASE;
  test_db_.clear();
  test_file_options_ =
      std::make_unique<FilebasedSQLTestFileOptions>(test_setup_driver_.get());
}

void SQLTestBase::InitStatementState(
    absl::string_view sql,
    file_based_test_driver::RunTestCaseResult* test_result) {
  statement_workflow_ = NORMAL;
  sql_ = sql;
  parameters_.clear();
  full_name_ = "";
  known_error_mode_ = KnownErrorMode::NONE;
  by_set_.clear();
  test_result_ = test_result;
  effective_labels_.clear();
  compliance_labels_.clear();

  // A creating-table section does not have a name. Here we create a location
  // string without the name. Later on we will update the location string
  // when name is available.
  location_ = Location(test_result->filename(), test_result->line());

  std::string reason = "";
  auto status_or = test_file_options_->ProcessTestCase(sql, &reason);
  if (status_or.ok()) {
    test_case_options_ = std::move(*status_or);
    sql_ = test_case_options_->sql();
    parameters_ = test_case_options_->params();
  } else {
    ZETASQL_CHECK(!reason.empty()) << status_or.status();
    CheckCancellation(status_or.status(), reason);
  }
}

void SQLTestBase::StepSkipUnsupportedTest() {
  if (statement_workflow_ != NORMAL) return;

  // If we are testing the reference implementation, all tests should be
  // supported.
  if (IsTestingReferenceImpl()) return;

  bool skip_test = false;
  for (LanguageFeature required_feature :
       test_case_options_->required_features()) {
    if (!driver_language_options().LanguageFeatureEnabled(required_feature)) {
      skip_test = true;
      break;
    }
  }

  for (LanguageFeature forbidden_feature :
       test_case_options_->forbidden_features()) {
    if (driver_language_options().LanguageFeatureEnabled(forbidden_feature)) {
      skip_test = true;
      break;
    }
  }

  absl::StatusOr<bool> status_or_skip_test_for_primary_key_mode =
      driver()->SkipTestsWithPrimaryKeyMode(
          test_case_options_->primary_key_mode());
  CheckCancellation(status_or_skip_test_for_primary_key_mode.status(),
                    "Failed to interpret primary key mode");
  if (statement_workflow_ == CANCELLED) return;

  skip_test = skip_test || status_or_skip_test_for_primary_key_mode.value() ||
              absl::GetFlag(FLAGS_zetasql_compliance_accept_all_test_output);

  if (skip_test) {
    test_result_->set_ignore_test_output(true);
    statement_workflow_ = FEATURE_MISMATCH;
  }
}

void SQLTestBase::StepPrepareTimeZoneProtosEnums() {
  if (statement_workflow_ != NORMAL) return;
  // Handles default time zone first, because it may affect results of
  // statements that are used to generate parameters or table contents.
  if (!test_file_options_->default_timezone().empty()) {
    CheckCancellation(
        SetDefaultTimeZone(std::string(test_file_options_->default_timezone())),
        "Failed to set default time zone");
  }
  if (statement_workflow_ == CANCELLED) return;

  // Handles proto and enum loading second, because the table being created
  // may use these types.
  ZETASQL_CHECK(test_case_options_ != nullptr);
  if (!test_case_options_->proto_file_names().empty() ||
      !test_case_options_->proto_message_names().empty() ||
      !test_case_options_->proto_enum_names().empty()) {
    if (CREATE_DATABASE == file_workflow_) {
      CheckCancellation(LoadProtosAndEnums(), "Failed to load protos or enums");
    } else {
      absl::Status status(absl::StatusCode::kInvalidArgument,
                          "A group of [load_proto_files], [load_proto_names], "
                          "and [load_enum_names] must collocate with either a "
                          "[prepare_database] or the first statement.");
      CheckCancellation(status, "Wrong placement of load protos or enums");
    }
  }
}

void SQLTestBase::StepPrepareDatabase() {
  if (statement_workflow_ != NORMAL) return;

  if (test_case_options_->prepare_database()) {
    std::string table_name;
    if (CREATE_DATABASE != file_workflow_) {
      absl::Status status(absl::StatusCode::kInvalidArgument,
                          "All [prepare_database] must be placed at the top "
                          "of a *.test file");
      CheckCancellation(status, "Wrong placement of prepare_database");
      return;
    }
    if (GetStatementKind(sql_) == RESOLVED_CREATE_FUNCTION_STMT) {
      if (!IsTestingReferenceImpl() &&
          test_case_options_->name() != "skip_failed_reference_setup") {
        ZETASQL_EXPECT_OK(reference_driver()->AddSqlUdfs({sql_}));
      }
      absl::Status driver_status = driver()->AddSqlUdfs({sql_});
      if (!driver_status.ok()) {
        // We don't want to fail the test because of a database setup failure.
        // Any test statements that depend on this schema object should cause
        // the test to fail in a more useful way.
        ZETASQL_LOG(ERROR) << "Prepare database failed with error: " << driver_status;
      }
      // The prepare database section is not a test. No need to proceed further.
      statement_workflow_ = NOT_A_TEST;
      return;
    }
    if (GetStatementKind(sql_) != RESOLVED_CREATE_TABLE_AS_SELECT_STMT) {
      absl::Status status(
          absl::StatusCode::kInvalidArgument,
          "Only CREATE TABLE AS (SELECT...) statements and CREATE TEMP FUNCTION"
          " statements are supported for [prepare_database]");
      CheckCancellation(status, "Invalid CREATE TABLE statement");
      if (statement_workflow_ == CANCELLED) return;
    }

    // Run everything when testing the reference implementation (even tests
    // for in-development features), but for a real engine, skip unsupported
    // tables.
    if (!IsTestingReferenceImpl()) {
      for (LanguageFeature required_feature :
           test_case_options_->required_features()) {
        if (!driver_language_options().LanguageFeatureEnabled(
                required_feature)) {
          statement_workflow_ = NOT_A_TEST;
          return;
        }
      }
    }

    bool is_deterministic_output;
    bool uses_unsupported_type = false;  // unused
    CheckCancellation(
        test_setup_driver_
            ->ExecuteStatementForReferenceDriver(
                sql_, parameters_, ReferenceDriver::ExecuteStatementOptions(),
                table_type_factory(), &is_deterministic_output,
                &uses_unsupported_type, &test_db_, &table_name)
            .status(),
        "Failed to create table");
    if (statement_workflow_ == CANCELLED) return;
    ZETASQL_CHECK(zetasql_base::ContainsKey(test_db_.tables, table_name));
    *test_db_.tables[table_name].options.mutable_required_features() =
        test_case_options_->required_features();

    // Output to golden files for validation purpose.
    test_result_->AddTestOutput(
        test_db_.tables[table_name].table_as_value.Format());

    // The create table section is not a test. No need to proceed further.
    statement_workflow_ = NOT_A_TEST;
  } else if (CREATE_DATABASE == file_workflow_) {
    // This is the first statement.
    file_workflow_ = FIRST_STATEMENT;
  }
}

void SQLTestBase::StepCheckKnownErrors() {
  if (statement_workflow_ != NORMAL) return;

  absl::string_view name = test_case_options_->name();

  if (statement_workflow_ == CANCELLED) return;

  if (!absl::GetFlag(FLAGS_query_name_pattern).empty()) {
    const RE2 regex(absl::GetFlag(FLAGS_query_name_pattern));
    if (!RE2::FullMatch(name, regex)) {
      test_result_->set_ignore_test_output(true);
      statement_workflow_ = SKIPPED;
      return;
    }
  }

  const std::string filename = test_result_->filename();

  // Name is available now. Update the location and full name string.
  location_ = Location(filename, test_result_->line(), name);
  full_name_ = FileBasedTestName(filename, name);

  effective_labels_ =
      EffectiveLabels(full_name_, test_case_options_->local_labels(),
                      test_file_options_->global_labels());
  known_error_mode_ = IsKnownError(effective_labels_, &by_set_);
  if (KnownErrorMode::CRASHES_DO_NOT_RUN == known_error_mode_) {
    RecordKnownErrorStatement();
    statement_workflow_ = KNOWN_CRASH;
  }
}

void SQLTestBase::StepCreateDatabase() {
  is_catalog_initialized_ = true;
  if (statement_workflow_ != NORMAL) return;

  if (!test_db_.empty()) {
    CheckCancellation(CreateDatabase(), "Failed to create database");
  }
}

void SQLTestBase::SkipEmptyTest() {
  // If "sql_" is empty, skip it. This allows sections containing only test
  // case options and comments.
  if (sql_.empty()) {
    test_result_->set_ignore_test_output(true);
    statement_workflow_ = NOT_A_TEST;
  }
}

void SQLTestBase::StepExecuteStatementCheckResult() {
  if (statement_workflow_ == NOT_A_TEST || statement_workflow_ == CANCELLED ||
      statement_workflow_ == SKIPPED) {
    return;
  }

  ReferenceDriver* ref_driver = GetReferenceDriver();
  bool should_extract_labels =
      // We don't include scripts in engine compliance report yet.
      !script_mode_
      // This case excludes some tests that use SQLTestBase but aren't
      // compliance tests. Such tests haven't initialized the requisite
      // data-structures.
      && is_catalog_initialized_;
  if (should_extract_labels) {
    bool require_resolver_success =
        !IsOnResolverErrorFilebasedAllowList(full_name_) &&
        // Only require success for reference right now. Failures in engine
        // test drivers (e.g. setting up the catalog) also cause these failures.
        IsTestingReferenceImpl();
    ExtractComplianceLabelsFromResolvedAST(
        sql_, parameters_, require_resolver_success, full_name_,
        execute_statement_type_factory(), ref_driver,
        test_case_options_->required_features(),
        test_case_options_->forbidden_features(), effective_labels_,
        compliance_labels_);
  } else {
    ZETASQL_LOG(INFO) << "Skip extracting compliance labels " << full_name_;
  }

  if (statement_workflow_ == FEATURE_MISMATCH) {
    stats_->RecordComplianceTestsLabelsProto(
        full_name_, sql_, KnownErrorMode::ALLOW_UNIMPLEMENTED,
        compliance_labels_, absl::StatusCode::kOk,
        absl::StatusCode::kUnimplemented);
  } else if (statement_workflow_ == KNOWN_CRASH) {
    stats_->RecordComplianceTestsLabelsProto(
        full_name_, sql_, KnownErrorMode::CRASHES_DO_NOT_RUN,
        compliance_labels_, absl::StatusCode::kOk,
        absl::StatusCode::kUnavailable);
  }

  if (statement_workflow_ != NORMAL) {
    return;
  }

  if (IsTestingReferenceImpl()) {
    // TODO: Push this down to ExecuteTestCase once different
    //     test_featuresN= groups have different names.
    if (!InspectTestCase().ok()) {
      return;
    }

    // Check results against golden files.
    // All features in [required_features] will be turned on.
    // If the test has [test_features1] or [test_features2], the test will run
    // multiple times with each of those features sets all enabled or disabled,
    // and will generate a test output for each, prefixed with
    // "WITH FEATURES: ..." to show which features were set to get that output.

    absl::btree_set<std::set<LanguageFeature>> features_sets =
        ExtractFeatureSets(test_case_options_->test_features1(),
                           test_case_options_->test_features2(),
                           test_case_options_->required_features());

    absl::btree_map<std::string, TestResults> test_results =
        RunTestAndCollectResults(features_sets);

    if (absl::GetFlag(FLAGS_zetasql_detect_falsly_required_features)) {
      RunAndCompareTestWithoutEachRequiredFeatures(
          test_case_options_->required_features(), features_sets, test_results);
    }

    ParseAndCompareExpectedResults(features_sets, test_results);
  } else {
    // Check results against the reference implementation.
    test_result_->set_ignore_test_output(true);

    // This runs just once, with the LanguageOptions specified by the engine's
    // TestDriver, and compares to the reference impl results for those same
    // LanguageOptions.
    bool is_deterministic_output;
    bool uses_unsupported_type = false;
    absl::StatusOr<ComplianceTestCaseResult> ref_result;
    if (script_mode_) {
      // Don't support plumbing deterministic output in scripting
      is_deterministic_output = false;
      ref_result = reference_driver()->ExecuteScriptForReferenceDriver(
          sql_, parameters_, GetExecuteStatementOptions(),
          execute_statement_type_factory(), &uses_unsupported_type);
    } else {
      ref_result = reference_driver()->ExecuteStatementForReferenceDriver(
          sql_, parameters_, GetExecuteStatementOptions(),
          execute_statement_type_factory(), &is_deterministic_output,
          &uses_unsupported_type);
    }
    if (uses_unsupported_type) {
      stats_->RecordComplianceTestsLabelsProto(
          full_name_, sql_, KnownErrorMode::ALLOW_UNIMPLEMENTED,
          compliance_labels_, absl::StatusCode::kOk,
          absl::StatusCode::kUnimplemented);
      return;  // Skip this test. It uses types not supported by the driver.
    }
    if (absl::IsUnimplemented(ref_result.status())) {
      // This test is not implemented by the reference implementation. Skip
      // checking the results because we have no results to check against.
      stats_->RecordComplianceTestsLabelsProto(
          full_name_, sql_, KnownErrorMode::ALLOW_UNIMPLEMENTED,
          compliance_labels_, absl::StatusCode::kUnimplemented,
          absl::StatusCode::kUnimplemented);
      return;
    }
    absl::StatusOr<ComplianceTestCaseResult> actual_result = ExecuteTestCase();
    SCOPED_TRACE(absl::StrCat("Testcase: ", full_name_, "\nSQL:\n", sql_));
    EXPECT_THAT(actual_result, Returns(ref_result, kDefaultFloatMargin));
  }
}

absl::btree_set<std::set<LanguageFeature>> SQLTestBase::ExtractFeatureSets(
    const std::set<LanguageFeature>& test_features1,
    const std::set<LanguageFeature>& test_features2,
    const std::set<LanguageFeature>& required_features) {
  absl::btree_set<std::set<LanguageFeature>> features_sets;
  for (int include1 = 0; include1 <= 1; ++include1) {
    for (int include2 = 0; include2 <= 1; ++include2) {
      std::set<LanguageFeature> features_set = required_features;
      if (include1 == 1) {
        features_set.insert(test_features1.begin(), test_features1.end());
      }
      if (include2 == 1) {
        features_set.insert(test_features2.begin(), test_features2.end());
      }
      features_sets.insert(std::move(features_set));
    }
  }
  return features_sets;
}

static std::string EnabledFeaturesAsNormalizedString(
    const std::set<LanguageFeature>& features_set) {
  if (features_set.empty()) {
    return "<none>";
  }

  return absl::StrJoin(
      features_set, ",", [](std::string* set_out, LanguageFeature feature) {
        absl::StrAppend(
            set_out,
            absl::StripPrefix(LanguageFeature_Name(feature), "FEATURE_"));
      });
}

absl::btree_map<std::string, SQLTestBase::TestResults>
SQLTestBase::RunTestAndCollectResults(
    const absl::btree_set<std::set<LanguageFeature>>& features_sets) {
  ZETASQL_CHECK(!features_sets.empty());
  absl::btree_map<std::string, SQLTestBase::TestResults> test_results;
  for (const std::set<LanguageFeature>& features_set : features_sets) {
    absl::StatusOr<ComplianceTestCaseResult> driver_result =
        RunTestWithFeaturesEnabled(features_set);

    TestResults& found = test_results[ToString(driver_result)];

    if (found.enabled_features.empty()) {
      // Since the same result_string always maps to the same driver_result, we
      // only need to set it the first time we see this result_string.
      found.driver_output = driver_result;
    }
    found.enabled_features.push_back(
        EnabledFeaturesAsNormalizedString(features_set));
  }
  return test_results;
}

absl::StatusOr<ComplianceTestCaseResult>
SQLTestBase::RunTestWithFeaturesEnabled(
    const std::set<LanguageFeature>& features_set) {
  ReferenceDriver* reference_driver = GetReferenceDriver();
  LanguageOptions language_options =
      reference_driver->GetSupportedLanguageOptions();
  language_options.SetEnabledLanguageFeatures(features_set);
  AutoLanguageOptions auto_options(reference_driver);
  reference_driver->SetLanguageOptions(language_options);
  return ExecuteTestCase();
}

void SQLTestBase::RunAndCompareTestWithoutEachRequiredFeatures(
    const std::set<LanguageFeature>& required_features,
    const absl::btree_set<std::set<LanguageFeature>>& features_sets,
    const absl::btree_map<std::string, TestResults>& test_results) {
  for (const auto feature_to_check : required_features) {
    EXPECT_TRUE(
        IsFeatureRequired(feature_to_check, features_sets, test_results))
        << LanguageFeature_Name(feature_to_check)
        << " was not actually required for " << full_name_ << "!";
  }
}

bool SQLTestBase::IsFeatureRequired(
    LanguageFeature feature_to_check,
    const absl::btree_set<std::set<LanguageFeature>>& features_sets,
    const absl::btree_map<std::string, TestResults>& test_results) {
  return absl::c_any_of(features_sets, [&](const auto& enabled_features) {
    return RemovingFeatureChangesResult(feature_to_check, enabled_features,
                                        test_results);
  });
}

bool SQLTestBase::RemovingFeatureChangesResult(
    LanguageFeature feature_to_check,
    std::set<LanguageFeature> enabled_features,
    const absl::btree_map<std::string, TestResults>& original_test_results) {
  std::string original_enabled_features =
      EnabledFeaturesAsNormalizedString(enabled_features);
  enabled_features.erase(feature_to_check);
  std::string new_result =
      ToString(RunTestWithFeaturesEnabled(enabled_features));

  for (const auto& [original_result, test_result] : original_test_results) {
    for (const auto& result_enabled_features : test_result.enabled_features) {
      if (result_enabled_features == original_enabled_features) {
        return !(new_result == original_result);
      }
    }
  }
  // This should never actually happen but if the enabled_features aren't Found
  // at all, we might as well call the result changed.
  ADD_FAILURE() << "This should never happen.  If you see this there is a bug.";
  return true;
}

void SQLTestBase::ParseAndCompareExpectedResults(
    const absl::btree_set<std::set<LanguageFeature>>& features_sets,
    const absl::btree_map<std::string, TestResults>& test_results) {
  ZETASQL_CHECK(!features_sets.empty());
  ZETASQL_CHECK(!test_results.empty());
  int result_part_number = 0;
  for (const auto& [driver_result, test_result] : test_results) {
    ++result_part_number;
    std::string result_prefix;
    // Expected results use WITH FEATURES exactly when there's more than one
    // enabled feature.  In these cases we append them to the actual result.
    if (features_sets.size() > 1) {
      for (absl::string_view feature_set_name : test_result.enabled_features) {
        absl::StrAppend(&result_prefix, "WITH FEATURES: ", feature_set_name,
                        "\n");
      }
    }
    const std::string actual_result =
        absl::StrCat(result_prefix, driver_result);
    test_result_->AddTestOutput(actual_result);

    ZETASQL_DCHECK_EQ(statement_workflow_, NORMAL);
    if (test_case_options_->extract_labels()) {
      test_result_->AddTestOutput(absl::StrJoin(compliance_labels_, "\n"));
    }
    absl::string_view expected_string = "";
    if (test_result_->parts().size() > result_part_number) {
      expected_string = test_result_->parts()[result_part_number];
    }
    expected_string = absl::StripSuffix(expected_string, "\n");
    while (absl::StartsWith(expected_string, "WITH FEATURES:")) {
      // Remove the prefix_string line for comparison.
      expected_string = expected_string.substr(expected_string.find('\n') + 1);
    }

    EXPECT_THAT(test_result.driver_output,
                Returns(std::string(expected_string)));
  }
}

void SQLTestBase::CheckCancellation(const absl::Status& status,
                                    const std::string& reason) {
  if (!status.ok()) {
    if (nullptr != test_result_) {
      // Code-based tests and random statements do not have test_result_
      // defined, in which case, no need to add test output.
      test_result_->AddTestOutput(absl::StrCat(
          reason, ": ", internal::StatusToString(status), " at ", location_));
    }

    statement_workflow_ = CANCELLED;
    if (known_error_mode_ < KnownErrorMode::ALLOW_ERROR) {
      // 1. known_error_mode_ = 0 < ALLOW_ERROR: A normal statement.
      // 2. 0 < known_error_mode_ < ALLOW_ERROR: A known error statement with
      // mode
      //    less severe than ALLOW_ERROR.
      // In either case, the statement fails the test.
      if (fail_unittest_on_cancelled_statement_) {
        ZETASQL_EXPECT_OK(status) << "  Reason: " << reason << " at " << location_;
      }
      stats_->RecordCancelledStatement(location_, full_name_, known_error_mode_,
                                       KnownErrorMode::ALLOW_ERROR, reason,
                                       internal::StatusToString(status));
    } else {
      RecordKnownErrorStatement();
      if (known_error_mode_ > KnownErrorMode::ALLOW_ERROR) {
        // A nontrivial known_error_mode_ implies the statement has a
        // full_name_. This statement can be upgraded.
        stats_->RecordToBeUpgradedStatement(full_name_, known_error_mode_,
                                            KnownErrorMode::ALLOW_ERROR);
      }
    }
  }
}

bool SQLTestBase::IsFeatureFalselyRequired(
    LanguageFeature feature, bool require_inclusive, absl::string_view sql,
    const std::map<std::string, Value>& param_map,
    const std::set<LanguageFeature>& required_features,
    const absl::Status& initial_run_status,
    const absl::StatusOr<ComplianceTestCaseResult>& expected_result,
    const FloatMargin& expected_float_margin) {
  LanguageOptions::LanguageFeatureSet features_minus_one(
      required_features.begin(), required_features.end());
  if (require_inclusive) {
    // this is the "required feature" case"
    ZETASQL_DCHECK(features_minus_one.contains(feature));
    features_minus_one.erase(feature);
  } else {
    // this is the "prohibited feature" case.
    ZETASQL_DCHECK(!features_minus_one.contains(feature));
    features_minus_one.insert(feature);
  }
  LanguageOptions language_options;
  language_options.SetEnabledLanguageFeatures(features_minus_one);
  GetReferenceDriver()->SetLanguageOptions(language_options);
  auto modified_run_result = RunSQL(sql, param_map);
  if (!modified_run_result.ok() &&
      modified_run_result.status() == initial_run_status) {
    // The test case is expecting an error with error message. We see the same
    // expected error and message with and without 'feature', we can conclude
    // that 'feature' is not actually required.
    return true;
  }

  if (absl::IsOutOfRange(initial_run_status) &&
      absl::IsInvalidArgument(modified_run_result.status())) {
    // This is an expected case where we are testing an evaluation time error in
    // the feature protected path, but without the flag we get a compiletime
    // error. We handle this as a special case because the 'ReturnsCheckOnly'
    // matcher is loose with these error codes. Since we are using the
    // complement of the matcher's result, its looseness becomes over-tightness
    // causing false postives.
    return false;
  }

  // The test case is expecting a result. We see the same result with
  // and without 'feature'. We can conclude that 'feature' is not
  // actually required.
  return ::testing::Value(
      modified_run_result,
      ReturnsCheckOnly(expected_result, expected_float_margin));
}

absl::Status SQLTestBase::AddKnownErrorEntry(
    const KnownErrorEntry& known_error_entry) {
  for (int j = 0; j < known_error_entry.label_size(); j++) {
    const std::string& label = known_error_entry.label(j);
    // Modes are ordered in severity. If a label presents in multiple
    // known error entries, the label's mode is the highest one.
    label_info_map_[label].mode =
        std::max(known_error_entry.mode(), label_info_map_[label].mode);
    // If the label is not a regex, store it directly.
    // Otherwise, store the compiled regex.
    if (SafeString(label) == label) {
      known_error_labels_.insert(label);
    } else if (zetasql_base::InsertIfNotPresent(&known_error_regex_strings_, label)) {
      std::unique_ptr<RE2> regex(new RE2(label));
      if (regex->ok()) {
        known_error_regexes_.push_back(std::move(regex));
      } else {
        return ::zetasql_base::UnknownErrorBuilder()
               << "Invalid known error regex " << label;
      }
    }

    if (!known_error_entry.reason().empty()) {
      label_info_map_[label].reason.insert(known_error_entry.reason());
    }
  }
  return absl::OkStatus();
}

void SQLTestBase::LoadKnownErrorFiles(const std::vector<std::string>& files) {
  for (const std::string& file : files) {
    ZETASQL_CHECK_OK(LoadKnownErrorFile(file));
  }

  if (ZETASQL_VLOG_IS_ON(3)) {
    LogStrings(known_error_labels_, "Known Error labels: ");
    LogStrings(known_error_regex_strings_, "Known Error regexes: ");
    LogReason();
    LogMode();
  }
}

absl::btree_set<std::string> SQLTestBase::EffectiveLabels(
    absl::string_view full_name, const std::vector<std::string>& labels,
    const std::vector<std::string>& global_labels) const {
  absl::btree_set<std::string> effective_labels;
  effective_labels.emplace(full_name);
  effective_labels.insert(labels.begin(), labels.end());
  effective_labels.insert(global_labels.begin(), global_labels.end());

  LogStrings(effective_labels, "Effective labels: ");

  return effective_labels;
}

template <typename ContainerType>
void SQLTestBase::LogStrings(const ContainerType& strings,
                             const std::string& prefix) const {
  const int flush_threshold = static_cast<int>(0.95 * kLogBufferSize);
  std::string buf;
  for (const auto& s : strings) {
    const int logged_size = s.size() + 2;
    const bool want_flush = buf.size() + logged_size > flush_threshold;
    if (!buf.empty() && want_flush) {
      ZETASQL_LOG(INFO) << prefix << buf;
      buf.clear();
    }
    absl::StrAppend(&buf, buf.empty() ? "" : ", ", s);
  }
  if (!buf.empty()) {
    ZETASQL_LOG(INFO) << prefix << buf;
  }
}

void SQLTestBase::AddCodeBasedLabels(std::vector<std::string> labels) {
  for (const std::string& label : labels) {
    code_based_labels_.emplace_back(label);
  }
}

void SQLTestBase::RemoveCodeBasedLabels(std::vector<std::string> labels) {
  for (std::vector<std::string>::const_reverse_iterator iter = labels.rbegin();
       iter != labels.rend(); iter++) {
    ZETASQL_CHECK_EQ(*iter, *(code_based_labels_.rbegin()))
        << "Found corrupted code-based labels. Always use "
           "auto label = MakeScopedLabel(...){...} to avoid this.";
    code_based_labels_.pop_back();
  }
}

std::string SQLTestBase::SafeString(absl::string_view str) const {
  return ReplaceChar(str, "[](){}.*+^$\\?|\"", kSafeChar);
}

std::string SQLTestBase::SignatureOfString(absl::string_view str) const {
  std::string escape = absl::CHexEscape(str);

  if (escape.size() <= kLengthOfLeftSlice + kLengthOfRightSlice) {
    std::string safe = SafeString(escape);
    return absl::StrCat("<", safe, ">");
  }
  uint64_t hash = farmhash::Fingerprint64(escape);
  uint64_t* hash_ptr = &hash;
  std::string hash_raw_str(reinterpret_cast<const char*>(hash_ptr),
                           sizeof(hash));
  std::string mid_raw = absl::WebSafeBase64Escape(hash_raw_str);
  ZETASQL_CHECK_EQ(mid_raw.size(), 11) << mid_raw;
  std::string mid = mid_raw.substr(0, 11);
  std::string left = SafeString(escape.substr(0, kLengthOfLeftSlice));
  std::string right = SafeString(
      escape.substr(escape.size() - kLengthOfRightSlice, kLengthOfRightSlice));
  return absl::StrCat("<", left, "_", mid, "_", right, ">");
}

std::string SQLTestBase::SignatureOfCompositeValue(const Value& value) const {
  std::string str = value.DebugString();
  absl::string_view trimmed_view;
  absl::Status status;
  ZETASQL_CHECK(zetasql::functions::TrimBytes(str, "[]{}", &trimmed_view, &status));
  return SignatureOfString(trimmed_view);
}

std::string SQLTestBase::ValueToSafeString(const Value& value) const {
  if (value.is_null()) return "_NULL";
  if (!value.is_valid()) return "_INVALID";
  std::string str;
  switch (value.type_kind()) {
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
      str = absl::StrReplaceAll(value.DebugString(),
                                {{".", kSafeString}, {"+", ""}});
      return absl::StrCat("_", str);
    case TYPE_STRING:
      return absl::StrCat("_", SignatureOfString(value.string_value()));
    case TYPE_BYTES:
      return absl::StrCat("_", SignatureOfString(value.bytes_value()));
    case TYPE_TIMESTAMP: {
      // We use unix micros since the epoch when the result is an
      // even number of micros, otherwise we use the full string
      // value (i.e. 2006-01-02 03:04:05.123456789+00).
      absl::Time base_time = value.ToTime();
      int64_t nanos =
          absl::ToInt64Nanoseconds(absl::UTCTimeZone().At(base_time).subsecond);
      if (nanos % 1000 == 0) {
        return absl::StrCat("_", value.ToUnixMicros());
      }
      return absl::StrCat("_", SignatureOfCompositeValue(value));
    }
    case TYPE_GEOGRAPHY:
      return absl::StrCat("_", SignatureOfString(value.DebugString()));
    case TYPE_JSON:
      return absl::StrCat("_", SignatureOfString(value.DebugString()));
    case TYPE_ENUM:
      return absl::StrCat("_", value.DebugString());
    case TYPE_DATE:
    case TYPE_ARRAY:
    case TYPE_STRUCT:
    case TYPE_PROTO:
      return absl::StrCat("_", SignatureOfCompositeValue(value));
    default:
      // TODO: This debugstring needs to be escaped for regex unsafe
      //     characters. And probably spaces too?
      return absl::StrCat("_", value.DebugString());
  }
}

void SQLTestBase::SetNamePrefix(absl::string_view name_prefix,
                                bool need_result_type_name,
                                zetasql_base::SourceLocation loc) {
  location_ = Location(loc.file_name(), loc.line());
  name_prefix_ = name_prefix;
  name_prefix_need_result_type_name_ = need_result_type_name;
  result_type_name_.clear();
}

void SQLTestBase::SetResultTypeName(const std::string& result_type_name) {
  result_type_name_ = result_type_name;
}

std::string SQLTestBase::GetNamePrefix() const {
  std::string name_prefix = name_prefix_;
  if (name_prefix.empty()) {
    // Uses test name as the prefix if not set.
    const ::testing::TestInfo* test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    if (test_info != nullptr) {
      name_prefix = test_info->name();
    }
  }
  ZETASQL_CHECK(!name_prefix.empty())
      << "Found an empty name prefix. Always use SetNamePrefix(...) or "
      << "TEST_F(...) to avoid this.";
  if (name_prefix_need_result_type_name_) {
    ZETASQL_CHECK(!result_type_name_.empty())
        << "Name prefix " << name_prefix_
        << " needs a result type, but none was specified";
    absl::StrAppend(&name_prefix, "_", result_type_name_);
  }
  return absl::StrCat("code:", name_prefix);
}

std::string SQLTestBase::GenerateCodeBasedStatementName(
    absl::string_view sql,
    const std::map<std::string, Value>& parameters) const {
  std::vector<std::string> param_strs;
  std::string value;
  param_strs.reserve(parameters.size());
  for (const std::pair<const std::string, Value>& entry : parameters) {
    param_strs.emplace_back(absl::StrCat(
        absl::StrReplaceAll(entry.second.type()->TypeName(product_mode()),
                            {{".", "_"}}),
        ValueToSafeString(entry.second)));
  }

  std::string name = absl::StrReplaceAll(GetNamePrefix(), {{".", "_"}});
  if (!param_strs.empty()) {
    absl::StrAppend(&name, "_", absl::StrJoin(param_strs, "_"));
  }
  // If the name is not safe then we cannot create known error entries.
  ZETASQL_CHECK(RE2::FullMatch(name, name)) << "Name is not RE2 safe " << name;
  return name;
}

std::string SQLTestBase::ToString(
    const absl::StatusOr<ComplianceTestCaseResult>& status) {
  std::string result_string;
  if (!status.ok()) {
    result_string =
        absl::StrCat("ERROR: ", internal::StatusToString(status.status()));
  } else if (std::holds_alternative<Value>(status.value())) {
    const Value& value = std::get<Value>(status.value());
    ZETASQL_CHECK(!value.is_null());
    ZETASQL_CHECK(value.is_valid());
    result_string = value.Format();
  } else {
    result_string =
        ScriptResultToString(std::get<ScriptResult>(status.value()));
  }
  absl::string_view trimmed_result;
  absl::Status ignored_status;
  ZETASQL_CHECK(zetasql::functions::RightTrimBytes(result_string, "\n",
                                             &trimmed_result, &ignored_status));
  return std::string(trimmed_result);
}

std::string SQLTestBase::ToString(
    const std::map<std::string, Value>& parameters) {
  std::vector<std::string> param_strs;
  param_strs.reserve(parameters.size());
  for (const std::pair<const std::string, Value>& entry : parameters) {
    param_strs.emplace_back(absl::StrCat("  @", entry.first, " = ",
                                         entry.second.FullDebugString()));
  }
  std::sort(param_strs.begin(), param_strs.end());
  return absl::StrJoin(param_strs, "\n");
}

void SQLTestBase::LogReason() const {
  std::vector<std::string> ref;
  ref.reserve(label_info_map_.size());
  for (const std::pair<const std::string, LabelInfo>& entry : label_info_map_) {
    ref.emplace_back(absl::StrCat(
        entry.first, ": {", absl::StrJoin(entry.second.reason, ", "), "}"));
  }
  ZETASQL_VLOG(3) << "Reason: " << absl::StrJoin(ref, ", ");
}

void SQLTestBase::LogMode() const {
  std::vector<std::string> ref;
  ref.reserve(label_info_map_.size());
  for (const std::pair<const std::string, LabelInfo>& entry : label_info_map_) {
    ref.emplace_back(absl::StrCat(entry.first, ": ",
                                  KnownErrorMode_Name(entry.second.mode)));
  }
  ZETASQL_VLOG(3) << "Mode: " << absl::StrJoin(ref, ", ");
}

absl::Status SQLTestBase::LoadKnownErrorFile(absl::string_view filename) {
  std::string text;
  absl::Status status = internal::GetContents(filename, &text);
  if (!status.ok()) {
    if (status.code() == absl::StatusCode::kNotFound) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "ERROR: Known Error file " << filename << " does not exist";
    } else {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "ERROR: Unable to get known error contents from " << filename
             << ": " << internal::StatusToString(status);
    }
  }

  KnownErrorFile known_error_file;
  if (!google::protobuf::TextFormat::ParseFromString(text, &known_error_file)) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "ERROR: Failed to parse known error file " << filename;
  }

  for (int i = 0; i < known_error_file.known_errors_size(); i++) {
    ZETASQL_RETURN_IF_ERROR(AddKnownErrorEntry(known_error_file.known_errors(i)));
  }

  return absl::OkStatus();
}

KnownErrorMode SQLTestBase::IsKnownError(
    const absl::btree_set<std::string>& effective_labels,
    absl::btree_set<std::string>* by_set) const {
  by_set->clear();

  KnownErrorMode mode = KnownErrorMode::NONE;
  KnownErrorMode individual_mode = KnownErrorMode::NONE;

  for (const std::string& label : effective_labels) {
    if (known_error_labels_.contains(label)) {
      individual_mode = zetasql_base::FindOrDie(label_info_map_, label).mode;
      mode = std::max(mode, individual_mode);
      by_set->insert(label);
      ZETASQL_LOG(INFO) << "Statement matches known error by label: " << label
                << " in mode: " << KnownErrorMode_Name(individual_mode);
    }

    for (const std::unique_ptr<RE2>& regex : known_error_regexes_) {
      if (RE2::FullMatch(label, *regex)) {
        individual_mode =
            zetasql_base::FindOrDie(label_info_map_, regex->pattern()).mode;
        mode = std::max(mode, individual_mode);
        by_set->insert(regex->pattern());
        ZETASQL_LOG(INFO) << "Statement matches known error by regex: "
                  << regex->pattern()
                  << " in mode: " << KnownErrorMode_Name(individual_mode);
      }
    }
  }

  // In --report_all_errors mode, ignore entries unless they are
  // CRASHES_DO_NOT_RUN mode.
  if (absl::GetFlag(FLAGS_report_all_errors) &&
      mode != KnownErrorMode::CRASHES_DO_NOT_RUN) {
    mode = KnownErrorMode::NONE;
    by_set->clear();
  }

  return mode;
}

void SQLTestBase::RecordKnownErrorStatement(KnownErrorMode mode) {
  // 'test_result_' is not set in a code-based test.
  if (test_result_ != nullptr) {
    // Among other things, 'ignore_test_output' flag controls the 'TEST_OUTPUT'
    // section in a log file. When set to false, statement result will be
    // outputted to the 'TEST_OUTPUT' section. When set to true, statement
    // result is ignored and the golden is copied into the 'TEST_OUTPUT'
    // section. Because the golden tool uses the 'TEST_OUTPUT' section to update
    // goldens, setting 'ignore_test_output' to true prevents a "known error"
    // statement (which usually generates wrong test result) from polluting the
    // goldens.
    //
    // Note that this only applies to the reference engine, which is tested
    // against the golden. Other engines are tested against the reference
    // engine, and 'ignore_test_output' is always set to true for other engines.
    // See the 'else' branch in StepExecuteStatementCheckResult().
    test_result_->set_ignore_test_output(true);
  }
  stats_->RecordKnownErrorStatement(location_, mode, by_set_);
}

std::string SQLTestBase::Location(absl::string_view filename, int line_num,
                                  absl::string_view name) const {
  if (name.empty()) {
    return absl::StrCat("FILE-", zetasql_base::Basename(filename), "-LINE-", line_num);
  } else {
    return absl::StrCat("FILE-", zetasql_base::Basename(filename), "-LINE-", line_num,
                        "-NAME-", name);
  }
}

ProductMode SQLTestBase::product_mode() const {
  return driver()->GetSupportedLanguageOptions().product_mode();
}

void SQLTestBase::SetUp() { ZETASQL_EXPECT_OK(CreateDatabase(TestDatabase{})); }

namespace {

// Returns true if the sql is of pattern "SELECT AS STRUCT",
// or if the value is of type ARRAY with a non-struct element type. See
// (broken link) for details.
bool IsValueTable(const std::string& sql, const Value& table) {
  RE2::Options options;
  options.set_dot_nl(true);
  static const RE2 pattern("(?i)\\s*SELECT\\s+AS\\s+STRUCT\\s+(.*)", options);
  if (RE2::FullMatch(sql, pattern)) return true;

  ZETASQL_CHECK(table.type()->IsArray());
  return !table.type()->AsArray()->element_type()->IsStruct();
}

bool TableContainsColumn(const Value& table, const std::string& column) {
  const Type* row_type = table.type()->AsArray()->element_type();
  if (row_type->IsStruct()) {
    bool is_ambiguous;
    int index;
    const StructField* field =
        row_type->AsStruct()->FindField(column, &is_ambiguous, &index);
    return field != nullptr;
  } else if (row_type->IsProto()) {
    const google::protobuf::FieldDescriptor* fd = ProtoType::FindFieldByNameIgnoreCase(
        row_type->AsProto()->descriptor(), column);
    return fd != nullptr;
  }
  return false;
}

}  // namespace

absl::Status SQLTestBase::CreateDatabase() {
  ZETASQL_RETURN_IF_ERROR(ValidateFirstColumnPrimaryKey(
      test_db_, driver()->GetSupportedLanguageOptions()));

  // Do not call SQLTestBase::CreateDatabase(TestDatabase), which will reset
  // type factory of the reference driver and invalidate all values in
  // test_db_.
  ZETASQL_RETURN_IF_ERROR(driver()->CreateDatabase(test_db_));
  if (!IsTestingReferenceImpl()) {
    // No need to load protos and enums into reference driver. They are
    // already loaded in StepPrepareDatabase().
    for (const auto& [table_name, test_table] : test_db_.tables) {
      reference_driver()->AddTable(table_name, test_table);
    }
  }

  // Only create test database once.
  test_db_.clear();

  return absl::OkStatus();
}

absl::Status SQLTestBase::ValidateStatementResult(
    const absl::StatusOr<Value>& result, const std::string& statement) const {
  if (!result.ok()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Statement failed: " << internal::StatusToString(result.status());
  }

  if (result.value().is_null() || !result.value().is_valid()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Generated values is null or invalid when evaluating "
           << statement;
  }

  return absl::OkStatus();
}

absl::Status SQLTestBase::LoadProtosAndEnums() {
  const std::set<std::string>& files = test_case_options_->proto_file_names();
  const std::set<std::string>& protos =
      test_case_options_->proto_message_names();
  const std::set<std::string>& enums = test_case_options_->proto_enum_names();

  if (!files.empty() || !protos.empty() || !enums.empty()) {
    test_db_.proto_files.insert(files.begin(), files.end());
    test_db_.proto_names.insert(protos.begin(), protos.end());
    test_db_.enum_names.insert(enums.begin(), enums.end());
    if (!IsTestingReferenceImpl()) {
      ZETASQL_RETURN_IF_ERROR(
          reference_driver_->LoadProtoEnumTypes(files, protos, enums));
    }
  }

  return absl::OkStatus();
}

absl::Status SQLTestBase::SetDefaultTimeZone(
    const std::string& default_time_zone) {
  ZETASQL_RETURN_IF_ERROR(test_setup_driver_->SetDefaultTimeZone(default_time_zone));
  ZETASQL_RETURN_IF_ERROR(test_driver_->SetDefaultTimeZone(default_time_zone));
  if (!IsTestingReferenceImpl()) {
    ZETASQL_RETURN_IF_ERROR(reference_driver_->SetDefaultTimeZone(default_time_zone));
  }

  return absl::OkStatus();
}

void SQLTestBase::LogParameters(
    const std::map<std::string, Value>& parameters) const {
  std::vector<std::string> pairs;
  pairs.reserve(parameters.size());
  for (const std::pair<const std::string, Value>& item : parameters) {
    pairs.emplace_back(
        absl::StrCat(item.first, ":", item.second.FullDebugString()));
  }

  ZETASQL_VLOG(3) << "Parameters: " << absl::StrJoin(pairs, ", ");
}

absl::Status SQLTestBase::ParseFeatures(const std::string& features_str,
                                        std::set<LanguageFeature>* features) {
  features->clear();
  const std::vector<std::string> feature_list =
      absl::StrSplit(features_str, ',', absl::SkipEmpty());
  for (const std::string& feature_name : feature_list) {
    const std::string full_feature_name =
        absl::StrCat("FEATURE_", feature_name);
    LanguageFeature feature;
    if (!LanguageFeature_Parse(full_feature_name, &feature)) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Invalid feature name: " << full_feature_name;
    }
    features->insert(feature);
  }
  return absl::OkStatus();
}

bool SQLTestBase::IsTestingReferenceImpl() const {
  return driver()->IsReferenceImplementation();
}

TypeFactory* SQLTestBase::table_type_factory() {
  return test_setup_driver_->type_factory();
}

void SQLTestBase::ResetExecuteStatementTypeFactory() {
  execute_statement_type_factory_ = std::make_unique<TypeFactory>();
}

ReferenceDriver::ExecuteStatementOptions
SQLTestBase::GetExecuteStatementOptions() const {
  ReferenceDriver::ExecuteStatementOptions options;
  options.primary_key_mode = test_case_options_ == nullptr
                                 ? PrimaryKeyMode::DEFAULT
                                 : test_case_options_->primary_key_mode();
  return options;
}

LanguageOptions SQLTestBase::driver_language_options() {
  return driver()->GetSupportedLanguageOptions();
}

bool SQLTestBase::DriverSupportsFeature(LanguageFeature feature) {
  const bool enabled =
      driver_language_options().LanguageFeatureEnabled(feature);
  if (driver()->IsReferenceImplementation()) {
    // If the tests depend on whether some feature is enabled in the reference
    // implementation, and that feature is disabled, then something is
    // probably wrong. This ZETASQL_CHECK helps prevent tests from being silently
    // skipped.
    ZETASQL_CHECK(enabled) << LanguageFeature_Name(feature);
  }
  return enabled;
}

bool SQLTestBase::IsFileBasedStatement() const {
  return stats_->IsFileBasedStatement();
}

MatcherCollection<absl::Status>* SQLTestBase::legal_runtime_errors() {
  if (legal_runtime_errors_ == nullptr) {
    legal_runtime_errors_ = LegalRuntimeErrorMatcher("LegalRuntimeErrors");
  }
  return legal_runtime_errors_.get();
}

// Collects global stats.
class SQLTestEnvironment : public ::testing::Environment {
 public:
  // Set up global resource SQLTestBase::stats_.
  void SetUp() override { SQLTestBase::stats_ = std::make_unique<Stats>(); }

  // Call Stats::LogGoogletestProperties() to output statistics to continuous
  // integration environments.
  //
  // Call Stats::LogReport() to output all failed statements to the log file
  // in the format:
  //
  //   ===== To Be Added To Known Errors Statements =====
  //     label: "<filename>:<statement_name>"
  //     label: "code:<test_name>"
  //     ...
  //   === End To Be Added To Known Errors Statements ===
  //
  // The labels can be copied/pasted into a known_errors file.
  void TearDown() override {
    SQLTestBase::stats_->LogGoogletestProperties();
    SQLTestBase::stats_->LogReport();
    if (absl::GetFlag(FLAGS_zetasql_compliance_write_labels_to_file)) {
      SQLTestBase::stats_->WriteComplianceLabels();
    }
  }
};

// Enable human-readable display of ScriptResult objects in Sponge logs of
// failing tests.
std::ostream& operator<<(std::ostream& os, const ScriptResult& result) {
  return os << SQLTestBase::ToString(result);
}

namespace {
static bool module_initialization_complete = []() {
  ::testing::AddGlobalTestEnvironment(new SQLTestEnvironment);
  return true;
} ();
}  // namespace

}  // namespace zetasql
