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
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/base/path.h"
#include "google/protobuf/text_format.h"
#include "zetasql/common/internal_value.h"
#include "zetasql/compliance/legal_runtime_errors.h"
#include "zetasql/compliance/parameters_test_util.h"
#include "zetasql/compliance/test_driver.h"
#include "zetasql/compliance/test_util.h"
#include "zetasql/public/functions/string.h"
#include "zetasql/public/parse_helpers.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/testing/type_util.h"
#include "gmock/gmock.h"
#include <cstdint>
#include "absl/container/btree_map.h"
#include "absl/container/btree_set.h"
#include "absl/container/node_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/functional/bind_front.h"
#include "absl/memory/memory.h"
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
#include "absl/time/time.h"
#include "file_based_test_driver/file_based_test_driver.h"
#include "zetasql/base/file_util.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "farmhash.h"
#include "re2/re2.h"
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
// Ideally we would rename this to --statement_name_pattern, but that might
// break some command line somewhere.
ABSL_FLAG(std::string, query_name_pattern, "",
          "Only test statements with names matching this pattern. This filter "
          "works only for statements in .test files. No effect if left empty.");
ABSL_FLAG(bool, auto_generate_test_names, false,
          "When true, test cases in file don't have to have [name] tag, the "
          "names will be automatically generated when name is missing.");
namespace zetasql {

using ComplianceTestCaseResult =
    ::zetasql::SQLTestBase::ComplianceTestCaseResult;

std::unique_ptr<SQLTestBase::Stats> SQLTestBase::stats_;
std::unique_ptr<MatcherCollection<absl::Status>>
    SQLTestBase::legal_runtime_errors_;

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

std::string ScriptResultToString(const ScriptResult& result) {
  return absl::StrCat(
      "ScriptResult\n",
      absl::StrJoin(result.statement_results, "\n",
                    [](std::string* out, const StatementResult& stmt_result) {
                      absl::StrAppend(out,
                                      StatementResultToString(stmt_result));
                    }));
}

}  // namespace

// gMock matcher that matches a statement result (StatusOr<Value>) with a
// StatusOr<value>. Not supposed to be called directly. Use KnownErrorFilter.
MATCHER_P2(ReturnsStatusOrValue, expected, float_margin,
           SQLTestBase::ToString(expected)) {
  bool passed = true;
  std::string reason;
  if (expected.ok() != arg.ok()) {
    passed = false;
  } else if (expected.ok()) {
    if (absl::holds_alternative<Value>(expected.value()) &&
        absl::holds_alternative<Value>(arg.value())) {
      passed = InternalValue::Equals(
          absl::get<Value>(expected.value()), absl::get<Value>(arg.value()),
          {.interval_compare_mode = IntervalCompareMode::kAllPartsEqual,
           .float_margin = float_margin,
           .reason = &reason});
    } else if (absl::holds_alternative<ScriptResult>(expected.value()) &&
               absl::holds_alternative<ScriptResult>(arg.value())) {
      passed = CompareScriptResults(absl::get<ScriptResult>(expected.value()),
                                    absl::get<ScriptResult>(arg.value()),
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
    if (expected.ok() && absl::holds_alternative<Value>(expected.value()) &&
        zetasql::testing::HasFloatingPointNumber(
            absl::get<Value>(expected.value()).type())) {
      error = absl::StrCat("\nFloat comparison: ", float_margin.DebugString());
    }
    *result_listener << absl::StrCat(
        error, trimmed_reason.empty() ? "" : "\n Details: ", trimmed_reason);
    return false;
  }
  return true;
}

namespace {

constexpr size_t kLogBufferSize =
    15000;

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
std::string FileBasedTestName(const std::string& filename,
                              const std::string& name) {
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
  KnownErrorFilter(
      SQLTestBase* sql_test,
      const ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>&
          matcher)
      : KnownErrorFilter(sql_test, matcher, false /* check_only */) {}

  KnownErrorFilter(
      SQLTestBase* sql_test,
      const ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>&
          matcher,
      bool check_only)
      : sql_test_(sql_test), matcher_(matcher), check_only_(check_only) {}

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
    if (KnownErrorMode::CRASHES_DO_NOT_RUN == from_mode) {
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
      } else if (to_mode < from_mode) {
        // A to-be-upgraded statement.
        sql_test_->stats_->RecordToBeUpgradedStatement(
            sql_test_->full_name_, from_mode, to_mode, check_only_);
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
  for (const auto& pair : test_db.tables) {
    const std::string& table_name = pair.first;
    const TestTable& test_table = pair.second;

    // Do not check primary key column if it is a value table.
    if (test_table.options.is_value_table()) continue;

    ZETASQL_RETURN_IF_ERROR(zetasql::ValidateFirstColumnPrimaryKey(
        table_name, test_table.table_as_value, language_options));
  }
  return absl::OkStatus();
}

::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
SQLTestBase::ReturnsSuccess() {
  return ::testing::MakeMatcher(new KnownErrorFilter(this, ReturnsOk()));
}

::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
SQLTestBase::Returns(const ComplianceTestCaseResult& result,
                     const absl::Status& status, FloatMargin float_margin) {
  if (status.ok()) {
    return ::testing::MakeMatcher(new KnownErrorFilter(
        this,
        ReturnsStatusOrValue(absl::StatusOr<ComplianceTestCaseResult>(result),
                             float_margin)));
  } else {
    return ::testing::MakeMatcher(new KnownErrorFilter(
        this,
        ReturnsStatusOrValue(absl::StatusOr<ComplianceTestCaseResult>(status),
                             float_margin)));
  }
}

::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
SQLTestBase::Returns(const absl::StatusOr<ComplianceTestCaseResult>& result,
                     FloatMargin float_margin) {
  return ::testing::MakeMatcher(
      new KnownErrorFilter(this, ReturnsStatusOrValue(result, float_margin)));
}

::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
SQLTestBase::ReturnsCheckOnly(
    const absl::StatusOr<ComplianceTestCaseResult>& result,
    FloatMargin float_margin) {
  return ::testing::MakeMatcher(new KnownErrorFilter(
      this, ReturnsStatusOrValue(result, float_margin), true /* check_only*/));
}

::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
SQLTestBase::Returns(const std::string& result) {
  return ::testing::MakeMatcher(
      new KnownErrorFilter(this, ReturnsString(result)));
}

::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
SQLTestBase::Returns(
    const ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
        matcher) {
  return ::testing::MakeMatcher(new KnownErrorFilter(this, matcher));
}

absl::StatusOr<ComplianceTestCaseResult> SQLTestBase::RunStatement(
    const std::string& sql, const std::vector<Value>& params,
    const std::vector<std::string>& param_names) {
  std::map<std::string, Value> param_map;

  for (int i = 0; i < params.size(); i++) {
    std::string param_name;
    if (i < param_names.size()) {
      param_name = param_names[i];
    } else {
      param_name = absl::StrCat("p", i);
    }
    param_map[param_name] = params[i];
  }

  return ExecuteTestCase(sql, param_map);
}

SimpleCatalog* SQLTestBase::catalog() const {
  if (!IsTestingReferenceImpl()) {
    // CreateDatabase() has already created a concrete database in the
    // reference driver.
    return reference_driver()->catalog();
  } else {
    // The test driver is a ReferenceDriver.
    ReferenceDriver* reference = static_cast<ReferenceDriver*>(driver());
    return reference->catalog();
  }
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

  auto driver = absl::make_unique<ReferenceDriver>(options);
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
      execute_statement_type_factory_(absl::make_unique<TypeFactory>()) {
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
      execute_statement_type_factory_(absl::make_unique<TypeFactory>()) {
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

// CHECKs if the input Type is an ARRAY of STRUCT and all fields have names.
void CheckIsTable(const Type* type, const std::string& table_name) {
  const std::string details =
      absl::StrCat("Table ", table_name, " is in type ",
                   type->DebugString(true /* details */));
  ZETASQL_CHECK(type->IsArray()) << details;
  const Type* element_type = type->AsArray()->element_type();
  ZETASQL_CHECK(element_type->IsStruct()) << details;
  for (int i = 0; i < element_type->AsStruct()->num_fields(); i++) {
    ZETASQL_CHECK(!element_type->AsStruct()->field(i).name.empty()) << details;
  }
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

absl::StatusOr<Value> SQLTestBase::ExecuteStatement(
    const std::string& sql, const std::map<std::string, Value>& parameters) {
  ZETASQL_CHECK(!script_mode_)
      << "SQLTestBase::ExecuteStatement() should not be called in script mode";
  ZETASQL_ASSIGN_OR_RETURN(ComplianceTestCaseResult result,
                   ExecuteTestCaseImpl(sql, parameters));
  return absl::get<Value>(result);
}

absl::StatusOr<ComplianceTestCaseResult> SQLTestBase::ExecuteTestCase(
    const std::string& sql, const std::map<std::string, Value>& parameters) {
  // For scripting tests, we can just invoke ExecuteTestCaseImpl() directly.
  // For non-scripting tests, we need to go through ExecuteStatement(), since
  // some engine-specific test driver override it.
  if (script_mode_) {
    return ExecuteTestCaseImpl(sql, parameters);
  } else {
    return ExecuteStatement(sql, parameters);
  }
}

absl::StatusOr<ComplianceTestCaseResult> SQLTestBase::ExecuteTestCaseImpl(
    const std::string& sql, const std::map<std::string, Value>& parameters) {
  absl::string_view trimmed_sql;
  absl::Status status;
  ZETASQL_CHECK(zetasql::functions::RightTrimBytes(sql, "\n", &trimmed_sql, &status));
  sql_ = std::string(trimmed_sql);
  parameters_ = parameters;
  if (!stats_->IsFileBasedStatement()) {
    GenerateCodeBasedStatementName(sql, parameters);
    absl::btree_set<std::string> labels = GetCodeBasedLabels();
    labels.insert(full_name_);

    effective_labels_ = labels;

    ZETASQL_LOG(INFO) << "Starting code-based test: " << full_name_;
    LogStrings(labels, "Effective labels: ");

    known_error_mode_ = IsKnownError(labels, &by_set_);
    if (KnownErrorMode::CRASHES_DO_NOT_RUN == known_error_mode_) {
      // Do not run CRASHES_DO_NOT_RUN.
      RecordKnownErrorStatement();
      if (script_mode_) {
        return absl::StatusOr<ScriptResult>(
            absl::Status(absl::StatusCode::kCancelled, "Known Error"));
      } else {
        return absl::StatusOr<Value>(
            absl::Status(absl::StatusCode::kCancelled, "Known Error"));
      }
    }
  }

  // Splits this long log statement into smaller pieces that are less than
  // kLogBufferSize and ideally, split on newlines.
  std::string sql_log_string = absl::StrCat(
      "Running statement:\n  ", sql,
      (parameters.empty()
           ? "\nNo Parameters\n"
           : absl::StrCat("\nParameters:\n", ToString(parameters))),
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
    ReferenceDriver* reference_driver = static_cast<ReferenceDriver*>(driver());
    bool is_deterministic_output;
    bool uses_unsupported_type = false;  // unused
    if (script_mode_) {
      // Don't support plumbing deterministic output in scripting
      is_deterministic_output = false;
      result = reference_driver->ExecuteScriptForReferenceDriver(
          sql, parameters, GetExecuteStatementOptions(),
          execute_statement_type_factory(), &uses_unsupported_type);
    } else {
      result = reference_driver->ExecuteStatementForReferenceDriver(
          sql, parameters, GetExecuteStatementOptions(),
          execute_statement_type_factory(), &is_deterministic_output,
          &uses_unsupported_type);
    }
  } else {
    if (script_mode_) {
      result = driver()->ExecuteScript(sql, parameters,
                                       execute_statement_type_factory());
    } else {
      result = driver()->ExecuteStatement(sql, parameters,
                                          execute_statement_type_factory());
    }
  }
  stats_->RecordStatementExecutionTime(absl::Now() - start_time);
  return result;
}

void SQLTestBase::RunSQLTests(const std::string& filename) {
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
  StepParsePrimaryKeyMode();
  StepSkipUnsupportedTest();
  StepPrepareTimeZoneProtosEnums();
  StepParseParameters();
  StepPrepareDatabase();
  StepCheckKnownErrors();
  StepCreateDatabase();
  SkipEmptyTest();
  StepExecuteStatementCheckResult();
}

SQLTestBase::Stats::Stats()
{}

void SQLTestBase::Stats::RecordExecutedStatement() { num_executed_++; }

std::string SQLTestBase::Stats::LabelString(
    const std::string& label, const KnownErrorMode was_mode,
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

void SQLTestBase::Stats::RecordToBeRemovedFromKnownErrorsStatement(
    const std::string& full_name, const KnownErrorMode was_mode) {
  to_be_removed_from_known_errors_.insert(
      LabelString(full_name, was_mode, KnownErrorMode::NONE));
}

void SQLTestBase::Stats::RecordToBeUpgradedStatement(
    const std::string& full_name, const KnownErrorMode was_mode,
    const KnownErrorMode new_mode) {
  to_be_upgraded_.insert(LabelString(full_name, was_mode, new_mode));
}

void SQLTestBase::Stats::RecordFailedStatement(const std::string& msg,
                                               const std::string& location,
                                               const std::string& full_name,
                                               const KnownErrorMode was_mode,
                                               const KnownErrorMode new_mode) {
  if (file_based_statements_) RecordProperty(location, "Failed");
  to_be_added_to_known_errors_.insert(
      LabelString(full_name, was_mode, new_mode));
  failures_.emplace_back(msg);
}

void SQLTestBase::Stats::RecordCancelledStatement(const std::string& location,
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

void SQLTestBase::Stats::RecordKnownErrorStatement(
    const std::string& location, const KnownErrorMode mode,
    const absl::btree_set<std::string>& by_set) {
  num_known_errors_++;

  const std::string by =
      absl::StrCat("Mode: ", KnownErrorMode_Name(mode),
                   ", Due to: ", absl::StrJoin(by_set, ", "));

  RecordProperty(location, by);
}

void SQLTestBase::Stats::RecordStatementExecutionTime(absl::Duration elapsed) {
}

void SQLTestBase::Stats::LogGoogletestProperties() const {
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
void SQLTestBase::Stats::LogBatches(const Iterable& iterable,
                                    const std::string& title,
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

void SQLTestBase::Stats::LogReport() const {
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
  names_.clear();
  global_labels_.clear();
  test_db_.clear();
  options_ = absl::make_unique<file_based_test_driver::TestCaseOptions>();
  options_->RegisterString(kName, "");
  options_->RegisterString(kLabels, "");
  options_->RegisterString(kDescription, "");
  options_->RegisterString(kGlobalLabels, "");
  options_->RegisterString(kParameters, "");
  options_->RegisterString(kLoadProtoFiles, "");
  options_->RegisterString(kLoadProtoNames, "");
  options_->RegisterString(kLoadEnumNames, "");
  options_->RegisterBool(kPrepareDatabase, false);
  options_->RegisterString(kTestFeatures1, "");
  options_->RegisterString(kTestFeatures2, "");
  options_->RegisterString(kRequiredFeatures, "");
  options_->RegisterString(kForbiddenFeatures, "");
  options_->RegisterString(kDefaultTimeZone, "");
  options_->RegisterString(kPrimaryKeyMode,
                           PrimaryKeyModeName(PrimaryKeyMode::DEFAULT));
}

void SQLTestBase::InitStatementState(
    absl::string_view sql,
    file_based_test_driver::RunTestCaseResult* test_result) {
  statement_workflow_ = NORMAL;
  sql_ = sql;
  parameters_.clear();
  full_name_ = "";
  driver()->SetTestName(full_name_);
  known_error_mode_ = KnownErrorMode::NONE;
  by_set_.clear();
  test_result_ = test_result;
  effective_labels_.clear();

  // A creating-table section does not have a name. Here we create a location
  // string without the name. Later on we will update the location string
  // when name is available.
  location_ = Location(test_result->filename(), test_result->line());

  CheckCancellation(options_->ParseTestCaseOptions(&sql_),
                    "Failed to parse options");
}

static absl::Status ParsePrimaryKeyMode(absl::string_view mode_string,
                                        PrimaryKeyMode* primary_key_mode) {
  const std::string lower_mode_string = absl::AsciiStrToLower(mode_string);

  const std::array<PrimaryKeyMode, 3> modes = {
      PrimaryKeyMode::FIRST_COLUMN_IS_PRIMARY_KEY,
      PrimaryKeyMode::NO_PRIMARY_KEY, PrimaryKeyMode::DEFAULT};
  for (PrimaryKeyMode mode : modes) {
    if (lower_mode_string == absl::AsciiStrToLower(PrimaryKeyModeName(mode))) {
      *primary_key_mode = mode;
      return absl::OkStatus();
    }
  }

  return ::zetasql_base::InvalidArgumentErrorBuilder()
         << "Invalid primary key mode: " << mode_string;
}

void SQLTestBase::StepParsePrimaryKeyMode() {
  if (statement_workflow_ != NORMAL) return;

  CheckCancellation(ParsePrimaryKeyMode(options_->GetString(kPrimaryKeyMode),
                                        &primary_key_mode_),
                    "Failed to parse primary key mode");
}

void SQLTestBase::StepSkipUnsupportedTest() {
  if (statement_workflow_ != NORMAL) return;

  // If we are testing the reference implementation, all tests should be
  // supported.
  if (IsTestingReferenceImpl()) return;

  std::set<LanguageFeature> required_features;
  CheckCancellation(
      ParseFeatures(options_->GetString(kRequiredFeatures), &required_features),
      "Failed to parse required_features");
  if (statement_workflow_ == CANCELLED) return;

  bool skip_test = false;
  for (const LanguageFeature& required_feature : required_features) {
    if (!driver_language_options().LanguageFeatureEnabled(required_feature)) {
      skip_test = true;
      break;
    }
  }

  std::set<LanguageFeature> forbidden_features;
  CheckCancellation(ParseFeatures(options_->GetString(kForbiddenFeatures),
                                  &forbidden_features),
                    "Failed to parse forbidden_features");
  if (statement_workflow_ == CANCELLED) return;

  for (const LanguageFeature& forbidden_feature : forbidden_features) {
    if (driver_language_options().LanguageFeatureEnabled(forbidden_feature)) {
      skip_test = true;
      break;
    }
  }

  absl::StatusOr<bool> status_or_skip_test_for_primary_key_mode =
      driver()->SkipTestsWithPrimaryKeyMode(primary_key_mode_);
  CheckCancellation(status_or_skip_test_for_primary_key_mode.status(),
                    "Failed to interpret primary key mode");
  if (statement_workflow_ == CANCELLED) return;

  skip_test |= status_or_skip_test_for_primary_key_mode.value();

  if (skip_test) {
    test_result_->set_ignore_test_output(true);
    statement_workflow_ = SKIPPED;
  }
}

void SQLTestBase::StepParseParameters() {
  if (statement_workflow_ != NORMAL) return;

  if (!options_->GetString(kParameters).empty()) {
    CheckCancellation(
        ParseParameters(options_->GetString(kParameters), &parameters_),
        "Failed to generate parameters");
  }
}

void SQLTestBase::StepPrepareTimeZoneProtosEnums() {
  if (statement_workflow_ != NORMAL) return;

  // Handles default time zone first, because it may affect results of
  // statements that are used to generate parameters or table contents.
  if (options_->IsExplicitlySet(kDefaultTimeZone)) {
    if (CREATE_DATABASE == file_workflow_) {
      CheckCancellation(SetDefaultTimeZone(),
                        "Failed to load default time zone");
    } else {
      absl::Status status(absl::StatusCode::kInvalidArgument,
                          "A [default_time_zone] must locate at the first "
                          "section of a *.test file.");
      CheckCancellation(status, "Wrong placement of default time zone");
    }
  }
  if (CANCELLED == statement_workflow_) return;

  // Handles proto and enum loading second, because the table being created
  // may use these types.
  std::vector<std::string> files, protos, enums;
  if (options_->IsExplicitlySet(kLoadProtoFiles) ||
      options_->IsExplicitlySet(kLoadProtoNames) ||
      options_->IsExplicitlySet(kLoadEnumNames)) {
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

  if (options_->GetBool(kPrepareDatabase)) {
    std::string table_name;
    if (CREATE_DATABASE != file_workflow_) {
      absl::Status status(absl::StatusCode::kInvalidArgument,
                          "All [prepare_database] must be placed at the top "
                          "of a *.test file");
      CheckCancellation(status, "Wrong placement of prepare_database");
      return;
    }
    if (GetStatementKind(sql_) != RESOLVED_CREATE_TABLE_AS_SELECT_STMT) {
      absl::Status status(
          absl::StatusCode::kInvalidArgument,
          "Only CREATE TABLE AS (SELECT...) statements are supported for "
          "[prepare_database]");
      CheckCancellation(status, "Invalid CREATE TABLE statement");
      if (CANCELLED == statement_workflow_) return;
    }

    std::set<LanguageFeature> required_features;
    CheckCancellation(ParseFeatures(options_->GetString(kRequiredFeatures),
                                    &required_features),
                      "Failed to parse required_features for prepare_database");
    if (CANCELLED == statement_workflow_) return;

    // Run everything when testing the reference implementation (even tests
    // for in-development features), but for a real engine, skip unsupported
    // tables.
    if (!IsTestingReferenceImpl()) {
      for (const LanguageFeature& required_feature : required_features) {
        if (!driver_language_options().LanguageFeatureEnabled(
                required_feature)) {
          statement_workflow_ = SKIPPED;
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
    if (CANCELLED == statement_workflow_) return;
    ZETASQL_CHECK(zetasql_base::ContainsKey(test_db_.tables, table_name));
    *test_db_.tables[table_name].options.mutable_required_features() =
        required_features;

    // Output to golden files for validation purpose.
    test_result_->AddTestOutput(
        test_db_.tables[table_name].table_as_value.Format());

    // The create table section is not a test. No need to proceed further.
    statement_workflow_ = CANCELLED;
  } else if (CREATE_DATABASE == file_workflow_) {
    // This is the first statement.
    file_workflow_ = FIRST_STATEMENT;
  }
}

void SQLTestBase::StepCheckKnownErrors() {
  if (statement_workflow_ != NORMAL) return;

  std::string name, description;
  std::vector<std::string> labels, global_labels;
  CheckCancellation(ExtractLabels(&name, &labels, &description, &global_labels),
                    "Failed to extract labels");
  if (CANCELLED == statement_workflow_) return;

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
  driver()->SetTestName(full_name_);

  effective_labels_ = EffectiveLabels(filename, name, labels, global_labels);
  known_error_mode_ = IsKnownError(effective_labels_, &by_set_);
  if (KnownErrorMode::CRASHES_DO_NOT_RUN == known_error_mode_) {
    RecordKnownErrorStatement();
    statement_workflow_ = KNOWN_ERROR;
  }
}

void SQLTestBase::StepCreateDatabase() {
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
    statement_workflow_ = SKIPPED;
  }
}

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

void SQLTestBase::StepExecuteStatementCheckResult() {
  if (statement_workflow_ != NORMAL) return;

  std::set<LanguageFeature> test_features1;
  std::set<LanguageFeature> test_features2;
  std::set<LanguageFeature> required_features;
  CheckCancellation(
      ParseFeatures(options_->GetString(kTestFeatures1), &test_features1),
      "Failed to parse test_features1");
  CheckCancellation(
      ParseFeatures(options_->GetString(kTestFeatures2), &test_features2),
      "Failed to parse test_features2");
  CheckCancellation(
      ParseFeatures(options_->GetString(kRequiredFeatures), &required_features),
      "Failed to parse required_features");

  if (IsTestingReferenceImpl()) {
    // Check results against golden files.
    // All features in [required_features] will be turned on.
    // If the test has [test_features1] or [test_features2], the test will run
    // multiple times with each of those features sets all enabled or disabled,
    // and will generate a test output for each, prefixed with
    // "WITH FEATURES: ..." to show which features were set to get that output.

    // Compute the set of sets of LanguageFeatures that are interesting to
    // test.  For each collection of features that are required or required to
    // be unset, we'll try the statement with those features on or off.
    // TODO We could also implicitly do a test with all features turned
    // on, to catch cases where the test writer didn't notice that there would
    // be diffs based on features. (We already always include a run with zero
    // features enabled.)
    std::set<std::set<LanguageFeature>> features_sets;
    for (int include1 = 0; include1 <= 1; ++include1) {
      for (int include2 = 0; include2 <= 1; ++include2) {
        std::set<LanguageFeature> features_set = required_features;
        if (include1 == 1) {
          features_set.insert(test_features1.begin(), test_features1.end());
        }
        if (include2 == 1) {
          features_set.insert(test_features2.begin(), test_features2.end());
        }
        features_sets.insert(features_set);
      }
    }
    ZETASQL_CHECK(!features_sets.empty());

    // Now run the test for each of features_sets, generating a separate
    // result part for each.
    // We run the test for each of the features_sets, generating the result and
    // collecting the list of feature set which produce the same output.
    absl::btree_map<std::string /* driver output */,
                    std::vector<std::string> /* features_set_name_list */>
        result_to_feature_map;
    // We need the result status to honor the known error filters.
    std::map<std::string /* driver output */,
             absl::StatusOr<ComplianceTestCaseResult> /* driver result */>
        result_to_status_map;
    static const std::string kFeaturePrefix = "FEATURE_";
    ReferenceDriver* reference_driver = static_cast<ReferenceDriver*>(driver());
    for (const std::set<LanguageFeature>& features_set : features_sets) {
      LanguageOptions language_options =
          reference_driver->GetSupportedLanguageOptions();
      language_options.DisableAllLanguageFeatures();
      std::vector<std::string> feature_names;
      for (const LanguageFeature feature : features_set) {
        language_options.EnableLanguageFeature(feature);
        const std::string feature_name = LanguageFeature_Name(feature);
        ZETASQL_CHECK_EQ(feature_name.substr(0, kFeaturePrefix.size()), kFeaturePrefix);
        feature_names.push_back(feature_name.substr(kFeaturePrefix.size()));
      }
      const std::string features_set_name =
          feature_names.empty() ? "<none>" : absl::StrJoin(feature_names, ",");

      AutoLanguageOptions auto_options(reference_driver);
      reference_driver->SetLanguageOptions(language_options);

      absl::StatusOr<ComplianceTestCaseResult> driver_result =
          ExecuteTestCase(sql_, parameters_);
      result_to_feature_map[ToString(driver_result)].push_back(
          features_set_name);
      zetasql_base::InsertIfNotPresent(&result_to_status_map, ToString(driver_result),
                              driver_result);
    }
    ZETASQL_CHECK(!result_to_feature_map.empty());

    int result_part_number = 0;
    for (const auto& entry : result_to_feature_map) {
      const std::string& driver_result = entry.first;
      const std::vector<std::string>& features_set_name_list = entry.second;

      ++result_part_number;
      std::string result_prefix;
      if (features_sets.size() > 1) {
        for (const std::string& feature_set_name : features_set_name_list) {
          absl::StrAppend(&result_prefix, "WITH FEATURES: ", feature_set_name,
                          "\n");
        }
      }

      const std::string actual_result =
          absl::StrCat(result_prefix, driver_result);
      test_result_->AddTestOutput(actual_result);

      absl::string_view expected_string = "";
      if (test_result_->parts().size() > result_part_number) {
        expected_string = test_result_->parts()[result_part_number];
      }
      expected_string = absl::StripSuffix(expected_string, "\n");
      while (absl::StartsWith(expected_string, "WITH FEATURES:")) {
        // Remove the prefix_string line for comparison.
        expected_string =
            expected_string.substr(expected_string.find('\n') + 1);
      }

      EXPECT_THAT(zetasql_base::FindOrDie(result_to_status_map, driver_result),
                  Returns(std::string(expected_string)));
    }
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
      return;  // Skip this test. It uses types not supported by the driver.
    }
    absl::StatusOr<ComplianceTestCaseResult> actual_result =
        ExecuteTestCase(sql_, parameters_);
    SCOPED_TRACE(absl::StrCat("Testcase: ", full_name_, "\nSQL:\n", sql_));
    EXPECT_THAT(actual_result, Returns(ref_result, kDefaultFloatMargin));
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

absl::Status SQLTestBase::ExtractLabels(
    std::string* name, std::vector<std::string>* labels,
    std::string* description, std::vector<std::string>* global_labels) {
  if (!options_->IsExplicitlySet(kName) &&
      absl::GetFlag(FLAGS_auto_generate_test_names)) {
    options_->SetString(kName, absl::StrCat("_test", names_.size()));
  }
  *name = options_->GetString(kName);

  if (name->empty() && !sql_.empty()) {
    // An empty test, which contains only test case options, can have empty
    // name.
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "ERROR: A non-empty name is required for each statement";
  }

  if (zetasql_base::ContainsKey(names_, *name)) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "ERROR: Duplicated name in *.test file";
  }
  names_.insert(*name);

  if (options_->IsExplicitlySet(kGlobalLabels)) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "ERROR: global_labels must not be explicitly defined";
  }

  if (FIRST_STATEMENT == file_workflow_) {
    file_workflow_ = REST_STATEMENTS;
    global_labels_ = options_->GetString(kGlobalLabels);
  } else if (global_labels_ != options_->GetString(kGlobalLabels)) {
    // Suppress repetitive error messages.
    global_labels_ = options_->GetString(kGlobalLabels);
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "ERROR: global_labels must not be (re)defined after the first "
              "test"
           << "section";
  }

  *description = options_->GetString(kDescription);

  SplitLabels(options_->GetString(kLabels), labels);
  SplitLabels(options_->GetString(kGlobalLabels), global_labels);

  return absl::OkStatus();
}

void SQLTestBase::SplitLabels(const std::string& labels_all,
                              std::vector<std::string>* labels) const {
  *labels = absl::StrSplit(labels_all, ',', absl::SkipEmpty());
  StripWhitespaceVector(labels);
}

absl::btree_set<std::string> SQLTestBase::EffectiveLabels(
    const std::string& filename, const std::string& name,
    const std::vector<std::string>& labels,
    const std::vector<std::string>& global_labels) const {
  absl::btree_set<std::string> effective_labels;
  effective_labels.insert(FileBasedTestName(filename, name));
  effective_labels.insert(labels.begin(), labels.end());
  effective_labels.insert(global_labels.begin(), global_labels.end());

  LogStrings(effective_labels, "Effective labels: ");

  return effective_labels;
}

void SQLTestBase::StripWhitespaceVector(std::vector<std::string>* list) const {
  for (std::string& str : *list) {
    absl::StripAsciiWhitespace(&str);
  }
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
  return ReplaceChar(str, "[](){}.*+^$\\?+|\"", kSafeChar);
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
      break;
    case TYPE_STRING:
      return absl::StrCat("_", SignatureOfString(value.string_value()));
      break;
    case TYPE_BYTES:
      return absl::StrCat("_", SignatureOfString(value.bytes_value()));
      break;
    case TYPE_TIMESTAMP: {
      // We use unix micros since the epoch when the result is an
      // even number of micros, otherwise we use the full string
      // value (i.e. 2006-01-02 03:04:05.123456789+00).
      absl::Time base_time = value.ToTime();
      if (absl::UTCTimeZone().At(base_time).subsecond / absl::Nanoseconds(1) <=
          999999000) {
        return absl::StrCat("_", value.ToUnixMicros());
      } else {
        return absl::StrCat("_", SignatureOfCompositeValue(value));
      }
    } break;
    case TYPE_GEOGRAPHY:
      return absl::StrCat("_", SignatureOfString(value.DebugString()));
    case TYPE_JSON:
      return absl::StrCat("_", SignatureOfString(value.DebugString()));
    case TYPE_ENUM:
      return absl::StrCat("_", value.DebugString());
      break;
    case TYPE_DATE:
    case TYPE_ARRAY:
    case TYPE_STRUCT:
    case TYPE_PROTO:
      return absl::StrCat("_", SignatureOfCompositeValue(value));
      break;
    default:
      return absl::StrCat("_", value.DebugString());
      break;
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

void SQLTestBase::GenerateCodeBasedStatementName(
    const std::string& sql, const std::map<std::string, Value>& parameters) {
  std::vector<std::string> param_strs;
  std::string value;
  param_strs.reserve(parameters.size());
  for (const std::pair<std::string, Value>& pair : parameters) {
    param_strs.emplace_back(absl::StrCat(
        absl::StrReplaceAll(pair.second.type()->TypeName(product_mode()),
                            {{".", "_"}}),
        ValueToSafeString(pair.second)));
  }

  full_name_ = absl::StrReplaceAll(GetNamePrefix(), {{".", "_"}});
  if (!param_strs.empty()) {
    absl::StrAppend(&full_name_, "_", absl::StrJoin(param_strs, "_"));
  }
  driver()->SetTestName(full_name_);
  // If the name is not safe then we cannot create known error entries.
  ZETASQL_CHECK(RE2::FullMatch(full_name_, full_name_))
      << "Name is not RE2 safe " << full_name_;
}

std::string SQLTestBase::ToString(
    const absl::StatusOr<ComplianceTestCaseResult>& status) {
  std::string result_string;
  if (!status.ok()) {
    result_string =
        absl::StrCat("ERROR: ", internal::StatusToString(status.status()));
  } else if (absl::holds_alternative<Value>(status.value())) {
    const Value& value = absl::get<Value>(status.value());
    ZETASQL_CHECK(!value.is_null());
    ZETASQL_CHECK(value.is_valid());
    result_string = value.Format();
  } else {
    result_string =
        ScriptResultToString(absl::get<ScriptResult>(status.value()));
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
  for (const std::pair<std::string, Value>& pair : parameters) {
    param_strs.emplace_back(
        absl::StrCat("  @", pair.first, " = ", pair.second.FullDebugString()));
  }
  std::sort(param_strs.begin(), param_strs.end());
  return absl::StrJoin(param_strs, "\n");
}

void SQLTestBase::LogReason() const {
  std::vector<std::string> ref;
  for (const std::pair<std::string, LabelInfo>& pair : label_info_map_) {
    ref.emplace_back(absl::StrCat(
        pair.first, ": {", absl::StrJoin(pair.second.reason, ", "), "}"));
  }
  ZETASQL_VLOG(3) << "Reason: " << absl::StrJoin(ref, ", ");
}

void SQLTestBase::LogMode() const {
  std::vector<std::string> ref;
  for (const std::pair<std::string, LabelInfo>& pair : label_info_map_) {
    ref.emplace_back(
        absl::StrCat(pair.first, ": ", KnownErrorMode_Name(pair.second.mode)));
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
  if ((  //
          absl::GetFlag(FLAGS_report_all_errors)) &&
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
    // engine, and 'ignore_test_output' is alway set to true for other engines.
    // See the 'else' branch in StepExecuteStatementCheckResult().
    test_result_->set_ignore_test_output(true);
  }
  stats_->RecordKnownErrorStatement(location_, mode, by_set_);
}

std::string SQLTestBase::Location(const std::string& filename, int line_num,
                                  const std::string& name) const {
  if (name.empty()) {
    return absl::StrCat("FILE-", zetasql_base::Basename(filename), "-LINE-", line_num);
  } else {
    return absl::StrCat("FILE-", zetasql_base::Basename(filename), "-LINE-", line_num,
                        "-NAME-", name);
  }
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
    for (const auto& pair : test_db_.tables) {
      const std::string& table_name = pair.first;
      const TestTable& test_table = pair.second;
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
  std::set<std::string> files, protos, enums;
  if (options_->IsExplicitlySet(kLoadProtoFiles)) {
    SplitProtosOrEnums(options_->GetString(kLoadProtoFiles),
                       test_db_.proto_files, &files);
  }
  if (options_->IsExplicitlySet(kLoadProtoNames)) {
    SplitProtosOrEnums(options_->GetString(kLoadProtoNames),
                       test_db_.proto_names, &protos);
  }
  if (options_->IsExplicitlySet(kLoadEnumNames)) {
    SplitProtosOrEnums(options_->GetString(kLoadEnumNames), test_db_.enum_names,
                       &enums);
  }

  if (!files.empty() || !protos.empty() || !enums.empty()) {
    test_db_.proto_files.insert(files.begin(), files.end());
    test_db_.proto_names.insert(protos.begin(), protos.end());
    test_db_.enum_names.insert(enums.begin(), enums.end());
    ZETASQL_RETURN_IF_ERROR(
        test_setup_driver_->LoadProtoEnumTypes(files, protos, enums));
    if (!IsTestingReferenceImpl()) {
      ZETASQL_RETURN_IF_ERROR(
          reference_driver_->LoadProtoEnumTypes(files, protos, enums));
    }
  }

  return absl::OkStatus();
}

absl::Status SQLTestBase::SetDefaultTimeZone() {
  const std::string& default_time_zone = options_->GetString(kDefaultTimeZone);

  ZETASQL_RETURN_IF_ERROR(test_setup_driver_->SetDefaultTimeZone(default_time_zone));
  ZETASQL_RETURN_IF_ERROR(test_driver_->SetDefaultTimeZone(default_time_zone));
  if (!IsTestingReferenceImpl()) {
    ZETASQL_RETURN_IF_ERROR(reference_driver_->SetDefaultTimeZone(default_time_zone));
  }

  return absl::OkStatus();
}

void SQLTestBase::SplitProtosOrEnums(const std::string& item_string,
                                     const std::set<std::string>& existing,
                                     std::set<std::string>* items) const {
  for (const absl::string_view& item : absl::StrSplit(item_string, ',')) {
    std::string item_str = std::string(item);
    absl::StripAsciiWhitespace(&item_str);
    if (!zetasql_base::ContainsKey(existing, item_str)) {
      items->insert(item_str);
    }
  }
}

absl::Status SQLTestBase::ParseParameters(
    const std::string& param_str, std::map<std::string, Value>* parameters) {
  ZETASQL_RETURN_IF_ERROR(ParseTestFileParameters(param_str, test_setup_driver_.get(),
                                          table_type_factory(), parameters));
  LogParameters(*parameters);
  return absl::OkStatus();
}

void SQLTestBase::LogParameters(
    const std::map<std::string, Value>& parameters) const {
  std::vector<std::string> pairs;
  pairs.reserve(parameters.size());
  for (const std::pair<std::string, Value>& item : parameters) {
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
  void SetUp() override {
    SQLTestBase::stats_ = absl::make_unique<SQLTestBase::Stats>();
  }

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
