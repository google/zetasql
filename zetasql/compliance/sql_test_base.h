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

// SQL End-To-End Testing Framework.
//
// SQLTestBase defines a SQL end-to-end testing framework. Features include:
//   * Known error handling
//   * Sponge report
//   * Per-file schema and data
//   * Parameters for file-based tests
//   * Names for code-based tests
//
// The infrastructure can be used for different types of testing, such as
// ZetaSQL compliance tesing.
//
// SQLTestBase implements a two-level workflow for file-based testing:
//   * File-level workflow
//     1. Prepare tables for SQL statements
//     2. Create all tables in one shot
//     3. Load protos
//     4. Process statements
//   * Statement-level workflow
//     1. Parse parameters
//     2. Check known errors that can be ignored
//     3. Execute a statement and check its result
//
// Each step of the workflow is implemented by a protected virtual member
// function. A subclass can enhance the workflow by overriding step functions.
//
// SQLTestBase uses a few member variables to control the progress of the
// workflow. A subclass needs to access those variables if it chooses to
// enhance the workflow. Thus they are declared protected as well.
//
// To use SQLTestBase, a subclass needs to:
//   1. Subclass SQLTestBase
//   2. Implement GetTestSuiteName()
//   3. Define TEST_F(...) for code-based tests
//   4. Define a TEST_P(...) that calls RunSQLTest(<filename>) for each *.test
//      file of test cases.
//   5. Define a cc_test and use "--known_error_files <file>,<file>,..." to
//      pass in known error files
//   6. Or use a zetasql_compliance_test BUILD rule, which accepts a
//      known_error_files argument.
//
// SQLTestBase is designed to be an abstract class. Because a TEST_F(...)
// cannot be defined on an abstract class, all tests using SQLTestBase must
// be defined in subclasses. This design enforces separation between the
// the infrastructure and the tests.
//
#ifndef ZETASQL_COMPLIANCE_SQL_TEST_BASE_H_
#define ZETASQL_COMPLIANCE_SQL_TEST_BASE_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/float_margin.h"
#include "zetasql/compliance/known_error.pb.h"
#include "zetasql/compliance/matchers.h"
#include "zetasql/compliance/test_driver.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/reference_driver.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/btree_set.h"
#include "absl/container/node_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "file_based_test_driver/file_based_test_driver.h"  
#include "file_based_test_driver/run_test_case_result.h"
#include "file_based_test_driver/test_case_options.h"
#include "re2/re2.h"
#include "zetasql/base/status.h"

namespace zetasql {
class SQLTestBase;
}  // namespace zetasql

namespace zetasql {

class SQLTestBase : public ::testing::TestWithParam<std::string> {
 public:
  using ComplianceTestCaseResult = absl::variant<Value, ScriptResult>;

  const std::string kDefaultTimeZone = "default_time_zone";
  const std::string kDescription = "description";
  const std::string kGlobalLabels = "global_labels";
  const std::string kLabels = "labels";
  const std::string kLoadEnumNames = "load_enum_names";
  const std::string kLoadProtoFiles = "load_proto_files";
  const std::string kLoadProtoNames = "load_proto_names";
  const std::string kName = "name";
  const std::string kParameters = "parameters";
  const std::string kPrepareDatabase = "prepare_database";

  // These are comma-separated lists of LanguageFeature enums, without the
  // FEATURE_ prefix.  If these are set, when testing against the reference
  // implementation, the test will run multiple times, with features in
  // test_features1 on or off, and features in test_features2 on or off, and
  // all outputs will be shown in the golden file.
  // When testing against a non-reference implementation, each test runs only
  // once, and the output is compared to the reference implementation's output
  // when running with the engines options, provided by
  // TestDriver::GetSupportedLanguageOptions.
  const std::string kTestFeatures1 = "test_features1";
  const std::string kTestFeatures2 = "test_features2";

  // A comma-separated list of LanguageFeature enums,
  // without the FEATURE_ prefix. If it is set, the test will be run
  // against the implementations that support the features.
  const std::string kRequiredFeatures = "required_features";

  // Same as kRequiredFeatures, but skip tests that have these features
  // enabled. These must all be features that are annotated with
  // ideally_enabled=false in options.proto.
  const std::string kForbiddenFeatures = "forbidden_features";

  // The name of a PrimaryKeyMode enum value. See the comment for that enum in
  // test_driver.h for details.
  const std::string kPrimaryKeyMode = "primary_key_mode";

  // Defines the default number of rows of random data per table.
  const int kDefaultTableSize = 100;

  // Constants for code-based statement names.
  // A name must be RE2 safe. All unsafe characters will be replaced by '@'.
  const char kSafeChar = '@';
  const std::string kSafeString = std::string(1, kSafeChar);
  // For a long string, a signature is generated, which is the left 8
  // characters of the string, followed by the fingerprint of the string,
  // followed by the right 8 characters of the string.
  const int kLengthOfLeftSlice = 8;
  const int kLengthOfRightSlice = 8;

  // Override this method to return the name of the test suite.
  // This makes SQLTestBase an abstract class.
  virtual const std::string GetTestSuiteName() = 0;

  // Returns a debug string.
  static std::string ToString(
      const absl::StatusOr<ComplianceTestCaseResult>& status);
  static std::string ToString(const std::map<std::string, Value>& parameters);

  // Returns the error matcher to match legal runtime errors.
  static MatcherCollection<absl::Status>* legal_runtime_errors();

 protected:
  friend class StatementResultMatcher;

  SQLTestBase();

  // Does not take ownership of the pointers. 'reference_driver' must be NULL if
  // and only if either 'test_driver' is NULL or
  // 'test_driver->IsReferenceImplementation()' is true.
  //
  // If 'test_driver' is NULL, the destructor is the only method that can be
  // called on the resulting object. This is a hack to facilitate creating the
  // code-based compliance test data structures through googletest but then
  // skipping the actual tests (which presumably are being run in another BUILD
  // target).
  SQLTestBase(TestDriver* test_driver, ReferenceDriver* reference_driver);
  SQLTestBase(const SQLTestBase&) = delete;
  SQLTestBase& operator=(const SQLTestBase&) = delete;
  ~SQLTestBase() override;

  void ClearParameters() { parameters_.clear(); }

  // Return true if we are testing the reference implementation.
  // In this mode, we should run all statements, in all relevant modes, and test
  // all outputs (against expected outputs from code or from files).
  //
  // When this is false, we are testing an engine and comparing its output to
  // the reference implementation's output.  We will run each statement using
  // the engine's options (from TestDriver::GetSupportedLanguageOptions) and
  // compare against the reference output with the same options.
  //
  // TODO: There are many blocks of code in the cc file that look like:
  // if (IsTestingReferenceImpl()) {
  //   ...  // Do something
  // } else {
  //   ... // Do something else
  // }
  // Consider creating another interface with two implementations of each
  // method: one that implements the behavior for testing the reference
  // implementation, and one that implements the behavior for testing a real
  // engine.
  bool IsTestingReferenceImpl() const {
    return driver()->IsReferenceImplementation();
  }

  virtual absl::Status CreateDatabase(const TestDatabase& test_db);

  // Executes a test case, either as a standalone statement, or as a script,
  // depending on <script_mode_>.
  absl::StatusOr<ComplianceTestCaseResult> ExecuteTestCase(
      const std::string& sql, const std::map<std::string, Value>& parameters);

 public:
  // Executes 'sql', as a standalone statement.
  virtual absl::StatusOr<Value> ExecuteStatement(
      const std::string& sql, const std::map<std::string, Value>& parameters);

  // Use file-based test driver to run tests of a given file. Will be called
  // by TEST_P(...).
  //
  // Make it protected so it can be called by TEST_P(...) from a subclass.
  void RunSQLTests(const std::string& filename);

  // Each test driver can call this method to specify known error entries
  // that are non file-based.
  absl::Status AddKnownErrorEntry(const KnownErrorEntry& known_error_entry);

  // Each engine should call this method to specify engine-specific known error
  // files. Will die if a file does not exist.
  void LoadKnownErrorFiles(const std::vector<std::string>& files);

  // By default, SetUp() calls CreateDatabase(TestDatabase{}) to clean up
  // existing database, so every TEST_F(...) or TEST_P(...) has a clean
  // environment to start with.  Can be overridden by subclasses if needed.
  void SetUp() override;

  // This function is used as a callback from
  // file_based_test_driver::RunTestCasesFromFiles
  //
  // When running in ValidateAgainstReference(), statement output will be
  // checked against the output found in the golden statements files.  Otherwise
  // statement output will be validated against the reference implementation.
  void RunTestFromFile(absl::string_view sql,
                       file_based_test_driver::RunTestCaseResult* test_result);

  // Known Error Mode
  //
  // A known error can be in four modes (ordered by severity):
  //   * ALLOW_UNIMPLEMENTED
  //   * ALLOW_ERROR
  //   * ALLOW_ERROR_OR_WRONG_ANSWER
  //   * CRASHES_DO_NOT_RUN
  //
  // The framework always tries to run statements in all modes except
  // CRASHES_DO_NOT_RUN. The outcome of a non-crashing known error statement
  // can be:
  //   * The statement passed the test:
  //     Record it in the to-be-removed-from-known-errors list.
  //     Still log it as a known error statement in stats. This will not change
  //     the compliance ratio. A developer still needs to manually remove the
  //     statement from known error files.
  //   * The statement failed the test, in the same mode:
  //     Log it as a known error statement in stats, thus the test will not
  //     fail.
  //   * The statement failed the test, in a more severe mode:
  //     Record it in to-be-added-to-known-errors list. Log it as a *FAILED*
  //     statement in stats and *FAIL* the test. This failure counts as a
  //     regression, and forces the developer to resolve the failure promptly.
  //   * The statement failed the test, in a less severe mode:
  //     Log it as a known error statement in stats. Record it in to-be-upgraded
  //     list.
  //
  // To run all tests (except CRASHES_DO_NOT_RUN) and compute a
  // compliance ratio that includes the tests that had known error entries but
  // passing, use the <flag to be named> flag.
  //
  // The following gMock matchers implement the logic of known error modes. All
  // tests must use these matchers to compare results.
  //
  // Sample syntax of the gMock matchers.
  //   EXPECT_THAT(RunStatement(sql), Returns(x));
  //   EXPECT_THAT(RunStatement(sql), ReturnsSuccess());
  ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&> Returns(
      const Value& result, const absl::Status& status = absl::OkStatus(),
      FloatMargin float_margin = kExactFloatMargin) {
    return Returns(ComplianceTestCaseResult(result), status, float_margin);
  }
  ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&> Returns(
      const ComplianceTestCaseResult& result,
      const absl::Status& status = absl::OkStatus(),
      FloatMargin float_margin = kExactFloatMargin);
  ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&> Returns(
      const absl::StatusOr<ComplianceTestCaseResult>& result,
      FloatMargin float_margin = kExactFloatMargin);
  ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&> Returns(
      const std::string& result);
  ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&> Returns(
      const ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
          matcher);
  // A googletest matcher that only checks if the result has OK status.
  ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
  ReturnsSuccess();
  // A googletest matcher that only checks the result and records nothing.
  ::testing::Matcher<const absl::StatusOr<ComplianceTestCaseResult>&>
  ReturnsCheckOnly(const absl::StatusOr<ComplianceTestCaseResult>& result,
                   FloatMargin float_margin = kExactFloatMargin);

  // Runs statements with optional parameters. Parameters can have optional
  // names.
  absl::StatusOr<ComplianceTestCaseResult> RunStatement(
      const std::string& sql, const std::vector<Value>& params = {},
      const std::vector<std::string>& param_names = {});

  // Returns a Catalog that includes the tables specified in the active
  // TestDatabase. Owned by the reference driver internal to this class.
  SimpleCatalog* catalog() const;

  // Return the location string. Name is optional as it is defined only in
  // a *.test file for a statement.
  std::string Location(const std::string& filename, int line_num,
                       const std::string& name = "") const;

  // Get the current product mode of the test driver.
  ProductMode product_mode() {
    return driver()->GetSupportedLanguageOptions().product_mode();
  }

  // Stats over all tests in a test case.
  class Stats {
   public:
    Stats();
    Stats(const Stats&) = delete;
    Stats& operator=(const Stats&) = delete;

    // Record executed statements.
    void RecordExecutedStatement();

    // As above, but no-op if `check_only` is true.
    void RecordExecutedStatement(bool check_only) {
      if (!check_only) {
        return RecordExecutedStatement();
      }
    }

    // A to-be-removed-from-known-errors statement was in `was_mode`.
    void RecordToBeRemovedFromKnownErrorsStatement(
        const std::string& full_name, const KnownErrorMode was_mode);

    // As above, but a no-op if `check_only` is true.
    void RecordToBeRemovedFromKnownErrorsStatement(
        const std::string& full_name, const KnownErrorMode was_mode,
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

    void LogGoogletestProperties() const;

    // Print report to log file.
    void LogReport() const;

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

    int num_known_errors_ = 0;

    bool file_based_statements_ = false;
  };

  // Shared resource of Stats over all tests in a test case.
  static std::unique_ptr<Stats> stats_;

  // Add labels for the remainder of the current scope.
  class ScopedLabel {
   public:
    ScopedLabel(SQLTestBase* sql_test_base, const std::string& label,
                bool condition = true)
        : ScopedLabel(sql_test_base, std::vector<std::string>{label}) {}

    ScopedLabel(SQLTestBase* sql_test_base,
                const std::vector<std::string>& labels, bool condition = true)
        : sql_test_base_(sql_test_base), labels_(labels) {
      sql_test_base_->AddCodeBasedLabels(labels_);
    }

    ScopedLabel(const ScopedLabel&) = delete;
    ScopedLabel& operator=(const ScopedLabel&) = delete;

    ~ScopedLabel() { sql_test_base_->RemoveCodeBasedLabels(labels_); }

   private:
    SQLTestBase* sql_test_base_;
    const std::vector<std::string> labels_;
  };

  //
  // MakeScopedLabel()
  //
  // This is used to set labels for all tests in the current scope. And can be
  // used with a string or vector of strings. Examples:
  //
  //   auto label1 = MakeScopedLabel("label_1");
  //   auto label2 = MakeScopedLabel(label_vector);
  //
  auto MakeScopedLabel(std::vector<std::string> labels) {
    AddCodeBasedLabels(labels);
    return absl::MakeCleanup(absl::bind_front(
        &SQLTestBase::RemoveCodeBasedLabels, this, std::move(labels)));
  }

  auto MakeScopedLabel(absl::string_view label) {
    return MakeScopedLabel(std::vector<std::string>{std::string(label)});
  }

  // Get the set of code-based labels. Make it protected so a subclass can
  // access it.
  absl::btree_set<std::string> GetCodeBasedLabels();

  // Helper function to implement ExecuteTestCase(), either when
  // executing a script, or when ExecuteStatement() isn't overridden.
  absl::StatusOr<ComplianceTestCaseResult> ExecuteTestCaseImpl(
      const std::string& sql, const std::map<std::string, Value>& parameters);

  // Internal state that controls the file-level workflow.
  enum FileWorkflow { CREATE_DATABASE, FIRST_STATEMENT, REST_STATEMENTS };
  FileWorkflow file_workflow() { return file_workflow_; }
  void set_file_workflow(FileWorkflow workflow) { file_workflow_ = workflow; }
  const TestDatabase& test_db() { return test_db_; }

  const std::unique_ptr<file_based_test_driver::TestCaseOptions>& options() {
    return options_;
  }

  // TODO: Move the following to private once all subclasses migrate to
  // the method version.
  FileWorkflow file_workflow_;
  std::unique_ptr<file_based_test_driver::TestCaseOptions> options_;

  // Initializes file level internal state. Can be overridden by a subclass to
  // initialize an extended state space.
  virtual void InitFileState();

  // Internal state that controls the statement-level workflow.
  enum StatementWorkflow { NORMAL, CANCELLED, KNOWN_ERROR, SKIPPED };
  StatementWorkflow statement_workflow() { return statement_workflow_; }
  const std::string& sql() { return sql_; }  // The SQL string.
  const std::string& full_name() {
    return full_name_;
  }  // <filename>:<statement_name>.
  const std::string& location() {
    return location_;
  }  // 'FILE-*-LINE-*-NAME-*'.
  const std::map<std::string, Value>&
  parameters() {  // Parameters to SQL statement.
    return parameters_;
  }
  const file_based_test_driver::RunTestCaseResult* test_result() {
    return test_result_;
  }
  file_based_test_driver::RunTestCaseResult* mutable_test_result() {
    return test_result_;
  }

  // A subclass can read but not update the known_error_mode.
  KnownErrorMode known_error_mode() const { return known_error_mode_; }

  // Initializes statement level internal state. Can be overridden by a subclass
  // to initialize an extended state space.
  virtual void InitStatementState(
      absl::string_view sql,
      file_based_test_driver::RunTestCaseResult* test_result);

  // Step functions.
  //
  // Step functions operate on file- and statement-level internal states and
  // take no input arguments. State variables file_workflow_ and
  // statement_workflow_ control behaviors of step functions. Step functions
  // advance file_workflow_ and statement_workflow_. Step functions are
  // responsible to record stats and log errors.
  //
  // File-level workflow step functions.
  // Prepare default timezone, protos and enums.
  virtual void StepPrepareTimeZoneProtosEnums();

  // Prepare a test database for a *.test file.
  // Per-File Schema and Data
  //
  // Tables can be created in a *.test file by one of the two test case options:
  //
  //   [prepare_database]
  //   CREATE TABLE <table_name> AS SELECT ...
  //
  // The "SELECT ..." or the "CREATE TABLE ..." is not considered a test case.
  // The statement will be executed by the reference implementation engine, and
  // the result is used to create a table under name "table_name". A
  // [prepare_database] and its CREATE TABLE ... statement define a section, and
  // must be separated from others by "==". Multiple tables can be created by a
  // group of [prepare_database] sections, which must precede any tests in the
  // file.
  //
  // Use "UNION ALL" construct to create multiple rows. Define value types and
  // column names in the first row and subsequent rows will inherit the types
  // and column names. For example:
  //
  //   SELECT int32_t(1) as Column_1, int64_t(2) as Column_2 UNION ALL
  //     SELECT 4, 5 UNION ALL
  //     SELECT 7, 8
  //
  // [prepare_database] sections are not considered test cases. They do not
  // require names and any names, labels, global labels, or descriptions will be
  // ignored.
  virtual void StepPrepareDatabase();

  // Create the prepared test database for a *.test file.
  virtual void StepCreateDatabase();
  // Statement-level workflow step functions.
  virtual void StepParsePrimaryKeyMode();
  // Skips a test if the test requires some features that are not supported.
  virtual void StepSkipUnsupportedTest();
  // Parse parameters for a statement.
  virtual void StepParseParameters();
  // Check known errors for a statement.
  virtual void StepCheckKnownErrors();
  // Skips an empty test by setting "statement_workflow_" to SKIPPED.
  virtual void SkipEmptyTest();
  // Execute a statement and check its result.
  virtual void StepExecuteStatementCheckResult();

  // Checks whether to cancel the current statement. Updates statement workflow
  // accordingly. Make it protected so a subclass can access it. A statement can
  // be cancelled for multiple reasons. To name a few: the statement has no
  // name; cannot create tables for the statement; or cannot load protos for the
  // statement. A cancelled statement fails the test if it is not a known error.
  // A cancelled statement with an associated known-error entry should be in
  // mode ALLOW_ERROR. If it's known error mode was less severe mode, the
  // statement still fails the test.
  //
  // CheckCancellation() will do proper error handling and stats logging.
  // Always follow the convention that a step method calls a helper method to
  // do something that may fail. The helper method returns a status and the
  // step method checks the status to decide whether to cancel the current
  // statement. Sample usage:
  //
  // absl::Status DoSomethingHelper(...) {...}
  //
  // void StepDoSomething() {
  //   CheckCancellation(DoSomethingHelper(), "Short description of reason");
  //   if (CANCELLED == statement_workflow_) return;
  //
  //   CheckCancellation(DoSomethingElseHelper(), "Reason of something else");
  //   if (CANCELLED == statement_workflow_) return;
  //
  //   ...
  // }
  void CheckCancellation(const absl::Status& status, const std::string& reason);

  // Enables negative testing so a cancelled statement won't really fail the
  // unittest.
  void EnableNegativeTesting() {
    fail_unittest_on_cancelled_statement_ = false;
  }

  // Name for code-based statements
  //
  // A name will be generated for each code-based statement in the format:
  //
  //   code:<name_prefix>[<result_type_name>]_<typed_parameters>
  //
  // Name prefix is set by the most recent SetNamePrefix(...). Optionally a
  // result type name can be attached to the name prefix. Use SetNamePrefix(...,
  // true) to turn on the option and use SetResultTypeName(...) to set the
  // result type name. In this way, a name prefix can have multiple result type
  // names that are set at later times.
  //
  // Typed parameters are a list of typed values in the format:
  //
  //   <type_name>_<value> for primitive types, or
  //   <type_name>_<signature_of_value> for composite types.
  //
  // Name is unique thus can be used in known-error entries for code-based
  // statements.

  //
  // Make it protected so a TEST_F(...) defined in a subclass can access it.
  // If need_result_type_name is true, the return type must be set with
  // SetResultTypeName and will be appended to the test name.
  void SetNamePrefix(
      absl::string_view name_prefix, bool need_result_type_name = false,
      zetasql_base::SourceLocation loc = zetasql_base::SourceLocation::current());

  // Sets result type name. Make it protected so a TEST_F(...) defined in a
  // subclass can access it.
  void SetResultTypeName(const std::string& result_type_name);

  // Generates a name for a code-based statement and store the name in
  // full_name_.
  virtual void GenerateCodeBasedStatementName(
      const std::string& sql, const std::map<std::string, Value>& parameters);

  // Make it protected so a subclass can access it, down cast it to an
  // engine-specific test driver, and invoke engine-specific features.
  //
  // Does not return NULL.
  TestDriver* driver() const { return test_driver_; }

  // Returns NULL if we are testing the reference implementation.
  ReferenceDriver* reference_driver() const { return reference_driver_; }

  // Generates failure report. A subclass can override to add more information.
  virtual std::string GenerateFailureReport(const std::string& expected,
                                            const std::string& actual,
                                            const std::string& extra) const;

  // Returns OK if the first column is the primary key column for all TestTables
  // in a TestDatabase (which is needed to support engines that require every
  // table to have a primary key). Specifically, the first column must meet the
  // following criteria:
  //   1. SupportsGrouping() is true for the type of the column
  //   2. Does not have NULL Values
  //   3. Does not have duplicated Values
  static absl::Status ValidateFirstColumnPrimaryKey(
      const TestDatabase& test_db, const LanguageOptions& language_options);

 protected:
  // File-based Parameters
  //
  // Parameters can be specified within *.test files by the following test
  // case options:
  //
  //   [parameters=<typed_value> as <param_name>, ...]
  //   SELECT @<param_name> ...
  //
  // A typed value is specified by a SQL expression, such as int32_t(2). The
  // parameters can be used by the statement in the same section.

  // Parse a parameters string, return a map of <param_name>:<typed_value>.
  absl::Status ParseParameters(const std::string& param_str,
                               std::map<std::string, Value>* parameters);

  // Parse a comma-separated list of LanguageFeatures.
  // The strings should be LanguageFeature enum names without the FEATURE_
  // prefix.
  static absl::Status ParseFeatures(const std::string& features_str,
                                    std::set<LanguageFeature>* features);

  // Accessor for the type factory used for populating test tables.
  TypeFactory* table_type_factory() {
    return test_setup_driver_->type_factory();
  }

  // Accessor for the type factory used for statement execution.
  TypeFactory* execute_statement_type_factory() const {
    return execute_statement_type_factory_.get();
  }

  // Resets the execute statement type factory to free types created during
  // statement execution.
  void ResetExecuteStatementTypeFactory() {
    execute_statement_type_factory_ = absl::make_unique<TypeFactory>();
  }

  ReferenceDriver::ExecuteStatementOptions GetExecuteStatementOptions() const {
    ReferenceDriver::ExecuteStatementOptions options;
    options.primary_key_mode = primary_key_mode_;
    return options;
  }

  // Accessor for language options of the driver.
  LanguageOptions driver_language_options() {
    return driver()->GetSupportedLanguageOptions();
  }

  // Returns true if 'driver()' supports 'feature'. ZETASQL_CHECK fails if 'driver()' is
  // the reference implementation and 'feature' is not enabled.
  bool DriverSupportsFeature(zetasql::LanguageFeature feature) {
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

  void set_script_mode(bool script_mode) { script_mode_ = script_mode; }

  // TODO: Move the following to private once all subclasses migrate to
  // the method version.
  StatementWorkflow statement_workflow_;
  std::string sql_;        // The SQL string
  std::string full_name_;  // <filename>:<statement_name>
  std::string
      location_;  // FILE-<filename>-LINE-<line_num>-NAME-<statement_name>

 private:
  // Accesses ValidateFirstColumnPrimaryKey
  friend class CodebasedTestsEnvironment;

  // A special ReferenceDriver that is only used to set up the tests. For
  // example, it is used to construct the tables specified by [prepare_database]
  // sections. It must outlive all of the other drivers/TypeFactories, which may
  // depend on this ReferenceDriver's TypeFactory.
  std::unique_ptr<ReferenceDriver> test_setup_driver_;

  // If true, queries are executed as scripts, rather than standalone
  // statements.
  bool script_mode_ = false;

  // NULL if this object does not own 'test_driver_'.
  std::unique_ptr<TestDriver> test_driver_owner_;

  // The TestDriver being tested, set in the constructor. If it is NULL, then
  // the only method call supported by this object is the destructor.
  TestDriver* test_driver_;

  // NULL if this object does not own 'reference_driver_'. If it is non-NULL, it
  // may have different options than 'test_setup_driver_'.
  std::unique_ptr<ReferenceDriver> reference_driver_owner_;

  // ReferenceDriver for comparison against 'test_driver_', set in the
  // constructor. NULL if we are testing the reference implementation, in which
  // cast 'test_driver_' is a ReferenceDriver.
  ReferenceDriver* reference_driver_;

  // TypeFactory that is used to execute statements.
  std::unique_ptr<TypeFactory> execute_statement_type_factory_;

  // Whether a cancelled statement fails the unittest. For negative testing
  // only.
  bool fail_unittest_on_cancelled_statement_ = true;

  std::map<std::string, Value>
      parameters_;  // The parameters to the SQL statement
  file_based_test_driver::RunTestCaseResult* test_result_ = nullptr;

  // Labels
  //
  // Each statement has a mandatory "name", an optional "labels" set, and an
  // optional "description". A *.test file has an optional "global_labels" set,
  // which has to be defined only once in the first non-create-table section.
  // The "global_labels" apply to all the statements in the file.
  //
  // Name and labels are used in known error entries. Name is referred by
  // <filename>:<name> in known error files.

  // Used to ensure uniques name within a *.test file.
  std::set<std::string> names_;

  // Used to ensure global labels are not redefined.
  std::string global_labels_;

  // Extract name, labels, description, and global_labels from options_.
  absl::Status ExtractLabels(std::string* name,
                             std::vector<std::string>* labels,
                             std::string* description,
                             std::vector<std::string>* global_labels);

  // Split a comma-delimited string into a vector of labels.
  void SplitLabels(const std::string& labels_all,
                   std::vector<std::string>* labels) const;

  // Return a set of effective labels, include <filename>:<statement_name>,
  // labels, and global_labels.
  absl::btree_set<std::string> EffectiveLabels(
      const std::string& filename, const std::string& name,
      const std::vector<std::string>& labels,
      const std::vector<std::string>& global_labels) const;

  // Strip whilespace for all strings in a vector.
  void StripWhitespaceVector(std::vector<std::string>* list) const;

  // Log a set of strings as a single line with a prefix.
  template <typename ContainerType>
  void LogStrings(const ContainerType& strings,
                  const std::string& prefix) const;

  // Current set of effective labels.
  absl::btree_set<std::string> effective_labels_;

  // PrimaryKeyMode for the current statement.
  PrimaryKeyMode primary_key_mode_ = PrimaryKeyMode::DEFAULT;

  // Known Errors
  //
  // Contains known errors labels and statements. Statements are referred by
  // <filename>:<statement_name>.
  // The entries are split into non-regex labels and regexes for faster
  // matching.
  absl::node_hash_set<std::string> known_error_labels_;
  absl::node_hash_set<std::string> known_error_regex_strings_;
  std::vector<std::unique_ptr<RE2>> known_error_regexes_;

  // Maps a label to its known error mode and the set of reasons as defined in
  // corresponding files.
  struct LabelInfo {
    KnownErrorMode mode;
    std::set<std::string> reason;
  };
  std::map<std::string, LabelInfo> label_info_map_;

  // Known Error mode for the current statement.
  KnownErrorMode known_error_mode_;

  // Log label_to_reason_map_ map in a single line.
  void LogReason() const;

  // Log label_known_error_mode_map_ map in a single line.
  void LogMode() const;

  // Will not clear known_error_ and label_to_reason_map_ before loading. This
  // is to support multiple known error files per engine.
  absl::Status LoadKnownErrorFile(absl::string_view filename);

  // Check if any labels in effective_labels is a known error. Returns the
  // maximum mode, where 0 means not a known error and non-zero means
  // it is a known error. Returns the subset in by_set.
  KnownErrorMode IsKnownError(
      const absl::btree_set<std::string>& effective_labels,
      absl::btree_set<std::string>* by_set) const;

  // Wraps Stats::RecordKnownErrorStatement(). Ignores test result of a
  // known error statement. This is required so the golden tool will not pick up
  // a possibly wrong test result generated by a known error statement.
  void RecordKnownErrorStatement(KnownErrorMode mode);

  // Similar to RecordKnownErrorStatement(). A no-op if `check_only is true.
  void RecordKnownErrorStatement(KnownErrorMode mode, bool check_only) {
    if (!check_only) {
      RecordKnownErrorStatement(mode);
    }
  }

  // Similar to RecordKnownErrorStatement(). Uses 'known_error_mode_' by
  // default.
  void RecordKnownErrorStatement() {
    RecordKnownErrorStatement(known_error_mode_);
  }

  // Set of labels in known_error files that affect current statement.
  absl::btree_set<std::string> by_set_;

  // Validate a statement result to make sure the status is OK, the value is not
  // null, and is valid.
  absl::Status ValidateStatementResult(const absl::StatusOr<Value>& result,
                                       const std::string& statement) const;

  // Processes test case options [load_proto_files], [load_proto_names], and
  // [load_enum_names], which load protos and enums in a *.test file. Each
  // takes a list of comma-delimited proto files, proto names, or enum names.
  // A group of these test case options need to collocate with either a
  // [prepare_database] or the first statement.
  //
  // The method also prepares protos and enums in the reference driver. This
  // is needed since a [prepare_database] may need to create a proto or enum
  // value, and the framework uses the reference driver to create values.
  absl::Status LoadProtosAndEnums();

  // Splits a list of proto files, proto names, or enum names into a
  // set<string>. The output 'items' contains no string that is already in
  // 'existing'. This is necessary as ReferenceDriver adds proto and enum types
  // incrementally and adding a type twice will cause a ZETASQL_CHECK() failure.
  void SplitProtosOrEnums(const std::string& item_string,
                          const std::set<std::string>& existing,
                          std::set<std::string>* items) const;

  // Processes test case option [default_time_zone]. Also sets default time
  // zone in the reference driver. This is needed since time zone may affect
  // results of statements that generate parameters or table contents.
  absl::Status SetDefaultTimeZone();

  // Create the prepared database. This includes the protos and enums, as well
  // as all the tables.
  virtual absl::Status CreateDatabase();

  // The container for proto files, proto names, enum names, and tables that
  // are used to create a test database.
  TestDatabase test_db_;

  // Log a map of <param_name>:<typed_value> as a single line.
  void LogParameters(const std::map<std::string, Value>& parameters) const;

  // Code-based label set. Use a vector since labels might be added multiple
  // times.
  std::vector<std::string> code_based_labels_;

  // Add and remove labels to the code-based label set. Duplicated labels are
  // allowed. Labels will be added in the specified order, and be removed in
  // the reverse order. When removing labels, validates the to-be removed
  // labels are the same with those added.
  void AddCodeBasedLabels(std::vector<std::string> labels);
  void RemoveCodeBasedLabels(std::vector<std::string> labels);

  // Variables and functions for code-based names.
  std::string name_prefix_;
  bool name_prefix_need_result_type_name_ = false;
  std::string result_type_name_;

  // Turns a string into an RE2 safe string. Used to generate names for
  // code-based statements. Names must be RE2 safe as known error list
  // processing uses RE2 regex to match names.
  std::string SafeString(absl::string_view str) const;

  // Generates a signature of a bytes or string. A signature is the left 8
  // characters of the string, followed by the fingerprint of the string,
  // followed by the right 8 characters of the string.
  std::string SignatureOfString(absl::string_view str) const;

  // Generates a signature of a array, proto, or struct value to be that of
  // its debug string.
  std::string SignatureOfCompositeValue(const Value& value) const;

  // Converts a value to a safe string.
  std::string ValueToSafeString(const Value& value) const;

  // Returns the name prefix. Append result type name to it when necessary.
  // The prefix is in the format: code:<name_prefix>[<result_type_name>].
  std::string GetNamePrefix() const;

  // KnownErrorFilter needs to use stats_ to record failed statements. It also
  // needs to read sql_, location_, full_name_, and known_error_mode() for
  // statements.
  friend class KnownErrorFilter;

  // SQLTestEnvironment needs to access stats_ for recording global stats.
  friend class SQLTestEnvironment;

  // An error matcher to match legal runtime errors.
  static std::unique_ptr<MatcherCollection<absl::Status>>
      legal_runtime_errors_;
};

}  // namespace zetasql

// Be explicit so it won't break the build in "-c opt" mode.
namespace testing {
namespace internal {

// Teaches googletest to print StatusOr<Value>.
template <>
class UniversalPrinter<absl::StatusOr<::zetasql::Value>> {
 public:
  static void Print(const absl::StatusOr<::zetasql::Value>& status_or,
                    ::std::ostream* os) {
    *os << ::zetasql::SQLTestBase::ToString(status_or);
  }
};

}  // namespace internal
}  // namespace testing

#endif  // ZETASQL_COMPLIANCE_SQL_TEST_BASE_H_
