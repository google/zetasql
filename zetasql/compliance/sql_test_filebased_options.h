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

// The classes in this file handle interaction with
// file_based_test_driver::TestCaseOptions APIs on behalf of SQLTestBase.
// SQLTestBase gets to use a strongly typed API and delegates managing of
// option key registration, parsing, and validating to this component.

#ifndef ZETASQL_COMPLIANCE_SQL_TEST_FILEBASED_OPTIONS_H_
#define ZETASQL_COMPLIANCE_SQL_TEST_FILEBASED_OPTIONS_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/compliance/known_error.pb.h"
#include "zetasql/compliance/test_driver.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/reference_driver.h"
#include "gtest/gtest.h"
#include "absl/container/btree_map.h"
#include "absl/container/btree_set.h"
#include "absl/container/node_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "file_based_test_driver/test_case_options.h"
#include "re2/re2.h"
#include "zetasql/base/status.h"

namespace zetasql {

class FilebasedSQLTestFileOptions;

// Owns the options context for a single test case at a time.
class FilebasedSQLTestCaseOptions {
 public:
  FilebasedSQLTestCaseOptions(const FilebasedSQLTestCaseOptions&) = delete;
  FilebasedSQLTestCaseOptions& operator=(const FilebasedSQLTestCaseOptions&) =
      delete;

  FilebasedSQLTestCaseOptions(FilebasedSQLTestCaseOptions&&) = default;
  FilebasedSQLTestCaseOptions& operator=(FilebasedSQLTestCaseOptions&&) =
      default;

  // The SQL statement or script text with [options=...] and whitespace
  // stripped away.
  absl::string_view sql() const { return sql_; }

  // The name of the test. This is the file local name without the filename:
  // prefix.
  absl::string_view name() const { return name_; }

  // Query parameters used for the evaluation of this test case.
  const std::map<std::string, Value>& params() const { return params_; }

  // Features that must be enabled for the test to produce any of the expected
  // results.
  const std::set<LanguageFeature>& required_features() const {
    return required_features_;
  }
  // Features that must be disabled for the test to produce and of the expected
  // results.
  const std::set<LanguageFeature>& forbidden_features() {
    return forbidden_features_;
  }
  // A set of features that indicates one of several possible expected results.
  const std::set<LanguageFeature>& test_features1() const {
    return test_features1_;
  }
  // A set of features that indicates one of several possible expected results.
  // Should be different that test_features1().
  const std::set<LanguageFeature>& test_features2() const {
    return test_features2_;
  }

  // [labels=...] specified specificallyh on this test case.
  const std::vector<std::string>& local_labels() const { return local_labels_; }

  // Filenames for proto types added for this test case.
  const std::set<std::string>& proto_file_names() const {
    return new_proto_file_names_;
  }
  // Proto message types added for this test case.
  const std::set<std::string>& proto_message_names() const {
    return new_proto_message_names_;
  }
  // Proto enum types added for this test case.
  const std::set<std::string>& proto_enum_names() const {
    return new_proto_enum_names_;
  }

  PrimaryKeyMode primary_key_mode() const { return primary_key_mode_; }

  // True for tet file segments marked [prepare_database]. These segments aren't
  // actually tests. Instead they set up schema objects such as tables or
  // functions.
  bool prepare_database() const { return prepare_database_; }

  // If true, the golden file will contain the computed labels in the printed
  // result.
  bool extract_labels() const { return extract_labels_; }

 private:
  // Encapsulation wise, these two classes are designed to work as one.
  friend class FilebasedSQLTestFileOptions;

  std::string sql_;
  std::string name_;
  std::map<std::string, Value> params_;
  std::set<LanguageFeature> required_features_;
  std::set<LanguageFeature> forbidden_features_;
  std::set<LanguageFeature> test_features1_;
  std::set<LanguageFeature> test_features2_;
  std::vector<std::string> local_labels_;
  std::set<std::string> new_proto_file_names_;
  std::set<std::string> new_proto_message_names_;
  std::set<std::string> new_proto_enum_names_;
  PrimaryKeyMode primary_key_mode_ = PrimaryKeyMode::DEFAULT;
  bool extract_labels_ = false;
  bool prepare_database_ = false;

  FilebasedSQLTestCaseOptions();
};

// Owns the options context for a single test file.
class FilebasedSQLTestFileOptions {
 public:
  FilebasedSQLTestFileOptions(const FilebasedSQLTestFileOptions&) = delete;
  FilebasedSQLTestFileOptions& operator=(const FilebasedSQLTestFileOptions&) =
      delete;

  FilebasedSQLTestFileOptions(FilebasedSQLTestFileOptions&&) = default;
  FilebasedSQLTestFileOptions& operator=(FilebasedSQLTestFileOptions&&) =
      default;

  // Create a new test file options handler. The 'reference_driver' is used
  // to evaluate SQL expressions to generate parameter values. This should be
  // the same reference driver used to evaluate schema objects. There are a tiny
  // number of compliance tests for which this matters, as some engines
  // depend on no-op cast removal that can be broken if parameter types and
  // table column types are merely equivalent rather than equal. Using the same
  // test driver ensures they are equal.
  explicit FilebasedSQLTestFileOptions(ReferenceDriver* reference_driver);

  // Called once per test case. This must be called in order from the first
  // test case in the file to the last test case. Some options are only allowed
  // on the first case in a file. Others, such as [default options=...] or
  // [proto_type_names=...] apply to subsequent cases but not previous ones.
  absl::StatusOr<std::unique_ptr<FilebasedSQLTestCaseOptions>> ProcessTestCase(
      absl::string_view test_case, std::string* failure_reason);

  // Labels that apply to all test cases in the file that are set once per file.
  const std::vector<std::string>& global_labels() const {
    return global_labels_;
  }

  // A default timezone that is set once per file.
  absl::string_view default_timezone() const { return default_timezone_; }

 private:
  // Encapsulation wise, these two classes are designed to work as one.
  friend class FilebasedSQLTestCaseOptions;

  // For accessing 'options_'. This breaks encapulation, but provides backward
  // compatibility for Spanner's test driver.
  // TODO: Migrate Spanner to FilebasedSQLTestFileOptions and remove.
  friend class SQLTestBase;

  // A reference driver is used to compute parameter values.
  ReferenceDriver* reference_driver_;

  std::unique_ptr<file_based_test_driver::TestCaseOptions> options_;

  // Used to ensure uniques name within a *.test file.
  absl::flat_hash_set<std::string> names_;
  int64_t statement_count_ = 0;

  std::string global_labels_string_;
  std::vector<std::string> global_labels_;
  std::string default_timezone_ = "";

  // Keep track of all proto types added so far within the file.
  absl::flat_hash_set<std::string> all_proto_file_names_;
  absl::flat_hash_set<std::string> all_proto_message_names_;
  absl::flat_hash_set<std::string> all_proto_enum_names_;

  absl::Status ExtractName(bool validate_name, std::string* name);

  // Processes test case options [load_proto_files], [load_proto_names], and
  // [load_enum_names], which load protos and enums in a *.test file. Each
  // takes a list of comma-delimited proto files, proto names, or enum names.
  // A group of these test case options need to collocate with either a
  // [prepare_database] or the first statement.
  absl::Status ExtractProtoAndEnumTypes(
      std::set<std::string>& new_proto_file_names,
      std::set<std::string>& new_proto_message_names,
      std::set<std::string>& new_proto_enum_names);

  absl::Status ExtractGlobalLabels();

  absl::Status ExtractDefaultTimezone(std::string* default_time_zone);
};

}  // namespace zetasql

#endif  // ZETASQL_COMPLIANCE_SQL_TEST_FILEBASED_OPTIONS_H_
