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

#include "zetasql/compliance/sql_test_filebased_options.h"

#include <array>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/text_format.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/parameters_test_util.h"
#include "zetasql/compliance/test_driver.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "gtest/gtest.h"
#include "absl/container/btree_map.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/file_util.h"  
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

ABSL_FLAG(bool, auto_generate_test_names, false,
          "When true, test cases in file don't have to have [name] tag, the "
          "names will be automatically generated when name is missing.");

namespace zetasql {

constexpr absl::string_view kDefaultTimeZone = "default_time_zone";
constexpr absl::string_view kGlobalLabels = "global_labels";
constexpr absl::string_view kLabels = "labels";
constexpr absl::string_view kLoadEnumNames = "load_enum_names";
constexpr absl::string_view kLoadProtoFiles = "load_proto_files";
constexpr absl::string_view kLoadProtoNames = "load_proto_names";
constexpr absl::string_view kName = "name";
constexpr absl::string_view kParameters = "parameters";
constexpr absl::string_view kPrepareDatabase = "prepare_database";
constexpr absl::string_view kExtractLabels = "extract_labels";  // boolean flag

// These are comma-separated lists of LanguageFeature enums, without the
// FEATURE_ prefix.  If these are set, when testing against the reference
// implementation, the test will run multiple times, with features in
// test_features1 on or off, and features in test_features2 on or off, and
// all outputs will be shown in the golden file.
// When testing against a non-reference implementation, each test runs only
// once, and the output is compared to the reference implementation's output
// when running with the engines options, provided by
// TestDriver::GetSupportedLanguageOptions.
constexpr absl::string_view kTestFeatures1 = "test_features1";
constexpr absl::string_view kTestFeatures2 = "test_features2";

// A comma-separated list of LanguageFeature enums,
// without the FEATURE_ prefix. If it is set, the test will be run
// against the implementations that support the features.
constexpr absl::string_view kRequiredFeatures = "required_features";

// Same as kRequiredFeatures, but skip tests that have these features
// enabled. These must all be features that are annotated with
// ideally_enabled=false in options.proto.
constexpr absl::string_view kForbiddenFeatures = "forbidden_features";

// The name of a PrimaryKeyMode enum value. See the comment for that enum in
// test_driver.h for details.
constexpr absl::string_view kPrimaryKeyMode = "primary_key_mode";

FilebasedSQLTestCaseOptions::FilebasedSQLTestCaseOptions() {}

static absl::flat_hash_set<std::string> SplitProtosOrEnums(
    absl::string_view item_string) {
  absl::flat_hash_set<std::string> items;
  for (absl::string_view item : absl::StrSplit(item_string, ',')) {
    items.emplace(absl::StripAsciiWhitespace(item));
  }
  return items;
}

static void SplitLabels(absl::string_view labels_all,
                        std::vector<std::string>* labels) {
  *labels = absl::StrSplit(labels_all, ',', absl::SkipEmpty());
  for (std::string& str : *labels) {
    absl::StripAsciiWhitespace(&str);
  }
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

  return absl::InvalidArgumentError(
      absl::StrCat("Invalid primary key mode: ", mode_string));
}

static absl::Status ParseFeatures(absl::string_view features_str,
                                  std::set<LanguageFeature>& features) {
  ZETASQL_RET_CHECK(features.empty());
  for (absl::string_view feature_name :
       absl::StrSplit(features_str, ',', absl::SkipEmpty())) {
    const std::string full_feature_name =
        absl::StrCat("FEATURE_", feature_name);
    LanguageFeature feature;
    if (!LanguageFeature_Parse(full_feature_name, &feature)) {
      return absl::InvalidArgumentError(
          absl::StrCat("Invalid feature name: ", full_feature_name));
    }
    features.insert(feature);
  }
  return absl::OkStatus();
}

absl::Status FilebasedSQLTestFileOptions::ExtractDefaultTimezone(
    std::string* default_time_zone) {
  ZETASQL_RET_CHECK_NE(default_time_zone, nullptr);
  if (options_->IsExplicitlySet(kDefaultTimeZone) && statement_count_ > 1) {
    return absl::InvalidArgumentError(
        "A [default_time_zone] must locate at the first "
        "section of a *.test file.");
  }
  *default_time_zone = options_->GetString(kDefaultTimeZone);
  return absl::OkStatus();
}

absl::Status FilebasedSQLTestFileOptions::ExtractGlobalLabels() {
  if (options_->IsExplicitlySet(kGlobalLabels)) {
    return absl::InvalidArgumentError(
        "ERROR: global_labels must not be explicitly defined");
  }

  if (statement_count_ > 1 &&
      global_labels_string_ != options_->GetString(kGlobalLabels)) {
    // Suppress repetitive error messages.
    global_labels_string_ = options_->GetString(kGlobalLabels);
    return absl::InvalidArgumentError(
        "ERROR: global_labels must not be (re)defined after the first test "
        "section");
  }

  if (statement_count_ == 1) {
    global_labels_string_ = options_->GetString(kGlobalLabels);
    SplitLabels(options_->GetString(kGlobalLabels), &global_labels_);
  }
  return absl::OkStatus();
}

absl::Status FilebasedSQLTestFileOptions::ExtractProtoAndEnumTypes(
    std::set<std::string>& new_proto_file_names,
    std::set<std::string>& new_proto_message_names,
    std::set<std::string>& new_proto_enum_names) {
  if (options_->IsExplicitlySet(kLoadProtoFiles)) {
    for (const auto& file_name :
         SplitProtosOrEnums(options_->GetString(kLoadProtoFiles))) {
      if (all_proto_file_names_.insert(file_name).second) {
        new_proto_file_names.insert(file_name);
      }
    }
  }
  if (options_->IsExplicitlySet(kLoadProtoNames)) {
    for (const auto& message_name :
         SplitProtosOrEnums(options_->GetString(kLoadProtoNames))) {
      if (all_proto_message_names_.insert(message_name).second) {
        new_proto_message_names.insert(message_name);
      }
    }
  }
  if (options_->IsExplicitlySet(kLoadEnumNames)) {
    for (const auto& enum_name :
         SplitProtosOrEnums(options_->GetString(kLoadEnumNames))) {
      if (all_proto_enum_names_.insert(enum_name).second) {
        new_proto_enum_names.insert(enum_name);
      }
    }
  }

  // Only load the type that are new for this statement.
  return reference_driver_->LoadProtoEnumTypes(
      new_proto_file_names, new_proto_message_names, new_proto_enum_names);
}

absl::Status FilebasedSQLTestFileOptions::ExtractName(bool validate_name,
                                                      std::string* name) {
  ZETASQL_RET_CHECK_NE(name, nullptr);
  ZETASQL_RET_CHECK(name->empty());
  if (!options_->IsExplicitlySet(kName) &&
      absl::GetFlag(FLAGS_auto_generate_test_names)) {
    options_->SetString(kName, absl::StrCat("_test", statement_count_ - 1));
  }
  *name = options_->GetString(kName);
  if (!validate_name) {
    return absl::OkStatus();
  }
  if (name->empty()) {
    return absl::InvalidArgumentError(
        "ERROR: A non-empty name is required for each statement");
  }
  if (!names_.insert(*name).second) {
    return absl::InvalidArgumentError("ERROR: Duplicated name in *.test file");
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<FilebasedSQLTestCaseOptions>>
FilebasedSQLTestFileOptions::ProcessTestCase(absl::string_view test_case,
                                             std::string* failure_reason) {
  statement_count_++;
  // This function helps make the ZETASQL_RETURN_IF_ERROR use below more readable. It
  // just sets 'failure_reason' to an appropriate string for each possible
  // ZETASQL_RETURN_IF_ERROR return path.
  auto reason = [failure_reason](absl::string_view reason) {
    return [failure_reason, reason](zetasql_base::StatusBuilder builder) {
      absl::StrAppend(failure_reason, reason);
      return builder;
    };
  };
  std::string test_case_string(test_case);
  ZETASQL_RETURN_IF_ERROR(options_->ParseTestCaseOptions(&test_case_string))
      .With(reason("Failed to parse options"));

  auto case_opts = absl::WrapUnique(new FilebasedSQLTestCaseOptions());
  case_opts->sql_ = absl::StripAsciiWhitespace(test_case_string);

  case_opts->prepare_database_ = options_->GetBool(kPrepareDatabase);
  case_opts->extract_labels_ = options_->GetBool(kExtractLabels);

  // Sometimes the first "...\n==" block in a test file is just setting up
  // option defaults. This is fine, but we want to skip name validation.
  bool defaults_only = statement_count_ == 1 && !case_opts->prepare_database_ &&
                       case_opts->sql_.empty();
  bool validate_name = !(defaults_only || case_opts->prepare_database_);
  ZETASQL_RETURN_IF_ERROR(ExtractName(validate_name, &case_opts->name_))
      .With(reason("Failed to extract name"));

  ZETASQL_RETURN_IF_ERROR(ExtractProtoAndEnumTypes(case_opts->new_proto_file_names_,
                                           case_opts->new_proto_message_names_,
                                           case_opts->new_proto_enum_names_))
      .With(reason("Failed to load protos or enums"));

  if (!options_->GetString(kParameters).empty()) {
    ZETASQL_RETURN_IF_ERROR(ParseTestFileParameters(
                        options_->GetString(kParameters), reference_driver_,
                        reference_driver_->type_factory(), &case_opts->params_))
        .With(reason("Failed to generate parameters"));
  }

  ZETASQL_RETURN_IF_ERROR(ParseFeatures(options_->GetString(kRequiredFeatures),
                                case_opts->required_features_))
      .With(reason("Failed to parse required_features"));
  ZETASQL_RETURN_IF_ERROR(ParseFeatures(options_->GetString(kForbiddenFeatures),
                                case_opts->forbidden_features_))
      .With(reason("Failed to parse forbidden_features"));
  ZETASQL_RETURN_IF_ERROR(ParseFeatures(options_->GetString(kTestFeatures1),
                                case_opts->test_features1_))
      .With(reason("Failed to parse test_features1"));
  ZETASQL_RETURN_IF_ERROR(ParseFeatures(options_->GetString(kTestFeatures2),
                                case_opts->test_features2_))
      .With(reason("Failed to parse test_features2"));

  ZETASQL_RETURN_IF_ERROR(ParsePrimaryKeyMode(options_->GetString(kPrimaryKeyMode),
                                      &case_opts->primary_key_mode_))
      .With(reason("Failed to parse primary key mode"));

  ZETASQL_RETURN_IF_ERROR(ExtractGlobalLabels())
      .With(reason("Failed to extract labels"));
  SplitLabels(options_->GetString(kLabels), &case_opts->local_labels_);

  ZETASQL_RETURN_IF_ERROR(ExtractDefaultTimezone(&default_timezone_))
      .With(reason("Wrong placement of default time zone"));
  return std::move(case_opts);
}

FilebasedSQLTestFileOptions::FilebasedSQLTestFileOptions(
    ReferenceDriver* reference_driver)
    : reference_driver_(reference_driver) {
  options_ = std::make_unique<file_based_test_driver::TestCaseOptions>();
  options_->RegisterString(kName, "");
  options_->RegisterString(kLabels, "");
  options_->RegisterString(kGlobalLabels, "");
  options_->RegisterString(kParameters, "");
  options_->RegisterString(kLoadProtoFiles, "");
  options_->RegisterString(kLoadProtoNames, "");
  options_->RegisterString(kLoadEnumNames, "");
  options_->RegisterBool(kPrepareDatabase, false);
  options_->RegisterBool(kExtractLabels, false);
  options_->RegisterString(kTestFeatures1, "");
  options_->RegisterString(kTestFeatures2, "");
  options_->RegisterString(kRequiredFeatures, "");
  options_->RegisterString(kForbiddenFeatures, "");
  options_->RegisterString(kDefaultTimeZone, "");
  options_->RegisterString(kPrimaryKeyMode,
                           PrimaryKeyModeName(PrimaryKeyMode::DEFAULT));
}

}  // namespace zetasql
