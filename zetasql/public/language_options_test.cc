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

#include "zetasql/public/language_options.h"

#include <string>

#include "google/protobuf/descriptor.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"

namespace zetasql {

using ::testing::IsEmpty;
using ::zetasql_base::testing::StatusIs;

TEST(LanguageOptions, TestAllAcceptingStatementKind) {
  LanguageOptions options;
  options.SetSupportedStatementKinds({});
  EXPECT_TRUE(options.SupportsStatementKind(RESOLVED_QUERY_STMT));
  EXPECT_TRUE(options.SupportsStatementKind(RESOLVED_EXPLAIN_STMT));
}

TEST(LanguageOptions, TestSeSupportsAllStatementKinds) {
  LanguageOptions options;
  options.SetSupportsAllStatementKinds();
  EXPECT_TRUE(options.SupportsStatementKind(RESOLVED_QUERY_STMT));
  EXPECT_TRUE(options.SupportsStatementKind(RESOLVED_EXPLAIN_STMT));
}

TEST(LanguageOptions, TestStatementKindRestriction) {
  LanguageOptions options;
  options.SetSupportedStatementKinds({RESOLVED_QUERY_STMT});
  EXPECT_TRUE(options.SupportsStatementKind(RESOLVED_QUERY_STMT));
  EXPECT_FALSE(options.SupportsStatementKind(RESOLVED_EXPLAIN_STMT));
}

TEST(LanguageOptions, TestStatementKindRestrictionWithDefault) {
  LanguageOptions options;  // Default is RESOLVED_QUERY_STMT.
  EXPECT_TRUE(options.SupportsStatementKind(RESOLVED_QUERY_STMT));
  EXPECT_FALSE(options.SupportsStatementKind(RESOLVED_EXPLAIN_STMT));
}

// Get the set of possible enum values for a proto enum type.
// ENUM is the c++ enum type, and <descriptor> is its EnumDescriptor.
template <class ENUM>
static std::set<ENUM> GetEnumValues(const google::protobuf::EnumDescriptor* descriptor) {
  std::set<ENUM> values;
  for (int i = 0; i < descriptor->value_count(); ++i) {
    const ENUM value = static_cast<ENUM>(descriptor->value(i)->number());
    values.insert(value);
  }
  return values;
}

TEST(LanguageOptions, GetLanguageFeaturesForVersion) {
  EXPECT_THAT(LanguageOptions::GetLanguageFeaturesForVersion(VERSION_1_0),
              IsEmpty());

  EXPECT_TRUE(LanguageOptions::GetLanguageFeaturesForVersion(VERSION_CURRENT)
                  .contains(FEATURE_V_1_1_ORDER_BY_COLLATE));
  EXPECT_FALSE(LanguageOptions::GetLanguageFeaturesForVersion(VERSION_CURRENT)
                   .contains(FEATURE_TABLESAMPLE));

  LanguageOptions::LanguageFeatureSet features_in_current =
      LanguageOptions::GetLanguageFeaturesForVersion(VERSION_CURRENT);

  // Now do some sanity checks on LanguageVersions vs LanguageFeatures.
  // This is done using the enum names, assuming the existing conventions are
  // followed.  We check that VERSION_x_y will include all features
  // FEATURE_V_a_b where (a,b) <= (x,y).
  for (const LanguageVersion version :
       GetEnumValues<LanguageVersion>(LanguageVersion_descriptor())) {
    if (version == VERSION_CURRENT) continue;
    if (version == __LanguageVersion__switch_must_have_a_default__) continue;

    const std::string version_name = LanguageVersion_Name(version);
    EXPECT_EQ("VERSION_", version_name.substr(0, 8));
    const std::string version_suffix =
        absl::StrCat(version_name.substr(8), "_");

    LanguageOptions::LanguageFeatureSet computed_features;
    for (const LanguageFeature feature :
         GetEnumValues<LanguageFeature>(LanguageFeature_descriptor())) {
      const std::string feature_name = LanguageFeature_Name(feature);
      if (feature_name.substr(0, 10) == "FEATURE_V_") {
        const std::string feature_version_suffix =
            feature_name.substr(10, version_suffix.size());
        if (feature_version_suffix <= version_suffix) {
          computed_features.insert(feature);
        }
      }
    }

    EXPECT_EQ(computed_features,
              LanguageOptions::GetLanguageFeaturesForVersion(version))
        << "for version " << LanguageVersion_Name(version)
        << "; Did you forget to update "
           "LanguageOptions::GetLanguageFeaturesForVersion after "
           "adding a new language feature?";

    // Also check that all features included in version X are also included in
    // VERSION_CURRENT.
    for (const LanguageFeature feature :
         LanguageOptions::GetLanguageFeaturesForVersion(version)) {
      EXPECT_TRUE(features_in_current.contains(feature))
          << "Features for VERSION_CURRENT does not include feature " << feature
          << " from " << version_name;
    }
  }
}

TEST(LanguageOptions, MaximumFeatures) {
  const LanguageOptions options = LanguageOptions::MaximumFeatures();

  // Some features that are ideally enabled and released.
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_GROUP_BY_ROLLUP));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_V_1_1_ORDER_BY_COLLATE));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_V_1_2_GROUP_BY_STRUCT));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_V_1_2_GROUP_BY_ARRAY));
  EXPECT_TRUE(
      options.LanguageFeatureEnabled(FEATURE_V_1_1_SELECT_STAR_EXCEPT_REPLACE));
  EXPECT_TRUE(
      options.LanguageFeatureEnabled(FEATURE_V_1_1_ORDER_BY_IN_AGGREGATE));
  EXPECT_TRUE(
      options.LanguageFeatureEnabled(FEATURE_V_1_1_CAST_DIFFERENT_ARRAY_TYPES));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_V_1_1_ARRAY_EQUALITY));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_V_1_1_LIMIT_IN_AGGREGATE));
  EXPECT_TRUE(
      options.LanguageFeatureEnabled(FEATURE_V_1_1_HAVING_IN_AGGREGATE));
  EXPECT_TRUE(options.LanguageFeatureEnabled(
      FEATURE_V_1_1_NULL_HANDLING_MODIFIER_IN_ANALYTIC));
  EXPECT_TRUE(options.LanguageFeatureEnabled(
      FEATURE_V_1_1_NULL_HANDLING_MODIFIER_IN_AGGREGATE));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_V_1_2_CIVIL_TIME));

  // Some features that are released but not ideally enabled.
  EXPECT_FALSE(options.LanguageFeatureEnabled(FEATURE_DISALLOW_GROUP_BY_FLOAT));

  EXPECT_FALSE(options.LanguageFeatureEnabled(FEATURE_TEST_IDEALLY_DISABLED));

  // A feature that is ideally enabled but under development.
  EXPECT_FALSE(options.LanguageFeatureEnabled(
      FEATURE_TEST_IDEALLY_ENABLED_BUT_IN_DEVELOPMENT));

  // A feature that is not ideally enabled and is under development.
  EXPECT_FALSE(options.LanguageFeatureEnabled(
      FEATURE_TEST_IDEALLY_DISABLED_AND_IN_DEVELOPMENT));

  EXPECT_FALSE(options.LanguageFeatureEnabled(
      __LanguageFeature__switch_must_have_a_default__));
}

TEST(LanguageOptions, EnableMaximumLanguageFeaturesForDevelopment) {
  LanguageOptions options;
  options.EnableMaximumLanguageFeaturesForDevelopment();

  // Some features that are ideally enabled and released.
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_GROUP_BY_ROLLUP));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_V_1_1_ORDER_BY_COLLATE));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_V_1_2_GROUP_BY_STRUCT));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_V_1_2_GROUP_BY_ARRAY));
  EXPECT_TRUE(
      options.LanguageFeatureEnabled(FEATURE_V_1_1_SELECT_STAR_EXCEPT_REPLACE));
  EXPECT_TRUE(
      options.LanguageFeatureEnabled(FEATURE_V_1_1_ORDER_BY_IN_AGGREGATE));
  EXPECT_TRUE(
      options.LanguageFeatureEnabled(FEATURE_V_1_1_CAST_DIFFERENT_ARRAY_TYPES));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_V_1_1_ARRAY_EQUALITY));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_V_1_1_LIMIT_IN_AGGREGATE));
  EXPECT_TRUE(
      options.LanguageFeatureEnabled(FEATURE_V_1_1_HAVING_IN_AGGREGATE));
  EXPECT_TRUE(options.LanguageFeatureEnabled(
      FEATURE_V_1_1_NULL_HANDLING_MODIFIER_IN_ANALYTIC));
  EXPECT_TRUE(options.LanguageFeatureEnabled(
      FEATURE_V_1_1_NULL_HANDLING_MODIFIER_IN_AGGREGATE));
  EXPECT_TRUE(options.LanguageFeatureEnabled(FEATURE_V_1_2_CIVIL_TIME));

  // Some features that are released but not ideally enabled.
  EXPECT_FALSE(options.LanguageFeatureEnabled(FEATURE_DISALLOW_GROUP_BY_FLOAT));

  EXPECT_FALSE(options.LanguageFeatureEnabled(FEATURE_TEST_IDEALLY_DISABLED));

  // A feature that is ideally enabled but under development.
  EXPECT_TRUE(options.LanguageFeatureEnabled(
      FEATURE_TEST_IDEALLY_ENABLED_BUT_IN_DEVELOPMENT));

  // A feature that is not ideally enabled and is under development.
  EXPECT_FALSE(options.LanguageFeatureEnabled(
      FEATURE_TEST_IDEALLY_DISABLED_AND_IN_DEVELOPMENT));

  EXPECT_FALSE(options.LanguageFeatureEnabled(
      __LanguageFeature__switch_must_have_a_default__));
}

TEST(LanguageOptions, Serialization) {
  LanguageOptionsProto proto;
  proto.set_product_mode(PRODUCT_EXTERNAL);
  proto.set_name_resolution_mode(NAME_RESOLUTION_STRICT);
  proto.set_error_on_deprecated_syntax(true);
  proto.add_enabled_language_features(
      FEATURE_V_1_1_SELECT_STAR_EXCEPT_REPLACE);
  proto.add_enabled_language_features(FEATURE_TABLESAMPLE);
  proto.add_supported_statement_kinds(RESOLVED_EXPLAIN_STMT);
  proto.add_supported_generic_entity_types("NEW_TYPE");
  proto.add_reserved_keywords("QUALIFY");

  LanguageOptions options(proto);
  ASSERT_EQ(PRODUCT_EXTERNAL, options.product_mode());
  ASSERT_EQ(NAME_RESOLUTION_STRICT, options.name_resolution_mode());
  ASSERT_TRUE(options.error_on_deprecated_syntax());
  ASSERT_TRUE(options.LanguageFeatureEnabled(
      FEATURE_V_1_1_SELECT_STAR_EXCEPT_REPLACE));
  ASSERT_TRUE(options.LanguageFeatureEnabled(
      FEATURE_TABLESAMPLE));
  ASSERT_FALSE(options.LanguageFeatureEnabled(
      FEATURE_V_1_1_ORDER_BY_COLLATE));
  ASSERT_TRUE(options.SupportsStatementKind(RESOLVED_EXPLAIN_STMT));
  ASSERT_FALSE(options.SupportsStatementKind(RESOLVED_QUERY_STMT));
  ASSERT_TRUE(options.GenericEntityTypeSupported("NEW_TYPE"));
  ASSERT_TRUE(options.GenericEntityTypeSupported("new_type"));
  ASSERT_FALSE(options.GenericEntityTypeSupported("unsupported"));
  ASSERT_TRUE(options.IsReservedKeyword("QUALIFY"));
}

TEST(LanguageOptions, GetEnabledLanguageFeaturesAsString) {
  LanguageOptions options;
  EXPECT_EQ("", options.GetEnabledLanguageFeaturesAsString());
  options.EnableLanguageFeature(FEATURE_TABLESAMPLE);
  EXPECT_EQ("FEATURE_TABLESAMPLE",
            options.GetEnabledLanguageFeaturesAsString());
  options.EnableLanguageFeature(FEATURE_ANALYTIC_FUNCTIONS);
  EXPECT_EQ("FEATURE_ANALYTIC_FUNCTIONS, FEATURE_TABLESAMPLE",
            options.GetEnabledLanguageFeaturesAsString());
  options.EnableLanguageFeature(FEATURE_V_1_2_CIVIL_TIME);
  EXPECT_EQ("FEATURE_ANALYTIC_FUNCTIONS, FEATURE_TABLESAMPLE, "
            "FEATURE_V_1_2_CIVIL_TIME",
            options.GetEnabledLanguageFeaturesAsString());
}

TEST(LanguageOptions, ReservedKeywords) {
  // GetReservableKeywords
  EXPECT_TRUE(LanguageOptions::GetReservableKeywords().contains("QUALIFY"));
  EXPECT_TRUE(LanguageOptions::GetReservableKeywords().contains("qualify"));
  EXPECT_FALSE(LanguageOptions::GetReservableKeywords().contains("SELECT"));
  EXPECT_FALSE(LanguageOptions::GetReservableKeywords().contains("DECIMAL"));
  EXPECT_FALSE(LanguageOptions::GetReservableKeywords().contains(""));

  // Initial LanguageOptions
  LanguageOptions options;
  EXPECT_FALSE(options.IsReservedKeyword("QUALIFY"));
  EXPECT_FALSE(options.IsReservedKeyword("qualify"));
  EXPECT_FALSE(options.IsReservedKeyword(""));
  EXPECT_FALSE(options.IsReservedKeyword("DECIMAL"));
  EXPECT_TRUE(options.IsReservedKeyword("SELECT"));

  // Reserving a keyword (uppercase)
  ZETASQL_EXPECT_OK(options.EnableReservableKeyword("QUALIFY", true));
  EXPECT_TRUE(options.IsReservedKeyword("QUALIFY"));
  EXPECT_TRUE(options.IsReservedKeyword("qualify"));
  EXPECT_TRUE(options.IsReservedKeyword("SELECT"));
  EXPECT_FALSE(options.IsReservedKeyword("DECIMAL"));

  // Reserving a keyword already reserved earlier is ok
  ZETASQL_EXPECT_OK(options.EnableReservableKeyword("QUALIFY", true));
  EXPECT_TRUE(options.IsReservedKeyword("QUALIFY"));

  // Equality test
  EXPECT_TRUE(options == options);
  EXPECT_FALSE(options == LanguageOptions());

  // Unreserving a keyword
  ZETASQL_EXPECT_OK(options.EnableReservableKeyword("QUALIFY", false));
  EXPECT_FALSE(options.IsReservedKeyword("QUALIFY"));

  // Unreserving a keyword already unreserved keyword is ok
  EXPECT_FALSE(options.IsReservedKeyword("QUALIFY"));
  EXPECT_FALSE(options.IsReservedKeyword("qualify"));

  // Reserving all reservable keywords
  options.EnableAllReservableKeywords(true);
  EXPECT_TRUE(options.IsReservedKeyword("QUALIFY"));
  EXPECT_TRUE(options.IsReservedKeyword("SELECT"));
  EXPECT_FALSE(options.IsReservedKeyword("DECIMAL"));

  // Unreserving all reservable keywords
  options.EnableAllReservableKeywords(false);
  EXPECT_FALSE(options.IsReservedKeyword("QUALIFY"));
  EXPECT_TRUE(options.IsReservedKeyword("SELECT"));
  EXPECT_FALSE(options.IsReservedKeyword("DECIMAL"));

  // EnableMaximumLanguageFeatures() also reserves all keywords
  options.EnableMaximumLanguageFeatures();
  EXPECT_TRUE(options.IsReservedKeyword("QUALIFY"));

  // Same with EnableMaximumLanguageFeaturesForDevelopment().
  options.EnableAllReservableKeywords(false);
  options.EnableMaximumLanguageFeaturesForDevelopment();
  EXPECT_TRUE(options.IsReservedKeyword("QUALIFY"));

  // Attempting to configure a keyword that cannot be configured
  EXPECT_THAT(options.EnableReservableKeyword("SELECT", true),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(options.EnableReservableKeyword("SELECT", false),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(options.EnableReservableKeyword("", true),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(options.EnableReservableKeyword("DECIMAL", true),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(options.EnableReservableKeyword("not a keyword", true),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(LanguageOptions, ClassAndProtoSize) {
  LanguageOptions options;

  EXPECT_EQ(16, sizeof(options) - sizeof(options.supported_statement_kinds_) -
                    sizeof(options.enabled_language_features_) -
                    sizeof(options.supported_generic_entity_types_) -
                    sizeof(options.reserved_keywords_))
      << "The size of LanguageOptions class has changed, please also update "
      << "the proto and serialization code if you added/removed fields in it.";
  EXPECT_EQ(7, LanguageOptionsProto::descriptor()->field_count())
      << "The number of fields in LanguageOptionsProto has changed, please "
      << "also update the serialization code accordingly.";
}

}  // namespace zetasql
