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

#include "zetasql/common/options_utils.h"

#include <string>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/options.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/btree_set.h"
#include "absl/flags/commandlineflag.h"
#include "absl/flags/flag.h"
#include "absl/flags/reflection.h"
#include "absl/status/statusor.h"

ABSL_FLAG(zetasql::internal::EnabledAstRewrites, test_enabled_ast_rewrites,
          zetasql::internal::EnabledAstRewrites{}, "");

ABSL_FLAG(zetasql::internal::EnabledLanguageFeatures,
          test_enabled_language_features,
          zetasql::internal::EnabledLanguageFeatures{}, "");

namespace zetasql::internal {

using testing::HasSubstr;
using testing::IsEmpty;
using testing::UnorderedElementsAre;
using testing::UnorderedElementsAreArray;
using zetasql_base::testing::StatusIs;

using zetasql::ResolvedASTRewrite;
using RewriteEnumOptionsEntry = EnumOptionsEntry<ResolvedASTRewrite>;
using LanguageFeatureEnumOptionsEntry = EnumOptionsEntry<LanguageFeature>;

static absl::btree_set<ResolvedASTRewrite> GetV1Rewrites() {
  return {REWRITE_PIVOT};
}

static absl::btree_set<ResolvedASTRewrite> GetV2Rewrites() {
  return {REWRITE_PIVOT, REWRITE_TYPEOF_FUNCTION};
}

absl::StatusOr<RewriteEnumOptionsEntry> CallParseEnumOptionsSet(
    absl::string_view options_str) {
  return ParseEnumOptionsSet<ResolvedASTRewrite>({{"NONE", {}},
                                                  {"ALL", GetAllRewrites()},
                                                  {"V1", GetV1Rewrites()},
                                                  {"V2", GetV2Rewrites()}},
                                                 "REWRITE_", "Rewrite",
                                                 options_str);
}

TEST(OptionsUtils, ParseEnumOptionsSet) {
  auto ExpectEmpty = [](absl::string_view str) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(RewriteEnumOptionsEntry result,
                         CallParseEnumOptionsSet(str));
    EXPECT_EQ(result.description, "NONE");
    EXPECT_THAT(result.options, IsEmpty());
  };
  ExpectEmpty("NONE");
  ExpectEmpty("none");
  ExpectEmpty("nonE");
  ExpectEmpty("nONE");
  ExpectEmpty(" nONE ");

  auto ExpectV1 = [](absl::string_view str) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(RewriteEnumOptionsEntry result,
                         CallParseEnumOptionsSet(str));
    EXPECT_EQ(result.description, "V1");
    EXPECT_THAT(result.options, UnorderedElementsAreArray(GetV1Rewrites()));
  };

  ExpectV1("V1");
  ExpectV1("v1");
  ExpectV1(" v1 ");

  auto ExpectAll = [](absl::string_view str) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(RewriteEnumOptionsEntry result,
                         CallParseEnumOptionsSet(str));
    EXPECT_EQ(result.description, "ALL");
    EXPECT_THAT(result.options, UnorderedElementsAreArray(GetAllRewrites()));
  };

  ExpectAll("ALL");
  ExpectAll("all");
  ExpectAll("AlL");
  ExpectAll("aLl  ");
  ExpectAll(" All ");
}

TEST(OptionsUtils, ParseEnumOptionsSet__EmptyIsError) {
  EXPECT_THAT(CallParseEnumOptionsSet("GARBAGE!"),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Rewrite list should always start with one of "
                                 "ALL, NONE, V1, V2")));
}

TEST(OptionsUtils, ParseEnumOptionsSet__GarbageEntryIsError) {
  EXPECT_THAT(CallParseEnumOptionsSet("GARBAGE!"),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Rewrite list should always start with one of "
                                 "ALL, NONE, V1, V2")));
}

TEST(OptionsUtils, ParseEnumOptionsSet__IncludedPrefixIsError) {
  EXPECT_THAT(
      CallParseEnumOptionsSet("NONE,+REWRITE_TYPEOF_FUNCTION"),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr("For consistency, do not include the REWRITE_ prefix")));
}

TEST(OptionsUtils, ParseEnumOptionsSet__RedundantOverrideIsError) {
  auto ExpectDuplicateEntryError = [](absl::string_view str) {
    EXPECT_THAT(
        CallParseEnumOptionsSet(str),
        StatusIs(
            absl::StatusCode::kInternal,
            HasSubstr("Duplicate entry for Rewrite: REWRITE_TYPEOF_FUNCTION")));
  };
  ExpectDuplicateEntryError("NONE,+TYPEOF_FUNCTION,+TYPEOF_FUNCTION");

  ExpectDuplicateEntryError("ALL,-TYPEOF_FUNCTION,-TYPEOF_FUNCTION");
  ExpectDuplicateEntryError("V1,+TYPEOF_FUNCTION,-TYPEOF_FUNCTION");
}

TEST(OptionsUtils, ParseEnumOptionsSet__AllPlusIsError) {
  EXPECT_THAT(
      CallParseEnumOptionsSet("ALL,+TYPEOF_FUNCTION"),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr(
                   "Attempting to add Rewrite, but already started from ALL")));
}

TEST(OptionsUtils, ParseEnumOptionsSet__NoneMinusIsError) {
  EXPECT_THAT(
      CallParseEnumOptionsSet("NONE,-TYPEOF_FUNCTION"),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr(
              "Attempting to remove Rewrite, but already started from NONE.")));
}

TEST(OptionsUtils, ParseEnumOptionsSet__WhitespaceCapsInsensitvity) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      RewriteEnumOptionsEntry result,
      CallParseEnumOptionsSet("NONE,  +typeof_function   ,  +Anonymization"));
  EXPECT_EQ(result.description, "NONE,+ANONYMIZATION,+TYPEOF_FUNCTION");
  EXPECT_THAT(result.options,
              UnorderedElementsAre(ResolvedASTRewrite::REWRITE_TYPEOF_FUNCTION,
                                   ResolvedASTRewrite::REWRITE_ANONYMIZATION));
}

TEST(OptionsUtils, ParseEnumOptionsSet__AdditionAndSubtraction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      RewriteEnumOptionsEntry result,
      CallParseEnumOptionsSet("V2,-typeof_function,+Anonymization"));
  EXPECT_EQ(result.description, "V2,+ANONYMIZATION,-TYPEOF_FUNCTION");
  EXPECT_THAT(result.options,
              UnorderedElementsAre(ResolvedASTRewrite::REWRITE_ANONYMIZATION,
                                   ResolvedASTRewrite::REWRITE_PIVOT));
}

TEST(OptionsUtils, ParseEnumOptionsSet__AddAlreadyIncludedOption) {
  // This is okay. Even though the entry is already in V2, we don't require
  // users to know exactly which features are currently in a given base. This
  // Also allows those sets to change without updating all the test,
  // which is mostly pointless toil.
  ZETASQL_ASSERT_OK_AND_ASSIGN(RewriteEnumOptionsEntry result,
                       CallParseEnumOptionsSet("V2,+TYPEOF_FUNCTION"));

  EXPECT_EQ(result.description, "V2,+TYPEOF_FUNCTION");

  EXPECT_THAT(result.options, UnorderedElementsAreArray(GetV2Rewrites()));
}

TEST(OptionsUtils, ParseEnabledAstRewritesBadStringReturnsError) {
  EXPECT_THAT(ParseEnabledAstRewrites("bad-rewrites"),
              StatusIs(absl::StatusCode::kInternal));
}

//////////////////////////////////////////////////////////////////////////
// ResolvedASTRewrite
//////////////////////////////////////////////////////////////////////////

TEST(OptionsUtils, ParseEnabledAstRewrites) {
  auto CheckResult = [](absl::string_view str,
                        absl::btree_set<ResolvedASTRewrite> expected_enabled,
                        absl::btree_set<ResolvedASTRewrite> expected_disabled) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(RewriteEnumOptionsEntry result,
                         ParseEnabledAstRewrites(str));
    for (const ResolvedASTRewrite rewrite : expected_enabled) {
      EXPECT_TRUE(result.options.contains(rewrite))
          << ResolvedASTRewrite_Name(rewrite)
          << " should be enabled when str is '" << str << "'";
    }
    for (const ResolvedASTRewrite rewrite : expected_disabled) {
      EXPECT_FALSE(result.options.contains(rewrite))
          << ResolvedASTRewrite_Name(rewrite)
          << " should be disabled when str is '" << str << "'";
    }
    EXPECT_EQ(result.description, str) << " str="
                                       << "'" << str << "'";
  };
  // We don't need to exhaustively test this, just ensure we are invoking the
  // the 'parser' for this format.
  CheckResult("NONE", {}, {REWRITE_PIVOT});
  CheckResult("ALL", {REWRITE_ANONYMIZATION}, {});

  CheckResult("DEFAULTS", AnalyzerOptions::DefaultRewrites(),
              {REWRITE_ANONYMIZATION});
  CheckResult("DEFAULTS,-PIVOT", {REWRITE_FLATTEN},
              {REWRITE_PIVOT, REWRITE_ANONYMIZATION});
  CheckResult("DEFAULTS,+ANONYMIZATION",
              {REWRITE_FLATTEN, REWRITE_PIVOT, REWRITE_ANONYMIZATION},
              {REWRITE_INVALID_DO_NOT_USE});
}

TEST(OptionsUtils, TestRewriteFlagSupport) {
  absl::CommandLineFlag* flag =
      absl::FindCommandLineFlag("test_enabled_ast_rewrites");
  EXPECT_EQ(flag->DefaultValue(), "NONE");

  EXPECT_EQ(flag->CurrentValue(), "NONE");
  absl::SetFlag(&FLAGS_test_enabled_ast_rewrites,
                EnabledAstRewrites{{REWRITE_ANONYMIZATION}});
  EXPECT_EQ(flag->CurrentValue(), "NONE,+ANONYMIZATION");

  absl::SetFlag(&FLAGS_test_enabled_ast_rewrites,
                EnabledAstRewrites{AnalyzerOptions::DefaultRewrites()});
  EXPECT_EQ(flag->CurrentValue(), "DEFAULTS");

  // Test roundtripping "ALL", which is special, but we don't have easy access
  // to its value.
  std::string error;
  EXPECT_TRUE(flag->ParseFrom("ALL", &error));
  EnabledAstRewrites all = absl::GetFlag(FLAGS_test_enabled_ast_rewrites);
  EXPECT_GE(all.enabled_ast_rewrites.size(), 12);

  absl::SetFlag(&FLAGS_test_enabled_ast_rewrites, all);
  EXPECT_EQ(flag->CurrentValue(), "ALL");
}

TEST(OptionsUtils, TestFlagSupport_BadFlag) {
  absl::CommandLineFlag* flag =
      absl::FindCommandLineFlag("test_enabled_ast_rewrites");

  std::string error;
  EXPECT_FALSE(flag->ParseFrom("GARBAGE!", &error));
  EXPECT_THAT(error, HasSubstr("Rewrite list should always start with one of "
                               "ALL, DEFAULTS, NONE"));
}

//////////////////////////////////////////////////////////////////////////
// LanguageFeature
//////////////////////////////////////////////////////////////////////////

TEST(OptionsUtils, ParseEnabledLanguageFeatures) {
  auto CheckResult = [](absl::string_view str,
                        absl::btree_set<LanguageFeature> expected_enabled,
                        absl::btree_set<LanguageFeature> expected_disabled) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(LanguageFeatureEnumOptionsEntry result,
                         ParseEnabledLanguageFeatures(str));
    for (const LanguageFeature value : expected_enabled) {
      EXPECT_TRUE(result.options.contains(value))
          << LanguageFeature_Name(value) << " should be enabled when str is '"
          << str << "'";
    }
    for (const LanguageFeature value : expected_disabled) {
      EXPECT_FALSE(result.options.contains(value))
          << LanguageFeature_Name(value) << " should be disabled when str is '"
          << str << "'";
    }
    EXPECT_EQ(result.description, str) << " str="
                                       << "'" << str << "'";
  };
  // We don't need to exhaustively test this, just ensure we are invoking the
  // the 'parser' for this format.
  CheckResult("NONE", {}, {FEATURE_TEST_IDEALLY_DISABLED});
  CheckResult("ALL", {FEATURE_NAMED_ARGUMENTS},
              {__LanguageFeature__switch_must_have_a_default__});

  CheckResult("MAXIMUM", {FEATURE_NAMED_ARGUMENTS},
              {FEATURE_TEST_IDEALLY_DISABLED,
               FEATURE_TEST_IDEALLY_ENABLED_BUT_IN_DEVELOPMENT});
  CheckResult("MAXIMUM,-NAMED_ARGUMENTS", {FEATURE_V_1_3_ALLOW_SLASH_PATHS},
              {FEATURE_NAMED_ARGUMENTS});
  CheckResult("MAXIMUM,+TEST_IDEALLY_ENABLED_BUT_IN_DEVELOPMENT",
              {FEATURE_TEST_IDEALLY_ENABLED_BUT_IN_DEVELOPMENT}, {});
  CheckResult("DEV", {FEATURE_TEST_IDEALLY_ENABLED_BUT_IN_DEVELOPMENT},
              {FEATURE_TEST_IDEALLY_DISABLED_AND_IN_DEVELOPMENT});
}

TEST(OptionsUtils, TestLanguageFeatureFlagSupport) {
  absl::CommandLineFlag* flag =
      absl::FindCommandLineFlag("test_enabled_language_features");
  EXPECT_EQ(flag->DefaultValue(), "NONE");

  EXPECT_EQ(flag->CurrentValue(), "NONE");
  absl::SetFlag(&FLAGS_test_enabled_language_features,
                EnabledLanguageFeatures{{FEATURE_NAMED_ARGUMENTS}});
  EXPECT_EQ(flag->CurrentValue(), "NONE,+NAMED_ARGUMENTS");

  auto ExpectRoundtrip = [flag](absl::string_view base) {
    std::string error;
    EXPECT_TRUE(flag->ParseFrom(base, &error));
    EnabledLanguageFeatures all =
        absl::GetFlag(FLAGS_test_enabled_language_features);
    absl::SetFlag(&FLAGS_test_enabled_language_features, all);
    EXPECT_EQ(flag->CurrentValue(), base);
  };
  ExpectRoundtrip("MAXIMUM");
  ExpectRoundtrip("DEV");
  ExpectRoundtrip("ALL");
  ExpectRoundtrip("NONE");
}

}  // namespace zetasql::internal
