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

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/options.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/btree_set.h"
#include "absl/status/statusor.h"

namespace zetasql::internal {

using testing::HasSubstr;
using testing::IsEmpty;
using testing::UnorderedElementsAre;
using testing::UnorderedElementsAreArray;
using zetasql_base::testing::StatusIs;

using zetasql::ResolvedASTRewrite;
using RewriteEnumOptionsEntry = EnumOptionsEntry<ResolvedASTRewrite>;

// Within this test, we pretend there are only three enum values in the world.
static absl::btree_set<ResolvedASTRewrite> GetAllRewrites() {
  return {REWRITE_ANONYMIZATION, REWRITE_PIVOT, REWRITE_TYPEOF_FUNCTION};
}

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

}  // namespace zetasql::internal
