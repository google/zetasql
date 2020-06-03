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

#include "zetasql/public/collator.h"

#include "zetasql/base/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_format.h"

namespace zetasql {

enum class CompareType {
  kCompare
};

class CollatorTest : public ::testing::TestWithParam<CompareType> {
 protected:
  void TestEquals(const std::string& s1, const std::string& s2,
                  const ZetaSqlCollator* collator) {
    switch (GetParam()) {
      case CompareType::kCompare: {
        absl::Status error;
        EXPECT_EQ(0, collator->CompareUtf8(s1, s2, &error)) << s1 << "==" << s2;
        ZETASQL_EXPECT_OK(error);
        EXPECT_EQ(0, collator->CompareUtf8(s2, s1, &error)) << s2 << "==" << s1;
        ZETASQL_EXPECT_OK(error);
        break;
      }
    }
  }

  void TestLessThan(const std::string& s1, const std::string& s2,
                    const ZetaSqlCollator* collator) {
    switch (GetParam()) {
      case CompareType::kCompare: {
        absl::Status error;
        EXPECT_EQ(-1, collator->CompareUtf8(s1, s2, &error)) << s1 << "<" << s2;
        ZETASQL_EXPECT_OK(error);
        EXPECT_EQ(1, collator->CompareUtf8(s2, s1, &error)) << s1 << ">" << s2;
        ZETASQL_EXPECT_OK(error);
        break;
      }
    }
  }
};

TEST_P(CollatorTest, CreateCollator) {
  std::unique_ptr<const ZetaSqlCollator> collator;
  static const std::string valid_collation_names[] = {
      "unicode", "en", "zh-cmn", "en_US", "zh_Hans_HK", "zh_Hant_HK",
      "de@collation=phonebook",
      // Collation names are case insensitive.
      "EN", "EN_us",
      // Collation names with attributes.
      "en:ci", "en_us:cs", "zh_Hans_HK:ci"};
  for (const std::string& name : valid_collation_names) {
    collator.reset(ZetaSqlCollator::CreateFromCollationName(name));
    EXPECT_NE(nullptr, collator.get()) << "name=" << name;
  }

  static const std::string invalid_collation_name[] = {
      "",
      // Note, it is not really desired that this difference exists, but it is
      // not clear how to replicate this using solely icu.  We err on the side
      // of being more permissive in zetasql.
      "en:ai", "en:ci:cs"};
  for (const std::string& name : invalid_collation_name) {
    collator.reset(ZetaSqlCollator::CreateFromCollationName(name));
    EXPECT_EQ(nullptr, collator.get()) << "name=" << name;
  }
}

TEST_P(CollatorTest, Comparison) {
  std::unique_ptr<const ZetaSqlCollator> collator;

  // Comparison with "unicode" collation. Is same as the comparison with no
  // collation.
  collator.reset(ZetaSqlCollator::CreateFromCollationName("unicode"));
  ASSERT_NE(collator.get(), nullptr);
  TestEquals("\u205abc", "\u205abc", collator.get());
  TestLessThan("", "a", collator.get());
  TestLessThan("B", "a", collator.get());
  TestLessThan("a", "aa", collator.get());
  TestLessThan("@", "a", collator.get());
  TestLessThan("Case sensitive", "case sensitive", collator.get());

  // Comparison with "unicode:ci" collation.
  collator.reset(ZetaSqlCollator::CreateFromCollationName("unicode:ci"));
  ASSERT_NE(collator.get(), nullptr);
  TestLessThan("@", "a", collator.get());
  TestEquals("Case sensitive", "case sensitive", collator.get());
  // Greek.
  TestEquals(absl::StrFormat("%c%c%c", 0xCE, 0x86, 'h'),  // Άh
             absl::StrFormat("%c%c%c", 0xCE, 0xAC, 'h'),  // άh
             collator.get());

  // Comparison with "unicode:cs" collation.
  collator.reset(ZetaSqlCollator::CreateFromCollationName("unicode:cs"));
  ASSERT_NE(collator.get(), nullptr);
  TestLessThan("Case sensitive", "case sensitive", collator.get());
  // Greek.
  TestLessThan(absl::StrFormat("%c%c%c", 0xCE, 0x86, 'h'),  // Άh
               absl::StrFormat("%c%c%c", 0xCE, 0xAC, 'h'),  // άh
               collator.get());

  // Comparison with "en_US" collation.
  collator.reset(ZetaSqlCollator::CreateFromCollationName("en_US"));
  ASSERT_NE(nullptr, collator.get());
  TestEquals("hello", "hello", collator.get());
  TestLessThan("", "a", collator.get());
  TestLessThan("a", "B", collator.get());
  TestLessThan("a", "aa", collator.get());
  TestLessThan("@", "a", collator.get());
  TestLessThan("case sensitive", "Case sensitive", collator.get());

  // Comparison with "en_US:ci" collation.
  collator.reset(ZetaSqlCollator::CreateFromCollationName("en_US:ci"));
  ASSERT_NE(collator.get(), nullptr);
  TestEquals("case sensitive", "Case sensitive", collator.get());

  // Comparison with "en_US:cs" collation.
  collator.reset(ZetaSqlCollator::CreateFromCollationName("en_US:cs"));
  ASSERT_NE(collator.get(), nullptr);
  TestLessThan("case sensitive", "Case sensitive", collator.get());

  // Comparison with "cs" collation. cs is Czech. In Czech "ch" is considered
  // as a single character and comes after h.
  collator.reset(ZetaSqlCollator::CreateFromCollationName("cs"));
  ASSERT_NE(nullptr, collator.get());
  TestLessThan("h", "ch", collator.get());
  TestLessThan("ci", "h", collator.get());
  TestLessThan("ci", "ch", collator.get());

  // Comparison with "de:ci" collation. de is German.
  collator.reset(ZetaSqlCollator::CreateFromCollationName("de:ci"));
  ASSERT_NE(collator.get(), nullptr);
  TestEquals("Ä", "ä", collator.get());

  // Comparison with "de:cs" collation. de is German.
  collator.reset(ZetaSqlCollator::CreateFromCollationName("de:cs"));
  ASSERT_NE(collator.get(), nullptr);
  TestLessThan("ä", "Ä", collator.get());

  // Very long string.
  collator.reset(ZetaSqlCollator::CreateFromCollationName("en_US"));
  for (int32_t length = 100; length < 10000; ++length) {
    TestEquals(std::string(length, 'a'), std::string(length, 'a'),
               collator.get());
    TestLessThan(std::string(length, 'a'), std::string(length + 1, 'a'),
                 collator.get());
  }
}

INSTANTIATE_TEST_SUITE_P(CollatorTest, CollatorTest,
                         ::testing::Values(
                             CompareType::kCompare));

}  // namespace zetasql
